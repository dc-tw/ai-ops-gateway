"""Atomic, opt-in Redis domain-write capture for the Postgres history plane.

The adapter is deliberately below Store pipelines, including WATCH/MULTI and
automation Lua. Quotas, checkpoints, queues and secret-bearing cache keys are
not archival resources. Nothing reads Postgres to authorize execution.
"""

from __future__ import annotations

import inspect
import json
from typing import Any

from redis.asyncio import Redis
from redis.asyncio.client import Pipeline
from redis.exceptions import ResponseError

from app.core.durable_types import classify_key


def outbox_key(source_id: str) -> str:
    return f"gateway:durable:{source_id}:outbox"


def maintenance_key(source_id: str) -> str:
    return f"gateway:durable:{source_id}:maintenance"


def _prelude(source_id: str) -> str:
    # source_id is validated by Settings and again here for programmatic callers.
    import re

    if not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", source_id):
        raise ValueError("Invalid durable source ID")
    return r"""
local OUTBOX = '__OUTBOX__'
local MAINTENANCE = '__MAINTENANCE__'
if redis.call('EXISTS', MAINTENANCE) == 1 then
  return redis.error_reply('DURABLE_MAINTENANCE: domain writes are temporarily fenced')
end
local typ = redis.call('TYPE', OUTBOX).ok
if typ ~= 'none' and typ ~= 'stream' then
  return redis.error_reply('DURABLE_OUTBOX_TYPE: expected stream')
end
local function family(key)
  local tenant, suffix = string.match(key, '^tenant:([%w%._%-]+):(.*)$')
  local function identifier(value, maximum)
    return value and #value <= maximum and string.match(value, '^[A-Za-z0-9][A-Za-z0-9%._%-]*$')
  end
  if not identifier(tenant, 64) then return nil end
  local conversation = string.match(suffix, '^conversation:(.+):messages$')
  if conversation and #conversation <= 128
    and string.match(conversation, '^[A-Za-z0-9][A-Za-z0-9%._:%-]*$') then return 'messages' end
  if suffix == 'audit' then return 'events' end
  for _, prefix in ipairs({'agent:events:', 'agent:goal:events:', 'planning:events:'}) do
    if string.sub(suffix, 1, #prefix) == prefix
      and identifier(string.sub(suffix, #prefix+1), 128) then return 'events' end
  end
  if identifier(string.match(suffix, '^automation:([^:]+):events$'), 128) then return 'events' end
  for _, prefix in ipairs({'agents:record:', 'skills:record:', 'planning:session:',
    'agent:goal:', 'agent:run:', 'security:threat-finding:', 'automation:'}) do
    if string.sub(suffix, 1, #prefix) == prefix
      and identifier(string.sub(suffix, #prefix+1), 128) then return 'record' end
  end
  for _, pattern in ipairs({'^skills:revision:([^:]+):([1-9]%d*)$',
    '^planning:revision:([^:]+):([1-9]%d*)$'}) do
    local id, revision = string.match(suffix, pattern)
    if identifier(id, 128) and revision then return 'record' end
  end
  local goal, action = string.match(suffix, '^goal:([^:]+):tool%-receipt:([^:]+)$')
  if identifier(goal, 128) and identifier(action, 128) then return 'record' end
  return nil
end
local function emit(command, key, value, event_id)
  local envelope = {schema_version=1, command=command, key=key, value=value}
  if event_id then envelope.event_id = event_id end
  redis.call('XADD', OUTBOX, '*', 'payload', cjson.encode(envelope))
end
local function valid_json(value, depth)
  if depth > 64 then error('DURABLE_PAYLOAD: nesting limit exceeded') end
  if type(value) == 'string' and string.find(value, '\0', 1, true) then
    error('DURABLE_PAYLOAD: unsupported JSON text')
  elseif type(value) == 'table' then
    for key, child in pairs(value) do
      valid_json(key, depth+1)
      valid_json(child, depth+1)
    end
  end
end
local function validate_raw(value)
  if type(value) ~= 'string' then return end
  -- A bounded per-source JSON input, not a limit on an entire transaction.
  if #value > 1048576 then error('DURABLE_PAYLOAD: source exceeds 1 MiB') end
  local ok, decoded = pcall(cjson.decode, value)
  if ok then valid_json(decoded, 0) else valid_json(value, 0) end
end
local function validate_command(args)
  local command = string.upper(args[1])
  local mode = type(args[2]) == 'string' and family(args[2]) or nil
  if not mode then return end
  -- The payload is JSON encoded twice in an outbox envelope. Reserve header
  -- space, and reject an oversized aggregate before changing source data.
  if #cjson.encode(args) > 8387584 then error('DURABLE_PAYLOAD: envelope exceeds capacity') end
  if command == 'SET' and mode == 'record' then
    for i=4,#args do
      if string.upper(tostring(args[i])) == 'GET' then error('DURABLE_UNSUPPORTED: SET GET') end
    end
    validate_raw(args[3])
  elseif command == 'XADD' and mode == 'events' then
    for i=3,#args do
      if tostring(args[i]) == '*' or string.match(tostring(args[i]), '^%d+%-%d+$') then break end
      if string.upper(tostring(args[i])) == 'MINID' then
        error('DURABLE_UNSUPPORTED: XADD MINID')
      end
    end
    for i=3,#args do validate_raw(args[i]) end
  elseif command == 'RPUSH' and mode == 'messages' then
    for i=3,#args do validate_raw(args[i]) end
  end
end
local function durable_call(...)
  local args = {...}
  validate_command(args)
  local command = string.upper(args[1])
  local key = args[2]
  local mode = type(key) == 'string' and family(key) or nil
  local result = redis.call(unpack(args))
  if command == 'SET' and mode == 'record' and result then
    emit('SET', key, args[3])
  elseif command == 'XADD' and mode == 'events' then
    local fields = {}
    local start = nil
    for i=3,#args do
      if args[i] == '*' or string.match(tostring(args[i]), '^%d+%-%d+$') then
        start = i+1
        break
      end
    end
    if not start then error('DURABLE_XADD_FORMAT') end
    for i=start,#args,2 do fields[args[i]] = args[i+1] end
    emit('XADD', key, fields, result)
  elseif command == 'RPUSH' and mode == 'messages' then
    local values = {}
    for i=3,#args do values[#values+1] = args[i] end
    emit('RPUSH', key, values)
  elseif command == 'DEL' then
    -- Finding capacity eviction and cache TTL are not logical history deletion.
    -- Only explicit conversation DELETE creates a durable deletion fence.
    for i=2,#args do
      if family(args[i]) == 'messages' then emit('DEL', args[i], cjson.null) end
    end
  end
  return result
end
""".replace("__OUTBOX__", outbox_key(source_id)).replace(
        "__MAINTENANCE__", maintenance_key(source_id)
    )


def _text(value: Any) -> Any:
    return value.decode("utf-8") if isinstance(value, bytes) else value


def _captured(args: tuple) -> bool:
    command = str(_text(args[0])).upper()
    if command in {"EVAL", "EVALSHA"}:
        return True  # Lua can mutate domain keys computed inside the script.
    if command == "DEL":
        return any(
            (item := classify_key(str(_text(key)))) and item[2] == "messages" for key in args[1:]
        )
    if command not in {"SET", "XADD", "RPUSH"} or len(args) < 2:
        return False
    match = classify_key(str(_text(args[1])))
    return bool(
        match and match[2] == {"SET": "record", "XADD": "events", "RPUSH": "messages"}[command]
    )


async def _parse(client, args, options, result):
    callback = client.response_callbacks.get(str(_text(args[0])).upper())
    if callback:
        result = callback(result, **options)
        if inspect.isawaitable(result):
            result = await result
    return result


def _safe_error(exc: ResponseError) -> ResponseError:
    # redis-py MULTI errors include the full EVAL command and private arguments.
    for code in (
        "DURABLE_PAYLOAD",
        "DURABLE_MAINTENANCE",
        "DURABLE_OUTBOX_TYPE",
        "DURABLE_UNSUPPORTED",
    ):
        if code in str(exc):
            return ResponseError(code)
    return ResponseError("DURABLE_COMMAND_FAILED")


class DurablePipeline(Pipeline):
    def __init__(self, *args, source_id: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_id = source_id

    async def execute(self, raise_on_error: bool = True):
        stack = list(self.command_stack)
        if not stack or not any(_captured(args) for args, _ in stack):
            return await super().execute(raise_on_error=raise_on_error)
        if any(str(_text(args[0])).upper() in {"EVAL", "EVALSHA"} for args, _ in stack):
            await self.reset()
            raise ValueError("Nested Lua in a durable pipeline is unsupported")
        # One guarded script inside the original WATCH/MULTI. A maintenance/type
        # failure is detected before ANY command, including indexes, is changed.
        script = (
            _prelude(self.source_id)
            + """
local commands = cjson.decode(ARGV[1])
local results = {}
for _, command in ipairs(commands) do validate_command(command) end
for i, command in ipairs(commands) do
  local result = durable_call(unpack(command))
  if result == nil then result = false end
  results[i] = result
end
return results
"""
        )
        encoded = json.dumps([[_text(v) for v in args] for args, _ in stack], ensure_ascii=False)
        self.command_stack = [(("EVAL", script, 0, encoded), {})]
        try:
            responses = await super().execute(raise_on_error=raise_on_error)
        except ResponseError as exc:
            raise _safe_error(exc) from None
        if isinstance(responses[0], ResponseError):
            raise _safe_error(responses[0]) from None
        values = responses[0]
        return [
            await _parse(self, args, options, value)
            for (args, options), value in zip(stack, values, strict=True)
        ]


class DurableRedis(Redis):
    durable_source_id = "gateway"

    def pipeline(self, transaction=True, shard_hint=None):
        return DurablePipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint,
            source_id=self.durable_source_id,
        )

    async def _execute_durable(self, *args, **options):
        try:
            return await super().execute_command(*args, **options)
        except ResponseError as exc:
            raise _safe_error(exc) from None

    async def execute_command(self, *args, **options):
        if str(_text(args[0])).upper() == "EVALSHA":
            raise ValueError("Durable writers must use instrumented EVAL, not EVALSHA")
        if not _captured(args):
            return await super().execute_command(*args, **options)
        command = str(_text(args[0])).upper()
        if command == "EVAL":
            # Instrument the existing trusted application script, not user input.
            # Its redis.call accesses, including computed Goal keys, are captured.
            script = str(_text(args[1])).replace("redis.call(", "durable_call(")
            if "redis.pcall" in script or "redis['call']" in script or 'redis["call"]' in script:
                raise ValueError("Unsupported durable Lua call syntax")
            return await self._execute_durable(
                "EVAL",
                _prelude(self.durable_source_id)
                + "for _, value in ipairs(ARGV) do validate_raw(value) end\n"
                + script,
                *args[2:],
                **options,
            )
        script = _prelude(self.durable_source_id) + "return durable_call(unpack(ARGV))"
        result = await self._execute_durable("EVAL", script, 0, *args)
        return await _parse(self, args, options, result)

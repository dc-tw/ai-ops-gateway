import json
import re
import time
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis

from app.core.internal_events import INTERNAL_STREAM, build_internal_event


class RedisRuntime:
    """Small Redis abstraction for event audit, task queueing, and short-term memory."""

    def __init__(self, url: str, stream_maxlen: int = 10_000) -> None:
        self._client = Redis.from_url(url, decode_responses=True)
        self._stream_maxlen = stream_maxlen

    async def ping(self) -> bool:
        return bool(await self._client.ping())

    @staticmethod
    def _tenant_key(tenant_id: str, suffix: str) -> str:
        return f"tenant:{tenant_id}:{suffix}"

    async def append_event(
        self, request_id: str, event: dict[str, Any], tenant_id: str = "default"
    ) -> str:
        return str(
            await self._client.xadd(
                self._tenant_key(tenant_id, f"agent:events:{request_id}"),
                {"payload": json.dumps(event, ensure_ascii=False)},
                maxlen=self._stream_maxlen,
                approximate=True,
            )
        )

    async def read_agent_events(
        self,
        request_id: str,
        last_id: str = "0-0",
        block_ms: int = 15_000,
        tenant_id: str = "default",
    ) -> list[tuple[str, dict[str, Any]]]:
        result = await self._client.xread(
            {self._tenant_key(tenant_id, f"agent:events:{request_id}"): last_id},
            count=100,
            block=block_ms,
        )
        if not result:
            return []
        return [
            (event_id, json.loads(fields["payload"]))
            for _, events in result
            for event_id, fields in events
        ]

    async def save_run_state(
        self, request_id: str, state: dict[str, Any], tenant_id: str = "default"
    ) -> None:
        await self._client.set(
            self._tenant_key(tenant_id, f"agent:run:{request_id}"),
            json.dumps(state, ensure_ascii=False),
            ex=86_400,
        )

    async def get_run_state(
        self, request_id: str, tenant_id: str = "default"
    ) -> dict[str, Any] | None:
        value = await self._client.get(self._tenant_key(tenant_id, f"agent:run:{request_id}"))
        return json.loads(value) if value else None

    async def enforce_rate_limit(self, tenant_id: str, limit: int) -> int:
        window = int(time.time() // 60)
        key = self._tenant_key(tenant_id, f"quota:requests:{window}")
        async with self._client.pipeline(transaction=True) as pipe:
            pipe.incr(key)
            pipe.expire(key, 120)
            count, _ = await pipe.execute()
        if int(count) > limit:
            raise RuntimeError("tenant request rate limit exceeded")
        return int(count)

    async def consume_daily_tokens(self, tenant_id: str, tokens: int, limit: int) -> int:
        day = datetime.now(UTC).strftime("%Y-%m-%d")
        key = self._tenant_key(tenant_id, f"quota:tokens:{day}")
        async with self._client.pipeline(transaction=True) as pipe:
            pipe.incrby(key, tokens)
            pipe.expire(key, 172_800)
            consumed, _ = await pipe.execute()
        if int(consumed) > limit:
            await self._client.decrby(key, tokens)
            raise RuntimeError("tenant daily token budget exceeded")
        return int(consumed)

    async def reconcile_daily_tokens(
        self,
        tenant_id: str,
        reserved_tokens: int,
        actual_tokens: int,
        *,
        request_id: str,
    ) -> int:
        day = datetime.now(UTC).strftime("%Y-%m-%d")
        key = self._tenant_key(tenant_id, f"quota:tokens:{day}")
        marker = self._tenant_key(tenant_id, f"quota:tokens:{day}:reconciled:{request_id}")
        delta = actual_tokens - reserved_tokens
        script = """
        if not redis.call('SET', KEYS[2], '1', 'EX', ARGV[2], 'NX') then
          return tonumber(redis.call('GET', KEYS[1]) or '0')
        end
        local value = redis.call('INCRBY', KEYS[1], ARGV[1])
        redis.call('EXPIRE', KEYS[1], ARGV[2])
        return value
        """
        value = await self._client.eval(script, 2, key, marker, delta, 172_800)
        return int(value)

    async def acquire_run_slot(
        self, tenant_id: str, request_id: str, limit: int, ttl_seconds: int = 600
    ) -> bool:
        key = self._tenant_key(tenant_id, "quota:active-runs")
        now = time.time()
        script = """
        redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
        if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[3]) then return 0 end
        redis.call('ZADD', KEYS[1], ARGV[2], ARGV[4])
        redis.call('EXPIRE', KEYS[1], ARGV[5])
        return 1
        """
        result = await self._client.eval(
            script, 1, key, now, now + ttl_seconds, limit, request_id, ttl_seconds
        )
        return bool(result)

    async def release_run_slot(self, tenant_id: str, request_id: str) -> None:
        await self._client.zrem(self._tenant_key(tenant_id, "quota:active-runs"), request_id)

    async def append_audit(self, tenant_id: str, event: dict[str, Any]) -> str:
        return str(
            await self._client.xadd(
                self._tenant_key(tenant_id, "audit"),
                {"payload": json.dumps(event, ensure_ascii=False)},
                maxlen=self._stream_maxlen,
                approximate=True,
            )
        )

    async def save_threat_findings(
        self,
        tenant_id: str,
        event: dict[str, Any],
        *,
        context_id: str,
        model: str,
        ttl_seconds: int,
        max_records: int,
        source_automation_id: str | None = None,
        emit_internal_events: bool = True,
    ) -> list[dict[str, Any]]:
        """Persist privacy-safe ATR metadata; raw scanned content is never stored."""
        now = datetime.now(UTC)
        index_key = self._tenant_key(tenant_id, "security:threat-findings")
        records: list[dict[str, Any]] = []
        for finding in event.get("findings", []):
            if not isinstance(finding, dict) or not finding.get("finding_id"):
                continue
            record = {
                "finding_id": str(finding["finding_id"]),
                "context_id": context_id,
                "model": model,
                "rule_id": str(finding.get("rule_id", "")),
                "title": str(finding.get("title", "")),
                "severity": str(finding.get("severity", "unknown")),
                "confidence": str(finding.get("confidence", "unknown")),
                "event_type": str(event.get("event_type", "unknown")),
                "location": str(event.get("location", "unknown")),
                "block_candidate": bool(finding.get("block_candidate", False)),
                "enforcement_status": str(finding.get("enforcement_action", "detected")),
                "review_status": "unreviewed",
                "review_reason": "",
                "reviewed_by": "",
                "reviewed_at": "",
                "created_at": now.isoformat(),
            }
            records.append(record)
        if not records:
            return []
        async with self._client.pipeline(transaction=True) as pipe:
            for record in records:
                finding_id = record["finding_id"]
                pipe.set(
                    self._tenant_key(tenant_id, f"security:threat-finding:{finding_id}"),
                    json.dumps(record, ensure_ascii=False),
                    ex=ttl_seconds,
                )
                pipe.zadd(index_key, {finding_id: now.timestamp()})
                internal = build_internal_event(
                    "threat.detected",
                    finding_id,
                    record,
                    source_automation_id=source_automation_id,
                )
                if emit_internal_events and internal is not None:
                    pipe.xadd(
                        INTERNAL_STREAM,
                        {
                            "tenant_id": tenant_id,
                            "payload": json.dumps(internal, ensure_ascii=False),
                        },
                    )
            pipe.expire(index_key, ttl_seconds)
            pipe.zremrangebyscore(index_key, "-inf", now.timestamp() - ttl_seconds)
            await pipe.execute()
        count = int(await self._client.zcard(index_key))
        if count > max_records:
            stale_ids = await self._client.zrange(index_key, 0, count - max_records - 1)
            if stale_ids:
                async with self._client.pipeline(transaction=True) as pipe:
                    pipe.zrem(index_key, *stale_ids)
                    pipe.delete(
                        *[
                            self._tenant_key(tenant_id, f"security:threat-finding:{finding_id}")
                            for finding_id in stale_ids
                        ]
                    )
                    await pipe.execute()
        return records

    async def get_threat_finding(self, tenant_id: str, finding_id: str) -> dict[str, Any] | None:
        value = await self._client.get(
            self._tenant_key(tenant_id, f"security:threat-finding:{finding_id}")
        )
        return json.loads(value) if value else None

    async def list_threat_findings(
        self,
        tenant_id: str,
        *,
        limit: int = 100,
        review_status: str = "",
        rule_id: str = "",
    ) -> list[dict[str, Any]]:
        index_key = self._tenant_key(tenant_id, "security:threat-findings")
        records: list[dict[str, Any]] = []
        expired: list[str] = []
        offset = 0
        batch_size = min(max(limit, 100), 1_000)
        while len(records) < limit:
            finding_ids = await self._client.zrevrange(index_key, offset, offset + batch_size - 1)
            if not finding_ids:
                break
            offset += len(finding_ids)
            keys = [
                self._tenant_key(tenant_id, f"security:threat-finding:{finding_id}")
                for finding_id in finding_ids
            ]
            values = await self._client.mget(keys)
            for finding_id, value in zip(finding_ids, values, strict=True):
                if not value:
                    expired.append(finding_id)
                    continue
                record = json.loads(value)
                if review_status and record.get("review_status") != review_status:
                    continue
                if rule_id and record.get("rule_id") != rule_id:
                    continue
                records.append(record)
                if len(records) >= limit:
                    break
            if len(finding_ids) < batch_size:
                break
        if expired:
            await self._client.zrem(index_key, *expired)
        return records

    async def review_threat_finding(
        self,
        tenant_id: str,
        finding_id: str,
        *,
        review_status: str,
        reason: str,
        reviewer: str,
        ttl_seconds: int,
    ) -> dict[str, Any] | None:
        record = await self.get_threat_finding(tenant_id, finding_id)
        if record is None:
            return None
        record.update(
            {
                "review_status": review_status,
                "review_reason": reason,
                "reviewed_by": reviewer,
                "reviewed_at": datetime.now(UTC).isoformat(),
            }
        )
        await self._client.set(
            self._tenant_key(tenant_id, f"security:threat-finding:{finding_id}"),
            json.dumps(record, ensure_ascii=False),
            ex=ttl_seconds,
        )
        return record

    async def remember(
        self, request_id: str, result: dict[str, Any], ttl_seconds: int = 3600
    ) -> None:
        await self._client.set(
            f"agent:result:{request_id}",
            json.dumps(result, ensure_ascii=False),
            ex=ttl_seconds,
        )

    async def get_conversation(
        self, conversation_id: str, limit: int = 40, tenant_id: str = "default"
    ) -> list[dict[str, str]]:
        values = await self._client.lrange(
            self._tenant_key(tenant_id, f"conversation:{conversation_id}:messages"),
            -limit,
            -1,
        )
        return [json.loads(value) for value in values]

    @staticmethod
    def _memory_terms(text: str) -> set[str]:
        return {
            token.lower()
            for token in re.findall(r"[\w\u3400-\u9fff]{2,}", text)
            if len(token) <= 64
        }

    async def get_conversation_context(
        self,
        conversation_id: str,
        *,
        query: str,
        limit: int = 40,
        tenant_id: str = "default",
    ) -> list[dict[str, str]]:
        recent = await self.get_conversation(conversation_id, limit, tenant_id)
        summary_value = await self._client.get(
            self._tenant_key(tenant_id, f"conversation:{conversation_id}:summary")
        )
        memory_values = await self._client.zrevrange(
            self._tenant_key(tenant_id, f"conversation:{conversation_id}:memory"), 0, 199
        )
        query_terms = self._memory_terms(query)
        ranked: list[tuple[float, dict[str, str]]] = []
        recent_ids = {str(item.get("request_id", "")) for item in recent}
        for value in memory_values:
            item = json.loads(value)
            if str(item.get("request_id", "")) in recent_ids:
                continue
            terms = self._memory_terms(str(item.get("content", "")))
            score = len(query_terms & terms) / max(1, len(query_terms | terms))
            if score:
                ranked.append((score, item))
        context: list[dict[str, str]] = []
        if summary_value:
            context.append({"role": "memory_summary", "content": summary_value})
        context.extend(item for _, item in sorted(ranked, reverse=True)[:4])
        context.extend(recent)
        return context

    async def append_conversation_turn(
        self,
        conversation_id: str,
        *,
        query: str,
        answer: str,
        model: str,
        request_id: str,
        max_turns: int = 40,
        ttl_seconds: int = 604_800,
        tenant_id: str = "default",
    ) -> None:
        key = self._tenant_key(tenant_id, f"conversation:{conversation_id}:messages")
        summary_key = self._tenant_key(tenant_id, f"conversation:{conversation_id}:summary")
        memory_key = self._tenant_key(tenant_id, f"conversation:{conversation_id}:memory")
        now = datetime.now(UTC).isoformat()
        entries = [
            json.dumps(
                {
                    "role": "user",
                    "content": query,
                    "request_id": request_id,
                    "created_at": now,
                },
                ensure_ascii=False,
            ),
            json.dumps(
                {
                    "role": "assistant",
                    "content": answer,
                    "model": model,
                    "request_id": request_id,
                    "created_at": now,
                },
                ensure_ascii=False,
            ),
        ]
        current_length = int(await self._client.llen(key))
        overflow = max(0, current_length + len(entries) - max_turns * 2)
        if overflow:
            evicted_values = await self._client.lrange(key, 0, overflow - 1)
            prior_summary = await self._client.get(summary_key) or ""
            fragments = []
            for value in evicted_values:
                item = json.loads(value)
                fragments.append(f"{item.get('role', 'unknown')}: {item.get('content', '')}")
            rolling_summary = (prior_summary + "\n" + "\n".join(fragments)).strip()[-8_000:]
            await self._client.set(summary_key, rolling_summary, ex=ttl_seconds)
        async with self._client.pipeline(transaction=True) as pipe:
            pipe.rpush(key, *entries)
            pipe.ltrim(key, -(max_turns * 2), -1)
            pipe.expire(key, ttl_seconds)
            for entry in entries:
                pipe.zadd(memory_key, {entry: time.time()})
            pipe.expire(memory_key, ttl_seconds)
            await pipe.execute()
        memory_count = int(await self._client.zcard(memory_key))
        if memory_count > 500:
            await self._client.zremrangebyrank(memory_key, 0, memory_count - 501)

    async def delete_conversation(self, conversation_id: str, tenant_id: str = "default") -> bool:
        return bool(
            await self._client.delete(
                self._tenant_key(tenant_id, f"conversation:{conversation_id}:messages"),
                self._tenant_key(tenant_id, f"conversation:{conversation_id}:summary"),
                self._tenant_key(tenant_id, f"conversation:{conversation_id}:memory"),
            )
        )

    async def save_goal(
        self, goal_id: str, goal: dict[str, Any], tenant_id: str = "default"
    ) -> None:
        async with self._client.pipeline(transaction=True) as pipe:
            pipe.set(
                self._tenant_key(tenant_id, f"agent:goal:{goal_id}"),
                json.dumps(goal, ensure_ascii=False),
            )
            pipe.sadd(self._tenant_key(tenant_id, "agent:goals"), goal_id)
            pipe.sadd("agent:goal:tenants", tenant_id)
            await pipe.execute()

    async def get_goal(self, goal_id: str, tenant_id: str = "default") -> dict[str, Any] | None:
        value = await self._client.get(self._tenant_key(tenant_id, f"agent:goal:{goal_id}"))
        return json.loads(value) if value else None

    async def list_goal_ids(self, tenant_id: str = "default") -> list[str]:
        return sorted(await self._client.smembers(self._tenant_key(tenant_id, "agent:goals")))

    async def list_goal_tenants(self) -> list[str]:
        return sorted(await self._client.smembers("agent:goal:tenants"))

    async def enqueue_goal(self, goal_id: str, tenant_id: str = "default") -> None:
        await self._client.xadd(
            "agent:goal:queue",
            {"goal_id": goal_id, "tenant_id": tenant_id},
        )

    async def ensure_goal_group(self, group: str) -> None:
        with suppress(Exception):
            await self._client.xgroup_create("agent:goal:queue", group, id="0-0", mkstream=True)

    async def claim_goal(
        self,
        *,
        group: str,
        consumer: str,
        block_seconds: int = 2,
        lease_ms: int = 60_000,
    ) -> tuple[str, str, str] | None:
        await self.ensure_goal_group(group)
        reclaimed = await self._client.xautoclaim(
            "agent:goal:queue",
            group,
            consumer,
            min_idle_time=lease_ms,
            start_id="0-0",
            count=1,
        )
        reclaimed_messages = reclaimed[1] if len(reclaimed) > 1 else []
        if reclaimed_messages:
            event_id, fields = reclaimed_messages[0]
            return str(event_id), str(fields["tenant_id"]), str(fields["goal_id"])

        result = await self._client.xreadgroup(
            group,
            consumer,
            {"agent:goal:queue": ">"},
            count=1,
            block=block_seconds * 1_000,
        )
        if not result:
            return None
        _, messages = result[0]
        event_id, fields = messages[0]
        return str(event_id), str(fields["tenant_id"]), str(fields["goal_id"])

    async def acknowledge_goal(
        self,
        event_id: str,
        *,
        group: str,
        goal_id: str,
        tenant_id: str = "default",
        error: str | None = None,
    ) -> None:
        if error:
            await self._client.xadd(
                "agent:goal:dead-letter",
                {
                    "goal_id": goal_id,
                    "tenant_id": tenant_id,
                    "queue_event_id": event_id,
                    "error": error,
                },
                maxlen=self._stream_maxlen,
                approximate=True,
            )
        await self._client.xack("agent:goal:queue", group, event_id)

    async def append_goal_event(
        self, goal_id: str, event: dict[str, Any], tenant_id: str = "default"
    ) -> str:
        return str(
            await self._client.xadd(
                self._tenant_key(tenant_id, f"agent:goal:events:{goal_id}"),
                {"payload": json.dumps(event, ensure_ascii=False)},
            )
        )

    async def read_goal_events(
        self,
        goal_id: str,
        last_id: str = "0-0",
        block_ms: int = 15_000,
        tenant_id: str = "default",
    ) -> list[tuple[str, dict[str, Any]]]:
        result = await self._client.xread(
            {self._tenant_key(tenant_id, f"agent:goal:events:{goal_id}"): last_id},
            count=100,
            block=block_ms,
        )
        if not result:
            return []
        return [
            (event_id, json.loads(fields["payload"]))
            for _, events in result
            for event_id, fields in events
        ]

    async def close(self) -> None:
        await self._client.aclose()

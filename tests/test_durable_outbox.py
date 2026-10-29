"""Real Redis coverage for archival capture below Store transactions and Lua."""

import asyncio
import json
import os
import time
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from redis.asyncio import Redis
from redis.exceptions import ResponseError, WatchError

from app.agents import AgentStore
from app.automations import AutomationCreate, AutomationPolicy, materialize_goal
from app.core.automation_store import AutomationStore
from app.core.delegation import DelegationStore
from app.core.durable_outbox import maintenance_key, outbox_key
from app.core.durable_types import classify_key
from app.core.execution_policy import PolicyToolExecutor, ToolReconciliationRequired
from app.core.goal_execution import GoalExecutionStore
from app.core.planning_store import PlanningStore, canonical_hash
from app.core.redis_runtime import RedisRuntime
from app.core.skill_store import SkillContent, SkillStore
from app.tools.registry import ToolSpec


@pytest.fixture
async def durable():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Set TEST_REDIS_URL to a dedicated Redis test server")
    source = "test-outbox-" + uuid4().hex
    tenant = "outbox-" + uuid4().hex
    runtime = RedisRuntime(url, durable_source_id=source)
    raw = Redis.from_url(url, decode_responses=True)
    await raw.ping()
    env = SimpleNamespace(
        runtime=runtime,
        client=runtime._client,
        raw=raw,
        tenant=tenant,
        source=source,
        outbox=outbox_key(source),
        fence=maintenance_key(source),
        key=lambda suffix: runtime._tenant_key(tenant, suffix),
    )
    try:
        yield env
    finally:
        # Only this fixture's keys/entries are removed; never flush a database.
        for stream in ("agent:goal:queue", "automation:internal-events"):
            rows = await raw.xrange(stream)
            ids = [entry for entry, fields in rows if fields.get("tenant_id") == tenant]
            if ids:
                await raw.xdel(stream, *ids)
        due = await raw.zrange("automation:due", 0, -1)
        owned = [member for member in due if json.loads(member)[0] == tenant]
        if owned:
            await raw.zrem("automation:due", *owned)
        keys = [key async for key in raw.scan_iter(f"tenant:{tenant}:*")]
        keys.extend((env.outbox, env.fence))
        await raw.delete(*keys)
        await raw.srem("agent:goal:tenants", tenant)
        await raw.srem("automation:tenants", tenant)
        await runtime.close()
        await raw.aclose()


async def envelopes(env):
    return [json.loads(fields["payload"]) for _, fields in await env.raw.xrange(env.outbox)]


async def test_direct_set_nx_results_and_outbox_are_exact(durable):
    env = durable
    key = env.key("agent:run:run-1")
    assert await env.client.set(key, '{"status":"queued"}', nx=True) is True
    assert await env.client.set(key, '{"status":"SHOULD_NOT_APPEAR"}', nx=True) is None
    assert await env.client.set(key, '{"status":"completed"}', xx=True) is True
    rows = await envelopes(env)
    assert [row["value"] for row in rows] == ['{"status":"queued"}', '{"status":"completed"}']
    assert all(row["schema_version"] == 1 and row["command"] == "SET" for row in rows)
    assert all(row["key"] == key for row in rows)


async def test_pipeline_preserves_redis_results_and_captures_in_command_order(durable):
    env = durable
    key = env.key("agent:goal:goal-1")
    events = env.key("agent:goal:events:goal-1")
    messages = env.key("conversation:incident:prod:messages")
    async with env.client.pipeline(transaction=True) as pipe:
        pipe.set(key, '{"status":"queued"}')
        pipe.sadd(env.key("agent:goals"), "goal-1")
        pipe.get(key)
        pipe.set(key, "not-written", nx=True)
        pipe.xadd(events, {"payload": '{"type":"goal_queued"}'})
        pipe.rpush(messages, '{"role":"user","content":"private message"}')
        pipe.mget([key, env.key("missing")])
        results = await pipe.execute()
    assert results[:4] == [True, 1, '{"status":"queued"}', None]
    assert isinstance(results[4], str) and "-" in results[4]
    assert results[5:] == [1, ['{"status":"queued"}', None]]
    rows = await envelopes(env)
    assert [row["command"] for row in rows] == ["SET", "XADD", "RPUSH"]
    assert rows[1]["event_id"] == results[4]
    assert rows[2]["value"] == ['{"role":"user","content":"private message"}']


async def test_watch_conflict_does_not_create_phantom_outbox_and_retry_succeeds(durable):
    env = durable
    key = env.key("agent:goal:goal-1")
    await env.client.set(key, "original")
    async with env.client.pipeline(transaction=True) as pipe:
        await pipe.watch(key)
        assert await pipe.get(key) == "original"
        await env.raw.set(key, "concurrent-owner")
        pipe.multi()
        pipe.set(key, "stale-write")
        with pytest.raises(WatchError):
            await pipe.execute()
    assert [row["value"] for row in await envelopes(env)] == ["original"]
    async with env.client.pipeline(transaction=True) as pipe:
        await pipe.watch(key)
        assert await pipe.get(key) == "concurrent-owner"
        pipe.multi()
        pipe.set(key, "fresh-write")
        assert await pipe.execute() == [True]
    assert [row["value"] for row in await envelopes(env)] == ["original", "fresh-write"]


@pytest.mark.parametrize("obstacle", ["maintenance", "outbox_type"])
async def test_preflight_failure_fences_record_and_index_before_any_write(durable, obstacle):
    env = durable
    if obstacle == "maintenance":
        await env.raw.set(env.fence, "owned-test-fence")
        message = "DURABLE_MAINTENANCE"
    else:
        await env.raw.set(env.outbox, "incorrect-type")
        message = "DURABLE_OUTBOX_TYPE"
    record, index = env.key("agents:record:worker"), env.key("agents")
    async with env.client.pipeline(transaction=True) as pipe:
        # Index first demonstrates the script preflight precedes all commands.
        pipe.sadd(index, "worker")
        pipe.set(record, "{}")
        with pytest.raises(ResponseError, match=message):
            await pipe.execute()
    assert not await env.raw.exists(record, index)
    with pytest.raises(ResponseError, match=message):
        await env.client.set(record, "{}")
    with pytest.raises(ResponseError, match=message):
        await env.client.eval(
            "redis.call('SADD', KEYS[1], 'worker'); return redis.call('SET', KEYS[2], '{}')",
            2,
            index,
            record,
        )
    assert not await env.raw.exists(record, index)


async def test_security_audit_archive_bypass_requires_matching_atomic_dedicated_pair(durable):
    env = durable
    await env.raw.set(env.fence, "archive-maintenance")
    main = env.key("audit")
    dedicated = env.key("security:credential-audit")
    payload = json.dumps({"action": "agent.credential.revoke", "credential_id": "one"})
    async with env.client.pipeline(transaction=True) as pipe:
        pipe.xadd(main, {"payload": payload})
        with pytest.raises(ResponseError, match="DURABLE_MAINTENANCE"):
            await pipe.execute()
    assert not await env.raw.exists(main, dedicated, env.outbox)

    async with env.client.pipeline(transaction=True) as pipe:
        pipe.xadd(dedicated, {"payload": payload})
        pipe.xadd(main, {"payload": payload})
        results = await pipe.execute()
    assert len(results) == 2
    assert await env.raw.xlen(main) == await env.raw.xlen(dedicated) == 1
    assert not await env.raw.exists(env.outbox)


@pytest.mark.parametrize(
    ("suffix", "command"),
    [
        ("agents:record:worker-1", "SET"),
        ("skills:record:skill-1", "SET"),
        ("skills:revision:skill-1:2", "SET"),
        ("planning:session:session-1", "SET"),
        ("planning:revision:session-1:3", "SET"),
        ("agent:goal:goal-1", "SET"),
        ("agent:run:run-1", "SET"),
        ("security:threat-finding:finding-1", "SET"),
        ("automation:job-1", "SET"),
        ("goal:goal-1:tool-receipt:action-1", "SET"),
        ("agent:events:run-1", "XADD"),
        ("agent:goal:events:goal-1", "XADD"),
        ("planning:events:session-1", "XADD"),
        ("automation:job-1:events", "XADD"),
        ("audit", "XADD"),
        ("conversation:incident:prod:messages", "RPUSH"),
        ("quota:active-runs", "SET"),
        ("skills:name:private", "SET"),
        ("goal:goal-1:browser-session:browser-1", "SET"),
        ("automation:job-1:pending", "SET"),
        ("conversation:incident:summary", "SET"),
        ("conversation:incident:memory", "SET"),
        ("checkpoint:secret", "SET"),
        ("credentials:secret", "SET"),
        ("skills:revision:skill-1:0", "SET"),
        ("skills:revision:skill-1:01", "SET"),
        ("agent:run:bad/id", "SET"),
        ("agent:run:" + "x" * 129, "SET"),
        ("conversation:has spaces:messages", "RPUSH"),
        ("conversation:" + "x" * 129 + ":messages", "RPUSH"),
    ],
)
async def test_lua_computed_key_capture_matches_python_family_allowlist(durable, suffix, command):
    env = durable
    key = env.key(suffix)
    if command == "XADD":
        script = "return redis.call('XADD', ARGV[1] .. ARGV[2], '*', 'payload', '{}')"
    else:
        script = f"return redis.call('{command}', ARGV[1] .. ARGV[2], '{{}}')"
    await env.client.eval(script, 0, f"tenant:{env.tenant}:", suffix)
    expected = classify_key(key)
    matching_mode = {"SET": "record", "XADD": "events", "RPUSH": "messages"}[command]
    rows = await envelopes(env)
    assert len(rows) == int(bool(expected and expected[2] == matching_mode)), suffix
    if rows:
        assert rows[0]["key"] == key and rows[0]["command"] == command


@pytest.mark.parametrize("prefix", ["-", ".", "_", "bad tenant-", ""])
async def test_lua_tenant_validation_matches_principal_pattern(durable, prefix):
    env = durable
    tenant = prefix + env.tenant
    key = f"tenant:{tenant}:agent:run:run-one"
    try:
        await env.client.eval("return redis.call('SET', KEYS[1], '{}')", 1, key)
        assert len(await envelopes(env)) == int(classify_key(key) is not None)
    finally:
        await env.raw.delete(key)


async def test_delete_conversation_is_tombstone_but_finding_eviction_is_not(durable):
    env = durable
    messages = env.key("conversation:incident:prod:messages")
    summary = env.key("conversation:incident:prod:summary")
    memory = env.key("conversation:incident:prod:memory")
    finding = env.key("security:threat-finding:finding-1")
    await env.client.rpush(messages, '{"content":"private"}')
    await env.client.set(summary, "private summary")
    await env.client.set(memory, "private memory")
    await env.client.set(finding, "{}")
    assert await env.client.delete(finding) == 1
    assert await env.client.delete(messages, summary, memory) == 3
    deleted = [row for row in await envelopes(env) if row["command"] == "DEL"]
    assert deleted == [{"schema_version": 1, "command": "DEL", "key": messages, "value": None}]


async def test_outbox_retains_payload_after_record_ttl_and_source_stream_trim(durable):
    env = durable
    record = env.key("agent:run:expiring-run")
    stream = env.key("agent:events:trimmed-run")
    await env.client.set(record, '{"status":"completed"}', px=25)
    for count in range(5):
        await env.client.xadd(
            stream,
            {"payload": json.dumps({"type": "tick", "count": count})},
            maxlen=2,
            approximate=False,
        )
    await asyncio.sleep(0.05)
    assert not await env.raw.exists(record)
    assert await env.raw.xlen(stream) == 2
    rows = await envelopes(env)
    assert len(rows) == 6
    assert rows[0]["value"] == '{"status":"completed"}'
    assert [json.loads(row["value"]["payload"])["count"] for row in rows[1:]] == list(range(5))
    assert await env.raw.ttl(env.outbox) == -1


async def test_legacy_runtime_without_dsn_does_not_produce_outbox(durable):
    env = durable
    legacy = RedisRuntime(os.environ["TEST_REDIS_URL"])
    try:
        await legacy.save_goal("legacy-goal", {"status": "queued"}, env.tenant)
        await legacy.append_audit(env.tenant, {"action": "legacy"})
        assert not await env.raw.exists(env.outbox)
    finally:
        await legacy.close()


def skill_content():
    return SkillContent(
        name="release-evidence",
        display_name="證據摘要",
        description="整理已提供的證據。",
        instructions="只整理已提供資料，不宣稱已執行外部操作。",
    ).model_dump()


def agent_record(tenant):
    now = datetime.now(UTC).isoformat()
    return {
        "tenant_id": tenant,
        "agent_id": "worker-one",
        "name": "Evidence worker",
        "description": "Review supplied evidence",
        "persona": "Retain uncertainty.",
        "role": "worker",
        "model": "local",
        "enabled": True,
        "skills": [],
        "delegate_agent_ids": [],
        "policy": AutomationPolicy(allowed_tools=[]).model_dump(),
        "created_by": "reviewer",
        "updated_by": "reviewer",
        "created_at": now,
        "updated_at": now,
    }


async def test_actual_agent_skill_planning_and_goal_stores_capture_all_direct_writes(durable):
    env = durable
    agents, skills, plans = (
        AgentStore(env.runtime),
        SkillStore(env.runtime),
        PlanningStore(env.runtime),
    )
    agent = await agents.create(agent_record(env.tenant))
    await agents.set_enabled(env.tenant, agent["agent_id"], False)
    content = skill_content()
    skill = await skills.create(env.tenant, content, "reviewer")
    await skills.publish(
        env.tenant,
        skill["skill_id"],
        content | {"description": "第二版描述。"},
        skill["version"],
        "reviewer",
    )
    snapshot = {
        "schema_version": 1,
        "tenant_id": env.tenant,
        "agent_id": agent["agent_id"],
        "root_agent": agent,
        "workers": [],
    }
    snapshot["snapshot_fingerprint"] = canonical_hash(snapshot)

    async def builder(pipe):
        return deepcopy(snapshot)

    session = await plans.create(env.tenant, agent["agent_id"], "reviewer", builder)
    await plans.save_plan(
        env.tenant,
        session["session_id"],
        {"objective": "Review evidence", "steps": [{"query": "Inspect sources"}]},
        session["version"],
        "reviewer",
        builder,
        complete=True,
    )
    goal = {
        "goal_id": str(uuid4()),
        "tenant_id": env.tenant,
        "status": "queued",
        "objective": "Review evidence",
        "steps": [],
        "policy": {"token_budget": 1_000},
        "created_by": "reviewer",
    }
    execution = GoalExecutionStore(env.runtime)
    await execution.submit_manual(goal)
    running = await execution.claim(goal["goal_id"], env.tenant, "worker", 60_000)
    assert running["status"] == "running"
    await execution.control(env.tenant, goal["goal_id"], "cancel", "reviewer")
    rows = await envelopes(env)
    kinds = {classify_key(row["key"])[0] for row in rows}
    assert {
        "agent",
        "skill",
        "skill_revision",
        "planning_session",
        "planning_revision",
        "goal",
        "planning",
        "audit",
    }.issubset(kinds)
    assert all(classify_key(row["key"]) is not None for row in rows)
    assert not any("skills:name:" in row["key"] or row["key"].endswith(":agents") for row in rows)
    goal_states = [
        json.loads(row["value"])["status"]
        for row in rows
        if row["command"] == "SET" and row["key"] == env.key(f"agent:goal:{goal['goal_id']}")
    ]
    assert goal_states == ["queued", "running", "cancelled"]


async def test_actual_automation_dispatch_captures_lua_computed_goal_and_events(durable):
    env = durable
    job = AutomationCreate.model_validate(
        {
            "name": "Deployment analysis",
            "goal": {"objective": "Review deployment", "model": "local"},
            "trigger": {"kind": "webhook", "event_types": ["deployment.failed"]},
        }
    ).model_dump(mode="json") | {
        "automation_id": "job-one",
        "tenant_id": env.tenant,
        "version": 1,
        "agent_id": "worker-one",
        "created_by": "reviewer",
        "next_run_at": None,
    }
    store = AutomationStore(env.runtime)
    await store.create(job)
    goal = materialize_goal(job)
    result = await store.submit(
        env.tenant,
        job["automation_id"],
        "deployment-1",
        goal,
        {"event_id": "deployment-1", "trigger_type": "webhook"},
        time.time(),
    )
    assert result["status"] == "queued"
    duplicate = await store.submit(
        env.tenant,
        job["automation_id"],
        "deployment-1",
        goal,
        {},
        time.time(),
    )
    assert duplicate["status"] == "duplicate"
    rows = await envelopes(env)
    target = env.key(f"agent:goal:{goal['goal_id']}")
    written = [row for row in rows if row["command"] == "SET" and row["key"] == target]
    assert len(written) == 1
    assert json.loads(written[0]["value"])["automation_id"] == "job-one"
    assert any(row["key"] == env.key(f"agent:goal:events:{goal['goal_id']}") for row in rows)
    assert any(row["key"] == env.key("automation:job-one:events") for row in rows)
    assert not any(":occurrence:" in row["key"] or row["key"].endswith(":active") for row in rows)


async def test_actual_delegation_transaction_captures_parent_child_and_lifecycle(durable):
    env = durable
    parent = {
        "goal_id": str(uuid4()),
        "tenant_id": env.tenant,
        "agent_id": "chief",
        "agent_config": {"agent_id": "chief", "role": "chief", "delegate_agent_ids": ["analyst"]},
        "objective": "Review evidence",
        "status": "running",
        "model": "local",
        "steps": [],
        "execution_owner": "owner",
        "execution_fence": 1,
        "execution_lease_until": time.time() + 60,
        "team_token_budget": 20_000,
        "policy": {"token_budget": 20_000},
    }
    await env.runtime.save_goal(parent["goal_id"], parent, env.tenant)
    policy = AutomationPolicy(allowed_tools=[], token_budget=8_000).model_dump()
    child = {
        "goal_id": str(uuid4()),
        "tenant_id": env.tenant,
        "agent_id": "analyst",
        "model": "local",
        "agent_config": {
            "agent_id": "analyst",
            "role": "worker",
            "model": "local",
            "delegate_agent_ids": [],
            "policy": policy,
        },
        "policy": policy,
        "token_budget": 8_000,
        "objective": "Inspect evidence",
        "steps": [],
    }
    store = DelegationStore(env.runtime)
    assert await store.create_children(
        parent | {"chief_budget": 4_000, "policy": {"token_budget": 4_000}},
        "owner",
        [child],
        {"policy_charged_tokens": 100},
    )
    current = await env.runtime.get_goal(parent["goal_id"], env.tenant)
    assert current["status"] == "waiting_children"
    child_id = current["children_goal_ids"][0]
    rows = await envelopes(env)
    assert any(row["key"] == env.key(f"agent:goal:{child_id}") for row in rows)
    assert any(row["key"] == env.key(f"agent:goal:events:{child_id}") for row in rows)
    assert any(row["key"] == env.key(f"agent:goal:events:{parent['goal_id']}") for row in rows)


async def test_real_policy_executor_receipts_never_capture_arguments_or_raw_result(durable):
    env = durable

    class ExternalTool:
        def specs(self):
            return [ToolSpec("browser_click", "Test write", requires_approval=True)]

        async def invoke(self, name, arguments):
            assert arguments["selector"] == "PRIVATE_ARGUMENT"
            return {"text": "UNSCANNED_RESPONSE"}

    executor = PolicyToolExecutor(
        ExternalTool(),
        {"allowed_tools": ["browser_click"]},
        runtime=env.runtime,
        tenant_id=env.tenant,
        goal_id="goal-one",
    )
    await executor.invoke_action("browser_click", {"selector": "PRIVATE_ARGUMENT"}, "action-one")
    with pytest.raises(ToolReconciliationRequired):
        await executor.invoke_action(
            "browser_click", {"selector": "PRIVATE_ARGUMENT"}, "action-one"
        )
    receipts = [json.loads(row["value"]) for row in await envelopes(env)]
    assert [row["status"] for row in receipts] == ["started", "completed"]
    assert all(set(row) == {"status", "tool", "action_id"} for row in receipts)
    encoded = json.dumps(receipts)
    assert "PRIVATE_ARGUMENT" not in encoded and "UNSCANNED_RESPONSE" not in encoded


@pytest.mark.parametrize("unsupported", ["set_get", "xadd_minid", "evalsha"])
async def test_unsupported_commands_fail_before_direct_or_pipeline_writes(durable, unsupported):
    env = durable
    record = env.key("agent:goal:unsupported-goal")
    events = env.key("agent:goal:events:unsupported-goal")
    index = env.key("agent:goals")

    def command(client):
        if unsupported == "set_get":
            return client.set(record, "{}", get=True)
        if unsupported == "xadd_minid":
            return client.xadd(events, {"payload": "{}"}, minid="1-0", approximate=False)
        return client.evalsha("0" * 40, 1, record)

    with pytest.raises((ValueError, ResponseError), match="unsupported|UNSUPPORTED|EVALSHA"):
        await command(env.client)
    assert not await env.raw.exists(record, events, index, env.outbox)
    async with env.client.pipeline(transaction=True) as pipe:
        pipe.sadd(index, "unsupported-goal")
        pipe.set(record, '{"status":"must-not-be-written"}')
        command(pipe)
        with pytest.raises((ValueError, ResponseError), match="unsupported|UNSUPPORTED|EVALSHA"):
            await pipe.execute()
    assert not await env.raw.exists(record, events, index, env.outbox)


@pytest.mark.parametrize("invalid", ["nul_value", "nul_key", "over_capacity"])
async def test_payload_preflight_rejects_second_command_without_partial_writes(durable, invalid):
    env = durable
    if invalid == "nul_value":
        payload = json.dumps({"nested": [{"private": "before\0after"}]})
    elif invalid == "nul_key":
        payload = json.dumps({"private\0key": "value"})
    else:
        payload = "x" * (1_048_576 + 1)
    first = env.key("agent:goal:first-goal")
    second = env.key("agent:goal:second-goal")
    index = env.key("agent:goals")
    with pytest.raises(ResponseError, match="DURABLE_PAYLOAD"):
        await env.client.set(second, payload)
    assert not await env.raw.exists(first, second, index, env.outbox)
    async with env.client.pipeline(transaction=True) as pipe:
        pipe.sadd(index, "first-goal")
        pipe.set(first, '{"status":"must-not-be-written"}')
        pipe.set(second, payload)
        with pytest.raises(ResponseError, match="DURABLE_PAYLOAD"):
            await pipe.execute()
    assert not await env.raw.exists(first, second, index, env.outbox)


async def test_trusted_automation_lua_rejects_nul_argv_before_configuration_and_indexes(durable):
    env = durable
    store = AutomationStore(env.runtime)
    job = {
        "automation_id": "nul-job",
        "tenant_id": env.tenant,
        "name": "unpersistable\0name",
        "enabled": True,
    }
    with pytest.raises(ResponseError, match="DURABLE_PAYLOAD"):
        await store.create(job)
    assert not await env.raw.exists(
        env.key("automation:nul-job"), env.key("automations"), env.outbox
    )
    assert not await env.raw.sismember("automation:tenants", env.tenant)
    assert await env.raw.zscore("automation:due", store._member(env.tenant, "nul-job")) is None


@pytest.mark.parametrize("text", [r"literal \u0000 text is not a NUL", "繁體中文證據與規劃 🧪🚀"])
async def test_valid_escaped_text_and_multibyte_unicode_are_captured_without_corruption(
    durable, text
):
    env = durable
    payload = json.dumps({"text": text}, ensure_ascii=False)
    key = env.key("agent:run:unicode-run")
    assert await env.client.set(key, payload)
    rows = await envelopes(env)
    assert len(rows) == 1
    assert rows[0]["value"] == payload
    assert json.loads(rows[0]["value"])["text"] == text


async def test_exact_one_mib_source_is_accepted_at_capacity_boundary(durable):
    env = durable
    payload = '{"text":"' + "x" * (1_048_576 - len('{"text":""}')) + '"}'
    assert len(payload.encode("utf-8")) == 1_048_576
    key = env.key("agent:run:capacity-run")
    assert await env.client.set(key, payload)
    rows = await envelopes(env)
    assert len(rows) == 1 and rows[0]["value"] == payload


async def test_oversized_rpush_aggregate_is_rejected_before_any_pipeline_mutation(durable):
    env = durable
    value = json.dumps({"request_id": "one", "role": "user", "content": "\\" * 250_000})
    first, messages = env.key("agent:goal:first-goal"), env.key("conversation:large:messages")
    async with env.client.pipeline() as pipe:
        pipe.set(first, "{}")
        pipe.rpush(messages, *[value] * 10)
        with pytest.raises(ResponseError, match="DURABLE_PAYLOAD"):
            await pipe.execute()
    assert not await env.raw.exists(first, messages, env.outbox)


async def test_pipeline_redis_errors_never_echo_private_command_arguments(durable):
    env = durable
    await env.raw.set(maintenance_key(env.source), "test-owner", px=60_000)
    async with env.client.pipeline() as pipe:
        pipe.set(env.key("agent:goal:private-goal"), '{"persona":"PRIVATE_PERSONA_CANARY"}')
        with pytest.raises(ResponseError) as error:
            await pipe.execute()
    assert str(error.value) == "DURABLE_MAINTENANCE"
    assert "PRIVATE" not in str(error.value) and error.value.__suppress_context__

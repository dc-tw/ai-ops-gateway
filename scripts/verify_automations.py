"""Container-side acceptance: real HTTP, Redis, schedulers and Goal workers.

Run through test_proactive_stack.sh. The inference service is deterministic;
these checks establish orchestration behavior, not model analysis quality.
"""

import asyncio
import hashlib
import hmac
import json
import os
import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx

from app.core.automation_store import AutomationStore
from app.core.config import get_settings
from app.core.redis_runtime import RedisRuntime
from app.core.threat_feedback import record_threat_findings

BASE = "http://127.0.0.1:8000"
TEST_SECRET = "automation-e2e-public-test-secret-20261025"


def post(path, payload):
    response = httpx.post(BASE + path, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def payload(name, trigger, task_type="agent"):
    return {
        "name": name,
        "task_type": task_type,
        "goal": {
            "objective": "根據提供的資料分析原因並提出建議",
            "model": "mock-model",
            "steps": [{"query": "依提供的資料產生有依據的報告"}],
            "max_retries": 0,
        },
        "trigger": trigger,
        "policy": {"allowed_tools": []},
    }


def prepare():
    security = post(
        "/v1/agents",
        {
            "name": "Security Analyst",
            "role": "worker",
            "description": "Analyse security findings and trust boundaries",
            "persona": "你是資安分析師，區分已知證據與推論。",
            "model": "mock-model",
            "policy": {"allowed_tools": [], "token_budget": 6000},
        },
    )
    operations = post(
        "/v1/agents",
        {
            "name": "Operations Analyst",
            "role": "worker",
            "description": "Analyse deployment failures and operational impact",
            "persona": "你是運維分析師，提出有依據的修復建議。",
            "model": "mock-model",
            "policy": {"allowed_tools": [], "token_budget": 6000},
        },
    )
    chief = post(
        "/v1/agents",
        {
            "name": "Chief Agent",
            "role": "chief",
            "description": "Delegate the main task to specialists and synthesise results",
            "persona": "你是 Chief，依專長派工並彙整有來源的結果。",
            "model": "mock-model",
            "delegate_agent_ids": [security["agent_id"], operations["agent_id"]],
            "policy": {"allowed_tools": [], "token_budget": 20000},
        },
    )
    daily = payload(
        "每日 ATR 摘要",
        {"kind": "schedule", "cron": "0 9 * * *", "timezone": "Asia/Taipei"},
        "atr_summary",
    )
    daily["agent_id"] = security["agent_id"]
    scheduled = post(
        "/v1/automations",
        daily,
    )
    webhook_job = payload("部署失敗分析", {"kind": "webhook", "event_types": ["deployment.failed"]})
    webhook_job["agent_id"] = chief["agent_id"]
    hooked = post(
        "/v1/automations",
        webhook_job,
    )
    finding = post(
        "/v1/automations",
        payload(
            "ATR 事件分析",
            {"kind": "internal", "event_types": ["threat.detected"], "severities": ["high"]},
        ),
    )
    failure = post(
        "/v1/automations",
        payload("Goal 失敗分析", {"kind": "internal", "event_types": ["goal.failed"]}),
    )
    print(
        json.dumps(
            {
                "schedule": scheduled["automation_id"],
                "hook": hooked["automation_id"],
                "finding": finding["automation_id"],
                "failure": failure["automation_id"],
            }
        )
    )


def wait_for_goal(automation_id):
    until = time.monotonic() + 90
    while time.monotonic() < until:
        response = httpx.get(BASE + f"/v1/automations/{automation_id}/goals", timeout=10)
        response.raise_for_status()
        goals = response.json()["goals"]
        if goals:
            current = goals[-1]
            if current["status"] in {"completed", "failed", "cancelled"}:
                assert current["status"] == "completed", current
                assert current["steps"][0]["answer"]
                return current
        time.sleep(0.2)
    raise AssertionError(f"Automation did not complete: {automation_id}")


async def seed_and_release_schedule(ids):
    settings = get_settings()
    runtime = RedisRuntime(settings.redis_url)
    try:
        store = AutomationStore(runtime)
        job = await store.get("default", ids["schedule"])
        zone = ZoneInfo("Asia/Taipei")
        local_now = datetime.now(zone)
        yesterday = local_now.replace(hour=12) - timedelta(days=1)
        finding_id = f"e2e-yesterday-{uuid4()}"
        # Seed retained metadata at a calendar boundary, without a scanned payload.
        await runtime.save_threat_findings(
            "default",
            {
                "event_type": "tool_response",
                "location": "test_fixture",
                "status": "detected",
                "findings": [
                    {
                        "finding_id": finding_id,
                        "rule_id": "ATR-E2E-YESTERDAY",
                        "severity": "high",
                        "title": "Synthetic retained finding",
                    }
                ],
            },
            context_id="daily-e2e",
            model="mock-model",
            ttl_seconds=2_592_000,
            max_records=10_000,
            source_automation_id="test-fixture",
        )
        key = runtime._tenant_key("default", f"security:threat-finding:{finding_id}")
        row = json.loads(await runtime._client.get(key))
        row["created_at"] = yesterday.isoformat()
        await runtime._client.set(key, json.dumps(row), ex=2_592_000)
        # Make the latest real 09:00 occurrence due, without waiting until morning.
        # Only the two independently running scheduler processes dispatch it;
        # this verifier never calls tick() or dispatch() itself.
        due = local_now.replace(hour=9, minute=0, second=0, microsecond=0)
        if due > local_now:
            due -= timedelta(days=1)
        await store.set_next_run(
            "default",
            ids["schedule"],
            due.astimezone(UTC).timestamp(),
            expected_version=job["version"],
        )
        await record_threat_findings(
            runtime,
            settings,
            "default",
            {
                "event_type": "tool_response",
                "location": "acceptance",
                "status": "detected",
                "findings": [
                    {
                        "finding_id": str(uuid4()),
                        "rule_id": "ATR-E2E-EVENT",
                        "severity": "high",
                        "title": "Synthetic event finding",
                    }
                ],
            },
            context_id="event-e2e",
            model="mock-model",
        )
    finally:
        await runtime.close()


def verify(ids):
    asyncio.run(seed_and_release_schedule(ids))
    event = {
        "event_id": "deployment-event-1",
        "type": "deployment.failed",
        "data": {"service": "checkout", "status": "failed", "reason": "readiness timeout"},
    }
    raw = json.dumps(event, separators=(",", ":")).encode()
    timestamp = str(int(time.time()))
    signature = hmac.new(
        TEST_SECRET.encode(), timestamp.encode() + b"." + raw, hashlib.sha256
    ).hexdigest()
    headers = {
        "content-type": "application/json",
        "x-automation-timestamp": timestamp,
        "x-automation-signature": f"sha256={signature}",
    }
    path = BASE + f"/v1/automations/hooks/default/{ids['hook']}"
    first = httpx.post(path, content=raw, headers=headers, timeout=30)
    first.raise_for_status()
    repeated = httpx.post(path, content=raw, headers=headers, timeout=30)
    repeated.raise_for_status()
    assert repeated.json()["status"] == "duplicate", repeated.text
    assert first.json()["goal_id"] == repeated.json()["goal_id"]
    invalid = httpx.post(
        path,
        content=raw,
        headers={**headers, "x-automation-signature": "sha256=invalid"},
        timeout=10,
    )
    assert invalid.status_code == 401
    # A real background timeout emits goal.failed and activates the internal job.
    post(
        "/v1/goals",
        {
            "objective": "Failure event acceptance",
            "model": "mock-model",
            "steps": [{"query": "checkpoint-resume simulated timeout"}],
            "max_retries": 0,
            "step_deadline_seconds": 1,
        },
    )
    results = {name: wait_for_goal(automation_id) for name, automation_id in ids.items()}
    summary = results["schedule"]["atr_summary"]
    assert summary["by_rule_id"]["ATR-E2E-YESTERDAY"] == 1, summary
    assert summary["timezone"] == "Asia/Taipei"
    chief_goal = results["hook"]
    assert chief_goal["agent_config"]["role"] == "chief"
    assert len(chief_goal["children_goal_ids"]) == 2
    children = [
        httpx.get(BASE + f"/v1/goals/{goal_id}").json()
        for goal_id in chief_goal["children_goal_ids"]
    ]
    assert {c["agent_config"]["name"] for c in children} == {
        "Security Analyst",
        "Operations Analyst",
    }
    assert all(c["status"] == "completed" for c in children)
    allocated = chief_goal["policy"]["token_budget"] + sum(
        c["policy"]["token_budget"] for c in children
    )
    assert allocated <= chief_goal["team_token_budget"] == 20000
    assert len({c["agent_config"]["persona"] for c in children}) == 2
    for name, automation_id in ids.items():
        rows = httpx.get(BASE + f"/v1/automations/{automation_id}/goals").json()["goals"]
        assert len(rows) == 1, (name, rows)
        with httpx.stream(
            "GET", BASE + f"/v1/automations/{automation_id}/events", timeout=15
        ) as response:
            response.raise_for_status()
            saw_completed = False
            for line in response.iter_lines():
                if line.startswith("data:") and '"status":"completed"' in line:
                    saw_completed = True
                    break
            assert saw_completed, name
    print(
        json.dumps(
            {
                "status": "passed",
                "scenarios": [
                    "daily ATR summary from yesterday retained metadata",
                    "signed deployment failure webhook and replay deduplication",
                    "internal ATR event and Goal failure event",
                    "two schedulers, two workers, durable results and SSE lifecycle replay",
                    "Chief delegation, independent personas and shared team token budget",
                ],
            },
            ensure_ascii=False,
        )
    )


def prepare_crash():
    goal = post(
        "/v1/goals",
        {
            "objective": "Recover a background Goal after worker process termination",
            "model": "mock-model",
            "max_retries": 0,
            "step_deadline_seconds": 60,
            "steps": [{"query": "checkpoint-resume background worker crash recovery"}],
        },
    )
    for _ in range(100):
        current = httpx.get(BASE + f"/v1/goals/{goal['goal_id']}").json()
        if current["status"] == "running" and current.get("usage", {}).get("policy_charged_tokens"):
            print(goal["goal_id"])
            return
        time.sleep(0.1)
    raise AssertionError("Crash test never reached model execution")


def verify_crash():
    goal_id = os.environ["AUTOMATION_TEST_CRASH_GOAL_ID"]
    for _ in range(300):
        current = httpx.get(BASE + f"/v1/goals/{goal_id}").json()
        if current["status"] in {"completed", "failed", "cancelled"}:
            assert current["status"] == "completed", current
            assert current["execution_fence"] >= 2
            assert current["steps"][0]["status"] == "completed"
            print(json.dumps({"worker_crash_checkpoint_recovery": "passed"}))
            return
        time.sleep(0.2)
    raise AssertionError("Worker crash recovery did not finish")


if __name__ == "__main__":
    phase = os.getenv("AUTOMATION_TEST_PHASE")
    if phase == "prepare":
        prepare()
    elif phase == "crash-prepare":
        prepare_crash()
    elif phase == "crash-verify":
        verify_crash()
    else:
        verify(json.loads(os.environ["AUTOMATION_TEST_JOB_IDS_JSON"]))

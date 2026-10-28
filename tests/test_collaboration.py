import json

from fastapi.testclient import TestClient

from app.core.config import Settings
from app.main import create_app


class Pipeline:
    def __init__(self, client):
        self.client = client
        self.commands = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    def __getattr__(self, name):
        def queue(*args, **kwargs):
            self.commands.append((name, args, kwargs))
            return self

        return queue

    async def execute(self):
        values = []
        for name, args, kwargs in self.commands:
            values.append(await getattr(self.client, name)(*args, **kwargs))
        return values


class Client:
    def __init__(self):
        self.values = {}
        self.zsets = {}

    def pipeline(self, **kwargs):
        return Pipeline(self)

    async def set(self, key, value, **kwargs):
        self.values[key] = value
        return True

    async def get(self, key):
        return self.values.get(key)

    async def mget(self, keys):
        return [self.values.get(key) for key in keys]

    async def delete(self, key):
        return int(self.values.pop(key, None) is not None)

    async def exists(self, *keys):
        return sum(key in self.values for key in keys)

    async def zadd(self, key, mapping):
        self.zsets.setdefault(key, {}).update(mapping)
        return len(mapping)

    async def expire(self, key, ttl):
        return key in self.zsets

    async def zrem(self, key, *members):
        values = self.zsets.setdefault(key, {})
        return sum(values.pop(member, None) is not None for member in members)

    async def zremrangebyscore(self, key, minimum, maximum):
        values = self.zsets.setdefault(key, {})
        stale = [member for member, score in values.items() if score <= float(maximum)]
        for member in stale:
            values.pop(member)
        return len(stale)

    async def zrangebyscore(self, key, minimum, maximum):
        low = float(minimum)
        return [member for member, score in self.zsets.get(key, {}).items() if score >= low]


class Runtime:
    def __init__(self):
        self._client = Client()

    @staticmethod
    def _tenant_key(tenant, suffix):
        return f"tenant:{tenant}:{suffix}"

    async def close(self):
        return None


def configured(runtime):
    keys = {
        "human": {"subject": "alice", "tenant_id": "acme", "role": "admin"},
        "agent": {
            "subject": "worker-one",
            "tenant_id": "acme",
            "role": "operator",
            "actor_type": "agent",
        },
        "other": {
            "subject": "worker-two",
            "tenant_id": "acme",
            "role": "operator",
            "actor_type": "agent",
        },
    }
    runtime._client.values["tenant:acme:agents:record:worker-one"] = "{}"
    return create_app(
        settings=Settings(_env_file=None, app_env="production", api_keys_json=json.dumps(keys)),
        redis_runtime=runtime,
    )


def test_human_presence_is_opt_in_bounded_and_advisory():
    runtime = Runtime()
    with TestClient(configured(runtime)) as client:
        updated = client.put(
            "/v1/collaboration/presence/agent/worker-one",
            json={"state": "viewing"},
            headers={"x-api-key": "human"},
        )
        assert updated.status_code == 200, updated.text
        assert updated.json()["advisory_only"] is True
        listed = client.get(
            "/v1/collaboration/presence/agent/worker-one",
            headers={"x-api-key": "human"},
        )
        assert listed.json()["presence"][0]["subject"] == "alice"
        assert "draft" not in json.dumps(listed.json()).lower()
        cleared = client.delete(
            "/v1/collaboration/presence/agent/worker-one",
            headers={"x-api-key": "human"},
        )
        assert cleared.json() == {"cleared": True}


def test_agent_heartbeat_is_self_only_and_not_a_lease():
    runtime = Runtime()
    with TestClient(configured(runtime)) as client:
        response = client.post(
            "/v1/collaboration/agents/worker-one/heartbeat",
            json={"status": "working", "instance_id": "pod-a"},
            headers={"x-api-key": "agent"},
        )
        assert response.status_code == 200, response.text
        assert response.json()["lease_or_lock"] is False
        denied = client.post(
            "/v1/collaboration/agents/worker-one/heartbeat",
            json={"status": "idle"},
            headers={"x-api-key": "other"},
        )
        assert denied.status_code == 403
        viewed = client.get(
            "/v1/collaboration/agents/worker-one/heartbeat",
            headers={"x-api-key": "human"},
        )
        assert viewed.json()["heartbeat"]["status"] == "working"

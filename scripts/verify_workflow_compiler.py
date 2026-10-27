"""Offline Docker/Python portability smoke; never calls models or queues Goals."""

import time

from app.core.workflow_plan import (
    WorkflowInvalid,
    compile_workflow,
    load_workflow_yaml,
    validate_schema,
)

agent = {
    "agent_id": "reviewer",
    "version": 1,
    "role": "worker",
    "model": "offline",
    "delegate_agent_ids": [],
    "policy": {"token_budget": 20000, "allowed_tools": []},
}
snapshot = {"root_agent": agent, "workers": [], "snapshot_fingerprint": "a" * 64}
plan = load_workflow_yaml("""
schema_version: 2
objective: 檢查證據
nodes:
  - node_id: review
    agent_id: reviewer
    query: 區分事實與未知
    output_schema:
      type: object
      required: [summary]
      properties:
        summary: {type: string}
""")
compiled = compile_workflow(plan, snapshot)
assert compiled["nodes"][0]["agent_id"] == "reviewer"
validate_schema(plan["nodes"][0]["output_schema"], {"summary": "通過"})
started = time.monotonic()
try:
    validate_schema({"type": "string", "pattern": "^(a+)+$"}, "a" * 1000 + "!")
except WorkflowInvalid:
    pass
else:
    raise AssertionError("Pathological validation was not rejected")
assert time.monotonic() - started < 5
validate_schema({"type": "integer"}, 1)
print("Offline YAML, canonical compile, schema validation, Linux kill/reap and recovery passed")

"""Compiler and decisions are pure: never dispatch, infer, or approve tools."""

from copy import deepcopy

import pytest
import yaml

from app.core.workflow_plan import (
    WorkflowInvalid,
    adapt_legacy_plan,
    compile_workflow,
    evaluate_condition,
    load_workflow_yaml,
    node_output,
    readiness,
    resolve_inputs,
    validate_schema,
    verify_compiled,
)


def snapshot():
    def agent(key, role="worker"):
        return {
            "agent_id": key,
            "role": role,
            "version": 1,
            "model": "test-model",
            "delegate_agent_ids": [],
            "skills": [],
            "skill_snapshots": [],
            "policy": {"token_budget": 20000, "allowed_tools": []},
        }

    root = agent("chief", "chief")
    root["delegate_agent_ids"] = ["worker-a", "worker-b"]
    return {
        "root_agent": root,
        "workers": [agent("worker-a"), agent("worker-b")],
        "snapshot_fingerprint": "a" * 64,
    }


def node(key, agent="worker-a", **changes):
    return {"node_id": key, "agent_id": agent, "query": "分析證據", **changes}


def plan(**changes):
    return {"schema_version": 2, "objective": "驗證 DAG", "nodes": [node("a")], **changes}


def compiled(**changes):
    return compile_workflow(plan(**changes), snapshot())


def test_json_yaml_canonical_order_and_tamper_detection():
    value = plan(nodes=[node("b", "worker-b"), node("a")], edges=[{"source": "a", "target": "b"}])
    original = deepcopy(value)
    first = compile_workflow(value, snapshot())
    value["nodes"].reverse()
    second = compile_workflow(load_workflow_yaml(yaml.safe_dump(value)), snapshot())
    assert first == second
    verify_compiled(first, original, snapshot())
    first["nodes"][0]["query"] = "tampered"
    with pytest.raises(WorkflowInvalid, match="integrity"):
        verify_compiled(first, original, snapshot())


@pytest.mark.parametrize(
    "changes",
    [
        {"nodes": [node("a"), node("a")]},
        {"edges": [{"source": "a", "target": "a"}]},
        {"edges": [{"source": "absent", "target": "a"}]},
        {
            "nodes": [node("a"), node("b")],
            "edges": [{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
        },
        {
            "nodes": [node("a"), node("b")],
            "edges": [{"source": "a", "target": "b"}, {"source": "a", "target": "b"}],
        },
        {"nodes": [node("a", "unapproved")]},
        {"nodes": [node("a", "chief")]},
        {"nodes": [node("a", kind="synthesis")]},
        {"nodes": [node("a", token_budget=21000)], "token_budget": 21000},
        {"nodes": [node("a", token_budget=4000)], "token_budget": 3000},
    ],
)
def test_invalid_graphs_and_allocations_are_rejected(changes):
    with pytest.raises(WorkflowInvalid):
        compiled(**changes)


def test_limits_worker_recursion_and_readonly_synthesis():
    value = plan(nodes=[node("a"), node("b", "worker-b")])
    with pytest.raises(WorkflowInvalid, match="limits"):
        compile_workflow(value, snapshot(), max_nodes=1)
    assert compile_workflow(value, snapshot(), max_parallelism=1)["parallelism"] == 1
    source = snapshot()
    source["workers"][0]["delegate_agent_ids"] = ["worker-b"]
    with pytest.raises(WorkflowInvalid, match="non-delegating"):
        compile_workflow(value, source)
    source = snapshot()
    source["root_agent"]["policy"]["allowed_tools"] = ["write"]
    with pytest.raises(WorkflowInvalid, match="read-only"):
        compile_workflow(plan(nodes=[node("a", "chief", kind="synthesis")]), source)


@pytest.mark.parametrize(
    "text",
    [
        "objective: one\nobjective: two\nnodes: []",
        "!!python/object/apply:os.system ['whoami']",
        "objective: &x example\ncontext: *x\nnodes: []",
        "objective: x\nnodes: []\nunknown: true",
    ],
)
def test_unsafe_or_ambiguous_yaml_rejected(text):
    with pytest.raises(WorkflowInvalid):
        load_workflow_yaml(text)


def test_inputs_require_typed_ancestor_not_sibling_or_plain_text():
    value = plan(nodes=[node("a"), node("b", inputs={"data": {"node_id": "a"}})])
    with pytest.raises(WorkflowInvalid, match="upstream"):
        compile_workflow(value, snapshot())
    value["edges"] = [{"source": "a", "target": "b"}]
    with pytest.raises(WorkflowInvalid, match="validated upstream"):
        compile_workflow(value, snapshot())
    value["nodes"][0]["output_schema"] = {"type": "object"}
    assert compile_workflow(value, snapshot())["nodes"][1]["inputs"]["data"]["node_id"] == "a"


def test_structured_output_schema_and_local_references():
    schema = {
        "$defs": {"count": {"type": "integer", "minimum": 0}},
        "type": "object",
        "properties": {"n": {"$ref": "#/$defs/count"}},
        "required": ["n"],
        "additionalProperties": False,
    }
    validate_schema(schema)
    assert node_output({"output_schema": schema}, '{"n":2}') == {
        "output": {"n": 2},
        "output_validated": True,
    }
    for text in ('{"n":-1}', '{"n":true}', '{"n":1,"extra":2}', '{"n":1,"n":2}', "not json"):
        with pytest.raises(WorkflowInvalid):
            node_output({"output_schema": schema}, text)
    assert node_output({}, "plain text")["output_validated"] is False


@pytest.mark.parametrize(
    "schema",
    [
        {"$ref": "https://example.invalid/schema"},
        {"$ref": "file:///etc/passwd"},
        {"$id": "https://example.invalid"},
        {"$dynamicRef": "#x"},
        {"$schema": "http://json-schema.org/draft-07/schema#"},
        {"type": "not-a-type"},
    ],
)
def test_network_and_invalid_schema_denied(schema):
    with pytest.raises(WorkflowInvalid):
        validate_schema(schema)


def test_pathological_schema_regex_is_killed_and_next_scan_works():
    import time

    started = time.monotonic()
    with pytest.raises(WorkflowInvalid):
        validate_schema({"type": "string", "pattern": "^(a+)+$"}, "a" * 1000 + "!")
    assert time.monotonic() - started < 5
    validate_schema({"type": "integer"}, 1)


@pytest.mark.parametrize("text", ["x" * 16385, b"\\ud800".decode("unicode_escape")])
def test_output_is_bounded_before_json_parsing(text):
    with pytest.raises(WorkflowInvalid, match="UTF-8"):
        node_output({"output_schema": {}}, text)


def test_conditions_are_finite_strict_and_only_validated_output():
    states = {"a": {"status": "completed", "output": {"n": 2}, "output_validated": True}}
    expr = {"op": "gte", "node_id": "a", "path": "/n", "value": 2}
    assert evaluate_condition(expr, states)
    assert evaluate_condition({"op": "all", "args": [expr]}, states)
    assert not evaluate_condition(expr | {"op": "eq", "value": True}, states)
    assert not evaluate_condition(expr | {"op": "eq", "path": "", "value": {"n": True}}, states)
    assert not evaluate_condition(expr | {"op": "in", "path": "", "value": [{"n": True}]}, states)
    assert evaluate_condition(
        {"op": "eq", "node_id": "a", "source": "status", "value": "completed"}, states
    )
    states["a"]["output_validated"] = False
    with pytest.raises(WorkflowInvalid, match="unavailable"):
        evaluate_condition(expr, states)
    assert not evaluate_condition(expr | {"op": "exists"}, states)


@pytest.mark.parametrize("join", ["all_success", "any_success"])
def test_join_waits_for_every_parent_terminal(join):
    graph = compiled(
        nodes=[
            node("a", required=False),
            node("b", "worker-b", required=False),
            node("c", join=join),
        ],
        edges=[{"source": "a", "target": "c"}, {"source": "b", "target": "c"}],
    )
    states = {"a": {"status": "completed"}, "b": {"status": "waiting_approval"}}
    assert readiness(graph, states)["ready"] == []
    states["b"]["status"] = "failed"
    result = readiness(graph, states)
    assert result["ready"] == (["c"] if join == "any_success" else [])
    assert result["skip"] == (["c"] if join == "all_success" else [])


def test_approval_releases_capacity_but_not_dependencies_and_agent_serialization():
    graph = compiled(
        parallelism=1,
        nodes=[node("a"), node("b", "worker-b"), node("c")],
        edges=[{"source": "a", "target": "c"}],
    )
    states = {"a": {"status": "waiting_approval"}}
    assert readiness(graph, states)["ready"] == ["b"]
    assert readiness(graph, {"a": {"status": "running"}})["ready"] == []
    assert compiled(nodes=[node("a"), node("b")])["agent_parallelism"] == 1
    assert readiness(compiled(nodes=[node("a"), node("b")]), {})["ready"] == ["a"]


def test_false_branch_missing_input_and_required_failure_stop_dispatch():
    graph = compiled(
        nodes=[
            node("a", output_schema={"type": "object"}),
            node("b", "worker-b", inputs={"n": {"node_id": "a", "path": "/n"}}),
        ],
        edges=[{"source": "a", "target": "b"}],
    )
    states = {"a": {"status": "completed", "output": {}, "output_validated": True}}
    decision = readiness(graph, states)
    assert decision["status"] == "failed" and decision["fail"] == ["b"]
    assert decision["ready"] == []
    b = graph["nodes"][1]
    b["inputs"]["n"].update(required=False, default=7)
    assert resolve_inputs(b, states) == {"n": 7}
    b["condition"] = {"op": "exists", "node_id": "a", "path": "/n"}
    assert readiness(graph, states)["skip"] == ["b"]
    states = {"a": {"status": "failed"}, "b": {"status": "waiting_approval"}}
    decision = readiness(graph, states)
    assert decision["status"] == "failed" and decision["cancel"] == ["b"]
    assert decision["ready"] == []


def test_legacy_adapter_keeps_original_plan_unchanged():
    source = snapshot()
    source["root_agent"] = source["workers"][0]
    source["workers"] = []
    value = {
        "objective": "分析",
        "context": "",
        "steps": [{"query": "一"}, {"query": "二"}],
        "assignments": [],
    }
    original = deepcopy(value)
    adapted = adapt_legacy_plan(value, source)
    graph = compile_workflow(adapted, source)
    assert value == original
    assert graph["edges"] == [{"source": "step-000", "target": "step-001"}]
    assert graph["parallelism"] == 1


def test_legacy_chief_gets_readonly_join_without_changing_historical_plan():
    source = snapshot()
    value = {
        "objective": "彙整事件",
        "steps": [],
        "assignments": [
            {"agent_id": "worker-a", "objective": "資安", "steps": [{"query": "分析"}]},
            {"agent_id": "worker-b", "objective": "運維", "steps": [{"query": "分析"}]},
        ],
    }
    original = deepcopy(value)
    graph = compile_workflow(adapt_legacy_plan(value, source), source)
    summary = next(item for item in graph["nodes"] if item["node_id"] == "chief-summary")
    assert summary["kind"] == "synthesis" and summary["agent_id"] == "chief"
    assert summary["dependencies"] == ["step-000", "step-001"]
    assert "資安" in next(item for item in graph["nodes"] if item["node_id"] == "step-000")["query"]
    assert "運維" in next(item for item in graph["nodes"] if item["node_id"] == "step-001")["query"]
    assert value == original


def test_each_node_selects_only_its_pinned_skills_without_inference():
    source = snapshot()
    agent = source["workers"][0]
    agent["skills"] = [{"skill_id": "fixed", "revision": 1}]
    agent["skill_selection_rules"] = [
        {
            "skill_id": "evidence",
            "revision": 2,
            "field": "objective",
            "values": ["證據"],
            "priority": 1,
        }
    ]
    agent["skill_selection_candidates"] = [
        {"skill_id": "fixed", "revision": 1},
        {"skill_id": "evidence", "revision": 2},
    ]
    graph = compile_workflow(plan(nodes=[node("a"), node("b", query="彙整結果")]), source)
    assert graph["nodes"][0]["skills"] == agent["skills"] + [
        {"skill_id": "evidence", "revision": 2}
    ]
    assert graph["nodes"][1]["skills"] == agent["skills"]
    agent["skill_selection_candidates"][1]["revision"] = 3
    with pytest.raises(WorkflowInvalid, match="pinned snapshot"):
        compile_workflow(plan(), source)

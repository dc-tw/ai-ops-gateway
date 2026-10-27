"""Canonical DAG compiler and pure readiness semantics; no model/tool calls."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
import threading
from collections import Counter
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, StrictBool, StrictInt, model_validator

from app.core.skill_packages import StrictLoader
from app.core.skill_selection import select_skills

IDENTIFIER = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$"
TERMINAL = {"completed", "failed", "cancelled", "skipped", "rejected", "timed_out"}
FAILED = TERMINAL - {"completed", "skipped"}
META_SCHEMA = "https://json-schema.org/draft/2020-12/schema"
_MISSING = object()
_SCHEMA_SLOTS = threading.BoundedSemaphore(4)


class WorkflowInvalid(ValueError):
    pass


def canonical(value, limit=65_536):
    try:
        # Iterative preflight prevents recursion/unbounded nesting in callers
        # that supply already-decoded JSON or YAML objects.
        stack = [(value, 0)]
        count = 0
        while stack:
            item, depth = stack.pop()
            count += 1
            if depth > 24 or count > 16_384:
                raise ValueError
            if isinstance(item, dict):
                if any(not isinstance(key, str) for key in item):
                    raise ValueError
                stack.extend((child, depth + 1) for child in item.values())
            elif isinstance(item, list):
                stack.extend((child, depth + 1) for child in item)
        encoded = json.dumps(
            value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        )
        if len(encoded.encode("utf-8")) > limit:
            raise ValueError
        return encoded
    except (ValueError, TypeError, RecursionError, UnicodeError):
        raise WorkflowInvalid("Workflow exceeds its JSON, depth or UTF-8 limits") from None


def digest(value):
    return hashlib.sha256(canonical(value, 524_288).encode()).hexdigest()


class WorkflowModel(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True, allow_inf_nan=False)


class Condition(WorkflowModel):
    op: Literal["eq", "ne", "gt", "gte", "lt", "lte", "in", "exists", "all", "any", "not"]
    node_id: str | None = Field(default=None, pattern=IDENTIFIER)
    source: Literal["status", "output"] = "output"
    path: str = Field(default="", max_length=256)
    value: Any = None
    args: list[Condition] = Field(default_factory=list, max_length=8)

    @model_validator(mode="after")
    def expression(self):
        if self.op in {"all", "any", "not"}:
            if (
                not self.args
                or self.node_id is not None
                or self.path
                or self.value is not None
                or (self.op == "not" and len(self.args) != 1)
            ):
                raise ValueError("Invalid Boolean condition")
        elif self.node_id is None or self.args:
            raise ValueError("Comparison requires one upstream node")
        if self.source == "status" and self.path:
            raise ValueError("Status conditions cannot select output fields")
        if self.op == "in" and (not isinstance(self.value, list) or len(self.value) > 32):
            raise ValueError("Membership requires a finite list")
        pointer_tokens(self.path)
        return self


class InputRef(WorkflowModel):
    node_id: str = Field(pattern=IDENTIFIER)
    path: str = Field(default="", max_length=256)
    required: StrictBool = True
    default: Any = None

    @model_validator(mode="after")
    def pointer(self):
        pointer_tokens(self.path)
        return self


class WorkflowNode(WorkflowModel):
    node_id: str = Field(pattern=IDENTIFIER)
    agent_id: str = Field(pattern=IDENTIFIER)
    kind: Literal["task", "synthesis"] = "task"
    query: str = Field(min_length=1, max_length=8_000)
    target_url: HttpUrl | None = None
    token_budget: StrictInt = Field(default=2_000, ge=256, le=10_000_000)
    required: StrictBool = True
    join: Literal["all_success", "any_success"] = "all_success"
    condition: Condition | None = None
    inputs: dict[str, InputRef] = Field(default_factory=dict, max_length=16)
    output_schema: dict[str, Any] | None = None


class WorkflowEdge(WorkflowModel):
    source: str = Field(pattern=IDENTIFIER)
    target: str = Field(pattern=IDENTIFIER)


class WorkflowPlan(WorkflowModel):
    schema_version: Literal[2] = 2
    objective: str = Field(min_length=1, max_length=8_000)
    context: str = Field(default="", max_length=16_000)
    nodes: list[WorkflowNode] = Field(min_length=1, max_length=128)
    edges: list[WorkflowEdge] = Field(default_factory=list, max_length=256)
    parallelism: StrictInt = Field(default=4, ge=1, le=16)
    token_budget: StrictInt = Field(default=20_000, ge=256, le=20_000_000)
    max_retries: StrictInt = Field(default=2, ge=0, le=5)
    max_tool_rounds: StrictInt = Field(default=8, ge=1, le=20)
    step_deadline_seconds: float = Field(default=300.0, ge=1, le=3600)

    @model_validator(mode="after")
    def bounded(self):
        canonical(self.model_dump(mode="json"))
        if not self.objective.strip() or any(not node.query.strip() for node in self.nodes):
            raise ValueError("Workflow objective and queries cannot be blank")
        return self


def pointer_tokens(path):
    if path and (not path.startswith("/") or re.search(r"~(?![01])", path)):
        raise WorkflowInvalid("Invalid JSON pointer")
    return (
        [part.replace("~1", "/").replace("~0", "~") for part in path[1:].split("/")] if path else []
    )


def output_value(value, path):
    for part in pointer_tokens(path):
        if isinstance(value, dict):
            value = value.get(part, _MISSING)
        elif isinstance(value, list) and re.fullmatch(r"0|[1-9][0-9]*", part):
            index = int(part)
            value = value[index] if index < len(value) else _MISSING
        else:
            return _MISSING
    return value


def _schema_policy(schema):
    canonical(schema, 8_192)
    pending = [schema]
    while pending:
        item = pending.pop()
        if isinstance(item, dict):
            for key, value in item.items():
                if key in {"$id", "$dynamicRef", "$recursiveRef"}:
                    raise WorkflowInvalid(
                        "Schema identifiers and dynamic references are not supported"
                    )
                if key == "$ref" and (not isinstance(value, str) or not value.startswith("#")):
                    raise WorkflowInvalid("Remote schema references are forbidden")
                if key == "$schema" and value != META_SCHEMA:
                    raise WorkflowInvalid("Only JSON Schema 2020-12 is supported")
                pending.append(value)
        elif isinstance(item, list):
            pending.extend(item)


def validate_schema(schema, instance=_MISSING):
    _schema_policy(schema)
    payload = {"operation": "schema" if instance is _MISSING else "instance", "schema": schema}
    if instance is not _MISSING:
        canonical(instance, 16_384)
        payload["instance"] = instance
    try:
        if not _SCHEMA_SLOTS.acquire(timeout=3):
            raise WorkflowInvalid("Structured validation capacity unavailable")
        try:
            result = subprocess.run(
                [sys.executable, "-m", "app.core.schema_worker"],
                input=canonical(payload, 131_072).encode(),
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                timeout=3,
                check=False,
            )
        finally:
            _SCHEMA_SLOTS.release()
    except (OSError, subprocess.TimeoutExpired):
        raise WorkflowInvalid("Structured output validation unavailable or timed out") from None
    if result.returncode != 0 or result.stdout != b"valid":
        raise WorkflowInvalid("Invalid schema or schema-validated output")


def _condition_refs(condition):
    pending = [(condition, 0)]
    refs, count = [], 0
    while pending:
        item, depth = pending.pop()
        count += 1
        if depth > 8 or count > 64:
            raise WorkflowInvalid("Condition exceeds finite evaluation limits")
        if item.node_id:
            refs.append((item.node_id, item.source))
        pending.extend((arg, depth + 1) for arg in item.args)
    return refs


def compile_workflow(
    plan, snapshot, *, max_nodes=32, max_edges=64, max_parallelism=4, check_schemas=True
):
    """Compile JSON/YAML/adapted legacy plans into the same immutable representation."""
    canonical(plan)
    parsed = WorkflowPlan.model_validate(plan)
    normalized = parsed.model_dump(mode="json")
    normalized["nodes"].sort(key=lambda item: item["node_id"])
    normalized["edges"].sort(key=lambda item: (item["source"], item["target"]))
    if len(parsed.nodes) > max_nodes or len(parsed.edges) > max_edges:
        raise WorkflowInvalid("Workflow exceeds configured graph limits")
    root = snapshot["root_agent"]
    agents = {agent["agent_id"]: agent for agent in [root, *snapshot.get("workers", [])]}
    nodes = {node.node_id: node for node in parsed.nodes}
    if len(nodes) != len(parsed.nodes):
        raise WorkflowInvalid("Duplicate workflow node")
    dependencies = {key: set() for key in nodes}
    outgoing = {key: set() for key in nodes}
    edges = {(edge.source, edge.target) for edge in parsed.edges}
    if len(edges) != len(parsed.edges):
        raise WorkflowInvalid("Duplicate workflow edge")
    for source, target in edges:
        if source not in nodes or target not in nodes or source == target:
            raise WorkflowInvalid("Invalid workflow dependency")
        dependencies[target].add(source)
        outgoing[source].add(target)
    order, remaining = [], {key: len(value) for key, value in dependencies.items()}
    ready = sorted(key for key, count in remaining.items() if count == 0)
    while ready:
        key = ready.pop(0)
        order.append(key)
        for child in sorted(outgoing[key]):
            remaining[child] -= 1
            if remaining[child] == 0:
                ready.append(child)
                ready.sort()
    if len(order) != len(nodes):
        raise WorkflowInvalid("Workflow cannot contain cycles")
    ancestors, compiled, allocations = {}, [], Counter()
    for key in order:
        node = nodes[key]
        agent = agents.get(node.agent_id)
        if not agent:
            raise WorkflowInvalid("Node Agent is outside the approved snapshot")
        if node.kind == "synthesis":
            if (
                agent["agent_id"] != root["agent_id"]
                or agent["role"] != "chief"
                or agent["policy"].get("allowed_tools")
            ):
                raise WorkflowInvalid("Synthesis requires the read-only root Chief")
        elif agent["role"] != "worker" or agent.get("delegate_agent_ids"):
            raise WorkflowInvalid("Task nodes require non-delegating Worker Agents")
        ancestors[key] = set(dependencies[key])
        for parent in dependencies[key]:
            ancestors[key].update(ancestors[parent])
        refs = [(ref.node_id, "output") for ref in node.inputs.values()]
        if node.condition:
            refs.extend(_condition_refs(node.condition))
        for source, kind in refs:
            if source not in ancestors[key]:
                raise WorkflowInvalid("Inputs and conditions must reference upstream nodes")
            if kind == "output" and nodes[source].output_schema is None:
                raise WorkflowInvalid("Output references require a validated upstream schema")
        if node.output_schema is not None and check_schemas:
            validate_schema(node.output_schema)
        allocations[node.agent_id] += node.token_budget
        if allocations[node.agent_id] > int(agent["policy"]["token_budget"]):
            raise WorkflowInvalid("Workflow exceeds an Agent's shared allocation")
        bindings, selection = select_skills(
            agent.get("skills", []),
            agent.get("skill_selection_rules", []),
            {"objective": node.query, "task_type": "agent"},
        )
        candidates = {
            item["skill_id"]: item
            for item in agent.get("skill_selection_candidates", agent.get("skill_snapshots", []))
        }
        skills = []
        for binding in bindings:
            item = candidates.get(binding["skill_id"])
            if not item or item["revision"] != binding["revision"]:
                raise WorkflowInvalid("Node Skill selection is outside the pinned snapshot")
            skills.append(item)
        compiled.append(
            node.model_dump(mode="json")
            | {
                "dependencies": sorted(dependencies[key]),
                "agent_version": agent["version"],
                "model": agent["model"],
                "skills": bindings,
                "skill_snapshots": skills,
                "skill_selection": selection,
            }
        )
    if sum(allocations.values()) > parsed.token_budget or parsed.token_budget > int(
        root["policy"]["token_budget"]
    ):
        raise WorkflowInvalid("Workflow exceeds the shared root token budget")
    body = {
        "schema_version": 2,
        "compiler_version": "gateway-dag-v1",
        "objective": parsed.objective,
        "context": parsed.context,
        "root_agent_id": root["agent_id"],
        "nodes": compiled,
        "edges": [{"source": source, "target": target} for source, target in sorted(edges)],
        "parallelism": min(parsed.parallelism, max_parallelism),
        "agent_parallelism": 1,
        "token_budget": parsed.token_budget,
        "max_retries": parsed.max_retries,
        "max_tool_rounds": parsed.max_tool_rounds,
        "step_deadline_seconds": parsed.step_deadline_seconds,
        "configuration_sha256": snapshot["snapshot_fingerprint"],
        "input_sha256": digest(normalized),
    }
    canonical(body, 524_288)
    return body | {"compiled_sha256": digest(body)}


def verify_compiled(compiled, plan, snapshot):
    normalized = WorkflowPlan.model_validate(plan).model_dump(mode="json")
    normalized["nodes"].sort(key=lambda item: item["node_id"])
    normalized["edges"].sort(key=lambda item: (item["source"], item["target"]))
    if (
        not isinstance(compiled, dict)
        or compiled.get("schema_version") != 2
        or compiled.get("compiler_version") != "gateway-dag-v1"
        or compiled.get("configuration_sha256") != snapshot["snapshot_fingerprint"]
        or compiled.get("input_sha256") != digest(normalized)
        or compiled.get("compiled_sha256")
        != digest({key: value for key, value in compiled.items() if key != "compiled_sha256"})
    ):
        raise WorkflowInvalid("Compiled workflow integrity check failed")


def load_workflow_yaml(text):
    try:
        if len(text.encode("utf-8")) > 65_536:
            raise ValueError
        depth = 0
        for token in yaml.scan(text, Loader=StrictLoader):
            if isinstance(
                token,
                (
                    yaml.tokens.BlockMappingStartToken,
                    yaml.tokens.BlockSequenceStartToken,
                    yaml.tokens.FlowMappingStartToken,
                    yaml.tokens.FlowSequenceStartToken,
                ),
            ):
                depth += 1
                if depth > 24:
                    raise ValueError
            elif isinstance(
                token,
                (
                    yaml.tokens.BlockEndToken,
                    yaml.tokens.FlowMappingEndToken,
                    yaml.tokens.FlowSequenceEndToken,
                ),
            ):
                depth -= 1
        value = yaml.load(text, Loader=StrictLoader)
        canonical(value)
        return WorkflowPlan.model_validate(value).model_dump(mode="json")
    except Exception:
        raise WorkflowInvalid("Invalid, ambiguous or unsafe workflow YAML") from None


def adapt_legacy_plan(plan, snapshot):
    """Explicit adapter only: old stored Plan bytes/hashes are never rewritten."""
    root = snapshot["root_agent"]
    from app.core.planning_snapshot import validate_planning_plan

    if plan.get("schema_version") == 2 or not validate_planning_plan(snapshot, plan):
        raise WorkflowInvalid("Legacy adapter requires a complete linear or Chief plan")
    groups = (
        [(root, plan.get("steps", []), "")]
        if root["role"] == "worker"
        else [
            (
                next(
                    worker
                    for worker in snapshot["workers"]
                    if worker["agent_id"] == item["agent_id"]
                ),
                item.get("steps", []),
                item.get("objective", ""),
            )
            for item in plan.get("assignments", [])
        ]
    )
    count = sum(len(steps) for _, steps, _ in groups)
    summary_count = int(root["role"] == "chief")
    if not 1 <= count or count + summary_count > 32:
        raise WorkflowInvalid("Legacy plan cannot fit the configured DAG node limit")
    budget = int(root["policy"]["token_budget"])
    summary_budget = max(1024, budget // 3) if summary_count else 0
    worker_budget = budget - summary_budget
    nodes, edges, index = [], [], 0
    leaves = []
    for agent, steps, objective in groups:
        previous = None
        allocation = min(
            worker_budget // count, int(agent["policy"]["token_budget"]) // max(1, len(steps))
        )
        if allocation < 256:
            raise WorkflowInvalid("Legacy plan cannot fund every node")
        for step in steps:
            key = f"step-{index:03d}"
            index += 1
            nodes.append(
                {
                    "node_id": key,
                    "agent_id": agent["agent_id"],
                    "query": f"{objective}\n\n{step['query']}" if objective else step["query"],
                    "target_url": step.get("target_url"),
                    "token_budget": allocation,
                }
            )
            if previous:
                edges.append({"source": previous, "target": key})
            previous = key
        if previous:
            leaves.append(previous)
    if summary_count:
        nodes.append(
            {
                "node_id": "chief-summary",
                "agent_id": root["agent_id"],
                "kind": "synthesis",
                "query": plan["objective"] + "\n彙整已提供證據，保留失敗、分歧與未知事項。",
                "token_budget": summary_budget,
            }
        )
        edges.extend({"source": leaf, "target": "chief-summary"} for leaf in leaves)
    return WorkflowPlan.model_validate(
        {
            "schema_version": 2,
            "objective": plan["objective"],
            "context": plan.get("context", ""),
            "nodes": nodes,
            "edges": edges,
            "parallelism": 1 if root["role"] == "worker" else 4,
            "token_budget": budget,
            **{
                key: plan[key]
                for key in ("max_retries", "max_tool_rounds", "step_deadline_seconds")
                if key in plan
            },
        }
    ).model_dump(mode="json")


def evaluate_condition(condition, states):
    expression = Condition.model_validate(condition)
    _condition_refs(expression)

    def same_value(left, right):
        # Python's container equality treats nested True == 1. Conditions must
        # retain JSON types at every depth, not just for the outer object.
        if type(left) is not type(right):
            return False
        if isinstance(left, dict):
            return left.keys() == right.keys() and all(
                same_value(value, right[key]) for key, value in left.items()
            )
        if isinstance(left, list):
            return len(left) == len(right) and all(
                same_value(a, b) for a, b in zip(left, right, strict=True)
            )
        return left == right

    def evaluate(item):
        if item.op == "all":
            return all(evaluate(arg) for arg in item.args)
        if item.op == "any":
            return any(evaluate(arg) for arg in item.args)
        if item.op == "not":
            return not evaluate(item.args[0])
        state = states.get(item.node_id, {})
        value = (
            state.get("status", _MISSING)
            if item.source == "status"
            else (
                output_value(state.get("output"), item.path)
                if state.get("output_validated") is True
                else _MISSING
            )
        )
        if item.op == "exists":
            return value is not _MISSING
        if value is _MISSING:
            raise WorkflowInvalid("Required condition input is unavailable")
        if item.op == "eq":
            return same_value(value, item.value)
        if item.op == "ne":
            return not same_value(value, item.value)
        if item.op == "in":
            return any(same_value(value, other) for other in item.value)
        if type(value) not in {int, float} or type(item.value) not in {int, float}:
            raise WorkflowInvalid("Ordered comparisons require numeric schema-validated values")
        return {
            "gt": lambda: value > item.value,
            "gte": lambda: value >= item.value,
            "lt": lambda: value < item.value,
            "lte": lambda: value <= item.value,
        }[item.op]()

    return evaluate(expression)


def resolve_inputs(node, states):
    result = {}
    for name, definition in node.get("inputs", {}).items():
        reference = InputRef.model_validate(definition)
        state = states.get(reference.node_id, {})
        value = (
            output_value(state.get("output"), reference.path)
            if state.get("output_validated") is True
            else _MISSING
        )
        if value is _MISSING:
            if reference.required:
                raise WorkflowInvalid("Required node input is unavailable")
            value = reference.default
        result[name] = value
    canonical(result, 16_384)
    return result


def node_output(node, text):
    try:
        if not isinstance(text, str) or len(text.encode("utf-8")) > 16_384:
            raise ValueError
    except (ValueError, UnicodeError):
        raise WorkflowInvalid("Node output exceeds its UTF-8 limit") from None
    if node.get("output_schema") is None:
        canonical(text, 16_384)
        return {"output": text, "output_validated": False}

    def unique_pairs(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("Duplicate output key")
            value[key] = item
        return value

    try:
        # Reject parser-dependent structured output, even if its last value
        # would pass schema validation.
        output = json.loads(text, object_pairs_hook=unique_pairs)
    except (ValueError, TypeError, RecursionError):
        raise WorkflowInvalid("Node did not return structured JSON") from None
    validate_schema(node["output_schema"], output)
    return {"output": output, "output_validated": True}


def readiness(compiled, states):
    """Pure decision; caller must atomically commit/recheck before enqueueing."""
    nodes = compiled["nodes"]
    live = [
        node
        for node in nodes
        if states.get(node["node_id"], {}).get("status")
        in {"running", "queued", "waiting_approval", "pausing", "paused"}
    ]
    active = [
        node
        for node in live
        if states[node["node_id"]]["status"] in {"running", "queued", "pausing"}
    ]
    failed = any(
        node["required"] and states.get(node["node_id"], {}).get("status") in FAILED
        for node in nodes
    )
    if failed:
        return {
            "status": "failed",
            "ready": [],
            "skip": [],
            "fail": [],
            "cancel": [node["node_id"] for node in live],
        }
    ready, skipped, errors = [], [], []
    occupied = Counter(node["agent_id"] for node in active)
    slots = max(0, compiled["parallelism"] - len(active))
    for node in nodes:
        key = node["node_id"]
        if states.get(key, {}).get("status", "pending") != "pending":
            continue
        parents = [
            states.get(parent, {}).get("status", "pending") for parent in node["dependencies"]
        ]
        if not all(status in TERMINAL for status in parents):
            continue
        if parents and (
            not all(status == "completed" for status in parents)
            if node["join"] == "all_success"
            else not any(status == "completed" for status in parents)
        ):
            skipped.append(key)
            continue
        try:
            if node["condition"] and not evaluate_condition(node["condition"], states):
                skipped.append(key)
                continue
            resolve_inputs(node, states)
        except WorkflowInvalid:
            errors.append(key)
            continue
        if slots and occupied[node["agent_id"]] < compiled.get("agent_parallelism", 1):
            ready.append(key)
            slots -= 1
            occupied[node["agent_id"]] += 1
    done = all(states.get(node["node_id"], {}).get("status") in TERMINAL for node in nodes)
    if any(node["required"] and node["node_id"] in errors for node in nodes):
        return {
            "status": "failed",
            "ready": [],
            "skip": skipped,
            "fail": errors,
            "cancel": [node["node_id"] for node in live],
        }
    return {
        "status": "completed" if done else "running",
        "ready": ready,
        "skip": skipped,
        "fail": errors,
        "cancel": [],
    }

"""Finite, administrator-owned Skill selection; no inference or regex execution."""

from __future__ import annotations

import hashlib
import json
from typing import Literal

from pydantic import Field, StrictInt, field_validator

from app.core.skill_store import MAX_BOUND_SKILLS, SkillBinding


class SkillSelectionRule(SkillBinding):
    field: Literal["objective", "task_type", "event_type", "tags"]
    values: list[str] = Field(min_length=1, max_length=16)
    priority: StrictInt = Field(default=0, ge=0, le=1_000)

    @field_validator("values")
    @classmethod
    def finite_words(cls, values):
        if any(not value.strip() or len(value.encode("utf-8")) > 128 for value in values):
            raise ValueError("Selection words must contain 1-128 UTF-8 bytes")
        return sorted(set(value.casefold() for value in values))


def selection_candidates(explicit, rules):
    """Validate the worst case at configuration time, not by runtime truncation."""
    fixed = [SkillBinding.model_validate(item).model_dump() for item in explicit]
    parsed = [SkillSelectionRule.model_validate(item) for item in rules]
    if len(parsed) > MAX_BOUND_SKILLS or len({item.skill_id for item in parsed}) != len(parsed):
        raise ValueError("At most eight distinct automatic Skill rules are permitted")
    if len({item["skill_id"] for item in fixed}) != len(fixed):
        raise ValueError("Duplicate explicit Skill binding")
    result = {item["skill_id"]: item for item in fixed}
    for item in parsed:
        result.setdefault(item.skill_id, {"skill_id": item.skill_id, "revision": item.revision})
    if len(result) > MAX_BOUND_SKILLS:
        raise ValueError("Explicit and automatic whitelist exceeds eight Skills")
    return list(result.values())


def select_skills(explicit, rules, task):
    selection_candidates(explicit, rules)
    parsed = [SkillSelectionRule.model_validate(item) for item in rules]
    normalized = [item.model_dump() for item in sorted(parsed, key=lambda item: item.skill_id)]
    digest = hashlib.sha256(
        json.dumps(normalized, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    selected = [SkillBinding.model_validate(item).model_dump() for item in explicit]
    reasons = [{**item, "source": "explicit"} for item in selected]
    ids = {item["skill_id"] for item in selected}
    for rule in sorted(parsed, key=lambda item: (-item.priority, item.skill_id)):
        if rule.skill_id in ids:
            continue
        if rule.field == "event_type":
            value = (task.get("trigger_metadata") or {}).get("event_type", "")
        else:
            value = task.get(rule.field, "")
        if rule.field == "tags":
            matched = isinstance(value, list) and any(
                isinstance(tag, str) and tag.casefold() in rule.values for tag in value
            )
        elif isinstance(value, str):
            matched = (
                any(word in value.casefold() for word in rule.values)
                if rule.field == "objective"
                else value.casefold() in rule.values
            )
        else:
            matched = False
        if matched:
            binding = {"skill_id": rule.skill_id, "revision": rule.revision}
            selected.append(binding)
            ids.add(rule.skill_id)
            reasons.append(
                {**binding, "source": "rule", "field": rule.field, "priority": rule.priority}
            )
    return selected, {"schema_version": 1, "rules_sha256": digest, "selected": reasons}


def plan_skill_selections(snapshot, plan):
    """Freeze execution selections from the already pinned planning whitelist."""
    if plan.get("schema_version") == 2:
        return None  # Native DAG selections live in each compiled node.
    agents = [snapshot.get("root_agent", {}), *snapshot.get("workers", [])]
    if not any(agent.get("skill_selection_rules") for agent in agents):
        return None  # Never change v1 Plan hashes/artifacts merely by upgrading code.
    root_id = snapshot["root_agent"]["agent_id"]
    tasks = {root_id: plan}
    tasks.update({item["agent_id"]: item for item in plan.get("assignments", [])})
    result = []
    for agent in agents:
        if agent["agent_id"] not in tasks:
            continue
        task = {**tasks[agent["agent_id"]], "task_type": "agent"}
        bindings, selection = select_skills(
            agent.get("skills", []), agent.get("skill_selection_rules", []), task
        )
        candidates = {
            item["skill_id"]: item
            for item in agent.get("skill_selection_candidates", agent.get("skill_snapshots", []))
        }
        frozen = []
        for binding in bindings:
            item = candidates.get(binding["skill_id"])
            if not item or item["revision"] != binding["revision"]:
                raise ValueError("Planning Skill selection is outside the pinned whitelist")
            frozen.append(
                {
                    **binding,
                    "content_sha256": item["content_sha256"],
                    "revocation_epoch": item["revocation_epoch"],
                }
            )
        result.append({"agent_id": agent["agent_id"], "bindings": frozen, "selection": selection})
    return {"schema_version": 1, "agents": result}

import re

import pytest

from app.core.builtin_skills import BUILTIN_SKILLS, get_builtin
from app.tools.registry import TOOL_SPECS

EXPECTED_NAMES = {
    "source-summary",
    "action-extraction",
    "option-comparison",
    "evidence-review",
    "investigation-plan",
    "incident-triage",
    "atr-finding-review",
    "handoff-report",
    "web-evidence",
    "chief-coordination",
}


def test_catalog_has_unique_expected_skills():
    names = [skill["name"] for skill in BUILTIN_SKILLS]
    assert len(names) == len(set(names))
    assert set(names) == EXPECTED_NAMES


@pytest.mark.parametrize("skill", BUILTIN_SKILLS, ids=lambda skill: skill["name"])
def test_builtin_metadata_is_bounded_and_compatible(skill):
    assert set(skill) == {
        "name",
        "display_name",
        "description",
        "instructions",
        "required_tools",
        "agent_roles",
        "tags",
        "builtin_version",
    }
    assert re.fullmatch(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*", skill["name"])
    for field in ("display_name", "description", "instructions"):
        assert isinstance(skill[field], str) and skill[field].strip()
    assert 300 <= len(skill["instructions"].encode("utf-8")) <= 800
    assert skill["builtin_version"] == 1
    for field in ("required_tools", "agent_roles", "tags"):
        values = skill[field]
        assert isinstance(values, list)
        assert all(isinstance(value, str) and value.strip() for value in values)
        assert len(values) == len(set(values))
    assert skill["agent_roles"]
    assert skill["tags"]
    assert set(skill["agent_roles"]) <= {"chief", "worker"}
    assert set(skill["required_tools"]) <= set(TOOL_SPECS)
    if "chief" in skill["agent_roles"]:
        assert skill["required_tools"] == []


def test_role_and_tool_specific_catalog_examples():
    chief = get_builtin("chief-coordination")
    assert chief["agent_roles"] == ["chief"]
    assert chief["required_tools"] == []
    web = get_builtin("web-evidence")
    assert web["agent_roles"] == ["worker"]
    assert web["required_tools"] == ["scrape_dynamic_page"]
    for name in EXPECTED_NAMES - {"chief-coordination", "web-evidence"}:
        skill = get_builtin(name)
        assert set(skill["agent_roles"]) == {"chief", "worker"}
        assert skill["required_tools"] == []


@pytest.mark.parametrize("name", sorted(EXPECTED_NAMES))
def test_lookup_returns_independent_records_and_lists(name):
    first = get_builtin(name)
    second = get_builtin(name)
    assert first == second
    assert first is not second
    first["instructions"] = "已變更"
    for field in ("required_tools", "agent_roles", "tags"):
        assert first[field] is not second[field]
        first[field].append("changed")
    assert get_builtin(name) == second
    assert next(skill for skill in BUILTIN_SKILLS if skill["name"] == name) == second


@pytest.mark.parametrize("name", ["unknown", "", "Source-Summary", " source-summary "])
def test_unknown_or_inexact_lookup_returns_none(name):
    assert get_builtin(name) is None

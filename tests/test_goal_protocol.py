from copy import deepcopy
from uuid import uuid4

import pytest

from app.core.goal_protocol import (
    LEGACY_QUEUE,
    WORKFLOW_QUEUE,
    goal_index,
    goal_key,
    goal_queue,
    require_goal_protocol,
    tenants_key,
)


def root():
    return {
        "goal_id": "wfv1-" + str(uuid4()),
        "workflow_protocol": 1,
        "workflow": {"version": 1, "acl_enforced": True},
    }


def test_namespaces_route_by_server_id_without_legacy_fallback():
    value = root()
    require_goal_protocol(value)
    assert goal_queue(value["goal_id"]) == WORKFLOW_QUEUE
    assert goal_key("tenant", value["goal_id"]).startswith("tenant:tenant:workflow:v1:goal:")
    assert goal_index("tenant", value["goal_id"]) == "tenant:tenant:workflow:v1:goals"
    assert tenants_key(value["goal_id"]) == "workflow:v1:tenants"
    assert goal_key("tenant", "legacy") == "tenant:tenant:agent:goal:legacy"
    assert goal_queue("legacy") == LEGACY_QUEUE
    require_goal_protocol({"goal_id": "legacy"})


@pytest.mark.parametrize(
    "field,value",
    [
        ("workflow_protocol", True),
        ("workflow_protocol", 2),
        ("workflow_protocol", "1"),
        ("workflow", {"acl_enforced": "false"}),
        ("workflow", {}),
        ("workflow", "invalid"),
        ("goal_id", "wfv1-" + "-" * 36),
        ("goal_id", "legacy"),
    ],
)
def test_protocol_envelope_is_strict(field, value):
    goal = root() | {field: value}
    with pytest.raises(ValueError):
        require_goal_protocol(goal)


def test_node_identity_cannot_claim_another_root_or_both_execution_kinds():
    parent = root()
    node = {
        "goal_id": "wfv1-" + str(uuid4()),
        "workflow_protocol": 1,
        "parent_goal_id": parent["goal_id"],
        "root_goal_id": parent["goal_id"],
        "workflow_node": {"root_id": parent["goal_id"]},
    }
    require_goal_protocol(node)
    changed = deepcopy(node)
    changed["parent_goal_id"] = "wfv1-" + str(uuid4())
    with pytest.raises(ValueError):
        require_goal_protocol(changed)
    with pytest.raises(ValueError):
        require_goal_protocol(node | {"workflow": parent["workflow"]})

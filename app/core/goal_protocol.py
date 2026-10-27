"""Workflow v1 has a fail-closed namespace; legacy executors cannot find it."""

import re

LEGACY_QUEUE = "agent:goal:queue"
WORKFLOW_QUEUE = "workflow:v1:queue"
WORKFLOW_PREFIX = "wfv1-"
QUEUES = (LEGACY_QUEUE, WORKFLOW_QUEUE)


def is_workflow_id(goal_id):
    return isinstance(goal_id, str) and goal_id.startswith(WORKFLOW_PREFIX)


def goal_key(tenant, goal_id):
    prefix = "workflow:v1:goal" if is_workflow_id(goal_id) else "agent:goal"
    return f"tenant:{tenant}:{prefix}:{goal_id}"


def goal_index(tenant, goal_id):
    suffix = "workflow:v1:goals" if is_workflow_id(goal_id) else "agent:goals"
    return f"tenant:{tenant}:{suffix}"


def tenants_key(goal_id):
    return "workflow:v1:tenants" if is_workflow_id(goal_id) else "agent:goal:tenants"


def goal_queue(goal_id):
    return WORKFLOW_QUEUE if is_workflow_id(goal_id) else LEGACY_QUEUE


def require_goal_protocol(goal):
    """Namespacing is not enough: reject missing/mismatched authority markers."""
    goal_id = goal.get("goal_id")
    workflow = bool(goal.get("workflow") or goal.get("workflow_node"))
    if is_workflow_id(goal_id):
        if (
            not re.fullmatch(r"wfv1-[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", goal_id)
            or not workflow
            or type(goal.get("workflow_protocol")) is not int
            or goal["workflow_protocol"] != 1
        ):
            raise ValueError("Workflow protocol envelope is invalid")
        root, node = goal.get("workflow"), goal.get("workflow_node")
        if bool(root) == bool(node):
            raise ValueError("Workflow envelope must identify one execution kind")
        if root and (not isinstance(root, dict) or type(root.get("acl_enforced")) is not bool):
            raise ValueError("Workflow admission posture is missing or invalid")
        if node and (
            not isinstance(node, dict)
            or not is_workflow_id(node.get("root_id"))
            or node["root_id"] != goal.get("parent_goal_id")
            or node["root_id"] != goal.get("root_goal_id")
        ):
            raise ValueError("Workflow node identity is invalid")
    elif workflow or "workflow_protocol" in goal:
        raise ValueError("Unversioned Workflow requires operator migration; execution denied")

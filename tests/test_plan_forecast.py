from app.core.plan_forecast import _schedule


def node(node_id, agent_id, dependencies=()):
    return {
        "node_id": node_id,
        "agent_id": agent_id,
        "dependencies": list(dependencies),
    }


def test_forecast_scheduler_uses_dag_critical_path_and_agent_capacity():
    parallel = {
        "parallelism": 4,
        "nodes": [
            node("chief-start", "chief"),
            node("worker-a", "a", ["chief-start"]),
            node("worker-b", "b", ["chief-start"]),
            node("chief-summary", "chief", ["worker-a", "worker-b"]),
        ],
    }
    durations = {
        "chief-start": 2.0,
        "worker-a": 5.0,
        "worker-b": 7.0,
        "chief-summary": 3.0,
    }
    included = set(durations)
    assert _schedule(parallel, durations, included) == 12.0

    serialized = parallel | {
        "nodes": [
            node("chief-start", "chief"),
            node("worker-a", "only-worker", ["chief-start"]),
            node("worker-b", "only-worker", ["chief-start"]),
            node("chief-summary", "chief", ["worker-a", "worker-b"]),
        ]
    }
    assert _schedule(serialized, durations, included) == 17.0


def test_forecast_scheduler_can_exclude_conditional_branch_without_naive_sum():
    compiled = {
        "parallelism": 2,
        "nodes": [
            node("root", "a"),
            node("optional", "b", ["root"]),
            node("required", "c", ["root"]),
        ],
    }
    durations = {"root": 2.0, "optional": 100.0, "required": 3.0}
    assert _schedule(compiled, durations, {"root", "required"}) == 5.0
    assert _schedule(compiled, durations, set(durations)) == 102.0

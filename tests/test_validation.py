"""The outer boundary (§3.2, §6).

Everything inside this system is guarded: the claim has rowcount checks, the
sweeps revalidate, the fence rejects what it does not own. The API is where that
discipline meets a caller who is not us, and the failures here all share a shape
— bad input that is ACCEPTED, and then surfaces later as an infrastructure
problem. A job with `max_duration_s: 0` is not an infra error; it is a typo. But
today it starts, the timeout sweep kills it, and the fleet gets the blame in a
record that says `infra_error`, which is exactly the misattribution the
FAILED-versus-INFRA_ERROR split exists to prevent (§4.3).
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

VG = {"product": "vehicle_gateway"}


def post(base: str, path: str, body: dict) -> httpx.Response:
    async def go():
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as client:
            return await client.post(path, json=body)

    return asyncio.run(go())


def submit(base: str, **overrides) -> httpx.Response:
    body = {"name": "probe", "requirements": [dict(VG)]}
    body.update(overrides)
    return post(base, "/v1/jobs", body)


def register(base: str, **overrides) -> httpx.Response:
    body = {
        "agent_id": "bench-01",
        "hostname": "bench-01.local",
        "agent_version": "0.1.0",
        "resources": [{"id": "vg-01", "capabilities": dict(VG)}],
    }
    body.update(overrides)
    return post(base, "/v1/agents/register", body)


# ------------------------------------------------------------------- bounds
@pytest.mark.parametrize("budget", [0, -1, 86_401, 10**9])
def test_an_impossible_budget_is_refused_at_submit(dispatch_server, budget):
    """`max_duration_s: 0` starts, is killed by the next timeout sweep, and is
    recorded as `infra_error` — the fleet blamed for the client's input. A huge
    one pins devices for as long as the caller likes."""
    base, _config = dispatch_server

    assert submit(base, max_duration_s=budget).status_code == 422


def test_a_sane_budget_is_accepted(dispatch_server):
    base, _config = dispatch_server
    assert submit(base, max_duration_s=30).status_code == 201


@pytest.mark.parametrize("priority", [-1, 10**9])
def test_priority_is_bounded(dispatch_server, priority):
    base, _config = dispatch_server
    assert submit(base, priority=priority).status_code == 422


@pytest.mark.parametrize("name", ["", "x" * 201])
def test_a_job_name_must_be_present_and_reasonable(dispatch_server, name):
    base, _config = dispatch_server
    assert submit(base, name=name).status_code == 422


def test_an_out_of_range_epoch_is_rejected_not_a_500(dispatch_server):
    """SQLite raises OverflowError on anything past 2^63-1, which surfaces as a
    500 — an unhandled server error for what is plainly a bad request."""
    base, _config = dispatch_server
    for epoch in (2**63, 2**70, -1):
        response = post(
            base, "/v1/jobs/job-anything/start", {"agent_id": "bench-01", "epoch": epoch}
        )
        assert response.status_code == 422, f"epoch={epoch} gave {response.status_code}"
        response = post(
            base,
            "/v1/jobs/job-anything/complete",
            {"agent_id": "bench-01", "epoch": epoch, "outcome": "passed"},
        )
        assert response.status_code == 422, f"epoch={epoch} gave {response.status_code}"


# ------------------------------------------------------- requirement specs
def test_a_null_requirement_value_is_refused(dispatch_server):
    """`{"product": null}` matches EVERY device that lacks the key, because the
    subset test compares `capabilities.get(k) == v` and `.get` returns None for
    an absent key. A typo'd null silently widens the match to the whole fleet
    instead of narrowing it."""
    base, _config = dispatch_server

    assert submit(base, requirements=[{"product": None}]).status_code == 422


@pytest.mark.parametrize("value", [["a", "b"], {"nested": 1}])
def test_a_non_scalar_requirement_value_is_refused(dispatch_server, value):
    base, _config = dispatch_server
    assert submit(base, requirements=[{"product": value}]).status_code == 422


@pytest.mark.parametrize("value", ["vehicle_gateway", 3, 1.5, True])
def test_scalar_requirement_values_are_fine(dispatch_server, value):
    base, _config = dispatch_server
    assert submit(base, requirements=[{"product": value}]).status_code == 201


# ------------------------------------------------------ registration hygiene
def test_duplicate_device_ids_in_one_inventory_are_refused(dispatch_server):
    """The upsert collapses them last-wins, so a bench that reports two devices
    ends up with one — while the register response and the daemon both go on
    counting two."""
    base, _config = dispatch_server

    response = register(
        base,
        resources=[
            {"id": "vg-01", "capabilities": dict(VG)},
            {"id": "vg-01", "capabilities": {"product": "asset_gateway"}},
        ],
    )

    assert response.status_code == 422


@pytest.mark.parametrize("agent_id", ["", "   ", "bench:01", "bench/01"])
def test_a_malformed_agent_id_is_refused(dispatch_server, agent_id):
    """An empty id registers happily today and then 404s on every heartbeat
    forever, re-registering every three seconds. A `:` misroutes the
    unquarantine CLI, which uses it to tell a device id from a bench id."""
    base, _config = dispatch_server

    assert register(base, agent_id=agent_id).status_code == 422


@pytest.mark.parametrize("device_id", ["", "vg:01", "vg/01", "bench-01:vg-01"])
def test_a_malformed_device_id_is_refused(dispatch_server, device_id):
    """`bench-01:vg-01` reported by agent `bench-01` qualifies to the same row as
    a plain `vg-01` — two physical devices merged into one."""
    base, _config = dispatch_server

    response = register(base, resources=[{"id": device_id, "capabilities": dict(VG)}])

    assert response.status_code == 422


def test_a_hostname_takeover_is_logged(dispatch_server, caplog):
    """Authn is deferred and named as deferred (§1.4) — but one bench claiming
    another's id should leave a trace, not happen in silence."""
    import logging

    base, _config = dispatch_server
    assert register(base).status_code == 200

    with caplog.at_level(logging.WARNING):
        assert register(base, hostname="somewhere-else.local").status_code == 200

    assert any("hostname" in record.message for record in caplog.records), (
        "a bench id changing machines left no trace at all"
    )


# --------------------------------------------------------------- payloads
@pytest.mark.parametrize("duration", ["abc", -1, None, [1]])
def test_a_malformed_payload_duration_is_refused(dispatch_server, duration):
    """It reaches the bench, the executor's `float()` raises outside its own
    try, the task dies silently, and the job burns its whole budget twice before
    dead-lettering as `infra_error` — for a typo in the request."""
    base, _config = dispatch_server

    assert submit(base, payload={"duration_s": duration}).status_code == 422


def test_a_payload_without_a_duration_is_fine(dispatch_server):
    base, _config = dispatch_server
    assert submit(base, payload={"suite": "smoke"}).status_code == 201


def test_the_executor_reports_a_bad_payload_as_failed_not_infra_error():
    """The safety net behind the validation. If a malformed payload ever reaches
    a bench — an older client, a hand-rolled POST — the result is FAILED with a
    reason, which is terminal.

    Not `infra_error`: that would retry the same bad payload on two more benches
    and then blame the fleet for it, which is precisely the misattribution the
    FAILED/INFRA_ERROR split exists to prevent (§4.3).
    """
    from tss.agent.executor import execute

    result = asyncio.run(execute("job-A", ["vg-01"], {"duration_s": "abc"}))

    assert result.outcome == "failed"
    assert "payload" in (result.detail or "")


def test_the_executor_still_runs_a_good_payload():
    from tss.agent.executor import execute

    result = asyncio.run(execute("job-A", ["vg-01"], {"duration_s": 0.01}))

    assert result.outcome == "passed"

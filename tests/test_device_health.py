"""A fault the bench reports against a BUSY device (§4.2).

`report_resource_health` applies `unhealthy` only `WHERE state='free'`, and the
docstring promised the completion path handled the busy case. It did not:
completion freed the device straight back into the pool, so a bench that noticed
a dead J-Link half way through a job was ignored and handed the same device
again on the next pass. The promise is implemented here instead — the report is
remembered on the row and applied the moment the device is released, whichever
path releases it.
"""

from __future__ import annotations

import pytest

from tests.conftest import inventory, submit
from tss.core.invariants import check_all
from tss.core.models import Outcome, ResourceState

T0 = 1_000_000.0
AGENT = "bench-01"
VG = {"product": "vehicle_gateway", "harness": "j1939"}


def busy_bench(store, *, devices=2):
    store.register_agent(AGENT, f"{AGENT}.local", inventory(devices, caps=VG), now=T0)
    submit(store, "job-A", 1, now=T0, caps=VG)
    held = f"{AGENT}:vg-01"
    assert store.claim_all("job-A", AGENT, [held], now=T0).ok
    return held


def test_a_fault_on_a_busy_device_is_remembered_and_applied_on_completion(store):
    held = busy_bench(store)

    store.report_resource_health(AGENT, {"vg-01": "unhealthy"}, now=T0 + 1)
    assert store.get_resource(held).state == ResourceState.BUSY, "it must finish its job first"
    assert store.get_resource(held).fault_reported_at == T0 + 1

    store.complete_job("job-A", AGENT, store.get_job("job-A").epoch, Outcome.PASSED, now=T0 + 2)

    device = store.get_resource(held)
    assert device.state == ResourceState.UNHEALTHY, "the fault evaporated on completion"
    assert device.current_job_id is None
    assert device.fault_reported_at is None, "applied once, not left pending forever"
    assert check_all(store) == []


def test_the_bench_can_withdraw_the_fault_before_the_job_ends(store):
    """The agent is the authority on its own hardware and may change its mind:
    a probe that failed once and passed twice since is not a fault."""
    held = busy_bench(store)
    store.report_resource_health(AGENT, {"vg-01": "unhealthy"}, now=T0 + 1)

    store.report_resource_health(AGENT, {"vg-01": "healthy"}, now=T0 + 2)
    store.complete_job("job-A", AGENT, store.get_job("job-A").epoch, Outcome.PASSED, now=T0 + 3)

    device = store.get_resource(held)
    assert device.state == ResourceState.FREE, "a withdrawn fault must not still land"
    assert device.fault_reported_at is None


def test_a_remembered_fault_survives_a_requeue_not_just_a_completion(store):
    """Whichever path ends the job. A reap is the one that matters most: the
    bench that reported the fault is gone, and the report is all that is left of
    what it knew."""
    held = busy_bench(store)
    store.report_resource_health(AGENT, {"vg-01": "unhealthy"}, now=T0 + 1)

    store.reap_agent(AGENT, now=T0 + 100, reason="presence_expired")

    device = store.get_resource(held)
    assert device.state == ResourceState.UNHEALTHY
    assert device.current_job_id is None, "I5: an offline bench holds nothing"
    assert check_all(store) == []


def test_a_remembered_fault_survives_a_cancel(store):
    held = busy_bench(store)
    store.report_resource_health(AGENT, {"vg-01": "unhealthy"}, now=T0 + 1)

    store.cancel_job("job-A", now=T0 + 2)

    assert store.get_resource(held).state == ResourceState.UNHEALTHY
    assert check_all(store) == []


def test_a_free_device_is_still_marked_immediately(store):
    """The path that already worked must keep working: nothing is deferred that
    can be applied now."""
    store.register_agent(AGENT, f"{AGENT}.local", inventory(2, caps=VG), now=T0)

    store.report_resource_health(AGENT, {"vg-01": "unhealthy"}, now=T0 + 1)

    device = store.get_resource(f"{AGENT}:vg-01")
    assert device.state == ResourceState.UNHEALTHY
    assert device.fault_reported_at is None, "an applied fault is not also pending"


def test_a_fault_never_resurrects_a_retired_device(store):
    """`retired` means gone from the bench. A deferred fault must not be a back
    door into the pool, and neither must it re-state a device that has left."""
    store.register_agent(AGENT, f"{AGENT}.local", inventory(2, caps=VG), now=T0)
    store.register_agent(AGENT, f"{AGENT}.local", inventory(1, caps=VG), now=T0 + 1)
    retired = f"{AGENT}:vg-02"
    assert store.get_resource(retired).state == ResourceState.RETIRED

    store.report_resource_health(AGENT, {"vg-02": "unhealthy"}, now=T0 + 2)

    device = store.get_resource(retired)
    assert device.state == ResourceState.RETIRED
    assert device.fault_reported_at is None, "a retired device has nothing to defer"


def test_a_deferred_fault_does_not_overwrite_tss_s_own_quarantine(store):
    """Two different claims about one device: the bench says "this looks broken",
    TSS says "this has failed three times running". They both end in `unhealthy`,
    but only TSS's needs an operator to clear, and the provenance is
    `quarantined_at`. The agent's report must not erase it."""
    held = busy_bench(store)
    store.report_resource_health(AGENT, {"vg-01": "unhealthy"}, now=T0 + 1)
    for _ in range(store.config.quarantine_threshold):
        store.conn.execute(
            "UPDATE resources SET consecutive_fails = consecutive_fails + 1 WHERE id = ?", (held,)
        )
    store.conn.execute(
        "UPDATE resources SET state = 'unhealthy', quarantined_at = ? WHERE id = ?", (T0, held)
    )
    store.conn.commit()

    store.report_resource_health(AGENT, {"vg-01": "healthy"}, now=T0 + 5)

    device = store.get_resource(held)
    assert device.state == ResourceState.UNHEALTHY, "the bench withdrew TSS's quarantine"
    assert device.quarantined_at == T0


@pytest.mark.parametrize("status", ["healthy", "unhealthy"])
def test_reporting_health_is_idempotent(store, status):
    busy_bench(store)
    first = store.report_resource_health(AGENT, {"vg-01": status}, now=T0 + 1)
    second = store.report_resource_health(AGENT, {"vg-01": status}, now=T0 + 2)

    assert not second, f"a repeated {status} report changed something twice: {first} then {second}"

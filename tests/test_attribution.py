"""Blame, and the evidence it is computed from (§4.2).

Three infra errors on one machine mean something is wrong; WHICH thing is decided
by the SPREAD. Clustered on one device, that device is quarantined and the bench
keeps working on its others. Spread across several devices, the machine is the
common factor and the machine goes out. Conflating them is how one unplugged
cable costs you a third of the fleet.

THE SPREAD IS ONLY MEANINGFUL IF THE EVIDENCE IS FRESH. The verdict is computed
from `resources.consecutive_fails > 0`, and those counters were never cleared when
blame was assigned — only the AGENT's counter was. So a device that failed once
weeks ago, in an incident that was investigated and closed, still counted as part
of the *next* incident's spread. Observed in rehearsal: three fresh errors all on
one device, and the event said "across 2 device(s)", because a neighbour was still
carrying a stale count. The device that had actually failed kept working and a
healthy machine went out.
"""

from __future__ import annotations

import pytest

from tss.core.models import AgentState, InventoryItem, Outcome, ResourceState

T0 = 1_000_000.0
AGENT = "bench-01"
VG = {"product": "vehicle_gateway", "harness": "j1939"}


@pytest.fixture
def bench(store):
    store.register_agent(
        AGENT,
        f"{AGENT}.local",
        [InventoryItem(id=f"vg-{i:02d}", capabilities=dict(VG)) for i in (1, 2, 3)],
        now=T0,
    )
    return [f"{AGENT}:vg-{i:02d}" for i in (1, 2, 3)]


def fail_on(store, device, *, at, tag="x"):
    """One job, run on one device, reporting infra_error.

    Presence is renewed first: the claim revalidates that the bench is online with
    a live lease, and these incidents are deliberately spread across more than one
    PRESENCE_TTL. A bench that is failing jobs is a bench that is very much alive.
    """
    store.renew_presence(AGENT, now=at)
    job_id = f"job-{tag}-{at:.0f}"
    store.submit_job(job_id, job_id, [dict(VG)], now=at)
    claim = store.claim_all(job_id, AGENT, [device], now=at)
    if not claim.ok:
        # The bench stopped accepting work part-way through the incident —
        # quarantined, most likely, which is itself a verdict the test is about.
        # Reported rather than asserted so the verdict assertions do the talking.
        return False
    store.start_job(job_id, AGENT, claim.epoch, now=at)
    store.complete_job(job_id, AGENT, claim.epoch, Outcome.INFRA_ERROR, detail="bad rig", now=at)
    return True


def pass_on(store, device, *, at, tag="ok"):
    """One job that SUCCEEDS. A pass is the only thing that ends a failure streak
    short of a verdict — `consecutive_fails` means consecutive."""
    store.renew_presence(AGENT, now=at)
    job_id = f"job-{tag}-{at:.0f}"
    store.submit_job(job_id, job_id, [dict(VG)], now=at)
    claim = store.claim_all(job_id, AGENT, [device], now=at)
    assert claim.ok, f"could not put a job on {device}"
    store.start_job(job_id, AGENT, claim.epoch, now=at)
    store.complete_job(job_id, AGENT, claim.epoch, Outcome.PASSED, now=at)


def counters(store):
    return {r.id.split(":", 1)[1]: r.consecutive_fails for r in store.list_resources(AGENT)}


def quarantine_events(store):
    return [e for e in store.events() if e.kind in ("agent.quarantined", "resource.quarantined")]


# ------------------------------------------------ (a) the rehearsal repro
def test_a_closed_incident_does_not_convict_the_machine_next_time(store, bench):
    """INCIDENT ONE clustered on vg-01, cleared by an operator. INCIDENT TWO
    clustered on vg-01 again. The second verdict must be about vg-01, not about
    the machine — the only 'spread' available is vg-01's own history."""
    device_a = bench[0]
    for i in range(3):
        fail_on(store, device_a, at=T0 + i, tag="one")
    assert store.get_resource(device_a).state == ResourceState.UNHEALTHY
    assert store.get_agent(AGENT).state != AgentState.QUARANTINED

    store.unquarantine_resource(device_a, now=T0 + 10)
    assert counters(store)["vg-01"] == 0, "unquarantining the device cleared its own count"

    # INCIDENT TWO — three more, all on the same device.
    for i in range(3):
        fail_on(store, device_a, at=T0 + 20 + i, tag="two")

    agent = store.get_agent(AGENT)
    assert agent.state != AgentState.QUARANTINED, (
        "a healthy machine was convicted by one device's history"
    )
    assert store.get_resource(device_a).state == ResourceState.UNHEALTHY
    assert [e.kind for e in quarantine_events(store)] == [
        "resource.quarantined",
        "resource.quarantined",
    ]


def test_blame_resets_every_devices_evidence_not_just_the_agents(store, bench):
    """The mechanism under (a). When the threshold fires, the incident is over and
    its evidence is spent — for the devices as much as for the bench. Leaving the
    device counters set is what lets one incident leak into the next."""
    for i in range(3):
        fail_on(store, bench[0], at=T0 + i)

    assert quarantine_events(store), "the threshold did not fire; the setup is wrong"
    assert store.get_agent(AGENT).consecutive_fails == 0
    # The condemned device keeps the number its verdict was based on — it is on
    # screen beside the word QUARANTINED, and un-quarantining it clears it. Every
    # OTHER device's contribution to the finished streak is spent.
    assert counters(store)["vg-01"] == 3
    assert counters(store)["vg-02"] == 0
    assert counters(store)["vg-03"] == 0


# ------------------------------------- (b) a stale singleton is not spread
def test_a_count_left_over_from_a_finished_streak_is_not_spread(store, bench):
    """THE REHEARSAL REPRO, exactly.

    vg-02 fails once — below any threshold, nothing happens, nobody looks. A job
    then PASSES on vg-03, which ends the bench's failure streak: `consecutive_fails`
    means consecutive, and the bench's counter goes back to zero.

    But vg-02's did not. It sat at 1 with no streak left to belong to, and the next
    time anything went wrong it was read as evidence. Three fresh errors clustered
    on vg-01 — one device, unambiguously — came out as "across 2 device(s)", the
    healthy machine was quarantined, and the device that had actually failed kept
    taking work.
    """
    fail_on(store, bench[1], at=T0, tag="old")
    assert counters(store)["vg-02"] == 1
    assert not quarantine_events(store), "one failure is below the threshold"

    pass_on(store, bench[2], at=T0 + 10)
    assert store.get_agent(AGENT).consecutive_fails == 0, "a pass ends the streak"
    assert counters(store)["vg-02"] == 0, (
        "the streak ended; every device's evidence for it ends too"
    )

    # A genuinely new incident, all of it on one device.
    for i in range(3):
        fail_on(store, bench[0], at=T0 + 100 + i, tag="new")

    assert store.get_agent(AGENT).state != AgentState.QUARANTINED, (
        "a count left over from a finished streak tipped a device fault into a machine fault"
    )
    quarantined = [e for e in quarantine_events(store) if e.kind == "resource.quarantined"]
    assert [e.resource_id for e in quarantined] == [bench[0]]
    assert store.get_resource(bench[1]).state == ResourceState.FREE, "vg-02 was never at fault"


# ------------------------------------------- (c) genuine spread still works
def test_failures_spread_across_devices_still_blame_the_machine(store, bench):
    """The case the split exists for, unchanged: three errors inside ONE incident
    touching two devices. The machine is the common factor."""
    fail_on(store, bench[0], at=T0, tag="s")
    fail_on(store, bench[1], at=T0 + 1, tag="s")
    fail_on(store, bench[0], at=T0 + 2, tag="s")

    agent = store.get_agent(AGENT)
    assert agent.state == AgentState.QUARANTINED
    assert agent.quarantined_at is not None
    event = next(e for e in quarantine_events(store) if e.kind == "agent.quarantined")
    assert sorted(event.detail["devices"]) == sorted(bench[:2])
    assert counters(store) == {"vg-01": 0, "vg-02": 0, "vg-03": 0}


# -------------------------------- unquarantining a bench clears its devices
def test_unquarantining_a_bench_clears_its_devices_counters_too(store, bench):
    """Letting a machine back in while its devices still carry old blame is the
    same staleness one level down: the bench returns at zero and the very next
    failure anywhere on it is judged against counts from before it went out."""
    fail_on(store, bench[0], at=T0, tag="s")
    fail_on(store, bench[1], at=T0 + 1, tag="s")
    fail_on(store, bench[0], at=T0 + 2, tag="s")
    assert store.get_agent(AGENT).state == AgentState.QUARANTINED
    # Simulate evidence accruing again while it is out (a job that was already
    # running reports late), so there is something stale to clear.
    store.conn.execute("UPDATE resources SET consecutive_fails = 2 WHERE id = ?", (bench[2],))
    store.conn.commit()

    store.unquarantine_agent(AGENT, now=T0 + 10)

    assert store.get_agent(AGENT).consecutive_fails == 0
    assert counters(store) == {"vg-01": 0, "vg-02": 0, "vg-03": 0}, (
        "the bench came back clean but its devices did not"
    )


def test_unquarantining_a_bench_does_not_revive_its_devices(store, bench):
    """Clearing the COUNTERS is not clearing the STATES. A device TSS quarantined
    on its own evidence stays out until someone lets that device back in — and a
    retired device is never revived by anything."""
    for i in range(3):
        fail_on(store, bench[0], at=T0 + i)
    assert store.get_resource(bench[0]).state == ResourceState.UNHEALTHY
    store.conn.execute("UPDATE resources SET state = 'retired' WHERE id = ?", (bench[2],))
    store.conn.commit()

    store.unquarantine_agent(AGENT, now=T0 + 10)

    assert store.get_resource(bench[0]).state == ResourceState.UNHEALTHY, (
        "device state is not the bench's to clear"
    )
    assert store.get_resource(bench[0]).quarantined_at is not None
    assert store.get_resource(bench[2]).state == ResourceState.RETIRED

"""The chaos harness and the checks that read agent ground truth (§3.7, §3.8).

Two kinds of test here. Seeded end-to-end runs, which are the merge gate in
miniature; and a NEGATIVE test per new check, which perturbs state and asserts
the checker fires. A checker nobody has watched fail is not evidence — it is a
function that returns an empty list, and so is `lambda: []`.
"""

from __future__ import annotations

import tempfile

import pytest

from tss.chaos import invariants as chaos_invariants
from tss.chaos.mock_agent import AgentTruth, RunningJob
from tss.chaos.profiles import MIXED, fleet_profiles
from tss.chaos.runner import CHAOS_CONFIG, ChaosRun
from tss.core.models import Outcome
from tss.core.store import Store

T0 = 1_000_000.0
VG = {"product": "vehicle_gateway", "harness": "j1939"}
AG = {"product": "asset_gateway"}


def bench_with_job(store, *, agent_id="bench-01", caps=VG, requirement=None):
    from tss.core.models import InventoryItem

    store.register_agent(
        agent_id,
        f"{agent_id}.local",
        [InventoryItem(id="vg-01", capabilities=dict(caps))],
        now=T0,
    )
    store.submit_job("job-A", "job-A", [dict(requirement or VG)], now=T0)
    claim = store.claim_all("job-A", agent_id, [f"{agent_id}:vg-01"], now=T0)
    assert claim.ok
    store.start_job("job-A", agent_id, claim.epoch, now=T0)
    return claim


def truth(agent_id, *, running=(), capabilities=None, alive=True, profile="clean"):
    return AgentTruth(
        agent_id=agent_id,
        profile=profile,
        alive=alive,
        running=tuple(running),
        capabilities=capabilities or {"vg-01": dict(VG)},
    )


# ------------------------------------------------------------------------ I1
def test_i1_accepts_the_zombie_overlap_it_is_supposed_to_accept(store):
    """The zombie sincerely believes it owns the job it is still running. That is
    at-least-once execution working as designed (§7.4), NOT a violation — and an
    invariant your own demo breaks teaches everyone to ignore the checker."""
    claim = bench_with_job(store)
    store.reap_agent("bench-01", now=T0 + 100)  # requeued; epoch moved on
    store.register_agent("bench-03", "b.local", [], now=T0 + 101)
    store.conn.execute("UPDATE jobs SET state='queued' WHERE id='job-A'")
    new_epoch = store.get_job("job-A").epoch

    fleet = [
        # the zombie, still running the old epoch, still convinced
        truth("bench-01", running=[RunningJob("job-A", claim.epoch, ("bench-01:vg-01",), 0.0)]),
        truth("bench-03", running=[RunningJob("job-A", new_epoch, ("bench-03:vg-01",), 0.0)]),
    ]

    assert chaos_invariants.check_i1(store, fleet) == [], (
        "only one of them is AUTHORIZED; both may be executing"
    )


def test_i1_catches_two_agents_authorized_at_the_same_epoch(store):
    """The real violation: two benches holding the CURRENT fence."""
    claim = bench_with_job(store)
    fleet = [
        truth("bench-01", running=[RunningJob("job-A", claim.epoch, ("bench-01:vg-01",), 0.0)]),
        truth("bench-02", running=[RunningJob("job-A", claim.epoch, ("bench-02:vg-01",), 0.0)]),
    ]

    violations = chaos_invariants.check_i1(store, fleet)

    assert len(violations) == 1
    assert "job-A" in violations[0]
    assert "bench-01" in violations[0] and "bench-02" in violations[0]


def test_i1_catches_a_second_accepted_result(store):
    """The half that cannot be missed by sampling: the event log keeps every
    accepted completion forever."""
    claim = bench_with_job(store)
    assert store.complete_job("job-A", "bench-01", claim.epoch, Outcome.PASSED, now=T0 + 1) == (
        "accepted"
    )
    assert chaos_invariants.check_i1(store, []) == []

    # Forge a second acceptance, as a broken fence would produce.
    store.append_event("job.completed", job_id="job-A", agent_id="bench-02", now=T0 + 2)

    violations = chaos_invariants.check_i1(store, [])
    assert len(violations) == 1
    assert "2 results accepted" in violations[0]


# ------------------------------------------------------------------------ I3
def test_i3_is_silent_before_the_deadline(store):
    bench_with_job(store)
    assert chaos_invariants.check_i3(store, submitted=["job-A"], deadline_reached=False) == [], (
        "an in-flight job legitimately has no terminal state"
    )


def test_i3_catches_a_job_that_never_finished(store):
    bench_with_job(store)

    violations = chaos_invariants.check_i3(store, submitted=["job-A"], deadline_reached=True)

    assert len(violations) == 1
    assert "never finished" in violations[0]
    assert "running" in violations[0] and "bench-01" in violations[0], (
        "the message must say where it is stuck"
    )


@pytest.mark.parametrize("terminal", ["passed", "failed", "cancelled", "dead_letter"])
def test_i3_counts_every_terminal_state(store, terminal):
    bench_with_job(store)
    store.conn.execute(
        "UPDATE jobs SET state = ?, outcome = ? WHERE id = 'job-A'",
        (terminal, "infra_error" if terminal == "dead_letter" else terminal),
    )

    assert chaos_invariants.check_i3(store, submitted=["job-A"], deadline_reached=True) == []


# ------------------------------------------------------------------------ I4
def test_i4_reads_the_hardware_not_tss(store):
    """A liar declares a vehicle gateway and has an asset gateway. TSS matched
    against the declaration and is perfectly consistent with itself; only the
    bench knows."""
    bench_with_job(store, caps=VG, requirement=VG)  # TSS thinks it is a j1939 VG
    lying_fleet = [
        truth(
            "bench-01",
            running=[RunningJob("job-A", 1, ("bench-01:vg-01",), 0.0)],
            capabilities={"vg-01": dict(AG)},  # ...it is really an asset gateway
        )
    ]

    violations = chaos_invariants.check_i4(store, lying_fleet)

    assert violations, "checking TSS against itself would have found nothing"
    assert "cannot satisfy it" in violations[0]
    assert "asset_gateway" in violations[0], "the message must show what the hardware really is"


def test_i4_is_quiet_when_the_hardware_matches(store):
    bench_with_job(store, caps=VG, requirement={"harness": "j1939"})
    honest = [
        truth(
            "bench-01",
            running=[RunningJob("job-A", 1, ("bench-01:vg-01",), 0.0)],
            capabilities={"vg-01": dict(VG)},
        )
    ]

    assert chaos_invariants.check_i4(store, honest) == []


def test_i4_checks_history_per_attempt_not_per_job(store):
    """A job that ran on bench-01 and later on bench-02 has devices from both in
    `job_resources`. Checked as one set they are not even co-located — the check
    has to group by attempt."""
    claim = bench_with_job(store)
    store.reap_agent("bench-01", now=T0 + 10)
    from tss.core.models import InventoryItem

    store.register_agent(
        "bench-02", "b.local", [InventoryItem(id="vg-01", capabilities=VG)], now=T0 + 11
    )
    assert store.claim_all("job-A", "bench-02", ["bench-02:vg-01"], now=T0 + 11).ok
    assert len(store.allocation_records("job-A")) == 2

    fleet = [
        truth("bench-01", capabilities={"vg-01": dict(VG)}),
        truth("bench-02", capabilities={"vg-01": dict(VG)}),
    ]
    assert chaos_invariants.check_i4(store, fleet) == []
    assert claim.epoch != store.get_job("job-A").epoch


# ------------------------------------------------------------------- profiles
def test_every_profile_appears_in_a_fifteen_bench_fleet():
    """A profile missing from the gate is a mechanism silently untested."""
    names = {p.name for p in fleet_profiles(15)}

    assert names == {p.name for p, _ in MIXED}
    assert "liar" not in names, "a gate with exceptions is not a gate"
    assert len(fleet_profiles(15)) == 15
    assert fleet_profiles(15) == fleet_profiles(15), "assignment must be deterministic"


def test_a_single_profile_fleet_can_be_requested():
    assert {p.name for p in fleet_profiles(3, "crasher")} == {"crasher"}
    with pytest.raises(ValueError, match="unknown profile"):
        fleet_profiles(3, "nonsense")


# ------------------------------------------------------------- quarantine
def test_repeated_infra_failures_quarantine_the_machine(store):
    """The liar's useful property. Failures spanning SEVERAL devices mean the
    machine is the common factor, so the bench is quarantined rather than TSS
    routing to it forever (§4.2)."""
    from tss.core.models import AgentState, InventoryItem

    store.register_agent(
        "bench-01",
        "b.local",
        [InventoryItem(id=f"vg-{i:02d}", capabilities=VG) for i in (1, 2)],
        now=T0,
    )
    for i in range(3):
        job_id = f"job-{i}"
        store.submit_job(job_id, job_id, [dict(VG)], now=T0)
        device = f"bench-01:vg-{(i % 2) + 1:02d}"
        claim = store.claim_all(job_id, "bench-01", [device], now=T0 + i)
        store.start_job(job_id, "bench-01", claim.epoch, now=T0 + i)
        store.complete_job(
            job_id, "bench-01", claim.epoch, Outcome.INFRA_ERROR, detail="bad rig", now=T0 + i
        )

    agent = store.get_agent("bench-01")
    assert agent.state == AgentState.QUARANTINED
    assert agent.quarantined_at is not None
    assert [e.kind for e in store.events(kind="agent.quarantined")] == ["agent.quarantined"]
    # ...and a quarantined bench is not offered work.
    assert [a.id for a in store.online_agents(now=T0 + 5)] == []


def test_repeated_failures_on_one_device_quarantine_that_device_only(store):
    """One dead J-Link costs you one device, not the bench."""
    from tss.core.models import AgentState, InventoryItem, ResourceState

    store.register_agent(
        "bench-01",
        "b.local",
        [InventoryItem(id=f"vg-{i:02d}", capabilities=VG) for i in (1, 2)],
        now=T0,
    )
    for i in range(3):
        job_id = f"job-{i}"
        store.submit_job(job_id, job_id, [dict(VG)], now=T0)
        claim = store.claim_all(job_id, "bench-01", ["bench-01:vg-01"], now=T0 + i)
        store.start_job(job_id, "bench-01", claim.epoch, now=T0 + i)
        store.complete_job(job_id, "bench-01", claim.epoch, Outcome.INFRA_ERROR, now=T0 + i)

    assert store.get_resource("bench-01:vg-01").state == ResourceState.UNHEALTHY
    assert store.get_resource("bench-01:vg-02").state == ResourceState.FREE
    assert store.get_agent("bench-01").state == AgentState.ONLINE, "the machine is fine"


def test_a_firmware_failure_never_blames_the_hardware(store):
    """FAILED is the engineer's problem and says nothing about the rig (§4.3).
    Counting it towards quarantine would take good benches out of rotation for
    bad firmware."""
    from tss.core.models import AgentState, InventoryItem

    store.register_agent(
        "bench-01", "b.local", [InventoryItem(id="vg-01", capabilities=VG)], now=T0
    )
    for i in range(5):
        job_id = f"job-{i}"
        store.submit_job(job_id, job_id, [dict(VG)], now=T0)
        claim = store.claim_all(job_id, "bench-01", ["bench-01:vg-01"], now=T0 + i)
        store.start_job(job_id, "bench-01", claim.epoch, now=T0 + i)
        store.complete_job(job_id, "bench-01", claim.epoch, Outcome.FAILED, now=T0 + i)

    assert store.get_agent("bench-01").state == AgentState.ONLINE
    assert store.get_resource("bench-01:vg-01").consecutive_fails == 0


# --------------------------------------------------------------- end to end
@pytest.mark.slow
def test_a_seeded_chaos_run_holds_every_invariant():
    """The merge gate in miniature: a mixed fleet, multi-device jobs, real
    crashes, and every invariant checked continuously throughout."""
    with tempfile.TemporaryDirectory() as tmp:
        report = ChaosRun(
            seed=99,
            agents=8,
            jobs=25,
            multi_pct=30,
            profile_mix="mixed",
            db_path=f"{tmp}/chaos.db",
            deadline_s=90.0,
        ).execute()

    assert report.violations == [], "\n".join(report.violations)
    assert report.unfinished == [], "I3: " + "\n".join(report.unfinished)
    assert report.jobs_submitted == 25
    assert report.safety_checks > 10, "the watchdog barely ran"
    # ...and the chaos actually happened. A run where nothing broke proves nothing.
    assert report.events.get("offline", 0) > 0, "no bench ever died"
    assert report.events.get("requeued", 0) > 0, "no job was ever taken back"


@pytest.mark.slow
def test_the_liar_is_caught_by_ground_truth_and_then_quarantined():
    """`liar` is out of the gate mix because it violates I4 BY CONSTRUCTION —
    that is the profile working, not TSS failing. Its own test asserts the two
    things that matter: the checker sees through the lie (which it could not do
    from TSS's database), and the fleet stops routing to the bad bench."""
    with tempfile.TemporaryDirectory() as tmp:
        db = f"{tmp}/liar.db"
        report = ChaosRun(
            seed=5,
            agents=3,
            jobs=12,
            multi_pct=0,
            profile_mix="liar",
            db_path=db,
            deadline_s=60.0,
            # One product across the whole fleet, so every job has a bench that
            # *claims* to be able to run it. Otherwise the jobs die unsatisfiable
            # and the lie is never even reached.
            products=({"product": "vehicle_gateway", "harness": "j1939"},),
        ).execute()

        assert report.violations, "the liar was not caught — the checker is reading TSS's copy"
        assert all(v.startswith("I4") for v in report.violations), (
            f"only I4 should fire for a liar: {report.violations}"
        )

        store = Store(db, CHAOS_CONFIG)
        quarantined = store.events(kind="agent.quarantined")
        assert quarantined, "TSS kept routing to benches that fail every job"
        # Every job still reached a conclusion rather than bouncing forever.
        assert report.unfinished == [], "I3: " + "\n".join(report.unfinished)
        assert set(report.outcomes) <= {"dead_letter", "passed", "infra_error"}
        store.close()


def test_the_runner_says_so_when_it_covers_less_than_it_claims():
    """No silent caps. A deadline reached with work outstanding must be loud in
    the summary, not a quietly smaller number."""
    with tempfile.TemporaryDirectory() as tmp:
        report = ChaosRun(
            seed=3,
            agents=1,
            jobs=40,
            multi_pct=0,
            profile_mix="hung",  # nothing will ever finish
            db_path=f"{tmp}/stuck.db",
            deadline_s=3.0,
        ).execute()

    assert report.deadline_hit
    assert any("deadline" in note for note in report.notes)
    assert not report.ok, "a truncated run must not report success"
    assert "FAILED" in report.render()
    assert str(report.seed) in report.render(), "the seed must be in the failure output"


def test_the_report_names_what_to_replay():
    report = ChaosRun(
        seed=1234, agents=1, jobs=1, multi_pct=0, profile_mix="clean", db_path=":memory:"
    ).report
    report.violations = ["I2: bench-01:vg-01 is held by ['job-a', 'job-b']"]

    rendered = report.render()

    assert "seed=1234" in rendered
    assert "just chaos-seed 1234" in rendered
    assert "I2:" in rendered

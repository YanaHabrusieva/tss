"""Reservations: where multi-device gets dangerous (§3.4.1, I9).

A job needing three free VGs on one bench can wait forever while a stream of
single-VG jobs nibbles capacity the instant it frees. Without a guard, big jobs
never run on a busy fleet.

THE DISTINCTION THE WHOLE FILE RESTS ON — reserve is not claim:

                        claim                 reserve
    resource state      busy                  stays FREE
    owner               the job               nobody
    where it lives      one SQL transaction   scheduler memory
    survives restart    yes                   no, and doesn't need to
    can deadlock        -                     no

A job that needs three devices and can see one free DOES NOT TAKE IT. It takes
nothing; the scheduler merely declines to offer that device to anyone else. That
is wait-without-hold, so no cycle can form. `tests/naive_reservation.py` is the
version that takes the device, and it deadlocks — that test is at the bottom.
"""

from __future__ import annotations

import time

import pytest

from tests.conftest import inventory
from tss.core.invariants import check_all, check_i9
from tss.core.models import InventoryItem, JobState, Outcome, ResourceState
from tss.core.scheduler import Scheduler
from tss.core.store import BLOCKED_NO_CAPABLE_AGENT

T0 = 1_000_000.0
VG = {"product": "vehicle_gateway", "harness": "j1939"}
OBD2 = {"product": "vehicle_gateway", "harness": "obd2"}
ANY_VG = {"product": "vehicle_gateway"}


@pytest.fixture
def scheduler(store, config):
    return Scheduler(store, config)


def bench(store, agent_id, *, devices=2, caps=None, now=T0):
    items = [
        InventoryItem(id=f"vg-{i:02d}", capabilities=dict(caps or VG))
        for i in range(1, devices + 1)
    ]
    store.register_agent(agent_id, f"{agent_id}.local", items, agent_version="0.1.0", now=now)
    return [f"{agent_id}:{item.id}" for item in items]


def submit(store, job_id, n, *, at=T0, caps=None):
    return store.submit_job(job_id, job_id, [dict(caps or VG)] * n, now=at)


def occupy(store, agent_id, resource_ids, *, at=T0):
    """Fill devices with a filler job, so the fleet starts busy."""
    filler = f"filler-{agent_id}-{len(resource_ids)}"
    store.submit_job(filler, filler, [dict(VG)] * len(resource_ids), now=at)
    assert store.claim_all(filler, agent_id, resource_ids, now=at).ok
    return filler


STARVED = 61.0  # just past STARVATION_THRESHOLD (60s)


def tick(scheduler, store, now):
    """Every bench heartbeats, then the scheduler makes a pass.

    The heartbeats matter: STARVATION_THRESHOLD (60s) is five times PRESENCE_TTL
    (12s), so a bench that registered and then went quiet would be reaped long
    before any job counts as starving. A fleet that is starving a job is a fleet
    that is very much alive.
    """
    for agent in store.agents():
        store.renew_presence(agent.id, now=now)
    return scheduler.pass_once(now=now)


# --------------------------------------------------------- the starving job
def test_a_big_job_eventually_runs_under_a_stream_of_small_ones(store, scheduler, config):
    """(a) from §8: a 3-device job under a stream of 1-device jobs eventually runs.

    Without reservation the small jobs take each device the instant it frees and
    the big one waits forever — the failure is not a crash, it is a queue that
    never moves for one class of job.
    """
    devices = bench(store, "bench-01", devices=3)
    filler = occupy(store, "bench-01", devices)
    submit(store, "job-big", 3, at=T0)

    now = T0 + STARVED
    for round_number in range(6):
        # A fresh single-device job arrives every round, as fast as devices free.
        submit(store, f"job-small-{round_number}", 1, at=now)
        if round_number == 0:
            store.complete_job(filler, "bench-01", 1, Outcome.PASSED, now=now)
        tick(scheduler, store, now)
        now += 1

    job = store.get_job("job-big")
    assert job.state == JobState.ASSIGNED, "the big job never got its bench"
    assert len(store.resources_held_by("job-big")) == 3
    assert check_all(store, scheduler) == []
    # ...and the small jobs that arrived after it are still waiting their turn.
    assert any(j.id.startswith("job-small") for j in store.queued_jobs())


def test_while_it_reserves_other_benches_keep_flowing(store, scheduler, config):
    """(b) from §8, and THE ONE THAT CATCHES AN UNSCOPED GUARD.

    Blocking every small job while a big one starves is easy and wrong: it idles
    benches the starving job could never have used. Throughput collapses and it
    reads as a scheduler bug rather than the policy decision it is.
    """
    # bench-01 runs j1939 gateways, bench-02 runs obd2 ones. The big job needs
    # two j1939s, so bench-02 can never satisfy it — but the small jobs, which
    # ask only for a vehicle gateway, are happy on either.
    reserved_bench = bench(store, "bench-01", devices=2)
    bench(store, "bench-02", devices=2, caps=OBD2)
    occupy(store, "bench-01", reserved_bench[:1])  # one device busy, one free
    submit(store, "job-big", 2, at=T0)
    for i in range(2):
        submit(store, f"job-small-{i}", 1, at=T0 + 10, caps=ANY_VG)

    results = tick(scheduler, store, T0 + STARVED)

    assert scheduler.reservation is not None
    assert scheduler.reservation.job_id == "job-big"
    assert scheduler.reservation.agent_id == "bench-01"
    # The free device on bench-01 is withheld...
    assert scheduler.reservation.resource_ids == frozenset(reserved_bench[1:])
    assert store.get_resource(reserved_bench[1]).state == ResourceState.FREE, (
        "a reserved device is FREE and owned by nobody — reserve is not claim"
    )
    assert store.get_resource(reserved_bench[1]).current_job_id is None
    # ...while bench-02 keeps taking work.
    assert len(results) == 2, "unrelated benches must keep flowing"
    assert {r.agent_id for r in results} == {"bench-02"}
    assert check_all(store, scheduler) == []


def test_exactly_one_job_reserves_at_a_time(store, scheduler, config):
    """(c): two starving jobs, one reserver. Two reservers each holding partial
    sets is a deadlock you built yourself, in bookkeeping instead of hardware."""
    devices = bench(store, "bench-01", devices=3)
    occupy(store, "bench-01", devices[:2])
    submit(store, "job-big-a", 3, at=T0)
    submit(store, "job-big-b", 3, at=T0 + 1)

    tick(scheduler, store, T0 + STARVED)

    assert scheduler.reservation is not None
    assert scheduler.reservation.job_id == "job-big-a", "always the oldest starving job"
    assert scheduler.reservation_for("job-big-b") is None
    assert check_i9(scheduler) == []


def test_the_reservation_follows_the_fleet(store, scheduler, config):
    """Recomputed every pass, so if another bench frees a full set first, the job
    takes that instead. A reservation never stops a job claiming a set it can
    actually get."""
    bench(store, "bench-01", devices=2)
    second = bench(store, "bench-02", devices=2)
    occupy(store, "bench-01", ["bench-01:vg-01"])
    occupy(store, "bench-02", second)
    submit(store, "job-big", 2, at=T0)

    tick(scheduler, store, T0 + STARVED)
    assert scheduler.reservation.agent_id == "bench-01", "closest to satisfying it"

    # bench-02 frees a complete set first.
    store.complete_job("filler-bench-02-2", "bench-02", 1, Outcome.PASSED, now=T0 + STARVED + 1)
    tick(scheduler, store, T0 + STARVED + 2)

    assert store.get_job("job-big").state == JobState.ASSIGNED
    assert sorted(store.resources_held_by("job-big")) == second
    assert scheduler.reservation is None, "nothing is starving any more"


# ------------------------------------------------------------- the release
def test_a_reservation_is_released_on_the_very_next_pass(store, scheduler, config):
    """(d): the instant it stops being needed, and not a pass later.

    A stale reservation idles hardware silently — the same shape as every latency
    bug this project has hit. Asserted by pass count, not by "eventually".
    """
    devices = bench(store, "bench-01", devices=2)
    occupy(store, "bench-01", devices[:1])
    submit(store, "job-big", 2, at=T0)
    submit(store, "job-small", 1, at=T0 + 1)

    tick(scheduler, store, T0 + STARVED)
    assert scheduler.reservation is not None
    assert store.get_job("job-small").state == JobState.QUEUED, "held back by the reservation"
    withheld = set(scheduler.reservation.resource_ids)

    # The starving job is cancelled: its reason to withhold anything is gone.
    store.cancel_job("job-big", now=T0 + STARVED + 1)
    results = tick(scheduler, store, T0 + STARVED + 2)

    assert scheduler.reservation is None
    assert len(results) == 1, "the withheld device is offered on the NEXT pass, not later"
    assert set(store.resources_held_by("job-small")) == withheld
    assert check_all(store, scheduler) == []


def test_dispatching_the_reserving_job_releases_the_rest(store, scheduler, config):
    """Four devices, two of them busy. The 3-device job cannot run, so the two
    free ones are withheld and the small job waits. When the filler finishes,
    the big job takes three and the fourth goes straight to the small job — in
    the same pass, because the reservation is gone the moment it is not needed."""
    devices = bench(store, "bench-01", devices=4)
    occupy(store, "bench-01", devices[:2])
    submit(store, "job-big", 3, at=T0)
    submit(store, "job-small", 1, at=T0 + 1)

    tick(scheduler, store, T0 + STARVED)
    assert scheduler.reservation is not None
    assert len(scheduler.reservation.resource_ids) == 2, "both free devices withheld"
    assert store.get_job("job-small").state == JobState.QUEUED

    # The filler finishes; the big job takes its set and the reservation ends.
    store.complete_job("filler-bench-01-2", "bench-01", 1, Outcome.PASSED, now=T0 + STARVED + 1)
    tick(scheduler, store, T0 + STARVED + 2)

    assert store.get_job("job-big").state == JobState.ASSIGNED
    assert scheduler.reservation is None
    assert store.get_job("job-small").state == JobState.ASSIGNED, (
        "the leftover device went to the small job in the same pass"
    )
    assert check_all(store, scheduler) == []


# --------------------------------------------------------- nothing to reserve
def test_a_job_no_bench_could_ever_run_reserves_nothing(store, scheduler, config):
    """(e), and the case that looks like a bug and isn't (§3.4.1).

    Two healthy VGs on the only bench, and a job wanting three. Reserving there
    idles a device forever, for nothing — and silently reserving toward an
    impossible job is indistinguishable from a broken scheduler.
    """
    bench(store, "bench-01", devices=2)
    bench(store, "bench-02", devices=2)
    submit(store, "job-impossible", 3, at=T0)

    tick(scheduler, store, T0 + STARVED)

    assert scheduler.reservation is None, "there is nothing to reserve toward"
    job = store.get_job("job-impossible")
    assert job.state == JobState.QUEUED, "kept queued — fleets get repaired"
    assert job.blocked_reason == BLOCKED_NO_CAPABLE_AGENT
    assert [e.kind for e in store.events(job_id="job-impossible")] == ["job.unsatisfiable"]
    assert all(r.state == ResourceState.FREE for r in store.list_resources())

    # Said once, not once per pass.
    tick(scheduler, store, T0 + STARVED + 1)
    assert len(store.events(job_id="job-impossible")) == 1


def test_an_unsatisfiable_job_runs_once_the_fleet_is_repaired(store, scheduler, config):
    """Why it stays queued: a bench gets fixed, un-quarantined, or added."""
    bench(store, "bench-01", devices=2)
    submit(store, "job-big", 3, at=T0)
    tick(scheduler, store, T0 + STARVED)
    assert store.get_job("job-big").blocked_reason == BLOCKED_NO_CAPABLE_AGENT

    # A third device is cabled to the bench and the agent re-registers.
    store.register_agent("bench-01", "bench-01.local", inventory(3), now=T0 + STARVED + 1)
    tick(scheduler, store, T0 + STARVED + 2)

    job = store.get_job("job-big")
    assert job.state == JobState.ASSIGNED
    assert job.blocked_reason is None, "the flag is cleared when it stops being true"


def test_an_unsatisfiable_job_dead_letters_after_the_timeout(store, scheduler, config):
    bench(store, "bench-01", devices=1)
    submit(store, "job-impossible", 3, at=T0)

    tick(scheduler, store, T0 + STARVED)
    assert store.get_job("job-impossible").state == JobState.QUEUED

    tick(scheduler, store, T0 + config.unsatisfiable_timeout_s + 1)

    job = store.get_job("job-impossible")
    assert job.state == JobState.DEAD_LETTER
    assert job.outcome == Outcome.INFRA_ERROR, "never outcome='dead_letter' — that is a state"
    assert job.result_detail.startswith(BLOCKED_NO_CAPABLE_AGENT)
    assert check_all(store, scheduler) == []


def test_an_unhealthy_device_can_make_a_bench_infeasible(store, scheduler, config):
    """Feasibility is about HEALTHY installed inventory: three devices, one of
    them broken, cannot satisfy a 3-device job."""
    bench(store, "bench-01", devices=3)
    store.report_resource_health("bench-01", {"vg-03": "unhealthy"}, now=T0)
    submit(store, "job-big", 3, at=T0)

    tick(scheduler, store, T0 + STARVED)

    assert scheduler.reservation is None
    assert store.get_job("job-big").blocked_reason == BLOCKED_NO_CAPABLE_AGENT


# ------------------------------------------------------------- I9 has teeth
def test_i9_catches_a_reservation_that_took_hardware(store, scheduler, config):
    """A checker nobody has seen fail is not evidence. Reach in, mark a reserved
    device busy, and I9 must say so — because that is the exact moment a
    reservation has turned into the partial hold."""
    devices = bench(store, "bench-01", devices=2)
    occupy(store, "bench-01", devices[:1])
    submit(store, "job-big", 2, at=T0)
    tick(scheduler, store, T0 + STARVED)
    assert check_i9(scheduler) == []

    reserved = next(iter(scheduler.reservation.resource_ids))
    submit(store, "job-thief", 1, at=T0)
    store.conn.execute(
        "UPDATE resources SET state = 'busy', current_job_id = 'job-thief' WHERE id = ?",
        (reserved,),
    )

    violations = check_i9(scheduler)

    assert len(violations) == 1
    assert "claimed by job-thief" in violations[0]
    assert "not by the reserving job job-big" in violations[0]


def test_i9_does_not_cry_wolf_when_the_reserving_job_takes_its_own_set(store, scheduler, config):
    """The reservation succeeding is not a violation, and a fleet that moves
    under a sampled checker is not one either. A checker that reports its own
    staleness gets ignored, which costs you every real violation after it."""
    devices = bench(store, "bench-01", devices=2)
    occupy(store, "bench-01", devices[:1])
    submit(store, "job-big", 2, at=T0)
    tick(scheduler, store, T0 + STARVED)
    reserved = next(iter(scheduler.reservation.resource_ids))

    # The reserving job gets its set...
    store.conn.execute(
        "UPDATE resources SET state = 'busy', current_job_id = 'job-big' WHERE id = ?",
        (reserved,),
    )
    assert check_i9(scheduler) == []

    # ...and a device going unhealthy under a stale reservation is the fleet
    # changing, not the scheduler misbehaving: the next pass recomputes.
    store.conn.execute(
        "UPDATE resources SET state = 'unhealthy', current_job_id = NULL WHERE id = ?",
        (reserved,),
    )
    assert check_i9(scheduler) == []


def test_i9_catches_a_reservation_on_a_bench_that_could_never_satisfy_it(store, scheduler, config):
    devices = bench(store, "bench-01", devices=2)
    occupy(store, "bench-01", devices[:1])
    submit(store, "job-big", 2, at=T0)
    tick(scheduler, store, T0 + STARVED)

    # A device is unplugged: the target can no longer ever satisfy the job.
    store.register_agent(
        "bench-01",
        "bench-01.local",
        [InventoryItem(id="vg-01", capabilities=VG)],
        now=T0 + STARVED + 1,
    )

    violations = check_i9(scheduler)
    assert any("could never satisfy" in v for v in violations)


# ------------------------------------------------- the reserve-by-claim foil
def test_reserving_by_claiming_deadlocks(store, config):
    """§7.5, in bookkeeping: two 2-device jobs, two devices, one each, forever.

    This is what `tests/naive_reservation.py` does — it takes the device rather
    than withholding it. Nothing errors and no invariant in the database fires:
    the jobs are still `queued`, so I8 has nothing to say, and the devices are
    busy with an owner that will never start. The fleet just stops.
    """
    from tests.naive_reservation import ClaimingReservationScheduler

    devices = bench(store, "bench-01", devices=2)
    filler = occupy(store, "bench-01", devices)
    submit(store, "job-a", 2, at=T0)
    submit(store, "job-b", 2, at=T0 + 1)

    naive = ClaimingReservationScheduler(store, config)
    now = T0 + STARVED
    # One device frees, then the other — as they do in a real fleet.
    store.complete_job(filler, "bench-01", 1, Outcome.PASSED, now=now)
    tick(naive, store, now)
    tick(naive, store, now + 1)
    for _ in range(5):
        now += 1
        tick(naive, store, now)

    held = {j: store.resources_held_by(j) for j in ("job-a", "job-b")}
    assert len(held["job-a"]) == 1 and len(held["job-b"]) == 1, (
        f"expected the classic partial hold, got {held}"
    )
    assert store.get_job("job-a").state == JobState.QUEUED
    assert store.get_job("job-b").state == JobState.QUEUED
    assert held["job-a"] != held["job-b"], "each holds a device the other needs — deadlock"

    # And the real scheduler, same fleet, same jobs: one of them runs.
    for job_id in ("job-a", "job-b"):
        store.conn.execute(
            "UPDATE resources SET state = 'free', current_job_id = NULL WHERE current_job_id = ?",
            (job_id,),
        )
    real = Scheduler(store, config)
    tick(real, store, now + 10)

    states = {j: store.get_job(j).state for j in ("job-a", "job-b")}
    assert JobState.ASSIGNED in states.values(), f"nobody ran: {states}"
    assert sorted(states.values()) == [JobState.ASSIGNED, JobState.QUEUED]
    assert check_all(store, real) == []


def test_the_real_reservation_writes_nothing_to_the_database(store, scheduler, config):
    """The property that makes a crash mid-wait cost nothing: there is no trace
    to clean up, and no schema column to migrate."""
    devices = bench(store, "bench-01", devices=2)
    occupy(store, "bench-01", devices[:1])
    submit(store, "job-big", 2, at=T0)

    before = store.conn.execute("SELECT * FROM resources ORDER BY id").fetchall()
    events_before = len(store.events())
    tick(scheduler, store, T0 + STARVED)
    after = store.conn.execute("SELECT * FROM resources ORDER BY id").fetchall()

    assert scheduler.reservation is not None
    assert [dict(r) for r in before] == [dict(r) for r in after]
    assert len(store.events()) == events_before, "a reservation is not an event either"

    # A restart loses it, and that is fine: the next pass recomputes it.
    fresh = Scheduler(store, config)
    assert fresh.reservation is None
    tick(fresh, store, T0 + STARVED + 1)
    assert fresh.reservation is not None
    assert fresh.reservation.job_id == "job-big"


def test_reservation_state_survives_a_pass_with_nothing_queued(store, scheduler, config):
    """Housekeeping: an empty queue clears the reservation rather than leaving a
    stale one pointing at a job that has gone."""
    devices = bench(store, "bench-01", devices=2)
    occupy(store, "bench-01", devices[:1])
    submit(store, "job-big", 2, at=T0)
    tick(scheduler, store, T0 + STARVED)
    assert scheduler.reservation is not None

    store.cancel_job("job-big", now=T0 + STARVED + 1)
    tick(scheduler, store, T0 + STARVED + 2)

    assert scheduler.reservation is None
    assert check_i9(scheduler) == []


def test_timing_the_release_over_a_running_scheduler(store, db_path, config):
    """(d) again, with a clock: the withheld device must be offered promptly, not
    on some later sweep. Measured against a scheduler loop whose backstop tick is
    turned off, so only the notify path can produce this."""
    import asyncio
    import dataclasses

    devices = bench(store, "bench-01", devices=2, now=time.time())
    occupy(store, "bench-01", devices[:1], at=time.time())
    started = time.time()
    submit(store, "job-big", 2, at=started - 3600)  # long since starving
    submit(store, "job-small", 1, at=started - 3600)

    loop_config = dataclasses.replace(config, scheduler_tick_s=3600.0)
    scheduler = Scheduler(store, loop_config)

    async def drive():
        scheduler.start()
        scheduler.notify()
        await asyncio.sleep(0.2)
        assert scheduler.reservation is not None, "job-big should be reserving"
        assert store.get_job("job-small").state == JobState.QUEUED

        began = time.monotonic()
        store.cancel_job("job-big")
        scheduler.notify()
        while time.monotonic() - began < 2:
            if store.get_job("job-small").state != JobState.QUEUED:
                return time.monotonic() - began
            await asyncio.sleep(0.01)
        return None

    elapsed = asyncio.run(drive())

    assert elapsed is not None, "the withheld device was never offered to anyone else"
    assert elapsed < 0.5, f"took {elapsed:.2f}s — that is a stale reservation idling hardware"

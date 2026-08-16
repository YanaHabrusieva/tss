"""I2 and I8 under real contention: 50 OS threads racing for overlapping sets.

WHY THREADS AND NOT ASYNCIO TASKS (§8). With async tasks over the synchronous
`sqlite3` driver there is no preemption point between the check and the act, so a
naive check-then-act claim passes and the test proves nothing. Real OS threads on
*separate connections* is what makes the race real — and it is what makes
`just test-naive` fail, which is the only evidence these tests are worth having.

WHY OVERLAPPING PAIRS AND NOT ONE SHARED DEVICE (§8). Fifty threads fighting over
a single resource only tests double-booking. Fifty threads claiming overlapping
*pairs* from a pool of three also tests partial allocation — a thread that gets
one device and loses the other must end up holding neither.

WHERE DOUBLE-BOOKING IS CHECKED. Not in `resources.current_job_id`: that is a
single column, so a second claimer silently overwrites the first and the final
state looks clean. `job_resources` is append-only, so if two jobs ever believed
they held one device, the record survives.
"""

from __future__ import annotations

import itertools
import os
import random
import threading
from collections import defaultdict

import pytest

from tests.conftest import make_bench, submit
from tss.core.config import Config
from tss.core.models import ResourceState
from tss.core.store import REASON_DB_BUSY, Store

THREADS = 50
SEED = int(os.environ.get("TSS_TEST_SEED", "1729"))
AGENT = "bench-sf-01"

pytestmark = pytest.mark.concurrency


def _race(db_path, agent_id, picks, claim):
    """Fire every claim at once and collect what each thread believed happened."""
    results, readback = {}, {}
    lock = threading.Lock()
    barrier = threading.Barrier(len(picks))

    def worker(job_id, resource_ids):
        thread_store = Store(db_path)  # this thread's own sqlite3 connection
        try:
            barrier.wait(timeout=30)
            result = claim(thread_store, job_id, agent_id, list(resource_ids))
            owners = None
            if result.ok:
                # Read our own devices back straight away. If someone can still
                # take one out from under us, it shows here — the final state
                # would not necessarily show it.
                owners = {
                    rid: thread_store.get_resource(rid).current_job_id
                    for rid in result.resource_ids
                }
            with lock:
                results[job_id] = result
                readback[job_id] = owners
        finally:
            thread_store.close()

    threads = [
        threading.Thread(target=worker, args=(job_id, rids), name=job_id, daemon=True)
        for job_id, rids in picks.items()
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    stuck = [t.name for t in threads if t.is_alive()]
    assert not stuck, f"claim threads never finished: {stuck}"
    assert len(results) == len(picks), "a claim thread died before reporting"
    return results, readback


def _assert_invariants(store, agent_id, results, readback, *, per_job):
    winners = sorted(j for j, r in results.items() if r.ok)
    losers = sorted(j for j, r in results.items() if not r.ok)

    # I2, from the claimants' own point of view: two threads must never both come
    # back believing they hold the same device.
    believed: dict[str, set[str]] = defaultdict(set)
    for job_id in winners:
        for rid in results[job_id].resource_ids:
            believed[rid].add(job_id)
    conflicts = {r: sorted(js) for r, js in believed.items() if len(js) > 1}
    assert not conflicts, f"I2 violated — two jobs were told they hold one device: {conflicts}"

    # I2 again, from the durable record — `job_resources` is append-only, so
    # unlike `resources.current_job_id` it cannot be quietly overwritten.
    holders: dict[str, set[str]] = defaultdict(set)
    for rec in store.allocation_records():
        if rec["released_at"] is None:
            holders[rec["resource_id"]].add(rec["job_id"])
    double_booked = {r: sorted(js) for r, js in holders.items() if len(js) > 1}
    assert not double_booked, f"I2 violated — one device, two allocations: {double_booked}"

    # No device is held by a job that did not win it: a thread that takes one
    # device and loses the next must leave nothing behind.
    orphans = {
        r.id: r.current_job_id
        for r in store.list_resources()
        if r.current_job_id is not None and r.current_job_id not in winners
    }
    assert not orphans, f"device held by a job that lost its claim: {orphans}"

    # The live column agrees with the allocation history.
    for rid, owning in holders.items():
        resource = store.get_resource(rid)
        assert resource.current_job_id == next(iter(owning)), rid
        assert resource.state == ResourceState.BUSY, rid

    # I8 — every job that claims anything claims exactly what it required.
    for job_id in winners:
        held = store.resources_held_by(job_id)
        records = store.allocation_records(job_id)
        assert len(held) == per_job, f"I8 violated — {job_id} holds {held}"
        assert len(records) == per_job, f"I8 violated — {job_id} recorded {records}"
        assert set(held) == set(results[job_id].resource_ids)
        assert readback[job_id] == dict.fromkeys(held, job_id), (
            f"{job_id} lost a device it had already claimed: {readback[job_id]}"
        )
        job = store.get_job(job_id)
        assert job.state == "assigned"
        assert job.agent_id == agent_id
        assert job.epoch == 1, "the epoch is bumped exactly once per assignment"
        assert job.attempt == 1
        assert job.tried_agents == [agent_id]

    # A loser holds nothing: no partial set, no residue, no bookkeeping.
    for job_id in losers:
        assert store.resources_held_by(job_id) == [], f"{job_id} lost but holds devices"
        assert store.allocation_records(job_id) == []
        job = store.get_job(job_id)
        assert job.state == "queued"
        assert (job.epoch, job.attempt, job.tried_agents, job.agent_id) == (0, 0, [], None)

    return winners, losers


def test_fifty_threads_claiming_overlapping_pairs(store, db_path, claim):
    """Three devices on one bench, 50 threads each claiming a random pair.

    Three devices cannot satisfy two 2-device jobs, so exactly one thread may win
    — no more (that would be a double-book) and no fewer (that would mean the
    claim path locked itself out entirely).
    """
    resources = make_bench(store, AGENT, ["vg-01", "vg-02", "vg-03"])
    pairs = list(itertools.combinations(resources, 2))
    rng = random.Random(SEED)
    print(f"\nseed={SEED}")

    picks = {}
    for i in range(THREADS):
        job_id = f"job-{i:02d}"
        submit(store, job_id, 2)
        pair = list(rng.choice(pairs))
        rng.shuffle(pair)  # the caller's order must not matter; the store sorts
        picks[job_id] = pair

    results, readback = _race(db_path, AGENT, picks, claim)
    winners, _ = _assert_invariants(store, AGENT, results, readback, per_job=2)

    assert len(winners) == 1, f"3 devices cannot host {len(winners)} 2-device jobs"
    busy = [r for r in store.list_resources() if r.state == ResourceState.BUSY]
    assert len(busy) == 2
    assert len(store.events(kind="job.assigned")) == 1


def test_a_contended_write_lock_is_a_lost_race_not_an_error(store, db_path, claim):
    """The honest caveat about SQLite (§3.3).

    With BEGIN IMMEDIATE a *second connection* does not get rowcount 0 — it blocks
    for `busy_timeout` and then raises SQLITE_BUSY. The rowcount guard defends
    against two logical passes racing; this defends against two connections. You
    need both, and in the tests above only the first one ever fires, because a 5s
    busy timeout means contenders wait for the lock instead of failing.

    Deterministic here: one connection holds the write lock, the claimant has no
    patience at all.
    """
    resources = make_bench(store, AGENT, ["vg-01", "vg-02"])
    submit(store, "job-2dev", 2)

    impatient = Store(db_path, Config(busy_timeout_ms=0))
    holder = Store(db_path).conn
    holder.execute("BEGIN IMMEDIATE")  # the write lock is taken and not given back
    try:
        result = claim(impatient, "job-2dev", AGENT, resources)
    finally:
        holder.execute("ROLLBACK")
        holder.close()

    assert not result.ok, "a contended lock must not look like a successful claim"
    assert result.reason == REASON_DB_BUSY
    assert store.resources_held_by("job-2dev") == []
    assert store.get_job("job-2dev").epoch == 0

    # ...and the job is simply still claimable once the lock is free.
    assert claim(impatient, "job-2dev", AGENT, resources).ok
    impatient.close()


def test_fifty_threads_on_a_six_device_bench(store, db_path, claim):
    """Same race with room for more than one winner — several claims commit
    concurrently and the disjointness still has to hold."""
    devices = [f"vg-{i:02d}" for i in range(1, 7)]
    resources = make_bench(store, AGENT, devices)
    pairs = list(itertools.combinations(resources, 2))
    rng = random.Random(SEED + 1)
    print(f"\nseed={SEED + 1}")

    picks = {}
    for i in range(THREADS):
        job_id = f"job-{i:02d}"
        submit(store, job_id, 2)
        pair = list(rng.choice(pairs))
        rng.shuffle(pair)
        picks[job_id] = pair

    results, readback = _race(db_path, AGENT, picks, claim)
    winners, _ = _assert_invariants(store, AGENT, results, readback, per_job=2)

    assert 1 <= len(winners) <= 3, f"6 devices, 2 per job: {len(winners)} winners"
    busy = [r for r in store.list_resources() if r.state == ResourceState.BUSY]
    assert len(busy) == 2 * len(winners)
    assert len(store.events(kind="job.assigned")) == len(winners)

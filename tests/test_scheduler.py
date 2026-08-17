"""The scheduler: who gets what, and the wakeup that must not be lost (§3.4, §7.3).

The scheduler decides and calls `store.claim_all`; it writes no SQL of its own.
These tests drive `pass_once()` directly where they can, because a scheduling
decision is worth asserting on without a service in the way.
"""

from __future__ import annotations

import asyncio
import dataclasses
import threading
import time

import pytest

from tests.conftest import DEVICE_CAPS, inventory, submit
from tss.core.invariants import check_all, check_i8
from tss.core.models import InventoryItem, JobState, ResourceState
from tss.core.scheduler import Scheduler
from tss.core.store import Store

T0 = 1_000_000.0
AG_CAPS = {"product": "asset_gateway", "hw_rev": "A"}


@pytest.fixture
def scheduler(store, config):
    return Scheduler(store, config)


def bench(store, agent_id="bench-sf-01", *, devices=3, caps=None, now=T0):
    items = (
        inventory(devices, caps=caps)
        if caps is None
        else [InventoryItem(id=f"vg-{i:02d}", capabilities=caps) for i in range(1, devices + 1)]
    )
    store.register_agent(agent_id, f"{agent_id}.local", items, agent_version="0.1.0", now=now)
    return [f"{agent_id}:{item.id}" for item in items]


# ------------------------------------------------------ §1.2, the whole point
def test_two_jobs_run_side_by_side_on_one_bench(store, scheduler):
    """A bench is not the unit of allocation. Two single-device jobs on a
    3-device bench both start; if one waits, the agent is being treated as one
    indivisible thing and two thirds of the fleet is idle by construction."""
    devices = bench(store, devices=3)
    submit(store, "job-A", 1, now=T0)
    submit(store, "job-B", 1, now=T0)

    results = scheduler.pass_once(now=T0 + 1)

    assert len(results) == 2, "both jobs must be assigned in the same pass"
    assert all(r.ok for r in results)
    assert store.get_job("job-A").state == JobState.ASSIGNED
    assert store.get_job("job-B").state == JobState.ASSIGNED
    assert store.get_job("job-A").agent_id == store.get_job("job-B").agent_id == "bench-sf-01"
    held = {r.id for r in store.list_resources() if r.state == ResourceState.BUSY}
    assert len(held) == 2, "two devices busy, two jobs, one bench"
    assert len(set(devices) - held) == 1, "and the third device is still free"
    assert check_all(store) == []


def test_a_bench_fills_up_and_then_stops(store, scheduler):
    bench(store, devices=2)
    for i in range(4):
        submit(store, f"job-{i}", 1, now=T0)

    results = scheduler.pass_once(now=T0 + 1)

    assert len(results) == 2, "two devices, two jobs, the rest wait"
    assert len(store.queued_jobs()) == 2
    assert check_i8(store) == []


# ---------------------------------------------------------------------- LRU
def test_the_scheduler_picks_the_least_recently_used_device(store, scheduler):
    devices = bench(store, devices=3)
    # vg-01 has just run something; vg-02 ran long ago; vg-03 never has.
    store.conn.execute("UPDATE resources SET last_assigned_at = 900 WHERE id = ?", (devices[0],))
    store.conn.execute("UPDATE resources SET last_assigned_at = 100 WHERE id = ?", (devices[1],))
    submit(store, "job-A", 1, now=T0)

    scheduler.pass_once(now=T0 + 1)

    assert store.resources_held_by("job-A") == [devices[2]], "never-used device goes first"


def test_the_scheduler_spreads_jobs_across_benches(store, scheduler):
    """Successive jobs should not pile onto one bench while its siblings idle."""
    bench(store, "bench-a", devices=1)
    bench(store, "bench-b", devices=1)
    submit(store, "job-A", 1, now=T0)
    scheduler.pass_once(now=T0 + 1)
    submit(store, "job-B", 1, now=T0)
    scheduler.pass_once(now=T0 + 2)

    assert store.get_job("job-A").agent_id != store.get_job("job-B").agent_id


# ------------------------------------------------------------ what it skips
def test_an_unsatisfiable_job_does_not_block_the_queue_behind_it(store, scheduler):
    """§1.3's utilization target: a bench with a free compatible device is never
    idle while a matching job waits. A literal head-of-line stop would idle the
    whole fleet behind one job asking for hardware nobody has free."""
    bench(store, devices=1)  # vehicle gateways only
    store.submit_job("job-needs-ag", "ag test", [AG_CAPS], now=T0)
    submit(store, "job-needs-vg", 1, now=T0)

    results = scheduler.pass_once(now=T0 + 1)

    assert [r.job_id for r in results] == ["job-needs-vg"]
    assert store.get_job("job-needs-ag").state == JobState.QUEUED


def test_offline_and_draining_benches_are_not_offered_work(store, scheduler, config):
    bench(store, "bench-online", devices=1)
    bench(store, "bench-draining", devices=1)
    bench(store, "bench-expired", devices=1, now=T0 - config.presence_ttl_s - 10)
    store.conn.execute("UPDATE agents SET state = 'draining' WHERE id = 'bench-draining'")
    for i in range(3):
        submit(store, f"job-{i}", 1, now=T0)

    results = scheduler.pass_once(now=T0 + 1)

    assert len(results) == 1
    assert results[0].agent_id == "bench-online", (
        "draining finishes what it has; an expired lease is about to be reaped"
    )


def test_unhealthy_and_retired_devices_are_never_offered(store, scheduler):
    bench(store, devices=3)
    store.report_resource_health("bench-sf-01", {"vg-01": "unhealthy"}, now=T0 + 1)
    store.register_agent(  # vg-03 vanishes from the inventory
        "bench-sf-01",
        "bench-sf-01.local",
        [InventoryItem(id=n, capabilities=DEVICE_CAPS) for n in ("vg-01", "vg-02")],
        agent_version="0.1.0",
        now=T0 + 2,
    )
    store.report_resource_health("bench-sf-01", {"vg-01": "unhealthy"}, now=T0 + 3)
    for i in range(3):
        submit(store, f"job-{i}", 1, now=T0)

    results = scheduler.pass_once(now=T0 + 4)

    assert len(results) == 1, "only vg-02 is offerable"
    assert store.resources_held_by(results[0].job_id) == ["bench-sf-01:vg-02"]


def test_a_lost_claim_falls_through_to_the_next_bench(store, scheduler, monkeypatch):
    """§3.4 step 5: on a rowcount-0 or SQLITE_BUSY, try the next agent rather
    than dropping the job for this pass."""
    bench(store, "bench-a", devices=1)
    bench(store, "bench-b", devices=1)
    submit(store, "job-A", 1, now=T0)

    real_claim = store.claim_all
    calls = []

    def flaky_claim(job_id, agent_id, resource_ids, **kw):
        calls.append(agent_id)
        if len(calls) == 1:
            # Someone else got there first.
            store.conn.execute(
                "UPDATE resources SET state = 'busy', current_job_id = NULL WHERE agent_id = ?",
                (agent_id,),
            )
        return real_claim(job_id, agent_id, resource_ids, **kw)

    monkeypatch.setattr(store, "claim_all", flaky_claim)
    results = scheduler.pass_once(now=T0 + 1)

    assert len(calls) == 2, "it must try the second bench"
    assert len(results) == 1
    assert results[0].ok


# ------------------------------------------------------------- the lost wakeup
class _FreeDuringPass(Scheduler):
    """Frees a device from ANOTHER THREAD while a pass is in flight.

    The point is a genuinely concurrent free — a sequential "free then notify"
    cannot reproduce the race, because the whole bug is that the notify lands
    while the pass that will clear it is already running.
    """

    def __init__(self, *args, resource_id: str, job_id: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.resource_id = resource_id
        self.job_id = job_id
        self.freed = threading.Event()

    def pass_once(self, *, now=None):
        results = super().pass_once(now=now)
        if not self.freed.is_set():
            self.freed.set()
            releaser = threading.Thread(target=self._release_from_another_connection)
            releaser.start()
            releaser.join()  # the commit is visible before this pass returns
            self.notify()
        return results

    def _release_from_another_connection(self):
        other = Store(self.store.path, self.config)
        other.complete_job(self.job_id, "bench-sf-01", 1, "passed")
        other.close()


@pytest.fixture
def wakeup_scheduler_class(request):
    """`TSS_SCHEDULER_IMPL=naive` swaps in the clear-after-pass loop."""
    import os

    if os.environ.get("TSS_SCHEDULER_IMPL") == "naive":
        from tests.naive_scheduler import NaiveScheduler

        class NaiveFreeDuringPass(_FreeDuringPass, NaiveScheduler):
            pass

        return NaiveFreeDuringPass
    return _FreeDuringPass


def test_a_device_freed_during_a_pass_is_not_a_lost_wakeup(
    store, db_path, config, wakeup_scheduler_class
):
    """The queue must not stall with a free device and a queued job.

    The 1s backstop tick is turned OFF here on purpose: with it on, a lost wakeup
    just delays dispatch by a second and both the correct and the broken loop
    pass. Only the notify path can wake this scheduler, so a wakeup dropped
    during a pass means the job never runs.
    """
    # Real wall-clock here, not the synthetic T0: this test runs the scheduler
    # loop for real, and the loop reads the presence lease against time.time().
    now = time.time()
    devices = bench(store, devices=1, now=now)
    submit(store, "job-A", 1, now=T0)
    submit(store, "job-B", 1, now=T0)
    assert store.claim_all("job-A", "bench-sf-01", devices[:1], now=now).ok
    assert store.start_job("job-A", "bench-sf-01", 1, now=now) == "started"

    no_backstop = dataclasses.replace(config, scheduler_tick_s=3600.0)
    scheduler = wakeup_scheduler_class(store, no_backstop, resource_id=devices[0], job_id="job-A")

    async def drive():
        scheduler.start()
        scheduler.notify()  # job-B was submitted
        for _ in range(50):  # 5s of real time, ~5000x the work involved
            await asyncio.sleep(0.1)
            if store.get_job("job-B").state != JobState.QUEUED:
                return True
        return False

    scheduled = asyncio.run(drive())

    assert scheduler.freed.is_set(), "the test did not actually free anything mid-pass"
    assert scheduled, (
        "job-B never ran: the device came free during a pass and that wakeup was dropped"
    )
    assert store.resources_held_by("job-B") == devices[:1]
    assert check_all(store) == []

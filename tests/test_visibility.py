"""A change nobody can see has not happened yet (§3.6, §3.9).

The live view is push-driven: the WebSocket sends a snapshot on connect, then
event lines as they happen, and a coalesced snapshot behind each burst. That
works exactly as far as the events go — and two things that visibly change the
queue were writing none.

  * **Submitting a job.** The store inserted a row and nothing else. On a quiet
    fleet the terminal said `queued — position 1` while the page said `queue
    empty`, until some unrelated event happened along and dragged a snapshot with
    it. The first thing a new user does was the thing least likely to show up.
  * **Placing a reservation.** It lives in scheduler memory by design and leaves
    no database trace, so it left no trace on the bus either: `RESERVING on
    bench-01` appeared only when something else fired.

Both now write an event. The reservation still lives only in memory — the event
records that it happened, it is not the reservation.
"""

from __future__ import annotations

import asyncio
import json
import time

import httpx
import pytest
import websockets

from tests.conftest import submit
from tss.core.scheduler import Scheduler

T0 = 1_000_000.0
VG = {"product": "vehicle_gateway", "harness": "j1939"}
ANY_VG = {"product": "vehicle_gateway"}


@pytest.fixture
def scheduler(store, config):
    return Scheduler(store, config)


async def _collect(socket, *, seconds: float) -> list[dict]:
    """Everything the socket says within a window, snapshot included."""
    messages: list[dict] = []
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            remaining = deadline - time.monotonic()
            messages.append(json.loads(await asyncio.wait_for(socket.recv(), timeout=remaining)))
        except TimeoutError:
            break
    return messages


def test_a_submitted_job_appears_without_waiting_for_unrelated_activity(dispatch_server):
    """THE REPRO. A quiet fleet — no benches, nothing running, nothing to reap —
    and one submit. The page must show the job, not `queue empty` until something
    else happens to it."""
    base, _config = dispatch_server
    ws_url = base.replace("http://", "ws://")

    async def scenario():
        async with (
            httpx.AsyncClient(base_url=base, timeout=20.0) as client,
            websockets.connect(f"{ws_url}/v1/events") as socket,
        ):
            first = json.loads(await socket.recv())
            assert first["type"] == "snapshot"
            assert first["queue"]["queued"] == [], "the fleet was supposed to be quiet"

            created = await client.post(
                "/v1/jobs", json={"name": "lonely", "requirements": [dict(ANY_VG)]}
            )
            assert created.status_code == 201
            return created.json()["job_id"], await _collect(socket, seconds=4.0)

    job_id, messages = asyncio.run(scenario())

    kinds = [m["event"]["kind"] for m in messages if m["type"] == "event"]
    assert "job.submitted" in kinds, f"submitting wrote no event; the bus saw {kinds}"

    snapshots = [m for m in messages if m["type"] == "snapshot"]
    assert snapshots, "no snapshot was pushed, so the page still says `queue empty`"
    queued = snapshots[-1]["queue"]["queued"]
    assert [j["job_id"] for j in queued] == [job_id]
    assert queued[0]["name"] == "lonely"


def test_the_submitted_event_carries_the_feasibility_verdict(dispatch_server):
    """The verdict is computed at the door anyway, to answer the submitter. Not
    putting it on the bus would mean the feed shows an ordinary-looking arrival
    for a job nothing in the fleet can run."""
    base, _config = dispatch_server
    ws_url = base.replace("http://", "ws://")

    async def scenario():
        async with (
            httpx.AsyncClient(base_url=base, timeout=20.0) as client,
            websockets.connect(f"{ws_url}/v1/events") as socket,
        ):
            await socket.recv()
            await client.post(
                "/v1/jobs",
                json={"name": "impossible", "requirements": [dict(ANY_VG), {"product": "nope"}]},
            )
            return await _collect(socket, seconds=4.0)

    events = [m["event"] for m in asyncio.run(scenario()) if m["type"] == "event"]
    submitted = [e for e in events if e["kind"] == "job.submitted"]

    assert len(submitted) == 1
    detail = submitted[0]["detail"]
    assert detail["name"] == "impossible"
    assert detail["feasible"] is False
    assert detail["reason"], "an infeasible submit must say what is missing"


def test_a_feasible_submit_says_so(dispatch_server):
    base, _config = dispatch_server
    ws_url = base.replace("http://", "ws://")

    async def scenario():
        async with (
            httpx.AsyncClient(base_url=base, timeout=20.0) as client,
            websockets.connect(f"{ws_url}/v1/events") as socket,
        ):
            await client.post(
                "/v1/agents/register",
                json={
                    "agent_id": "bench-01",
                    "hostname": "bench-01.local",
                    "agent_version": "0.1.0",
                    "resources": [{"id": "vg-01", "capabilities": dict(VG)}],
                },
            )
            await socket.recv()
            await client.post("/v1/jobs", json={"name": "fine", "requirements": [dict(ANY_VG)]})
            return await _collect(socket, seconds=4.0)

    events = [m["event"] for m in asyncio.run(scenario()) if m["type"] == "event"]
    submitted = [e for e in events if e["kind"] == "job.submitted"]

    assert submitted, "no job.submitted event"
    assert submitted[0]["detail"]["feasible"] is True
    assert submitted[0]["detail"].get("reason") is None


# ------------------------------------------------------------- reservations
def bench(store, agent_id, *, devices=2, caps=None, now=T0):
    from tss.core.models import InventoryItem

    items = [
        InventoryItem(id=f"vg-{i:02d}", capabilities=dict(caps or VG))
        for i in range(1, devices + 1)
    ]
    store.register_agent(agent_id, f"{agent_id}.local", items, agent_version="0.1.0", now=now)
    return [f"{agent_id}:{item.id}" for item in items]


def tick(scheduler, store, now):
    for agent in store.agents():
        store.renew_presence(agent.id, now=now)
    return scheduler.pass_once(now=now)


def reserving_events(store):
    return [e for e in store.events() if e.kind == "job.reserving"]


def test_placing_a_reservation_is_announced(store, scheduler, config):
    """It leaves no database trace on purpose — so without an event it leaves no
    trace on the bus either, and `RESERVING on bench-01` reaches the page only
    when something unrelated happens."""
    devices = bench(store, "bench-01", devices=2)
    filler = "filler"
    store.submit_job(filler, filler, [dict(VG)], now=T0)
    assert store.claim_all(filler, "bench-01", devices[:1], now=T0).ok
    submit(store, "job-big", 2, now=T0, caps=VG)

    tick(scheduler, store, T0 + config.starvation_threshold_s + 1)

    assert scheduler.reservation is not None, "the setup did not produce a reservation"
    events = reserving_events(store)
    assert len(events) == 1, f"expected exactly one announcement, got {len(events)}"
    assert events[0].job_id == "job-big"
    assert events[0].agent_id == "bench-01"
    assert events[0].detail["reserving"] is True
    assert events[0].detail["resource_ids"] == devices[1:], "it must name the device it is holding"


def test_a_steady_reservation_is_not_re_announced_every_pass(store, scheduler, config):
    """The feed drowns otherwise: the scheduler recomputes the reservation from
    scratch on EVERY pass, several times a second under load, and re-announcing a
    state that has not changed would bury every event that has."""
    devices = bench(store, "bench-01", devices=2)
    store.submit_job("filler", "filler", [dict(VG)], now=T0)
    assert store.claim_all("filler", "bench-01", devices[:1], now=T0).ok
    submit(store, "job-big", 2, now=T0, caps=VG)

    for i in range(6):
        tick(scheduler, store, T0 + config.starvation_threshold_s + 1 + i)

    assert scheduler.reservation is not None
    assert len(reserving_events(store)) == 1, "one transition, one event — not one per pass"


def test_the_end_of_a_reservation_is_announced_too(store, scheduler, config):
    """Stopping is a transition. A page that heard the start and never the end
    shows a reservation that no longer exists."""
    devices = bench(store, "bench-01", devices=2)
    store.submit_job("filler", "filler", [dict(VG)], now=T0)
    assert store.claim_all("filler", "bench-01", devices[:1], now=T0).ok
    submit(store, "job-big", 2, now=T0, caps=VG)
    started = T0 + config.starvation_threshold_s + 1
    tick(scheduler, store, started)
    assert len(reserving_events(store)) == 1

    # The filler finishes, the big job takes both devices, nothing is starving.
    store.complete_job("filler", "bench-01", store.get_job("filler").epoch, "passed", now=started)
    tick(scheduler, store, started + 1)

    assert scheduler.reservation is None
    events = reserving_events(store)
    assert len(events) == 2, "the end of a reservation was never announced"
    assert events[1].detail["reserving"] is False
    assert events[1].job_id == "job-big"


def test_moving_the_reservation_to_another_bench_is_a_transition(store, scheduler, config):
    """Same job, different target: the page must be told, because `RESERVING on
    bench-01` is now the wrong sentence.

    A THREE-device job, deliberately. With a two-device job any bench that scores
    better already has both devices free — so the job stops starving and simply
    dispatches, and there is no move to observe. Three devices leaves room for one
    bench to be a better target than another while neither can run it yet.
    """
    first = bench(store, "bench-01", devices=3)
    store.submit_job("filler-a", "filler-a", [dict(VG)] * 2, now=T0)
    assert store.claim_all("filler-a", "bench-01", first[:2], now=T0).ok
    submit(store, "job-big", 3, now=T0, caps=VG)
    started = T0 + config.starvation_threshold_s + 1
    tick(scheduler, store, started)
    assert scheduler.reservation is not None
    assert scheduler.reservation.agent_id == "bench-01", "one free device, one candidate"

    # A second bench with TWO free matching devices: a better target for a job
    # that needs three, and still unable to run it.
    second = bench(store, "bench-02", devices=3, now=started)
    store.submit_job("filler-b", "filler-b", [dict(VG)], now=started)
    assert store.claim_all("filler-b", "bench-02", second[:1], now=started).ok
    tick(scheduler, store, started + 1)

    assert scheduler.reservation is not None, "the job can still not run anywhere"
    assert scheduler.reservation.agent_id == "bench-02", "bench-02 is closer to satisfying it"
    events = reserving_events(store)
    assert len(events) == 2, "the move was never announced"
    assert events[1].agent_id == "bench-02"
    assert events[1].detail["reserving"] is True
    assert events[1].detail["resource_ids"] == second[1:], "it names the devices now held"

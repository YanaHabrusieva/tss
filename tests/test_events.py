"""The event bus, the WebSocket, and `tss why` (§3.6, §3.9).

The property worth guarding here is the one that is invisible when it breaks: the
durable `events` row is written inside its state change's transaction, and
publication happens only AFTER that transaction commits. Get it backwards and the
live stream announces things the audit log never records — and the audit log is
what answers "why did my job move?" a week later.
"""

from __future__ import annotations

import asyncio
import io
import json
import sqlite3
import time

import httpx
import pytest
import websockets

from tests.conftest import DEVICE_CAPS, RunningAgent, inventory, submit
from tss.core.events import EventBus
from tss.core.models import Event, Outcome

T0 = 1_000_000.0
AGENT = "bench-sf-01"


@pytest.fixture
def bus():
    return EventBus()


def collect(bus: EventBus) -> list[Event]:
    seen: list[Event] = []
    bus._subscribers.add(_Recorder(seen))  # noqa: SLF001 - test seam
    return seen


class _Recorder:
    """Stands in for a subscriber queue, so publication can be asserted without
    an event loop."""

    def __init__(self, sink: list[Event]) -> None:
        self.sink = sink

    def put_nowait(self, event: Event) -> None:
        self.sink.append(event)


# ------------------------------------------------------------ after commit
def test_events_are_published_only_after_the_transaction_commits(store, bus):
    published = collect(bus)
    store.publish = bus.publish_all
    store.register_agent(AGENT, "b.local", inventory(2), now=T0)
    submit(store, "job-A", 1)
    published.clear()

    result = store.claim_all("job-A", AGENT, [f"{AGENT}:vg-01"], now=T0)

    assert result.ok
    assert [e.kind for e in published] == ["job.assigned"]
    # ...and what was published is exactly what was persisted.
    assert [e.kind for e in store.events(job_id="job-A")] == ["job.assigned"]


def test_a_rolled_back_transaction_publishes_nothing(store, bus):
    """The whole reason publication waits: a claim that loses its race writes an
    event row inside a transaction that never commits. Announcing it would put a
    job.assigned on the live stream that the audit log has never heard of."""
    published = collect(bus)
    store.publish = bus.publish_all
    store.register_agent(AGENT, "b.local", inventory(2), now=T0)
    submit(store, "job-A", 1)
    submit(store, "job-B", 1)
    assert store.claim_all("job-A", AGENT, [f"{AGENT}:vg-01"], now=T0).ok
    published.clear()

    lost = store.claim_all("job-B", AGENT, [f"{AGENT}:vg-01"], now=T0)

    assert not lost.ok
    assert published == [], "a lost race announced itself"
    assert store.events(job_id="job-B") == []


def test_a_failed_transaction_publishes_nothing(store, bus):
    published = collect(bus)
    store.publish = bus.publish_all
    store.register_agent(AGENT, "b.local", inventory(1), now=T0)
    submit(store, "job-A", 1)
    store.conn.execute("DROP TABLE job_resources")
    published.clear()

    with pytest.raises(sqlite3.OperationalError, match="job_resources"):
        store.claim_all("job-A", AGENT, [f"{AGENT}:vg-01"], now=T0)

    assert published == []


def test_a_slow_subscriber_never_blocks_a_publisher(bus):
    """A wedged WebSocket must not be able to stall a scheduling pass. It loses
    events instead, and recovers by re-snapshotting when it reconnects."""

    async def scenario():
        bus.bind()
        async with bus.subscription() as queue:
            for i in range(600):  # more than QUEUE_SIZE
                bus.publish(Event(ts=float(i), kind="job.assigned"))
            return queue.qsize()

    size = asyncio.run(scenario())

    assert size == 512
    assert bus.dropped > 0, "the overflow should be reported, not silent"
    assert bus.published == 600


def test_events_stop_when_nobody_is_listening(bus):
    bus.publish(Event(ts=1.0, kind="job.assigned"))
    assert bus.published == 0, "no subscribers, no work"


# --------------------------------------------------------- over the socket
def test_the_websocket_sends_a_snapshot_before_any_deltas(dispatch_server, db_path):
    """A stream-only client starts blank and fills in as things happen, which is
    exactly wrong for a fleet view: a bench quietly running two jobs and emitting
    nothing would simply not appear."""
    base, _config = dispatch_server
    ws_url = base.replace("http://", "ws://")

    async def scenario():
        async with (
            httpx.AsyncClient(base_url=base, timeout=20.0) as client,
            RunningAgent(base, count=2),
        ):
            deadline = time.monotonic() + 10
            while not (await client.get("/v1/fleet")).json()["agents"]:
                assert time.monotonic() < deadline
                await asyncio.sleep(0.02)

            async with websockets.connect(f"{ws_url}/v1/events") as socket:
                first = json.loads(await socket.recv())
                # A job submitted AFTER connecting must arrive as a delta.
                submitted = await client.post(
                    "/v1/jobs",
                    json={
                        "name": "ws",
                        "requirements": [{"product": "vehicle_gateway"}],
                        "payload": {"duration_s": 0.1},
                    },
                )
                job_id = submitted.json()["job_id"]
                kinds, snapshots = [], 0
                began = time.monotonic()
                while time.monotonic() - began < 6:
                    message = json.loads(await asyncio.wait_for(socket.recv(), timeout=5))
                    if message["type"] == "snapshot":
                        snapshots += 1
                    else:
                        kinds.append(message["event"]["kind"])
                    if "job.completed" in kinds:
                        break
                return first, kinds, snapshots, job_id

    first, kinds, snapshots, job_id = asyncio.run(scenario())

    assert first["type"] == "snapshot"
    assert len(first["fleet"]["agents"]) == 1, "the bench that was already there is in frame one"
    assert first["queue"]["queued"] == [] and first["queue"]["running"] == []
    assert "job.assigned" in kinds and "job.completed" in kinds
    assert snapshots >= 1, "state frames follow the deltas, coalesced"


def test_the_websocket_survives_a_client_that_walks_away(dispatch_server):
    """It will be closed by a terminal being closed, every time."""
    base, _config = dispatch_server
    ws_url = base.replace("http://", "ws://")

    async def scenario():
        async with websockets.connect(f"{ws_url}/v1/events") as socket:
            await socket.recv()
        # ...and the service is still perfectly happy.
        async with httpx.AsyncClient(base_url=base, timeout=10.0) as client:
            return (await client.get("/v1/fleet")).status_code

    assert asyncio.run(scenario()) == 200


# ------------------------------------------------------------------- why
def test_why_explains_a_reserving_job(store, config, dispatch_server):
    """The customer feature: "why is my test stuck?" answered in the tool."""
    base, _config = dispatch_server

    async def scenario():
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as client:
            for agent_id, caps in (
                ("bench-vg", {"product": "vehicle_gateway", "harness": "j1939"}),
                ("bench-ag", {"product": "asset_gateway"}),
            ):
                await client.post(
                    "/v1/agents/register",
                    json={
                        "agent_id": agent_id,
                        "hostname": f"{agent_id}.local",
                        "resources": [{"id": f"d-{i}", "capabilities": caps} for i in (1, 2)],
                    },
                )
            big = await client.post(
                "/v1/jobs",
                json={
                    "name": "gw2gw",
                    "requirements": [{"product": "vehicle_gateway"}] * 2,
                },
            )
            job_id = big.json()["job_id"]
            return (await client.get(f"/v1/jobs/{job_id}/why")).json()

    why = asyncio.run(scenario())

    assert why["resource_count"] == 2
    feasible = {b["agent_id"] for b in why["feasible"]}
    infeasible = {b["agent_id"]: b["why_not"] for b in why["infeasible"]}
    assert feasible == {"bench-vg"}
    assert "bench-ag" in infeasible
    assert "0 healthy matching device" in infeasible["bench-ag"], (
        "it must say WHY that bench can never run this, not just that it cannot"
    )


def test_why_on_a_finished_job_reports_the_outcome_not_a_fleet_survey(store):
    from tss.api.client import why as why_endpoint

    store.register_agent(AGENT, "b.local", inventory(1), now=T0)
    submit(store, "job-A", 1)
    claim = store.claim_all("job-A", AGENT, [f"{AGENT}:vg-01"], now=T0)
    store.start_job("job-A", AGENT, claim.epoch, now=T0)
    store.complete_job("job-A", AGENT, claim.epoch, Outcome.FAILED, detail="assert", now=T0 + 5)

    class _NoReservation:
        reservation = None

        @staticmethod
        def reservation_for(_job_id):
            return None

    view = asyncio.run(why_endpoint("job-A", store, _NoReservation()))

    assert view.outcome == Outcome.FAILED
    assert view.result_detail == "assert"
    assert view.feasible == [] and view.infeasible == []


def test_why_is_404_for_a_job_that_does_not_exist(dispatch_server):
    base, _config = dispatch_server

    async def scenario():
        async with httpx.AsyncClient(base_url=base, timeout=10.0) as client:
            return await client.get("/v1/jobs/job-nope/why")

    assert asyncio.run(scenario()).status_code == 404


def test_the_watch_screen_renders_an_empty_fleet_and_a_full_one():
    """It will be on a projector, in front of people, at some point showing
    nothing at all. That must not be the moment it raises."""
    from rich.console import Console

    from tss.cli.watch import FleetScreen

    screen = FleetScreen("http://127.0.0.1:8000")
    console = Console(width=100, record=True, file=io.StringIO())

    console.print(screen.render())  # empty: no snapshot has arrived yet
    assert "no benches registered" in console.export_text()

    screen.apply(
        {
            "type": "snapshot",
            "fleet": {
                "now": T0,
                "agents": [
                    {
                        "id": "bench-01",
                        "hostname": "b",
                        "state": "offline",
                        "last_heartbeat_at": T0,
                        "presence_expires_at": T0,
                        "seconds_since_beat": 14.0,
                        "requeued_on_last_reap": ["job-A"],
                        "resources": [
                            {
                                "id": "bench-01:vg-01",
                                "local_id": "vg-01",
                                "state": "free",
                                "current_job_id": None,
                                "capabilities": DEVICE_CAPS,
                            }
                        ],
                    }
                ],
            },
            "queue": {
                "now": T0,
                "queued": [
                    {
                        "job_id": "job-B",
                        "name": "gw2gw",
                        "state": "queued",
                        "requirements": [{"product": "vehicle_gateway"}] * 2,
                        "resource_count": 2,
                        "agent_id": None,
                        "resource_ids": [],
                        "waited_s": 12.0,
                        "elapsed_s": None,
                        "max_duration_s": 600,
                        "attempt": 0,
                        "tried_agents": [],
                        "blocked_reason": None,
                        "reserving_on": "bench-01",
                    }
                ],
                "running": [],
            },
        }
    )
    screen.apply({"type": "event", "event": {"ts": T0, "kind": "agent.offline", "detail": {}}})
    console.print(screen.render())

    rendered = console.export_text()
    assert "OFFLINE" in rendered, "state must be readable without colour"
    assert "RESERVING on bench-01" in rendered
    assert "ONE BENCH" in rendered


def test_an_event_committed_while_the_snapshot_is_being_sent_is_not_lost(
    dispatch_server, monkeypatch
):
    """SUBSCRIBE FIRST, THEN SNAPSHOT.

    Building and sending the snapshot takes long enough for a claim to commit,
    and if the subscription is created afterwards everything published in that
    window goes to nobody. On a fleet that then falls quiet — which is exactly
    what a fleet does between jobs — `tss watch` shows stale state indefinitely,
    the precise failure the snapshot exists to prevent.

    The window is a real race, so it is constructed rather than hoped for: the
    snapshot builder publishes through the store's own hook, which is the same
    call a committing transaction makes.
    """
    base, _config = dispatch_server
    ws_url = base.replace("http://", "ws://")

    import tss.api.ws as ws_module

    real_snapshot = ws_module.snapshot
    marker = Event(ts=1.0, kind="job.assigned", job_id="job-during-snapshot")

    def snapshot_that_races(store, scheduler):
        frame = real_snapshot(store, scheduler)
        if store.publish is not None:
            store.publish([marker])  # ...committed while this frame was in flight
        return frame

    monkeypatch.setattr(ws_module, "snapshot", snapshot_that_races)

    async def scenario():
        async with websockets.connect(f"{ws_url}/v1/events") as socket:
            first = json.loads(await socket.recv())
            second = json.loads(await asyncio.wait_for(socket.recv(), timeout=5))
            return first, second

    first, second = asyncio.run(scenario())

    assert first["type"] == "snapshot"
    assert second["type"] == "event"
    assert second["event"]["job_id"] == "job-during-snapshot", (
        "an event published while the snapshot was in flight went to nobody"
    )

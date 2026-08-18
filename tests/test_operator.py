"""Operator verbs and the state-machine exits (§4.1, §4.2).

Every non-automatic transition needs a control surface. A state with no way out
is a slow fleet-drain dressed up as a health feature: quarantine a few benches
over a month and the fleet gets smaller with nobody deciding that it should.

The other half is what does NOT get a code path. Draining has no
DRAINING -> OFFLINE transition: the agent finishes, reports, exits, and its lease
expires like any other absent machine. Adding a special case there would mean two
mechanisms for "this bench is gone", and the second one would be the one nobody
tests.
"""

from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from tests.conftest import RunningAgent, inventory, submit
from tss.core.invariants import check_all
from tss.core.models import AgentState, InventoryItem, JobState, Outcome, ResourceState
from tss.core.scheduler import Scheduler
from tss.core.store import BLOCKED_NO_CAPABLE_AGENT, Store

T0 = 1_000_000.0
AGENT = "bench-sf-01"
VG = {"product": "vehicle_gateway", "harness": "j1939"}
STARVED = 61.0


@pytest.fixture
def scheduler(store, config):
    return Scheduler(store, config)


def bench(store, agent_id=AGENT, *, devices=2, version="0.1.0", now=T0):
    store.register_agent(
        agent_id, f"{agent_id}.local", inventory(devices), agent_version=version, now=now
    )
    return [f"{agent_id}:vg-{i:02d}" for i in range(1, devices + 1)]


def tick(scheduler, store, now):
    for agent in store.agents():
        store.renew_presence(agent.id, now=now)
    return scheduler.pass_once(now=now)


# ------------------------------------------------- quarantine survives a reboot
def test_quarantine_survives_a_reboot(store):
    """THE EDGE THAT WAS WRONG.

    The presence sweep reaps everything that is not already offline, quarantined
    benches included. So a quarantined machine that reboots goes
    quarantined -> offline -> re-registers, and if the re-registration decides
    what to do by reading `state` it finds `offline`, clears the quarantine, and
    hands work straight back to a bench nobody fixed. Restarting a broken machine
    is the FIRST thing anyone tries, so this is not an exotic path.

    The mark that survives is `quarantined_at`, and §4.1's rule is that only a
    new `agent_version` clears it.
    """
    bench(store, version="0.1.0")
    store.conn.execute(
        "UPDATE agents SET state = 'quarantined', quarantined_at = ? WHERE id = ?", (T0, AGENT)
    )

    store.reap_agent(AGENT, now=T0 + 100)  # its lease expires while it reboots
    assert store.get_agent(AGENT).state == AgentState.OFFLINE

    result = bench(store, version="0.1.0", now=T0 + 200)  # same build, same fault

    agent = store.get_agent(AGENT)
    assert agent.state == AgentState.QUARANTINED, (
        "a restarted-but-unfixed bench must not come back online"
    )
    assert agent.quarantined_at == T0, "quarantined since 14:02 is the useful fact; keep it"
    assert result == [f"{AGENT}:vg-01", f"{AGENT}:vg-02"]
    assert [a.id for a in store.online_agents(now=T0 + 201)] == [], "and it gets no work"


def test_a_new_agent_version_clears_quarantine(store):
    """The way out that does not need an operator: someone deployed a fix."""
    bench(store, version="0.1.0")
    store.conn.execute(
        "UPDATE agents SET state = 'quarantined', quarantined_at = ? WHERE id = ?", (T0, AGENT)
    )
    store.reap_agent(AGENT, now=T0 + 100)

    bench(store, version="0.2.0", now=T0 + 200)

    agent = store.get_agent(AGENT)
    assert agent.state == AgentState.ONLINE
    assert agent.quarantined_at is None
    assert [a.id for a in store.online_agents(now=T0 + 201)] == [AGENT]


def test_re_registering_clears_draining(store):
    """After maintenance the operator restarts the daemon and the bench is back.
    Draining leaves no mark, so nothing has to be undone."""
    bench(store)
    assert store.drain_agent(AGENT, now=T0 + 1) == "draining"
    store.reap_agent(AGENT, now=T0 + 100)

    bench(store, now=T0 + 200)

    assert store.get_agent(AGENT).state == AgentState.ONLINE


# --------------------------------------------------------------------- drain
def test_draining_stops_new_work_but_not_running_work(store, scheduler):
    devices = bench(store, devices=2)
    submit(store, "job-running", 1, now=T0)
    claim = store.claim_all("job-running", AGENT, devices[:1], now=T0)
    store.start_job("job-running", AGENT, claim.epoch, now=T0)
    submit(store, "job-next", 1, now=T0)

    assert store.drain_agent(AGENT, now=T0 + 1) == "draining"
    results = tick(scheduler, store, T0 + 2)

    assert results == [], "a draining bench is offered nothing"
    assert store.get_job("job-next").state == JobState.QUEUED
    assert store.get_job("job-running").state == JobState.RUNNING, "and keeps what it has"
    assert store.resources_held_by("job-running") == devices[:1]

    # A drained bench is NOT fenced: its result is a real result.
    assert (
        store.complete_job("job-running", AGENT, claim.epoch, Outcome.PASSED, now=T0 + 3)
        == "accepted"
    )
    assert store.get_job("job-running").outcome == Outcome.PASSED
    assert check_all(store, scheduler) == []


def test_a_drained_bench_goes_offline_by_the_ordinary_presence_sweep(store, scheduler, config):
    """No DRAINING -> OFFLINE code path. The daemon exits, the lease expires, and
    the same reaper that handles every other absent machine handles this one —
    with nothing to requeue, because the jobs finished."""
    devices = bench(store, devices=1)
    submit(store, "job-A", 1, now=T0)
    claim = store.claim_all("job-A", AGENT, devices, now=T0)
    store.start_job("job-A", AGENT, claim.epoch, now=T0)
    store.drain_agent(AGENT, now=T0 + 1)
    store.complete_job("job-A", AGENT, claim.epoch, Outcome.PASSED, now=T0 + 2)

    # ...the daemon exits, so the beats stop.
    expired = store.expired_agents(now=T0 + config.presence_ttl_s + 1)
    assert expired == [AGENT], "draining is not exempt from presence expiry"
    result = store.reap_agent(AGENT, now=T0 + config.presence_ttl_s + 1)

    assert result.requeued_jobs == [], "nothing was left running to requeue"
    assert store.get_agent(AGENT).state == AgentState.OFFLINE
    assert store.get_job("job-A").outcome == Outcome.PASSED
    assert check_all(store, scheduler) == []


def test_draining_an_agent_that_is_not_online_is_refused(store):
    bench(store)
    store.conn.execute("UPDATE agents SET state = 'offline' WHERE id = ?", (AGENT,))

    assert store.drain_agent(AGENT, now=T0 + 1) == "not_online:offline"
    assert store.drain_agent("bench-nope", now=T0 + 1) == "unknown_agent"


# -------------------------------------------------------------- unquarantine
def test_unquarantining_a_bench_resets_its_failure_count(store):
    bench(store)
    store.conn.execute(
        """UPDATE agents SET state = 'quarantined', quarantined_at = ?, consecutive_fails = 3
            WHERE id = ?""",
        (T0, AGENT),
    )

    assert store.unquarantine_agent(AGENT, now=T0 + 10) == "unquarantined"

    agent = store.get_agent(AGENT)
    assert agent.state == AgentState.ONLINE
    assert agent.quarantined_at is None
    assert agent.consecutive_fails == 0, (
        "back in at zero, not one bad job away from going straight out again"
    )
    assert [e.kind for e in store.events(kind="agent.unquarantined")] == ["agent.unquarantined"]


def test_unquarantining_a_device_leaves_the_bench_alone(store):
    devices = bench(store, devices=2)
    store.conn.execute(
        """UPDATE resources SET state = 'unhealthy', quarantined_at = ?, consecutive_fails = 3
            WHERE id = ?""",
        (T0, devices[0]),
    )

    assert store.unquarantine_resource(devices[0], now=T0 + 10) == "unquarantined"

    resource = store.get_resource(devices[0])
    assert resource.state == ResourceState.FREE
    assert resource.consecutive_fails == 0
    assert resource.quarantined_at is None
    assert store.get_agent(AGENT).state == AgentState.ONLINE


def test_a_retired_device_cannot_be_unquarantined(store):
    """Retired is not a health state — that device is not on the bench any more,
    and putting it back in the pool would be inventing hardware."""
    bench(store, devices=2)
    store.register_agent(
        AGENT,
        f"{AGENT}.local",
        [InventoryItem(id="vg-01", capabilities=inventory(1)[0].capabilities)],
        agent_version="0.1.0",
        now=T0 + 1,
    )
    assert store.get_resource(f"{AGENT}:vg-02").state == ResourceState.RETIRED

    assert store.unquarantine_resource(f"{AGENT}:vg-02", now=T0 + 2) == "retired"
    assert store.get_resource(f"{AGENT}:vg-02").state == ResourceState.RETIRED


def test_unquarantining_something_that_does_not_exist(store):
    assert store.unquarantine_agent("bench-nope") == "unknown_agent"
    assert store.unquarantine_resource("bench-nope:vg-01") == "unknown_resource"


# ------------------------------------------- feasibility and the two states
def test_a_reserver_retargets_when_its_bench_is_drained(store, scheduler, config):
    """Reserving toward a draining bench waits forever: its devices will never be
    offered again. The reservation is recomputed every pass, so it moves."""
    first = bench(store, "bench-01", devices=2)
    bench(store, "bench-02", devices=2)
    submit(store, "filler-a", 1, now=T0)
    submit(store, "filler-b", 1, now=T0)
    assert store.claim_all("filler-a", "bench-01", first[:1], now=T0).ok
    assert store.claim_all("filler-b", "bench-02", ["bench-02:vg-01"], now=T0).ok
    submit(store, "job-big", 2, now=T0)

    tick(scheduler, store, T0 + STARVED)
    assert scheduler.reservation.agent_id == "bench-01"

    store.drain_agent("bench-01", now=T0 + STARVED + 1)
    tick(scheduler, store, T0 + STARVED + 2)

    assert scheduler.reservation is not None
    assert scheduler.reservation.agent_id == "bench-02", "it must not wait on a bench that is going"
    assert check_all(store, scheduler) == []


def test_a_job_whose_only_bench_is_quarantined_is_unsatisfiable_then_runs(store, scheduler, config):
    """§3.4.1's "closest: bench-sf-09 has 3, but it is QUARANTINED" case. The job
    stays queued, because fleets get repaired — and this one does."""
    bench(store, "bench-01", devices=2)
    store.conn.execute(
        "UPDATE agents SET state = 'quarantined', quarantined_at = ? WHERE id = 'bench-01'", (T0,)
    )
    submit(store, "job-big", 2, now=T0)

    tick(scheduler, store, T0 + STARVED)

    assert scheduler.reservation is None, "nothing to reserve toward on a quarantined bench"
    assert store.get_job("job-big").blocked_reason == BLOCKED_NO_CAPABLE_AGENT
    assert store.get_job("job-big").state == JobState.QUEUED

    store.unquarantine_agent("bench-01", now=T0 + STARVED + 1)
    tick(scheduler, store, T0 + STARVED + 2)

    job = store.get_job("job-big")
    assert job.state == JobState.ASSIGNED
    assert job.blocked_reason is None
    assert len(store.resources_held_by("job-big")) == 2
    assert check_all(store, scheduler) == []


# ------------------------------------------------------------- over the wire
def test_drain_reaches_the_agent_and_it_finishes_and_exits(dispatch_server, db_path):
    """End to end: the directive is carried on the heartbeat, the daemon finishes
    what it is running, reports it, and stops. The result is accepted — a drained
    bench is not fenced."""
    base, _config = dispatch_server

    async def scenario():
        async with (
            httpx.AsyncClient(base_url=base, timeout=20.0) as client,
            RunningAgent(base, count=2) as agent,
        ):
            deadline = time.monotonic() + 10
            while not (await client.get("/v1/fleet")).json()["agents"]:
                assert time.monotonic() < deadline
                await asyncio.sleep(0.02)

            submitted = await client.post(
                "/v1/jobs",
                json={
                    "name": "long",
                    "requirements": [{"product": "vehicle_gateway"}],
                    "payload": {"duration_s": 1.5},
                },
            )
            job_id = submitted.json()["job_id"]
            while job_id not in agent.running:
                assert time.monotonic() < deadline
                await asyncio.sleep(0.02)

            began = time.monotonic()
            drained = await client.post(f"/v1/agents/{agent.agent_id}/drain")
            assert drained.status_code == 200
            # It must hear about it on this beat, not when its long-poll times
            # out: a bench that does not know it is draining keeps taking work.
            while not agent.draining and time.monotonic() - began < 5:
                await asyncio.sleep(0.02)
            told_in = time.monotonic() - began

            # It must NOT be handed anything else...
            await client.post(
                "/v1/jobs",
                json={"name": "next", "requirements": [{"product": "vehicle_gateway"}]},
            )
            # ...and it must finish what it had.
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                job = (await client.get(f"/v1/jobs/{job_id}")).json()
                if job["state"] != "running":
                    break
                await asyncio.sleep(0.05)
            fleet = (await client.get("/v1/fleet")).json()
            queue = (await client.get("/v1/queue")).json()
            return job, fleet, queue, agent.draining, told_in

    job, fleet, queue, draining, told_in = asyncio.run(scenario())

    assert draining is True, "the daemon never saw the directive"
    assert told_in < 1.0, (
        f"the bench took {told_in:.1f}s to learn it was draining — that is a long-poll "
        "timeout, not a wake-up, and it keeps accepting work until it lands"
    )
    assert job["state"] == "passed", "a drained bench's result is accepted, not fenced"
    assert fleet["agents"][0]["state"] == "draining"
    assert [j["name"] for j in queue["queued"]] == ["next"], "the new job waits for another bench"


def test_the_operator_endpoints_answer_sensibly(dispatch_server, db_path):
    base, _config = dispatch_server

    async def scenario():
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as client:
            await client.post(
                "/v1/agents/register",
                json={
                    "agent_id": "bench-01",
                    "hostname": "b.local",
                    "agent_version": "0.1.0",
                    "resources": [{"id": "vg-01", "capabilities": VG}],
                },
            )
            missing_agent = await client.post("/v1/agents/nope/drain")
            store = Store(db_path)
            store.conn.execute(
                "UPDATE agents SET state='quarantined', quarantined_at=1 WHERE id='bench-01'"
            )
            store.close()
            drain_quarantined = await client.post("/v1/agents/bench-01/drain")
            unq = await client.post("/v1/agents/bench-01/unquarantine")
            unq_device = await client.post("/v1/resources/bench-01:vg-01/unquarantine")
            missing_device = await client.post("/v1/resources/bench-01:nope/unquarantine")
            fleet = (await client.get("/v1/fleet")).json()
            return missing_agent, drain_quarantined, unq, unq_device, missing_device, fleet

    missing_agent, drain_quarantined, unq, unq_device, missing_device, fleet = asyncio.run(
        scenario()
    )

    assert missing_agent.status_code == 404
    assert drain_quarantined.status_code == 409
    assert "not online" in drain_quarantined.json()["detail"]
    assert unq.status_code == 200
    assert unq_device.status_code == 200
    assert missing_device.status_code == 404
    assert fleet["agents"][0]["state"] == "online"


def test_the_fleet_view_spells_out_draining_and_quarantined():
    """On a projector, in text — colour is reinforcement, never the message."""
    import io

    from rich.console import Console

    from tss.cli.main import render_fleet
    from tss.cli.watch import FleetScreen

    fleet = {
        "now": T0,
        "agents": [
            {
                "id": "bench-01",
                "hostname": "b",
                "state": state,
                "last_heartbeat_at": T0,
                "presence_expires_at": T0,
                "seconds_since_beat": 1.0,
                "requeued_on_last_reap": [],
                "resources": [
                    {
                        "id": "bench-01:vg-01",
                        "local_id": "vg-01",
                        "state": "free",
                        "current_job_id": None,
                        "capabilities": VG,
                    }
                ],
            }
            for state in ("draining", "quarantined")
        ],
    }
    console = Console(width=110, record=True, file=io.StringIO())
    render_fleet(fleet, console)

    screen = FleetScreen("http://x")
    screen.apply({"type": "snapshot", "fleet": fleet, "queue": {"queued": [], "running": []}})
    console.print(screen.render())

    rendered = console.export_text()
    assert rendered.count("DRAINING") >= 2, "both surfaces must say it, not just colour it"
    assert rendered.count("QUARANTINED") >= 2


# ---------------------------------------------- post-review hardening (§4.1/4.2)
def test_draining_takes_back_jobs_that_were_never_started(store, scheduler):
    """An assignment the daemon has not started yet is limbo: it is redelivered
    on the next heartbeat to a bench that is draining, which discards it. The job
    then sits `assigned` until presence expiry takes it — having burned an
    attempt and a bench, for nothing."""
    devices = bench(store, devices=2)
    submit(store, "job-started", 1, now=T0)
    submit(store, "job-limbo", 1, now=T0)
    started = store.claim_all("job-started", AGENT, devices[:1], now=T0)
    store.start_job("job-started", AGENT, started.epoch, now=T0)
    limbo = store.claim_all("job-limbo", AGENT, devices[1:], now=T0)
    assert store.get_job("job-limbo").state == JobState.ASSIGNED

    assert store.drain_agent(AGENT, now=T0 + 1) == "draining"

    limbo_job = store.get_job("job-limbo")
    assert limbo_job.state == JobState.QUEUED, "an unstarted assignment must come back"
    assert limbo_job.epoch == limbo.epoch + 1
    assert store.resources_held_by("job-limbo") == []
    # ...and the one that IS running is left alone to finish.
    assert store.get_job("job-started").state == JobState.RUNNING
    assert store.resources_held_by("job-started") == devices[:1]
    assert check_all(store, scheduler) == []


def test_failures_during_a_drain_do_not_convert_it_to_quarantine(store):
    """An operator's acknowledged order outranks automated blame.

    A bench is drained for maintenance — often BECAUSE its jobs have been
    failing — and the three runs it was already carrying now fail on the way
    down. Attribution would quarantine the machine, and the operator's `tss
    drain` would vanish from the fleet view, replaced by a verdict nobody chose
    and a different way out (unquarantine, not a restart).
    """
    devices = bench(store, devices=3)
    claims = {}
    for i, device in enumerate(devices):
        job_id = f"job-{i}"
        submit(store, job_id, 1, now=T0)
        claims[job_id] = store.claim_all(job_id, AGENT, [device], now=T0)
        assert claims[job_id].ok
        store.start_job(job_id, AGENT, claims[job_id].epoch, now=T0)

    assert store.drain_agent(AGENT, now=T0 + 1) == "draining"

    # ...and all three of its in-flight runs fail as it goes down.
    for i, job_id in enumerate(claims):
        store.complete_job(job_id, AGENT, claims[job_id].epoch, Outcome.INFRA_ERROR, now=T0 + 2 + i)

    assert store.get_agent(AGENT).state == AgentState.DRAINING, (
        "the drain was silently converted to a quarantine"
    )
    assert store.events(kind="agent.quarantined") == []


def test_device_quarantine_survives_a_reboot(store):
    """Step 7's bug, one level down. A device quarantined for repeated failures
    is revived to `free` by the inventory upsert on the next re-register — and a
    bench restart is, again, the first thing anyone tries."""
    devices = bench(store, devices=2, version="0.1.0")
    store.conn.execute(
        """UPDATE resources SET state = 'unhealthy', quarantined_at = ?, consecutive_fails = 3
            WHERE id = ?""",
        (T0, devices[0]),
    )

    bench(store, devices=2, version="0.1.0", now=T0 + 100)  # same build, same fault

    resource = store.get_resource(devices[0])
    assert resource.state == ResourceState.UNHEALTHY, (
        "a re-register must not revive a device TSS quarantined"
    )
    assert resource.quarantined_at == T0
    assert store.get_resource(devices[1]).state == ResourceState.FREE


def test_a_new_agent_version_clears_device_quarantine(store):
    devices = bench(store, devices=2, version="0.1.0")
    store.conn.execute(
        "UPDATE resources SET state = 'unhealthy', quarantined_at = ? WHERE id = ?",
        (T0, devices[0]),
    )

    bench(store, devices=2, version="0.2.0", now=T0 + 100)

    resource = store.get_resource(devices[0])
    assert resource.state == ResourceState.FREE
    assert resource.quarantined_at is None


def test_an_agent_cannot_clear_tss_s_own_quarantine_verdict(store):
    """`resource_health` is the agent's opinion of its hardware. A device TSS
    quarantined after three failures is TSS's verdict, and the bench reporting
    itself healthy is not an appeal — `tss unquarantine` is."""
    devices = bench(store, devices=2)
    store.conn.execute(
        "UPDATE resources SET state = 'unhealthy', quarantined_at = ? WHERE id = ?",
        (T0, devices[0]),
    )

    store.report_resource_health(AGENT, {"vg-01": "healthy"}, now=T0 + 1)
    assert store.get_resource(devices[0]).state == ResourceState.UNHEALTHY

    # An operator can, and that is the difference.
    assert store.unquarantine_resource(devices[0], now=T0 + 2) == "unquarantined"
    assert store.get_resource(devices[0]).state == ResourceState.FREE


def test_an_agent_reported_fault_is_still_the_agent_s_to_clear(store):
    """The other half: a device the AGENT called unhealthy has no
    `quarantined_at`, and the agent saying it recovered is exactly the signal
    TSS has (§3.1 — TSS never probes hardware itself)."""
    devices = bench(store, devices=2)
    store.report_resource_health(AGENT, {"vg-01": "unhealthy"}, now=T0)
    assert store.get_resource(devices[0]).state == ResourceState.UNHEALTHY
    assert store.get_resource(devices[0]).quarantined_at is None

    store.report_resource_health(AGENT, {"vg-01": "healthy"}, now=T0 + 1)
    assert store.get_resource(devices[0]).state == ResourceState.FREE

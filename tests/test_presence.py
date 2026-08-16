"""Presence leases: the agent is the thing that lives or dies (§3.5, §4.1).

Three properties, and the first is the one most implementations get wrong:

  1. Presence is per-AGENT and always set — idle, partly loaded, or full. A bench
     powered off while holding nothing must still go OFFLINE. Leasing only busy
     agents leaves a hole big enough to fail Pillar 4: the bench stays eligible
     forever and burns a full TTL on every job it is handed.
  2. Renewal is guarded by `state != 'offline'`. A reaped agent's row still
     exists, so an unguarded UPDATE would resurrect a dead lease and the agent
     would go on believing it owns devices that have been freed and reassigned.
  3. Re-registering while loaded requeues everything the bench held, in one
     transaction. A restarted agent has lost its hardware state.

Time is injected rather than slept: the expiry is a wall-clock number in a column
(§3.3), so a test can simply say "it is now 20 seconds later". One test at the
bottom does use a real clock and a real HTTP server, because the 410 handshake is
a protocol behaviour and mocking the transport would prove nothing.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging

import httpx
import pytest

from tests.conftest import DEVICE_CAPS, assert_i5, inventory, submit
from tss.core.models import AgentState, InventoryItem, JobState, PresenceStatus, ResourceState
from tss.core.reaper import Reaper

AGENT = "bench-sf-01"
T0 = 1_000_000.0


@pytest.fixture
def reaper(store, config):
    return Reaper(store, config)


def register(store, agent_id=AGENT, *, devices=3, version="0.1.0", now=T0):
    return store.register_agent(
        agent_id,
        f"{agent_id}.local",
        inventory(devices),
        agent_version=version,
        now=now,
    )


# --------------------------------------------------------------- registration
def test_registration_pushes_the_inventory_and_opens_a_lease(store, config):
    result = register(store)

    assert result.is_new
    agent = store.get_agent(AGENT)
    assert agent.state == AgentState.ONLINE
    # The lease is open the moment the bench appears, before it holds anything.
    assert agent.presence_expires_at == T0 + config.presence_ttl_s
    resources = store.list_resources(AGENT)
    assert [r.id for r in resources] == [f"{AGENT}:vg-{i:02d}" for i in (1, 2, 3)]
    assert all(r.state == ResourceState.FREE for r in resources)
    assert resources[0].capabilities == DEVICE_CAPS


def test_re_registering_replaces_the_inventory_and_never_duplicates(store):
    register(store, devices=3)
    register(store, devices=3, now=T0 + 1)

    assert len(store.list_resources(AGENT)) == 3, "a flapping bench must not duplicate devices"
    assert len(store.get_agent(AGENT).id) > 0


def test_re_registering_while_loaded_requeues_every_job_it_held(store):
    """A restarted agent has lost its hardware state. Pretending otherwise
    orphans those jobs forever, with no lease left to expire them."""
    register(store, devices=3)
    submit(store, "job-A", 2)
    claimed = store.claim_all("job-A", AGENT, [f"{AGENT}:vg-01", f"{AGENT}:vg-02"])
    assert claimed.ok

    result = register(store, devices=3, now=T0 + 5)

    assert result.requeued_jobs == ["job-A"]
    job = store.get_job("job-A")
    assert job.state == JobState.QUEUED
    assert job.agent_id is None
    assert job.epoch == claimed.epoch + 1, "ownership ended, so the epoch moves"
    assert job.tried_agents == [AGENT], "the bench was recorded at claim time, not again here"
    assert store.resources_held_by("job-A") == []
    assert all(r.state == ResourceState.FREE for r in store.list_resources(AGENT))
    assert [e.kind for e in store.events(job_id="job-A")] == ["job.assigned", "job.requeued"]


def test_re_registering_keeps_quarantine_unless_the_version_changed(store):
    register(store, version="0.1.0")
    store.conn.execute("UPDATE agents SET state = 'quarantined' WHERE id = ?", (AGENT,))

    same = register(store, version="0.1.0", now=T0 + 1)
    assert same.quarantine_retained
    assert store.get_agent(AGENT).state == AgentState.QUARANTINED, (
        "a restarted-but-unfixed bench must stay out of rotation"
    )

    upgraded = register(store, version="0.2.0", now=T0 + 2)
    assert not upgraded.quarantine_retained
    assert store.get_agent(AGENT).state == AgentState.ONLINE


def _reregister_with_only_vg01(store, now):
    return store.register_agent(
        AGENT,
        f"{AGENT}.local",
        [InventoryItem(id="vg-01", capabilities=DEVICE_CAPS)],
        agent_version="0.1.0",
        now=now,
    )


def test_a_vanished_device_with_history_is_retired_never_deleted(store):
    """`job_resources` records which devices a past attempt ran on. Deleting the
    device to tidy up the fleet view destroys the answer to "what did attempt 1
    actually run on?" — so it is parked, not removed."""
    register(store, devices=3)
    submit(store, "job-A", 1)
    assert store.claim_all("job-A", AGENT, [f"{AGENT}:vg-02"], now=T0 + 1).ok
    assert store.allocation_records("job-A")

    result = _reregister_with_only_vg01(store, T0 + 2)

    assert result.retired_resource_ids == [f"{AGENT}:vg-02", f"{AGENT}:vg-03"]
    assert result.requeued_jobs == ["job-A"], "its job comes back before its device goes away"
    vg02 = store.get_resource(f"{AGENT}:vg-02")
    assert vg02 is not None, "a device with allocation history is never deleted"
    assert vg02.state == ResourceState.RETIRED
    assert vg02.current_job_id is None
    assert len(store.allocation_records("job-A")) == 1, "the history survives the retirement"


def test_a_vanished_device_without_history_is_also_retired(store):
    """ONE RULE, not two. vg-03 never ran anything and could be deleted without
    breaking a foreign key — it is retired anyway, so what the fleet view shows
    does not depend on whether a device happened to run something before it was
    unplugged."""
    register(store, devices=3)
    assert store.allocation_records() == []

    result = _reregister_with_only_vg01(store, T0 + 1)

    assert result.retired_resource_ids == [f"{AGENT}:vg-02", f"{AGENT}:vg-03"]
    states = {r.id: r.state for r in store.list_resources(AGENT)}
    assert states == {
        f"{AGENT}:vg-01": ResourceState.FREE,
        f"{AGENT}:vg-02": ResourceState.RETIRED,
        f"{AGENT}:vg-03": ResourceState.RETIRED,
    }


def test_retired_is_not_unhealthy(store):
    """Present-but-broken and gone are different conditions with different human
    responses, so they get different names (§4.2)."""
    register(store, devices=2)
    store.report_resource_health(AGENT, {"vg-02": "unhealthy"}, now=T0 + 1)
    assert store.get_resource(f"{AGENT}:vg-02").state == ResourceState.UNHEALTHY

    _reregister_with_only_vg01(store, T0 + 2)
    assert store.get_resource(f"{AGENT}:vg-02").state == ResourceState.RETIRED

    # ...and a stale health report cannot bring a gone device back into the pool.
    store.report_resource_health(AGENT, {"vg-02": "healthy"}, now=T0 + 3)
    assert store.get_resource(f"{AGENT}:vg-02").state == ResourceState.RETIRED


def test_a_retired_device_cannot_be_claimed(store):
    register(store, devices=2)
    _reregister_with_only_vg01(store, T0 + 1)
    submit(store, "job-A", 1)

    result = store.claim_all("job-A", AGENT, [f"{AGENT}:vg-02"], now=T0 + 2)

    assert not result.ok, "the claim's own state='free' guard is what enforces this"
    assert store.get_resource(f"{AGENT}:vg-02").state == ResourceState.RETIRED


def test_a_reap_releases_claims_and_touches_nothing_else(store, reaper, config):
    """One bench, three devices in three different states. The reap frees the
    claimed one and leaves the other two exactly as they were.

    TSS never infers device health — the agent reports it. Marking vg-02 free
    because its machine died would be TSS deciding the J-Link got fixed, and
    marking vg-03 free would put a device that is no longer on the bench back
    into the schedulable pool.
    """
    register(store, devices=3)
    _reregister_with_only_vg01(store, T0 + 1)  # vg-02, vg-03 -> retired
    store.register_agent(  # vg-02 comes back, vg-03 stays gone
        AGENT,
        f"{AGENT}.local",
        [InventoryItem(id=n, capabilities=DEVICE_CAPS) for n in ("vg-01", "vg-02")],
        agent_version="0.1.0",
        now=T0 + 2,
    )
    store.report_resource_health(AGENT, {"vg-02": "unhealthy"}, now=T0 + 3)
    submit(store, "job-A", 1)
    assert store.claim_all("job-A", AGENT, [f"{AGENT}:vg-01"], now=T0 + 4).ok
    before = {r.id: r.state for r in store.list_resources(AGENT)}
    assert before == {
        f"{AGENT}:vg-01": ResourceState.BUSY,
        f"{AGENT}:vg-02": ResourceState.UNHEALTHY,
        f"{AGENT}:vg-03": ResourceState.RETIRED,
    }

    result = reaper.sweep_presence(now=T0 + 4 + config.presence_ttl_s + 1)

    assert result[0].freed_resources == [f"{AGENT}:vg-01"], "only the claim is released"
    assert result[0].requeued_jobs == ["job-A"]
    after = {r.id: r.state for r in store.list_resources(AGENT)}
    assert after == {
        f"{AGENT}:vg-01": ResourceState.FREE,
        f"{AGENT}:vg-02": ResourceState.UNHEALTHY,  # still broken; nobody fixed it
        f"{AGENT}:vg-03": ResourceState.RETIRED,  # still gone
    }
    assert_i5(store, AGENT)


# -------------------------------------------------------------------- presence
def test_a_live_agent_renews_its_lease(store, config):
    register(store)

    status, agent = store.renew_presence(AGENT, now=T0 + 3)

    assert status is PresenceStatus.RENEWED
    assert agent.presence_expires_at == T0 + 3 + config.presence_ttl_s
    assert agent.last_heartbeat_at == T0 + 3


def test_an_unknown_agent_is_told_to_register(store):
    status, agent = store.renew_presence("bench-that-never-was")
    assert status is PresenceStatus.UNKNOWN_AGENT
    assert agent is None


def test_an_idle_bench_that_is_powered_off_still_goes_offline(store, reaper, config):
    """THE CASE MOST IMPLEMENTATIONS MISS.

    This bench never took a job. There is no allocation to notice, no running
    work to time out — the only thing that expires is the machine's own lease.
    Lease busy agents only and this bench stays eligible forever, collecting
    assignments it will never run.
    """
    register(store, devices=3)
    assert store.get_agent(AGENT).state == AgentState.ONLINE

    dead_at = T0 + config.presence_ttl_s + 0.001
    assert store.expired_agents(now=dead_at) == [AGENT]
    results = reaper.sweep_presence(now=dead_at)

    assert [r.agent_id for r in results] == [AGENT]
    assert results[0].requeued_jobs == [], "it held nothing; there is nothing to requeue"
    assert store.get_agent(AGENT).state == AgentState.OFFLINE
    assert_i5(store, AGENT)
    assert [e.kind for e in store.events(kind="agent.offline")] == ["agent.offline"]


def test_a_loaded_bench_that_dies_frees_its_devices(store, reaper, config):
    register(store, devices=3)
    submit(store, "job-A", 2)
    assert store.claim_all("job-A", AGENT, [f"{AGENT}:vg-01", f"{AGENT}:vg-02"]).ok

    results = reaper.sweep_presence(now=T0 + config.presence_ttl_s + 1)

    assert results[0].requeued_jobs == ["job-A"]
    assert sorted(results[0].freed_resources) == [f"{AGENT}:vg-01", f"{AGENT}:vg-02"]
    assert store.get_job("job-A").state == JobState.QUEUED
    assert_i5(store, AGENT)
    assert all(r.state == ResourceState.FREE for r in store.list_resources(AGENT))


def test_a_reaped_agent_cannot_renew_itself_back_to_life(store, reaper, config):
    """The race that looks trivial and is not: a heartbeat landing microseconds
    after the reaper ran. Without `state != 'offline'` the lease is resurrected
    and the agent believes it still owns devices that have been reassigned."""
    register(store, devices=3)
    dead_at = T0 + config.presence_ttl_s + 1
    reaper.sweep_presence(now=dead_at)
    expiry_when_reaped = store.get_agent(AGENT).presence_expires_at

    status, agent = store.renew_presence(AGENT, now=dead_at + 0.000_001)

    assert status is PresenceStatus.EXPIRED, "the agent must be told to re-register"
    assert agent.state == AgentState.OFFLINE
    assert agent.presence_expires_at == expiry_when_reaped, "the dead lease moved forward"


def test_re_registering_after_a_reap_brings_the_bench_back_clean(store, reaper, config):
    register(store, devices=3)
    submit(store, "job-A", 1)
    assert store.claim_all("job-A", AGENT, [f"{AGENT}:vg-01"]).ok
    reaper.sweep_presence(now=T0 + config.presence_ttl_s + 1)

    again = register(store, devices=3, now=T0 + 30)

    assert not again.is_new
    assert again.requeued_jobs == [], "its job was already taken back when it was reaped"
    agent = store.get_agent(AGENT)
    assert agent.state == AgentState.ONLINE
    assert agent.presence_expires_at == T0 + 30 + config.presence_ttl_s
    assert store.get_job("job-A").state == JobState.QUEUED
    assert len(store.list_resources(AGENT)) == 3


def test_the_sweep_only_takes_agents_whose_lease_actually_ran_out(store, reaper, config):
    register(store, "bench-a", devices=1)
    register(store, "bench-b", devices=1)
    store.renew_presence("bench-b", now=T0 + 10)  # still beating

    reaped = reaper.sweep_presence(now=T0 + config.presence_ttl_s + 1)

    assert [r.agent_id for r in reaped] == ["bench-a"]
    assert store.get_agent("bench-b").state == AgentState.ONLINE


def test_the_reaper_loop_survives_a_failing_sweep(store, config, monkeypatch, caplog):
    """A background loop that dies on one unhandled exception dies silently, and
    every failure story in this design runs through this loop."""
    reaper = Reaper(store, dataclasses.replace(config, reaper_interval_s=0.05))
    boom = iter([RuntimeError("database went away")])

    def flaky(*, now=None):
        with contextlib.suppress(StopIteration):
            raise next(boom)
        return []

    monkeypatch.setattr(reaper, "sweep_presence", flaky)

    async def run_briefly():
        task = reaper.start()
        await asyncio.sleep(0.25)
        await reaper.stop()
        return task

    with caplog.at_level(logging.ERROR):
        task = asyncio.run(run_briefly())

    assert task.cancelled() or task.done()
    assert "reaper sweep failed" in caplog.text
    assert "database went away" in caplog.text


# ------------------------------------------------------------ over real HTTP
def test_the_full_presence_handshake_over_real_http(live_server):
    """Register, be reaped for going quiet, get 410, come back — against a real
    uvicorn on a real socket. The 410 is a protocol behaviour: an ASGI shortcut
    would exercise our own function calls and prove nothing about the contract an
    agent actually meets.

    The service under test runs with PRESENCE_TTL=1s so the whole handshake takes
    about two seconds instead of fourteen.
    """
    base, config = live_server
    payload = {
        "agent_id": AGENT,
        "hostname": "bench.local",
        "agent_version": "0.1.0",
        "resources": [{"id": f"vg-{i:02d}", "capabilities": DEVICE_CAPS} for i in (1, 2, 3)],
    }

    with httpx.Client(base_url=base, timeout=10.0) as client:
        registered = client.post("/v1/agents/register", json=payload)
        assert registered.status_code == 200
        assert registered.json()["presence_ttl_s"] == config.presence_ttl_s

        assert client.post(f"/v1/agents/{AGENT}/heartbeat", json={}).status_code == 200

        fleet = client.get("/v1/fleet").json()
        assert [a["state"] for a in fleet["agents"]] == ["online"]
        assert len(fleet["agents"][0]["resources"]) == 3

        # Go quiet. The lease expires on its own; nobody decides anything.
        deadline = config.presence_ttl_s + config.reaper_interval_s * 3
        _wait_until(
            lambda: client.get("/v1/fleet").json()["agents"][0]["state"] == "offline",
            timeout=deadline + 2,
        )

        fleet = client.get("/v1/fleet").json()
        agent = fleet["agents"][0]
        assert agent["state"] == "offline"
        assert [r["state"] for r in agent["resources"]] == ["free"] * 3

        # The row still exists, so an unguarded renewal would return a cheerful
        # 200 and strand this bench in OFFLINE forever.
        beat = client.post(f"/v1/agents/{AGENT}/heartbeat", json={})
        assert beat.status_code == 410
        assert beat.json() == {"error": "presence_expired", "action": "register"}

        assert client.post("/v1/agents/register", json=payload).status_code == 200
        fleet = client.get("/v1/fleet").json()
        assert fleet["agents"][0]["state"] == "online"
        assert len(fleet["agents"][0]["resources"]) == 3, "re-registering must not duplicate"


def test_an_unknown_agent_gets_404_over_real_http(live_server):
    base, _ = live_server
    with httpx.Client(base_url=base, timeout=10.0) as client:
        response = client.post("/v1/agents/ghost-bench/heartbeat", json={})
    assert response.status_code == 404
    assert response.json() == {"error": "unknown_agent", "action": "register"}


def _wait_until(predicate, *, timeout: float, interval: float = 0.05) -> None:
    import time

    deadline = time.monotonic() + timeout  # in-process duration: monotonic is correct here
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s")

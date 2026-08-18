"""Fencing: epochs, lease loss, cancel, and the zombie (§3.5, §7.4).

THE FAILURE THIS FILE EXISTS FOR:

    t=0   Agent-7 gets job J on vg-01, epoch 4. Starts running it.
    t=3   Agent-7's network drops. It keeps running — it doesn't know.
    t=12  Presence expires. The reaper frees the device, requeues J, epoch -> 5.
    t=14  Agent-3 gets J at epoch 5 on its own device. Starts running it.
    t=40  Agent-7's network returns: "job J complete, PASSED."

Without the epoch, TSS records J as passed — and a firmware engineer ships on a
result from a run that was abandoned. The epoch is a fencing token, the same
mechanism distributed locks use, and it is the most important thing in the
design. Execution is at-least-once by nature; the RESULT is exactly-once (§7.4).

The zombie test at the bottom runs over a real socket. It is a protocol
behaviour — what the agent is told, and what it does about it — and an in-process
shortcut would be testing our own function calls.
"""

from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from tests.conftest import DEVICE_CAPS, RunningAgent, inventory, submit
from tss.core.invariants import check_all, check_i7
from tss.core.models import JobState, Outcome
from tss.core.store import Store

T0 = 1_000_000.0
AGENT = "bench-sf-01"


def assigned_job(store, job_id="job-A", *, agent_id=AGENT, devices=1, now=T0):
    store.register_agent(agent_id, f"{agent_id}.local", inventory(devices), now=now)
    submit(store, job_id, 1)
    claim = store.claim_all(job_id, agent_id, [f"{agent_id}:vg-01"], now=now)
    assert claim.ok
    return claim


# ------------------------------------------------------------------- fencing
def test_the_epoch_fences_a_completion_from_a_previous_owner(store):
    claim = assigned_job(store)
    store.start_job("job-A", AGENT, claim.epoch, now=T0)

    # The bench dies and the job goes back to the queue at a new epoch.
    store.reap_agent(AGENT, now=T0 + 100)
    assert store.get_job("job-A").epoch == claim.epoch + 1

    late = store.complete_job("job-A", AGENT, claim.epoch, Outcome.PASSED, now=T0 + 200)

    assert late == "stale_epoch"
    job = store.get_job("job-A")
    assert job.state == JobState.QUEUED, "a stale report must not resurrect ownership"
    assert job.outcome is None


def test_start_is_fenced_too(store):
    """The gap between claim and /start is real. An agent that was reaped and is
    only now getting round to starting must not move a job someone else owns."""
    claim = assigned_job(store)
    store.reap_agent(AGENT, now=T0 + 100)

    assert store.start_job("job-A", AGENT, claim.epoch, now=T0 + 101) == "stale_epoch"
    assert store.get_job("job-A").state == JobState.QUEUED


def test_each_reported_job_is_fenced_independently(store):
    """A bench with four devices may be running two jobs. Losing one must not
    disturb the other — which is why the 409 names a job."""
    store.register_agent(AGENT, "b.local", inventory(2), now=T0)
    submit(store, "job-keep", 1)
    submit(store, "job-lose", 1)
    keep = store.claim_all("job-keep", AGENT, [f"{AGENT}:vg-01"], now=T0)
    lose = store.claim_all("job-lose", AGENT, [f"{AGENT}:vg-02"], now=T0)
    store.start_job("job-keep", AGENT, keep.epoch, now=T0)
    store.start_job("job-lose", AGENT, lose.epoch, now=T0)

    store.cancel_job("job-lose", now=T0 + 10)

    reported = [("job-keep", keep.epoch), ("job-lose", lose.epoch)]
    assert store.fence_running_jobs(AGENT, reported) == "job-lose"
    assert store.fence_running_jobs(AGENT, [("job-keep", keep.epoch)]) is None
    assert store.get_job("job-keep").state == JobState.RUNNING
    assert store.resources_held_by("job-keep") == [f"{AGENT}:vg-01"]


def test_a_job_reassigned_to_the_SAME_bench_is_fenced_by_the_epoch_alone(store):
    """The one scenario where the epoch is the only thing standing there.

    Every other fenced path has a second guard that happens to cover it. A stale
    report after a requeue is blocked by `agent_id IS NULL`; a report for a
    finished job is blocked by `state IN ('assigned','running')`. Remove the
    epoch check and chaos still passes, because those two catch everything it
    generates.

    Not this. The bench dies, its job is requeued, and the SAME bench comes back
    and gets it again — a normal thing to happen on a small fleet. Now agent_id
    matches, the state is live again, and only the epoch separates attempt 1's
    late report from attempt 2's ownership. Without it, the first attempt's
    result lands on the second attempt's run.

    Found by sabotage (§8.1): removing the epoch check passed the chaos gate, so
    the scenario is written directly rather than hoped for.
    """
    first = assigned_job(store, agent_id=AGENT, devices=1)
    store.start_job("job-A", AGENT, first.epoch, now=T0)

    # The bench goes quiet, is reaped, and the job comes back to the queue.
    store.reap_agent(AGENT, now=T0 + 100)
    assert store.get_job("job-A").state == JobState.QUEUED

    # ...and the very same bench re-registers and picks it up again.
    store.register_agent(AGENT, f"{AGENT}.local", inventory(1), now=T0 + 101)
    second = store.claim_all("job-A", AGENT, [f"{AGENT}:vg-01"], now=T0 + 101)
    assert second.ok
    store.start_job("job-A", AGENT, second.epoch, now=T0 + 101)

    job = store.get_job("job-A")
    assert job.agent_id == AGENT, "same bench: agent_id cannot tell the attempts apart"
    assert job.state == JobState.RUNNING, "live state: the state guard cannot either"
    assert second.epoch > first.epoch, "only the epoch moved"

    # Attempt 1's report finally arrives, from the same bench, for the same job.
    late = store.complete_job("job-A", AGENT, first.epoch, Outcome.PASSED, now=T0 + 200)

    assert late == "stale_epoch"
    job = store.get_job("job-A")
    assert job.state == JobState.RUNNING, "attempt 2 is still running"
    assert job.outcome is None, "a result from attempt 1 must not land on attempt 2"
    assert store.resources_held_by("job-A") == [f"{AGENT}:vg-01"], (
        "attempt 2 still holds its devices — the stale report must not free them"
    )

    # ...and attempt 2's own report is accepted normally.
    assert store.complete_job("job-A", AGENT, second.epoch, Outcome.FAILED, now=T0 + 201) == (
        "accepted"
    )
    assert store.get_job("job-A").outcome == Outcome.FAILED
    assert check_all(store) == []


def test_a_job_reassigned_to_another_bench_is_fenced_from_the_first(store):
    claim = assigned_job(store, agent_id="bench-a")
    store.reap_agent("bench-a", now=T0 + 100)
    store.register_agent("bench-b", "b.local", inventory(1), now=T0 + 101)
    store.claim_all("job-A", "bench-b", ["bench-b:vg-01"], now=T0 + 101)

    assert store.fence_running_jobs("bench-a", [("job-A", claim.epoch)]) == "job-A"
    assert store.fence_running_jobs("bench-b", [("job-A", claim.epoch + 2)]) is None


# -------------------------------------------------------------------- cancel
def test_cancelling_a_queued_job_kills_it_quietly(store):
    submit(store, "job-A", 1)

    assert store.cancel_job("job-A", now=T0) == "cancelled"

    job = store.get_job("job-A")
    assert job.state == JobState.CANCELLED
    assert job.outcome == Outcome.CANCELLED
    assert job.finished_at == T0
    assert check_all(store) == []


def test_cancelling_a_running_job_bumps_the_epoch_and_frees_its_devices(store):
    claim = assigned_job(store)
    store.start_job("job-A", AGENT, claim.epoch, now=T0)

    assert store.cancel_job("job-A", now=T0 + 5) == "cancelled_running"

    job = store.get_job("job-A")
    assert job.state == JobState.CANCELLED
    assert job.epoch == claim.epoch + 1, "the bump is what fences the late report"
    assert store.resources_held_by("job-A") == []
    assert all(r["released_at"] is not None for r in store.allocation_records("job-A"))
    assert check_all(store) == []


def test_a_late_pass_never_overwrites_a_cancel(store):
    """I7, and the reason cancel bumps the epoch at all. The agent was already
    running the test when the engineer cancelled; its result arrives afterwards
    and must be dropped, not recorded."""
    claim = assigned_job(store)
    store.start_job("job-A", AGENT, claim.epoch, now=T0)
    store.cancel_job("job-A", now=T0 + 5)

    replayed = store.complete_job("job-A", AGENT, claim.epoch, Outcome.PASSED, now=T0 + 30)

    assert replayed == "stale_epoch"
    job = store.get_job("job-A")
    assert job.state == JobState.CANCELLED
    assert job.outcome == Outcome.CANCELLED, "CANCELLED is a result; PASSED does not land on it"
    assert check_i7(store) == []


def test_cancelling_a_finished_job_is_refused(store):
    claim = assigned_job(store)
    store.start_job("job-A", AGENT, claim.epoch, now=T0)
    store.complete_job("job-A", AGENT, claim.epoch, Outcome.FAILED, detail="assert", now=T0 + 5)

    assert store.cancel_job("job-A", now=T0 + 6) == "already_terminal"

    job = store.get_job("job-A")
    assert job.state == JobState.FAILED
    assert job.outcome == Outcome.FAILED, "a real result is never overwritten (I7)"
    assert job.result_detail == "assert"


def test_a_reaper_never_resurrects_a_cancelled_job(store, config):
    claim = assigned_job(store)
    store.start_job("job-A", AGENT, claim.epoch, now=T0)
    store.cancel_job("job-A", now=T0 + 5)

    store.reap_agent(AGENT, now=T0 + config.presence_ttl_s + 1)

    assert store.get_job("job-A").state == JobState.CANCELLED
    assert check_all(store) == []


# ------------------------------------------------------- the zombie, for real
def test_the_zombie_scenario_over_real_http(live_server, db_path):
    """§3.5's timeline, end to end, over a socket.

    bench-7 is assigned the job and starts it, then goes silent. Its lease
    expires, the reaper requeues the job, and bench-3 picks it up at the next
    epoch. bench-7 comes back and reports PASSED for the run TSS abandoned.

    The heartbeats and reports here are made by hand rather than by the daemon:
    the daemon is exercised in test_dispatch.py, and going silent on demand is
    the whole point of this test.
    """
    base, config = live_server
    store = Store(db_path)

    async def register(client, agent_id, devices=1):
        response = await client.post(
            "/v1/agents/register",
            json={
                "agent_id": agent_id,
                "hostname": f"{agent_id}.local",
                "agent_version": "0.1.0",
                "resources": [
                    {"id": f"vg-{i:02d}", "capabilities": DEVICE_CAPS}
                    for i in range(1, devices + 1)
                ],
            },
        )
        assert response.status_code == 200

    async def beat(client, agent_id, running=()):
        return await client.post(
            f"/v1/agents/{agent_id}/heartbeat",
            json={
                "running_jobs": [{"job_id": j, "epoch": e} for j, e in running],
                "resource_health": {},
            },
        )

    async def scenario():
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as client:
            # bench-7 is the only bench in the fleet, so the job can only land
            # there. bench-3 arrives afterwards, to receive the requeue.
            await register(client, "bench-7")

            submitted = await client.post(
                "/v1/jobs",
                json={
                    "name": "zombie",
                    "requirements": [{"product": "vehicle_gateway"}],
                    "payload": {"duration_s": 0},
                },
            )
            job_id = submitted.json()["job_id"]

            deadline = time.monotonic() + 10
            assignment = None
            while assignment is None and time.monotonic() < deadline:
                assignment = (await beat(client, "bench-7")).json()["assignment"]
            assert assignment is not None, "bench-7 was never offered the job"
            old_epoch = assignment["epoch"]
            started = await client.post(
                f"/v1/jobs/{job_id}/start", json={"agent_id": "bench-7", "epoch": old_epoch}
            )
            assert started.status_code == 200

            # ...and now bench-7 goes silent. bench-3 arrives and keeps beating.
            await register(client, "bench-3")
            reassigned = None
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                response = await beat(client, "bench-3")
                if response.status_code == 200 and response.json()["assignment"]:
                    reassigned = response.json()["assignment"]
                    break
                await asyncio.sleep(0.05)
            assert reassigned is not None, "the job was never requeued to the live bench"
            assert reassigned["job_id"] == job_id
            new_epoch = reassigned["epoch"]
            assert new_epoch > old_epoch, "requeueing must move the fence"
            assert (
                await client.post(
                    f"/v1/jobs/{job_id}/start",
                    json={"agent_id": "bench-3", "epoch": new_epoch},
                )
            ).status_code == 200

            # The zombie returns and reports the run TSS gave up on.
            zombie = await client.post(
                f"/v1/jobs/{job_id}/complete",
                json={"agent_id": "bench-7", "epoch": old_epoch, "outcome": "passed"},
            )
            assert zombie.status_code == 409
            assert zombie.json() == {
                "error": "stale_epoch",
                "action": "abandon_job",
                "job_id": job_id,
            }

            # Its heartbeat tells it the same thing, naming the job.
            fenced = await beat(client, "bench-7", running=[(job_id, old_epoch)])
            assert fenced.status_code in (409, 410)
            if fenced.status_code == 409:
                assert fenced.json()["error"] == "lease_lost"
                assert fenced.json()["job_id"] == job_id

            state = (await client.get(f"/v1/jobs/{job_id}")).json()
            assert state["state"] == "running", "the zombie's report changed nothing"
            assert state["agent_id"] == "bench-3"

            # The real owner's result is the one that counts.
            accepted = await client.post(
                f"/v1/jobs/{job_id}/complete",
                json={
                    "agent_id": "bench-3",
                    "epoch": new_epoch,
                    "outcome": "failed",
                    "detail": "regression",
                },
            )
            assert accepted.status_code == 200
            final = (await client.get(f"/v1/jobs/{job_id}")).json()
            assert final["outcome"] == "failed"
            assert final["result_detail"] == "regression"
            assert final["agent_id"] == "bench-3"
            return job_id

    job_id = asyncio.run(scenario())

    # Exactly one result was ever accepted for this job (I1).
    completions = [e for e in store.events(job_id=job_id) if e.kind == "job.completed"]
    assert len(completions) == 1
    assert completions[0].agent_id == "bench-3"
    assert check_all(store) == []
    store.close()


def test_a_cancel_directive_reaches_the_agent_and_stops_the_run(dispatch_server, db_path):
    """Item 5: directives are delivered AND acted on.

    The bench is parked in an 8-second long-poll when the cancel lands, so this
    also pins the delivery latency: queueing a directive releases that poll
    immediately. Without it the agent finds out when the poll times out, and
    spends up to LONGPOLL_TIMEOUT running a test nobody wants — the exact waste
    the directive exists to avoid.
    """
    base, config = dispatch_server

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
                    "payload": {"duration_s": 120},
                },
            )
            job_id = submitted.json()["job_id"]
            deadline = time.monotonic() + 10
            while job_id not in agent.running:
                assert time.monotonic() < deadline, "the agent never picked the job up"
                await asyncio.sleep(0.02)

            began = time.monotonic()
            cancelled = await client.delete(f"/v1/jobs/{job_id}")
            assert cancelled.json() == {
                "cancelled": True,
                "job_id": job_id,
                "name": "long",
                "was_running": True,
            }

            while job_id in agent.running and time.monotonic() - began < 5:
                await asyncio.sleep(0.02)
            elapsed = time.monotonic() - began
            assert job_id not in agent.running, "the agent kept running a cancelled job"
            return job_id, elapsed

    job_id, elapsed = asyncio.run(scenario())

    assert elapsed < 1.0, (
        f"the cancel took {elapsed:.1f}s to reach the bench — that looks like a "
        f"long-poll timeout ({config.longpoll_timeout_s}s), not a wakeup"
    )
    store = Store(db_path)
    job = store.get_job(job_id)
    assert job.state == JobState.CANCELLED
    assert job.outcome == Outcome.CANCELLED
    assert store.resources_held_by(job_id) == []
    # The agent abandoned the run instead of reporting it.
    assert [e.kind for e in store.events(job_id=job_id) if e.kind == "job.completed"] == []
    assert check_all(store) == []
    store.close()


def test_queueing_a_directive_wakes_that_bench(store, config):
    """The wakeup hook, on its own: pushing a directive pokes exactly one bench."""
    woken = []
    from tss.core.directives import DirectiveQueue

    queue = DirectiveQueue(on_push=woken.append)
    queue.cancel_job("bench-a", "job-1")
    queue.cancel_job("bench-a", "job-1")  # deduplicated — a hint is worth sending once

    assert woken == ["bench-a", "bench-a"]
    assert queue.drain("bench-a") == [{"cancel_job": "job-1"}]
    assert queue.drain("bench-a") == []
    assert queue.drain("bench-b") == []


@pytest.mark.parametrize("outcome", ["passed", "failed"])
def test_a_replayed_completion_is_rejected(store, outcome):
    """Exactly-once RESULT (I1): the network may deliver the same report twice."""
    claim = assigned_job(store)
    store.start_job("job-A", AGENT, claim.epoch, now=T0)

    first = store.complete_job("job-A", AGENT, claim.epoch, Outcome(outcome), now=T0 + 5)
    second = store.complete_job("job-A", AGENT, claim.epoch, Outcome(outcome), now=T0 + 6)

    assert first == "accepted"
    assert second == "stale_epoch"
    assert store.get_job("job-A").finished_at == T0 + 5
    assert check_i7(store) == []


# --------------------------------------------------------- the inverse fence
def _beat(client, agent_id, running=()):
    return client.post(
        f"/v1/agents/{agent_id}/heartbeat",
        json={
            "running_jobs": [{"job_id": j, "epoch": e} for j, e in running],
            "resource_health": {},
        },
    )


async def _register(client, agent_id, devices=1):
    response = await client.post(
        "/v1/agents/register",
        json={
            "agent_id": agent_id,
            "hostname": f"{agent_id}.local",
            "agent_version": "0.1.0",
            "resources": [
                {"id": f"vg-{i:02d}", "capabilities": DEVICE_CAPS} for i in range(1, devices + 1)
            ],
        },
    )
    assert response.status_code == 200


async def _assignment_for(client, agent_id, *, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = (await _beat(client, agent_id)).json()
        if body["assignment"]:
            return body["assignment"]
    raise AssertionError(f"{agent_id} was never offered anything")


@pytest.mark.parametrize(
    ("name", "start_it"),
    [("lost_start_response", False), ("lost_complete_response", True)],
)
def test_a_job_the_agent_stops_reporting_is_taken_back(dispatch_server, db_path, name, start_it):
    """THE INVERSE FENCE.

    The epoch fences what the agent DOES report. Nothing looks at what it fails
    to report — and the two ways that happens are both ordinary network events:

      * the /start response is lost, so the agent never learns it owns the job;
      * the /complete response is lost, so the agent drops the job locally and
        stops mentioning it while TSS still believes it is running.

    Either way the job sits in `assigned`/`running` holding devices until
    presence expiry, which only fires if the whole bench dies. The bench here is
    perfectly healthy and heartbeating.
    """
    base, _config = dispatch_server
    store = Store(db_path)

    async def scenario():
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as client:
            await _register(client, AGENT)
            submitted = await client.post(
                "/v1/jobs",
                json={"name": name, "requirements": [{"product": "vehicle_gateway"}]},
            )
            job_id = submitted.json()["job_id"]
            assignment = await _assignment_for(client, AGENT)
            if start_it:
                await client.post(
                    f"/v1/jobs/{job_id}/start",
                    json={"agent_id": AGENT, "epoch": assignment["epoch"]},
                )
            # ...and from here the agent never mentions it again.
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                await _beat(client, AGENT)
                job = (await client.get(f"/v1/jobs/{job_id}")).json()
                if job["epoch"] > assignment["epoch"]:
                    return job, assignment
                await asyncio.sleep(0.05)
            return (await client.get(f"/v1/jobs/{job_id}")).json(), assignment

    job, assignment = asyncio.run(scenario())

    # Asserted on the epoch and the audit log rather than on a glimpse of
    # `queued`: the fleet here is one healthy bench, so the job is re-dispatched
    # almost immediately and the queued state is a window a poll can miss.
    assert job["epoch"] > assignment["epoch"], (
        "a healthy, heartbeating bench held this job forever without ever running it"
    )
    requeues = [
        e
        for e in store.events(job_id=job["id"])
        if e.kind == "job.requeued" and (e.detail or {}).get("reason") == "unreported_by_agent"
    ]
    assert len(requeues) == 1, "it must be taken back exactly once, and say why"
    assert check_all(store) == []
    store.close()


def test_a_job_that_is_still_being_reported_is_left_alone(dispatch_server, db_path):
    """The negative. A job can legitimately take a long time to say anything
    interesting; what matters is that the bench keeps claiming it."""
    base, _config = dispatch_server
    store = Store(db_path)

    async def scenario():
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as client:
            await _register(client, AGENT)
            await client.post(
                "/v1/jobs",
                json={"name": "slow", "requirements": [{"product": "vehicle_gateway"}]},
            )
            assignment = await _assignment_for(client, AGENT)
            reported = [(assignment["job_id"], assignment["epoch"])]
            for _ in range(8):  # many more beats than the miss threshold
                assert (await _beat(client, AGENT, reported)).status_code == 200
                await asyncio.sleep(0.02)
            return (await client.get(f"/v1/jobs/{assignment['job_id']}")).json()

    job = asyncio.run(scenario())

    assert job["state"] in ("assigned", "running"), "a reported job must not be taken back"
    assert job["agent_id"] == AGENT
    store.close()


def test_one_missed_beat_is_not_enough(dispatch_server, db_path):
    """Two CONSECUTIVE misses. One beat that crosses in flight with a /start is
    ordinary, and taking a job off a bench for it would be its own bug."""
    base, _config = dispatch_server

    async def scenario():
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as client:
            await _register(client, AGENT)
            await client.post(
                "/v1/jobs",
                json={"name": "flicker", "requirements": [{"product": "vehicle_gateway"}]},
            )
            assignment = await _assignment_for(client, AGENT)
            job_id, epoch = assignment["job_id"], assignment["epoch"]
            await _beat(client, AGENT)  # one miss...
            await _beat(client, AGENT, [(job_id, epoch)])  # ...then it speaks up
            await _beat(client, AGENT)  # miss again — the count restarted
            return (await client.get(f"/v1/jobs/{job_id}")).json()

    job = asyncio.run(scenario())

    assert job["state"] in ("assigned", "running")

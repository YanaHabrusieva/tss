"""Priority, and the fact that it is load-bearing (§3.4).

`queued_jobs` orders by `priority` and the scheduler walks that order, so it
decides what runs first on a contended fleet. Nothing asserted it: delete
`priority,` from the ORDER BY and the whole suite stayed green, which meant the
one thing an operator would reach for in an incident — push this job to the
front — was held up by a comma nobody was watching.

DIRECTION: LOWER RUNS FIRST. 0 is the front, 1000 the back, 100 the default. It
reads backwards said out loud, which is exactly why it is pinned here and spelled
out in `SubmitRequest`.
"""

from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from tests.conftest import inventory, submit
from tss.core.scheduler import Scheduler

T0 = 1_000_000.0
VG = {"product": "vehicle_gateway", "harness": "j1939"}


@pytest.fixture
def scheduler(store, config):
    return Scheduler(store, config)


def test_a_lower_number_runs_first(store):
    """The direction itself. Submitted in the wrong order on purpose."""
    submit(store, "job-normal", 1, now=T0, caps=VG)
    store.conn.execute("UPDATE jobs SET priority = 100 WHERE id = 'job-normal'")
    submit(store, "job-urgent", 1, now=T0 + 10, caps=VG)
    store.conn.execute("UPDATE jobs SET priority = 10 WHERE id = 'job-urgent'")

    assert [j.id for j in store.queued_jobs()] == ["job-urgent", "job-normal"]


def test_equal_priority_is_still_oldest_first(store):
    """Priority orders between bands; FIFO orders inside one. A queue that
    reordered equal work would make submission time meaningless."""
    for i in range(3):
        submit(store, f"job-{i}", 1, now=T0 + i, caps=VG)

    assert [j.id for j in store.queued_jobs()] == ["job-0", "job-1", "job-2"]


def test_an_urgent_job_submitted_later_is_dispatched_first(store, scheduler):
    """The property that matters, through the scheduler rather than the SELECT:
    one free device, two waiting jobs, and the one that arrived SECOND wins
    because it is more urgent."""
    store.register_agent("bench-01", "bench-01.local", inventory(1, caps=VG), now=T0)
    submit(store, "job-normal", 1, now=T0, caps=VG)
    store.conn.execute("UPDATE jobs SET priority = 100 WHERE id = 'job-normal'")
    submit(store, "job-urgent", 1, now=T0 + 5, caps=VG)
    store.conn.execute("UPDATE jobs SET priority = 1 WHERE id = 'job-urgent'")

    scheduler.pass_once(now=T0 + 10)

    assert store.get_job("job-urgent").agent_id == "bench-01"
    assert store.get_job("job-normal").agent_id is None, "the older job jumped the urgent one"


def test_the_submit_response_says_where_you_are_in_the_queue(dispatch_server):
    """`queue_position` is the only thing the submitter gets back about waiting,
    and it was never asserted — a constant 0 would have passed."""
    base, _config = dispatch_server

    async def scenario():
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as client:
            # No bench at all, so nothing drains the queue underneath us.
            positions = []
            for i in range(3):
                response = await client.post(
                    "/v1/jobs",
                    json={"name": f"q{i}", "requirements": [dict(VG)], "priority": 100},
                )
                assert response.status_code == 201
                positions.append(response.json()["queue_position"])
            # ...and an urgent one lands in front of all of them.
            urgent = await client.post(
                "/v1/jobs",
                json={"name": "urgent", "requirements": [dict(VG)], "priority": 1},
            )
            return positions, urgent.json()

    positions, urgent = asyncio.run(scenario())

    assert positions == [1, 2, 3], f"queue_position did not advance: {positions}"
    assert urgent["queue_position"] == 1, "a more urgent job must be told it is at the front"


def test_priority_survives_the_round_trip(dispatch_server):
    base, _config = dispatch_server

    async def scenario():
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as client:
            created = await client.post(
                "/v1/jobs", json={"name": "p", "requirements": [dict(VG)], "priority": 7}
            )
            job_id = created.json()["job_id"]
            return (await client.get(f"/v1/jobs/{job_id}")).json()

    assert asyncio.run(scenario())["priority"] == 7


def test_a_wide_job_id_leaves_the_short_form_alone(dispatch_server):
    """ITEM 6's other half: ids got wider, the human label did not. 32 bits of
    randomness is even odds of a collision around 77k jobs, and a collision was
    an unhandled IntegrityError — a 500 for a valid request."""
    from tss.api.client import JOB_ID_HEX
    from tss.core.models import short_id

    base, _config = dispatch_server

    async def scenario():
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as client:
            created = await client.post("/v1/jobs", json={"name": "wide", "requirements": [VG]})
            return created.json()["job_id"]

    job_id = asyncio.run(scenario())

    assert JOB_ID_HEX >= 16, "under 64 bits, collisions are a matter of when"
    assert len(job_id) == len("job-") + JOB_ID_HEX
    assert len(short_id(job_id)) == 5, "the human short id is unchanged"


def test_a_colliding_job_id_is_retried_not_a_500(dispatch_server, monkeypatch):
    """The retry, forced. Without it the second submit is an unhandled
    IntegrityError straight out of SQLite."""
    import uuid as uuid_module

    from tss.api import client as client_module

    base, _config = dispatch_server
    real_uuid4 = uuid_module.uuid4  # captured BEFORE the patch, or it recurses
    fixed = real_uuid4()
    calls = {"n": 0}

    def collide():
        calls["n"] += 1
        return fixed if calls["n"] <= 2 else real_uuid4()

    monkeypatch.setattr(client_module.uuid, "uuid4", collide)

    async def scenario():
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as c:
            first = await c.post("/v1/jobs", json={"name": "a", "requirements": [VG]})
            second = await c.post("/v1/jobs", json={"name": "b", "requirements": [VG]})
            return first, second

    first, second = asyncio.run(scenario())

    assert first.status_code == 201
    assert second.status_code == 201, f"a collision surfaced as {second.status_code}"
    assert second.json()["job_id"] != first.json()["job_id"]


# ------------------------------------------------- the error paths nobody ran
def test_an_unknown_job_is_a_404_not_a_500(dispatch_server):
    base, _config = dispatch_server

    async def scenario():
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as client:
            return (
                await client.get("/v1/jobs/job-nosuchjob"),
                await client.delete("/v1/jobs/job-nosuchjob"),
                await client.get("/v1/jobs/job-nosuchjob/why"),
            )

    for response in asyncio.run(scenario()):
        assert response.status_code == 404, f"{response.request.url} gave {response.status_code}"


def test_cancelling_a_finished_job_is_a_409_and_never_rewrites_it(dispatch_server, db_path):
    """I7: a terminal outcome is never overwritten, and the caller is told why
    rather than being given a cheerful 200 that did nothing."""
    from tss.core.models import Outcome
    from tss.core.store import Store

    base, _config = dispatch_server
    store = Store(db_path)

    async def scenario():
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as client:
            created = await client.post("/v1/jobs", json={"name": "done", "requirements": [VG]})
            job_id = created.json()["job_id"]
            store.conn.execute(
                "UPDATE jobs SET state = 'passed', outcome = 'passed', finished_at = 1 "
                "WHERE id = ?",
                (job_id,),
            )
            store.conn.commit()
            return job_id, await client.delete(f"/v1/jobs/{job_id}")

    job_id, response = asyncio.run(scenario())

    assert response.status_code == 409
    assert "already finished" in response.json()["detail"]
    job = store.get_job(job_id)
    assert job.state == "passed" and job.outcome == Outcome.PASSED
    store.close()


def test_cancelling_an_assigned_job_that_never_started(dispatch_server, db_path):
    """The untested middle state. QUEUED cancels quietly and RUNNING is fenced;
    ASSIGNED is neither — the bench has been given the job and has not called
    /start, so its devices are held and a directive still has to reach it."""
    from tss.core.models import JobState, ResourceState
    from tss.core.store import Store

    base, _config = dispatch_server
    store = Store(db_path)
    agent_id = "bench-mid-01"

    async def scenario():
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as client:
            await client.post(
                "/v1/agents/register",
                json={
                    "agent_id": agent_id,
                    "hostname": f"{agent_id}.local",
                    "agent_version": "0.1.0",
                    "resources": [{"id": "vg-01", "capabilities": VG}],
                },
            )
            created = await client.post("/v1/jobs", json={"name": "mid", "requirements": [VG]})
            job_id = created.json()["job_id"]
            deadline = time.monotonic() + 15
            while store.get_job(job_id).state != JobState.ASSIGNED:
                assert time.monotonic() < deadline, "the job was never assigned"
                await asyncio.sleep(0.02)
            # ...and NOT started: no /start call is ever made.
            return job_id, await client.delete(f"/v1/jobs/{job_id}")

    job_id, response = asyncio.run(scenario())

    assert response.status_code == 200
    body = response.json()
    assert body["cancelled"] is True
    assert body["was_running"] is True, "an assigned job holds hardware; the bench must be told"
    job = store.get_job(job_id)
    assert job.state == JobState.CANCELLED
    assert store.resources_held_by(job_id) == [], "its device was left held"
    assert store.get_resource(f"{agent_id}:vg-01").state == ResourceState.FREE
    store.close()


def test_a_heartbeat_reporting_a_job_tss_never_issued_is_fenced(dispatch_server):
    """The fence's unknown_job branch. A bench inventing a job id — or one that
    survived a database it no longer shares — must be told to abandon it, not
    quietly believed."""
    base, _config = dispatch_server
    agent_id = "bench-ghost-01"

    async def scenario():
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as client:
            await client.post(
                "/v1/agents/register",
                json={
                    "agent_id": agent_id,
                    "hostname": f"{agent_id}.local",
                    "agent_version": "0.1.0",
                    "resources": [{"id": "vg-01", "capabilities": VG}],
                },
            )
            return await client.post(
                f"/v1/agents/{agent_id}/heartbeat",
                json={
                    "running_jobs": [{"job_id": "job-neverexisted", "epoch": 1}],
                    "resource_health": {},
                },
            )

    response = asyncio.run(scenario())

    assert response.status_code == 409
    body = response.json()
    assert body["error"] == "lease_lost"
    assert body["job_id"] == "job-neverexisted"
    assert body["action"] == "abandon_job"

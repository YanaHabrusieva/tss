"""The happy path, end to end, over real HTTP (§8, "Integration").

A real uvicorn, a real `TestbedAgent` daemon, real sockets. The transport is
never mocked: a mocked test passes while the real thing deadlocks, and almost
everything interesting in this step — the long-poll blocking server-side, the
epoch coming back on /complete, two jobs running side by side on one bench — is
only real over a socket.

The service here runs with an 8s long-poll and a 30s scheduler backstop, so
"dispatched in under a second" can only be true if the wakeups actually work. If
either were broken, dispatch would still happen, just seconds later — which is
exactly the bug this asserts against.
"""

from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from tests.conftest import RunningAgent
from tss.core.invariants import check_all
from tss.core.store import Store

AGENT = "bench-sf-01"


async def poll_until(client: httpx.AsyncClient, path: str, predicate, *, timeout=15.0):
    """Ask the API until it says what we are waiting for, or give up."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        response = await client.get(path)
        if response.status_code == 200:
            last = response.json()
            if predicate(last):
                return last
        await asyncio.sleep(0.02)
    raise AssertionError(f"timed out after {timeout}s waiting on {path}; last saw {last}")


async def submit(client: httpx.AsyncClient, name: str, *, duration_s=0.2, outcome="passed", **kw):
    response = await client.post(
        "/v1/jobs",
        json={
            "name": name,
            "requirements": [{"product": "vehicle_gateway"}],
            "payload": {"duration_s": duration_s, "outcome": outcome},
            **kw,
        },
    )
    assert response.status_code == 201, response.text
    return response.json()["job_id"]


@pytest.mark.parametrize("count", [3])
def test_a_job_lands_runs_reports_and_frees_its_device(dispatch_server, db_path, count):
    """The done-criteria for this step, in one test.

    A job goes in through the API, lands on a bench, runs, reports, frees its
    device, and the next queued job takes that device — while a second job runs
    on the same bench the whole way through.
    """
    base, _config = dispatch_server

    async def scenario():
        async with (
            httpx.AsyncClient(base_url=base, timeout=20.0) as client,
            RunningAgent(base, count=count),
        ):
            await poll_until(client, "/v1/fleet", lambda f: len(f["agents"]) == 1)

            # A long-running job that holds one device for the whole test.
            long_job = await submit(client, "the-long-one", duration_s=6.0)
            await poll_until(client, f"/v1/jobs/{long_job}", lambda j: j["state"] == "running")

            first = await submit(client, "first", duration_s=0.2)
            second = await submit(client, "second", duration_s=0.2)

            # Both short jobs finish, and the long one is still going.
            done = await poll_until(client, f"/v1/jobs/{first}", lambda j: j["state"] == "passed")
            assert done["outcome"] == "passed"
            assert done["agent_id"] == AGENT
            assert done["attempt"] == 1
            assert done["tried_agents"] == [AGENT]
            assert done["finished_at"] > done["started_at"] > done["assigned_at"]

            await poll_until(client, f"/v1/jobs/{second}", lambda j: j["state"] == "passed")

            still_running = await client.get(f"/v1/jobs/{long_job}")
            assert still_running.json()["state"] == "running", (
                "the long job must not have been disturbed by anything around it"
            )

            # A device is free again, so the next job takes it.
            third = await submit(client, "third", duration_s=0.2)
            await poll_until(client, f"/v1/jobs/{third}", lambda j: j["state"] == "passed")

            await poll_until(
                client, f"/v1/jobs/{long_job}", lambda j: j["state"] == "passed", timeout=20
            )

            fleet = (await client.get("/v1/fleet")).json()
            assert [r["state"] for r in fleet["agents"][0]["resources"]] == ["free"] * count

    asyncio.run(scenario())

    store = Store(db_path)
    assert check_all(store) == []
    store.close()


def test_two_jobs_run_concurrently_on_one_bench(dispatch_server, db_path):
    """§1.2 over the wire: the bench is not the unit of allocation."""
    base, _config = dispatch_server

    async def scenario():
        async with (
            httpx.AsyncClient(base_url=base, timeout=20.0) as client,
            RunningAgent(base, count=3),
        ):
            await poll_until(client, "/v1/fleet", lambda f: len(f["agents"]) == 1)
            job_a = await submit(client, "a", duration_s=3.0)
            job_b = await submit(client, "b", duration_s=3.0)

            queue = await poll_until(
                client,
                "/v1/queue",
                lambda q: len([j for j in q["running"] if j["state"] == "running"]) == 2,
            )
            running = {j["job_id"] for j in queue["running"] if j["state"] == "running"}
            assert running == {job_a, job_b}
            assert len({j["agent_id"] for j in queue["running"]}) == 1, "same bench"
            assert len({j["resource_ids"][0] for j in queue["running"]}) == 2, "different devices"

            fleet = (await client.get("/v1/fleet")).json()
            busy = [r for r in fleet["agents"][0]["resources"] if r["state"] == "busy"]
            assert len(busy) == 2
            assert len({r["current_job_id"] for r in busy}) == 2

    asyncio.run(scenario())

    store = Store(db_path)
    assert check_all(store) == []
    store.close()


def test_dispatch_is_sub_second(dispatch_server):
    """§1.3: under a second from "resource freed" to "agent has job".

    Measured to RUNNING, not to ASSIGNED — the agent has to have actually
    received it. With an 8s long-poll and a 30s backstop tick, anything above a
    second means a wakeup was missed and a fallback picked it up.
    """
    base, config = dispatch_server

    async def scenario():
        async with (
            httpx.AsyncClient(base_url=base, timeout=20.0) as client,
            RunningAgent(base, count=1),
        ):
            await poll_until(client, "/v1/fleet", lambda f: len(f["agents"]) == 1)
            # Let the agent settle into a long-poll before we time anything.
            await asyncio.sleep(0.6)

            started = time.monotonic()
            job_id = await submit(client, "fast", duration_s=0.1)
            await poll_until(client, f"/v1/jobs/{job_id}", lambda j: j["state"] == "running")
            return time.monotonic() - started

    latency = asyncio.run(scenario())

    assert latency < 1.0, f"dispatch took {latency:.2f}s"
    assert latency < config.longpoll_timeout_s / 4, (
        "this looks like a long-poll timeout, not a wake"
    )


def test_a_stale_epoch_is_rejected_and_the_result_dropped(dispatch_server, db_path):
    """Fencing (§3.5). A completion for an epoch TSS has moved past is refused —
    without this, a run TSS had already abandoned overwrites a real result."""
    base, _config = dispatch_server
    store = Store(db_path)

    async def scenario():
        async with (
            httpx.AsyncClient(base_url=base, timeout=20.0) as client,
            RunningAgent(base, count=1),
        ):
            await poll_until(client, "/v1/fleet", lambda f: len(f["agents"]) == 1)
            job_id = await submit(client, "zombie", duration_s=10.0)
            job = await poll_until(client, f"/v1/jobs/{job_id}", lambda j: j["state"] == "running")
            epoch = job["epoch"]

            stale = await client.post(
                f"/v1/jobs/{job_id}/complete",
                json={"agent_id": AGENT, "epoch": epoch - 1, "outcome": "passed"},
            )
            assert stale.status_code == 409
            assert stale.json() == {
                "error": "stale_epoch",
                "action": "abandon_job",
                "job_id": job_id,
            }
            assert (await client.get(f"/v1/jobs/{job_id}")).json()["state"] == "running"

            # The current epoch is accepted, exactly once.
            body = {"agent_id": AGENT, "epoch": epoch, "outcome": "failed", "detail": "assert"}
            accepted = await client.post(f"/v1/jobs/{job_id}/complete", json=body)
            assert accepted.status_code == 200
            replayed = await client.post(f"/v1/jobs/{job_id}/complete", json=body)
            assert replayed.status_code == 409, "exactly one result is ever accepted (I1)"

            final = (await client.get(f"/v1/jobs/{job_id}")).json()
            assert final["state"] == "failed"
            assert final["outcome"] == "failed"
            assert final["result_detail"] == "assert"

    asyncio.run(scenario())
    assert check_all(store) == []
    store.close()


def test_completing_frees_every_device_the_job_held(dispatch_server, db_path):
    """I8's other half. The claim-time guard proves a job took the right number
    of devices; this proves the release gives all of them back."""
    base, _config = dispatch_server
    store = Store(db_path)

    async def scenario():
        async with (
            httpx.AsyncClient(base_url=base, timeout=20.0) as client,
            RunningAgent(base, count=2),
        ):
            await poll_until(client, "/v1/fleet", lambda f: len(f["agents"]) == 1)
            job_id = await submit(client, "one", duration_s=0.2)
            await poll_until(client, f"/v1/jobs/{job_id}", lambda j: j["state"] == "passed")
            return job_id

    job_id = asyncio.run(scenario())

    assert store.resources_held_by(job_id) == []
    assert all(r["released_at"] is not None for r in store.allocation_records(job_id))
    assert check_all(store) == []
    store.close()


def test_a_multi_device_job_is_accepted_and_counted(dispatch_server):
    """Multi-device is on (step 5a): `resource_count` is len(requirements), and
    the job is queued rather than refused."""
    base, _config = dispatch_server

    async def scenario():
        async with httpx.AsyncClient(base_url=base, timeout=10.0) as client:
            two = await client.post(
                "/v1/jobs",
                json={
                    "name": "gateway-to-gateway",
                    "requirements": [
                        {"product": "vehicle_gateway", "harness": "j1939"},
                        {"product": "asset_gateway"},
                    ],
                },
            )
            assert two.status_code == 201, two.text
            job = (await client.get(f"/v1/jobs/{two.json()['job_id']}")).json()
            return job

    job = asyncio.run(scenario())

    assert job["resource_count"] == 2
    assert job["state"] == "queued"
    assert len(job["requirements"]) == 2
    assert job["reserving"] is None, "not starving yet"


def test_a_job_asking_for_absurdly_many_devices_is_refused(dispatch_server):
    """Matching is a backtracking search; the bound keeps one silly job from
    pinning a scheduling pass. An empty requirement list is refused too."""
    base, config = dispatch_server

    async def scenario():
        async with httpx.AsyncClient(base_url=base, timeout=10.0) as client:
            too_many = await client.post(
                "/v1/jobs",
                json={
                    "name": "greedy",
                    "requirements": [{"product": "vehicle_gateway"}]
                    * (config.max_resources_per_job + 1),
                },
            )
            none = await client.post("/v1/jobs", json={"name": "nothing", "requirements": []})
            return too_many, none

    too_many, none = asyncio.run(scenario())

    assert too_many.status_code == 400
    assert "at most" in too_many.json()["detail"]
    assert none.status_code == 400

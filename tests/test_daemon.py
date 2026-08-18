"""The bench daemon's own failure modes (§3.1).

Everything here is about the agent side of the wire, and the theme is executions
that outlive TSS's knowledge of them. TSS can fence a *result* — that is what the
epoch is for — but it cannot stop a process on another machine from driving a
J-Link. Only the daemon can do that, and only if it notices.
"""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from tss.agent.daemon import TestbedAgent

DEVICES = [{"id": "vg-01", "capabilities": {"product": "vehicle_gateway"}}]


def agent(handler, **kwargs) -> TestbedAgent:
    """A daemon wired to a scripted server, with no socket in the way."""
    bench = TestbedAgent("bench-01", DEVICES, base_url="http://tss.test", **kwargs)
    bench._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return bench


# ------------------------------------------------------- executions that leak
def test_a_410_cancels_the_runs_it_is_abandoning():
    """A reaped bench must STOP, not just forget.

    Clearing `running` without cancelling the tasks leaves the old executions
    physically driving devices TSS has already re-issued to another bench. The
    fence keeps the stale *result* out; nothing keeps the stale *run* out except
    this.
    """
    started = asyncio.Event()

    async def scenario():
        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("/register"):
                return httpx.Response(
                    200,
                    json={
                        "heartbeat_interval_s": 0.05,
                        "presence_ttl_s": 1.0,
                        "longpoll_timeout_s": 0.2,
                    },
                )
            if request.url.path.endswith("/heartbeat"):
                if started.is_set():
                    return httpx.Response(
                        410, json={"error": "presence_expired", "action": "register"}
                    )
                return httpx.Response(
                    200,
                    json={
                        "assignment": {
                            "job_id": "job-A",
                            "epoch": 1,
                            "agent_id": "bench-01",
                            "resource_ids": ["vg-01"],
                            "payload": {"duration_s": 30},
                            "max_duration_s": 600,
                        },
                        "directives": [],
                    },
                )
            if request.url.path.endswith("/start"):
                started.set()
                return httpx.Response(200, json={"accepted": True})
            return httpx.Response(200, json={"accepted": True})

        bench = agent(handler)
        stop = asyncio.Event()
        loop = asyncio.create_task(bench.run(stop))
        await asyncio.wait_for(started.wait(), timeout=5)
        task = bench._tasks.get("job-A")
        assert task is not None, "the job should be running"

        # ...now the 410 lands.
        for _ in range(100):
            await asyncio.sleep(0.02)
            if not bench.running:
                break
        stop.set()
        loop.cancel()
        return task

    task = asyncio.run(scenario())

    assert task.cancelled() or task.done(), (
        "the execution kept driving hardware after the bench lost its lease"
    )


def test_a_run_that_outlives_its_budget_is_abandoned_locally():
    """The daemon enforces `max_duration_s` itself.

    TSS's job-timeout sweep frees the devices at the same moment — but a
    partitioned agent never hears about that, and no directive can reach it. If
    the daemon does not hold its own deadline, it drives hardware unboundedly
    while TSS gives that hardware to somebody else.
    """
    reported: list[str] = []

    async def scenario():
        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("/register"):
                return httpx.Response(
                    200,
                    json={
                        "heartbeat_interval_s": 0.05,
                        "presence_ttl_s": 1.0,
                        "longpoll_timeout_s": 0.2,
                    },
                )
            if request.url.path.endswith("/heartbeat"):
                return httpx.Response(200, json={"assignment": None, "directives": []})
            if request.url.path.endswith("/complete"):
                reported.append(json.loads(request.content)["outcome"])
            return httpx.Response(200, json={"accepted": True})

        bench = agent(handler)
        bench.registered = True
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            await bench._run_job(
                client,
                {
                    "job_id": "job-A",
                    "epoch": 1,
                    "resource_ids": ["vg-01"],
                    "payload": {"duration_s": 30},  # far past its budget
                    "max_duration_s": 1,
                },
            )

    asyncio.run(asyncio.wait_for(scenario(), timeout=15))

    assert reported == [], "an abandoned run must report nothing at all"


# ------------------------------------------------------ hostile responses
@pytest.mark.parametrize("body", [b"<html>502 Bad Gateway</html>", b"", b"{oops"])
def test_a_non_json_200_does_not_kill_the_bench(body):
    """A proxy error page with a 200 status must not take a bench off the fleet.

    `response.json()` raises `JSONDecodeError`, which is a ValueError — not an
    `httpx.HTTPError` — so it goes straight past the only except clause and out
    of the loop. The daemon dies silently, the bench is reaped, and its work is
    requeued. In exactly the NAT'd, proxied network this design assumes.
    """
    beats = 0

    async def scenario():
        nonlocal beats

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal beats
            if request.url.path.endswith("/register"):
                return httpx.Response(
                    200,
                    json={
                        "heartbeat_interval_s": 0.02,
                        "presence_ttl_s": 1.0,
                        "longpoll_timeout_s": 0.1,
                    },
                )
            beats += 1
            return httpx.Response(200, content=body, headers={"content-type": "text/html"})

        bench = agent(handler)
        stop = asyncio.Event()
        task = asyncio.create_task(bench.run(stop))
        await asyncio.sleep(0.5)
        alive = not task.done()
        stop.set()
        task.cancel()
        return alive

    alive = asyncio.run(scenario())

    assert beats > 1, "it stopped heartbeating after the first bad body"
    assert alive, "the daemon died on a malformed response"


def test_a_non_json_registration_response_is_survivable():
    attempts = 0

    async def scenario():
        nonlocal attempts

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            if attempts <= 2:
                return httpx.Response(200, content=b"<html>hello</html>")
            return httpx.Response(
                200,
                json={
                    "heartbeat_interval_s": 0.05,
                    "presence_ttl_s": 1.0,
                    "longpoll_timeout_s": 0.2,
                },
            )

        bench = agent(handler)
        stop = asyncio.Event()
        task = asyncio.create_task(bench.run(stop))
        await asyncio.sleep(0.5)
        registered = bench.registered
        stop.set()
        task.cancel()
        return registered

    assert asyncio.run(scenario()), "it never got past a bad registration response"

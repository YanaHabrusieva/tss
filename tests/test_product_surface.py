"""What the customer sees: feasibility at the door, and /v1/stats (§3.9).

TSS queues a job no bench can run ON PURPOSE — fleets get repaired and extended,
so a job that is impossible today may be ordinary tomorrow (§3.4.1). What was
missing was telling the person who just typed the command. They found out when
the starvation threshold eventually flagged it, or from `tss why`, or never.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from tests.conftest import RunningAgent
from tss.cli.submit import requirements

VG = {"product": "vehicle_gateway"}
AG = {"product": "asset_gateway"}


def post(base: str, body: dict) -> dict:
    async def go():
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as client:
            response = await client.post("/v1/jobs", json=body)
            assert response.status_code == 201, response.text
            return response.json()

    return asyncio.run(go())


# ------------------------------------------------------------- feasibility
def test_a_job_no_bench_can_run_says_so_in_the_submit_response(dispatch_server):
    base, _config = dispatch_server

    body = post(base, {"name": "impossible", "requirements": [VG, AG]})

    assert body["feasible"] is False
    assert "no bench" in body["infeasible_reason"] or "no benches" in body["infeasible_reason"]
    assert "vehicle_gateway" in body["infeasible_reason"]
    assert "asset_gateway" in body["infeasible_reason"], "it must name what is missing"


def test_an_infeasible_job_is_still_queued(dispatch_server):
    """The warning is advisory, not a rejection. Refusing it would be the wrong
    call — the fleet changes, and §3.4.1 is explicit that the job waits."""
    base, _config = dispatch_server

    body = post(base, {"name": "impossible", "requirements": [VG, AG]})

    async def go():
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as client:
            return (await client.get(f"/v1/jobs/{body['job_id']}")).json()

    assert asyncio.run(go())["state"] == "queued"
    assert body["queue_position"] >= 1


def test_a_job_the_fleet_can_run_is_quiet(dispatch_server):
    base, _config = dispatch_server

    async def scenario():
        async with (
            httpx.AsyncClient(base_url=base, timeout=20.0) as client,
            RunningAgent(base, count=2),
        ):
            deadline = asyncio.get_running_loop().time() + 15
            while not (await client.get("/v1/fleet")).json()["agents"]:
                assert asyncio.get_running_loop().time() < deadline
                await asyncio.sleep(0.05)
            response = await client.post("/v1/jobs", json={"name": "fine", "requirements": [VG]})
            return response.json()

    body = asyncio.run(scenario())

    assert body["feasible"] is True
    assert body["infeasible_reason"] is None


def test_feasibility_uses_the_schedulers_own_filter(store, config):
    """Not a second copy of the matching semantics. A bench with two j1939
    gateways cannot ever run a job needing three, and counting devices would get
    that wrong — which is exactly the case `could_ever_satisfy` exists for."""
    from tests.conftest import inventory
    from tss.core.scheduler import Scheduler

    scheduler = Scheduler(store, config)
    store.register_agent("bench-01", "b.local", inventory(2, caps=VG), now=1_000_000.0)
    store.renew_presence("bench-01", now=1_000_000.0)

    assert scheduler.feasible_agents([VG], now=1_000_000.0) == ["bench-01"]
    assert scheduler.feasible_agents([VG, VG], now=1_000_000.0) == ["bench-01"]
    assert scheduler.feasible_agents([VG, VG, VG], now=1_000_000.0) == [], (
        "two devices cannot satisfy three specs"
    )
    assert scheduler.feasible_agents([AG], now=1_000_000.0) == []


def test_the_wrapper_builds_requirements_from_two_counts():
    assert requirements(1, 0) == [VG]
    assert requirements(0, 2) == [AG, AG]
    assert requirements(2, 1) == [VG, VG, AG]
    assert requirements(0, 0) == []


def test_the_wrapper_refuses_a_job_with_no_devices(capsys):
    from tss.cli.submit import main

    assert main(["nothing", "0", "0"]) == 2
    assert "at least one device" in capsys.readouterr().out


# ------------------------------------------------------------------ stats
def test_stats_reports_the_fleet_without_touching_a_hot_path(dispatch_server):
    """Everything here is counted out of rows that were already being written —
    no counter is bumped on the claim, heartbeat or completion path to make this
    endpoint cheaper."""
    base, _config = dispatch_server

    async def scenario():
        async with (
            httpx.AsyncClient(base_url=base, timeout=20.0) as client,
            RunningAgent(base, count=2),
        ):
            deadline = asyncio.get_running_loop().time() + 15
            while not (await client.get("/v1/fleet")).json()["agents"]:
                assert asyncio.get_running_loop().time() < deadline
                await asyncio.sleep(0.05)
            await client.post(
                "/v1/jobs",
                json={"name": "s", "requirements": [VG], "payload": {"duration_s": 0.2}},
            )
            deadline = asyncio.get_running_loop().time() + 15
            while True:
                stats = (await client.get("/v1/stats")).json()
                if stats["completed_in_window"]:
                    return stats
                assert asyncio.get_running_loop().time() < deadline, stats
                await asyncio.sleep(0.1)

    stats = asyncio.run(scenario())

    assert stats["uptime_s"] > 0
    assert stats["now"] > 1_700_000_000
    assert sum(stats["devices"].values()) == 2
    assert 0.0 <= stats["utilization"] <= 1.0
    assert stats["jobs"], "jobs by state"
    assert stats["completed_in_window"].get("passed", 0) >= 1
    assert stats["throughput_per_min"] > 0
    assert stats["window_s"] > 0
    assert stats["requeues_in_window"] >= 0
    assert stats["quarantined_agents"] == []
    assert stats["quarantined_devices"] == []
    assert stats["event_bus_drops"] == 0


def test_stats_is_fine_on_an_empty_fleet(dispatch_server):
    """Division by zero is the obvious way a utilization number dies."""
    base, _config = dispatch_server

    async def go():
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as client:
            return (await client.get("/v1/stats")).json()

    stats = asyncio.run(go())

    assert stats["utilization"] == 0.0
    assert stats["devices"] == {}
    assert stats["throughput_per_min"] == 0.0


@pytest.mark.parametrize("path", ["/v1/stats"])
def test_stats_is_read_only(dispatch_server, path):
    base, _config = dispatch_server

    async def go():
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as client:
            return await client.post(path, json={})

    assert asyncio.run(go()).status_code == 405

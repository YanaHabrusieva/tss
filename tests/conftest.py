"""Shared fixtures.

The `claim` fixture is the seam that lets the same tests run against two
implementations of the N-way claim:

    just test           -> tss.core.store.Store.claim_all    (§3.3, one transaction)
    just test-naive     -> tests.naive_claim.naive_claim_all (check-then-act loop)
    TSS_CLAIM_IMPL=naive_cleanup
                        -> the same loop, with "release what I took" on failure —
                           the version that looks fine in review (§7.5)

The naive ones are kept in the repo on purpose. A test that passes before the fix
proves nothing, and `just test-naive` is the re-runnable evidence that these two
files actually catch the bug they were written for.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from collections.abc import Callable, Sequence
from contextlib import contextmanager

import pytest

from tss.agent.daemon import TestbedAgent
from tss.core.config import Config
from tss.core.models import AgentState, ClaimResult, InventoryItem, ResourceState
from tss.core.store import Store

ClaimFn = Callable[[Store, str, str, Sequence[str]], ClaimResult]

IMPL_ENV = "TSS_CLAIM_IMPL"


IMPLS = ("atomic", "naive", "naive_cleanup")


@pytest.fixture(scope="session")
def claim_impl() -> str:
    impl = os.environ.get(IMPL_ENV, "atomic")
    if impl not in IMPLS:
        raise ValueError(f"{IMPL_ENV} must be one of {IMPLS}, got {impl!r}")
    return impl


@pytest.fixture
def claim(claim_impl: str) -> ClaimFn:
    if claim_impl == "naive":
        from tests.naive_claim import naive_claim_all

        return naive_claim_all
    if claim_impl == "naive_cleanup":
        from tests.naive_claim import naive_claim_all_cleanup

        return naive_claim_all_cleanup

    def atomic(store: Store, job_id: str, agent_id: str, resource_ids: Sequence[str]):
        return store.claim_all(job_id, agent_id, resource_ids)

    return atomic


REAP_IMPL_ENV = "TSS_REAP_IMPL"


@pytest.fixture(scope="session")
def reap_impl() -> str:
    impl = os.environ.get(REAP_IMPL_ENV, "fanout")
    if impl not in {"fanout", "naive"}:
        raise ValueError(f"{REAP_IMPL_ENV} must be 'fanout' or 'naive', got {impl!r}")
    return impl


@pytest.fixture
def reap(reap_impl: str):
    """The presence-sweep requeue under test — see `claim` above for the pattern."""
    if reap_impl == "naive":
        from tests.naive_reap import naive_reap_agent

        return naive_reap_agent

    def fanout(store: Store, agent_id: str, *, now: float | None = None, **kw):
        return store.reap_agent(agent_id, now=now, **kw)

    return fanout


@pytest.fixture
def db_path(tmp_path) -> str:
    return str(tmp_path / "tss.db")


@pytest.fixture
def config() -> Config:
    return Config()


@pytest.fixture
def store(db_path: str, config: Config) -> Store:
    s = Store(db_path, config)
    s.init_schema()
    yield s
    s.close()


#: Short-lease config for tests that must use a real clock. The ratios still hold
#: (TTL = 4 x heartbeat; TTL > longpoll + heartbeat), just scaled down 12x.
FAST_CONFIG = Config(
    heartbeat_interval_s=0.25,
    presence_ttl_s=1.0,
    reaper_interval_s=0.2,
    longpoll_timeout_s=0.5,
)


#: Dispatch-latency config. The long-poll and the backstop tick are deliberately
#: LONG: if the scheduler's wakeup or the per-agent wakeup were broken, dispatch
#: would fall back to one of those and take seconds. A sub-second assertion is
#: only meaningful when the slow paths are slow.
DISPATCH_CONFIG = Config(
    heartbeat_interval_s=0.5,
    presence_ttl_s=12.0,
    reaper_interval_s=2.0,
    longpoll_timeout_s=8.0,
    scheduler_tick_s=30.0,
)


@contextmanager
def running_server(db_path: str, config: Config):
    """A real uvicorn on a real socket, on an ephemeral port.

    CLAUDE.md: integration tests go over real HTTP, never a mocked transport —
    mocked tests pass while the real thing deadlocks. This costs about a second
    of startup and is the only way the 404/410 handshake, the long-poll and the
    epoch fencing mean anything.
    """
    import threading

    import uvicorn

    from tss.api.app import create_app

    store = Store(db_path, config)
    app = create_app(config, store)
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning", lifespan="on")
    )
    thread = threading.Thread(target=server.run, name="tss-test-server", daemon=True)
    thread.start()

    deadline = time.monotonic() + 15
    while not server.started:
        if time.monotonic() > deadline:  # pragma: no cover
            raise RuntimeError("test server did not start")
        time.sleep(0.02)
    port = server.servers[0].sockets[0].getsockname()[1]

    try:
        yield f"http://127.0.0.1:{port}", config
    finally:
        server.should_exit = True
        thread.join(timeout=15)


@pytest.fixture
def live_server(db_path: str):
    """Short-lease server, for presence and reaping."""
    with running_server(db_path, FAST_CONFIG) as running:
        yield running


@pytest.fixture
def dispatch_server(db_path: str):
    """Normal leases, slow fallbacks — for dispatch and completion."""
    with running_server(db_path, DISPATCH_CONFIG) as running:
        yield running


AGENT_ID = "bench-sf-01"


def _devices(count: int) -> list[dict]:
    return [{"id": f"vg-{i:02d}", "capabilities": DEVICE_CAPS} for i in range(1, count + 1)]


class RunningAgent:
    """The real daemon, in this test's event loop, talking over the socket."""

    def __init__(self, base: str, agent_id: str = AGENT_ID, count: int = 3):
        self.agent = TestbedAgent(agent_id, _devices(count), base_url=base, hostname="test.local")
        self.stop = asyncio.Event()
        self.task: asyncio.Task | None = None

    async def __aenter__(self) -> TestbedAgent:
        self.task = asyncio.create_task(self.agent.run(self.stop))
        return self.agent

    async def __aexit__(self, *exc):
        self.stop.set()
        if self.task is not None:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self.task


#: What a Vehicle Gateway on a heavy-duty harness looks like to the matcher.
DEVICE_CAPS = {"product": "vehicle_gateway", "harness": "j1939"}


def inventory(count: int, *, caps: dict | None = None) -> list[InventoryItem]:
    """`count` identical devices, as an agent would report them (bench-local ids)."""
    return [
        InventoryItem(id=f"vg-{i:02d}", capabilities=dict(caps or DEVICE_CAPS))
        for i in range(1, count + 1)
    ]


def make_bench(store: Store, agent_id: str, devices: dict[str, dict] | list[str]) -> list[str]:
    """One machine with several devices cabled to it (§1.2). Returns resource ids."""
    store.create_agent(agent_id, hostname=f"{agent_id}.local", agent_version="0.1.0")
    if isinstance(devices, list):
        devices = {d: {"product": "vehicle_gateway", "harness": "j1939"} for d in devices}
    ids = []
    for name, caps in devices.items():
        rid = f"{agent_id}:{name}"
        store.add_resource(rid, agent_id, caps)
        ids.append(rid)
    return ids


def assert_i5(store: Store, agent_id: str) -> None:
    """I5, in the negative form the spec states it in: no resource of an offline
    agent is busy or holds a `current_job_id`.

    Stated positively ("all free") it would be wrong — a device that was broken,
    or that has been unplugged from the bench, is still broken or unplugged after
    the machine dies. The reap releases claims; it does not diagnose hardware.
    """
    assert store.get_agent(agent_id).state == AgentState.OFFLINE
    for resource in store.list_resources(agent_id):
        assert resource.state != ResourceState.BUSY, f"I5: {resource.id} still busy"
        assert resource.current_job_id is None, f"I5: {resource.id} still holds a job"


def submit(
    store: Store,
    job_id: str,
    n_devices: int,
    *,
    name: str | None = None,
    caps: dict | None = None,
    now: float | None = None,
) -> None:
    """A job needing `n_devices` identical devices, all on one bench.

    PASS `now` IN ANY TEST THAT REASONS ON A SYNTHETIC CLOCK. `submitted_at`
    defaults to wall-clock, and a job stamped `time.time()` is never starving
    relative to a T0 of 1,000,000 — so the starvation and reservation paths
    quietly do nothing and the test fails somewhere else entirely.
    """
    store.submit_job(
        job_id,
        name or job_id,
        [dict(caps or {"product": "vehicle_gateway"})] * n_devices,
        payload={"suite": "smoke"},
        now=now,
    )

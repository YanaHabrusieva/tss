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

import os
from collections.abc import Callable, Sequence

import pytest

from tss.core.models import ClaimResult
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


@pytest.fixture
def db_path(tmp_path) -> str:
    return str(tmp_path / "tss.db")


@pytest.fixture
def store(db_path: str) -> Store:
    s = Store(db_path)
    s.init_schema()
    yield s
    s.close()


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


def submit(store: Store, job_id: str, n_devices: int, *, name: str | None = None) -> None:
    """A job needing `n_devices` identical devices, all on one bench."""
    store.submit_job(
        job_id,
        name or job_id,
        [{"product": "vehicle_gateway"}] * n_devices,
        payload={"suite": "smoke"},
    )

"""Fan-out: one dead bench, several jobs, one requeue each (§3.5, §7.3 race 5).

The bench in these tests has four devices and is running TWO jobs — one across
three devices, one on the fourth. Iterating resources instead of jobs requeues
the three-device job three times, which bumps its epoch three times and burns its
retry budget on a single bench failure. Nothing errors. The job simply
dead-letters early, and there is no log line saying why.

`just test-naive` runs this file against tests/naive_reap.py, where the DISTINCT
and the state guard are both missing.
"""

from __future__ import annotations

import pytest

from tests.conftest import assert_i5, inventory, submit
from tss.core.models import AgentState, JobState, ResourceState

AGENT = "bench-sf-01"
T0 = 1_000_000.0
DEVICES = [f"{AGENT}:vg-{i:02d}" for i in (1, 2, 3, 4)]


@pytest.fixture
def loaded_bench(store):
    """4 devices: job-X spans three of them, job-Y has the fourth."""
    store.register_agent(AGENT, "bench.local", inventory(4), agent_version="0.1.0", now=T0)
    submit(store, "job-X", 3)
    submit(store, "job-Y", 1)
    x = store.claim_all("job-X", AGENT, DEVICES[:3], now=T0 + 1)
    y = store.claim_all("job-Y", AGENT, DEVICES[3:], now=T0 + 1)
    assert x.ok and y.ok
    return {"job-X": x, "job-Y": y}


def test_one_bench_two_jobs_two_requeues(store, reap, loaded_bench, config):
    dead_at = T0 + config.presence_ttl_s + 2

    result = reap(store, AGENT, now=dead_at)

    # Exactly two requeues — not four, one per device.
    assert sorted(result.requeued_jobs) == ["job-X", "job-Y"]
    assert result.dead_lettered_jobs == []

    for job_id in ("job-X", "job-Y"):
        job = store.get_job(job_id)
        claimed_epoch = loaded_bench[job_id].epoch
        assert job.state == JobState.QUEUED
        assert job.agent_id is None
        assert job.epoch == claimed_epoch + 1, (
            f"{job_id}: the epoch moved {job.epoch - claimed_epoch} times for one bench failure"
        )
        assert len(job.tried_agents) == 1, (
            f"{job_id}: one bench failure must cost exactly one entry in tried_agents, "
            f"got {job.tried_agents}"
        )
        assert job.tried_agents == [AGENT]

        requeues = [e for e in store.events(job_id=job_id) if e.kind == "job.requeued"]
        assert len(requeues) == 1, f"{job_id}: {len(requeues)} requeue events for one bench death"


def test_the_bench_goes_offline_holding_nothing(store, reap, loaded_bench, config):
    result = reap(store, AGENT, now=T0 + config.presence_ttl_s + 2)

    assert store.get_agent(AGENT).state == AgentState.OFFLINE
    resources = store.list_resources(AGENT)
    assert len(resources) == 4
    assert_i5(store, AGENT)
    # Every device here was claimed, so every device comes back free.
    assert all(r.state == ResourceState.FREE for r in resources)
    assert sorted(result.freed_resources) == DEVICES
    assert store.resources_held_by("job-X") == []
    assert store.resources_held_by("job-Y") == []


def test_the_allocation_record_is_closed_not_deleted(store, reap, loaded_bench, config):
    """`job_resources` is history — it survives the release, which is what makes
    it possible to answer "which devices did attempt 1 run on?" afterwards."""
    reap(store, AGENT, now=T0 + config.presence_ttl_s + 2)

    records = store.allocation_records("job-X")
    assert len(records) == 3
    assert {r["epoch"] for r in records} == {1}, "the record keeps the epoch it was claimed at"
    assert all(r["released_at"] is not None for r in records)


def test_a_job_on_its_last_bench_dead_letters_instead_of_requeueing(store, reap, config):
    """After MAX_DISTINCT_AGENTS benches, the job is the problem, not the fleet —
    and the benches it killed are not blamed for it (§4.3)."""
    submit(store, "job-poison", 1)
    for i, bench in enumerate(["bench-a", "bench-b", "bench-c"]):
        store.register_agent(bench, f"{bench}.local", inventory(1), now=T0 + i)
        assert store.claim_all("job-poison", bench, [f"{bench}:vg-01"], now=T0 + i).ok
        result = reap(store, bench, now=T0 + config.presence_ttl_s + 2 + i)

        job = store.get_job("job-poison")
        if i < config.max_distinct_agents - 1:
            assert result.requeued_jobs == ["job-poison"]
            assert job.state == JobState.QUEUED
        else:
            assert result.dead_lettered_jobs == ["job-poison"], (
                "3 distinct benches tried — this job is poison"
            )
            assert job.state == JobState.DEAD_LETTER
            assert job.outcome == "dead_letter"

    assert len(store.get_job("job-poison").tried_agents) == 3


def test_a_terminal_job_is_never_resurrected_by_a_reap(store, reap, loaded_bench, config):
    """I7. The bench died, but this job had already finished — a requeue here
    would overwrite a real outcome with a phantom retry."""
    store.conn.execute(
        "UPDATE jobs SET state = 'passed', outcome = 'passed', finished_at = ? WHERE id = 'job-Y'",
        (T0 + 5,),
    )

    result = reap(store, AGENT, now=T0 + config.presence_ttl_s + 2)

    assert result.requeued_jobs == ["job-X"]
    job = store.get_job("job-Y")
    assert job.state == JobState.PASSED
    assert job.outcome == "passed"
    assert job.epoch == loaded_bench["job-Y"].epoch, "a terminal job's epoch must not move"

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

import sqlite3

import pytest

from tests.conftest import assert_i5, inventory, submit
from tss.core.models import AgentState, JobState, Outcome, ResourceState

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
            assert job.outcome == Outcome.INFRA_ERROR

    assert len(store.get_job("job-poison").tried_agents) == 3


def test_a_dead_letter_is_an_infra_error_not_its_own_outcome(store, reap, config):
    """`state` says what happened; `outcome` says whose problem it is.

    A dead letter is the *worst* kind of infra failure — the job walked three
    benches and broke on all of them — so `outcome='dead_letter'` would repeat
    the state and throw away the one distinction the data model exists for, on
    exactly the jobs that failed worst. It is never the engineer's problem:
    dead-lettering only happens on the infra retry path, because FAILED is a real
    result and never retries (§4.2).
    """
    submit(store, "job-poison", 1)
    submit(store, "job-ordinary", 1)
    for i, bench in enumerate(["bench-a", "bench-b", "bench-c"]):
        store.register_agent(bench, f"{bench}.local", inventory(1), now=T0 + i)
        assert store.claim_all("job-poison", bench, [f"{bench}:vg-01"], now=T0 + i).ok
        reap(store, bench, now=T0 + config.presence_ttl_s + 2 + i)

    job = store.get_job("job-poison")
    assert job.state == JobState.DEAD_LETTER
    assert job.outcome == Outcome.INFRA_ERROR
    assert job.result_detail == "presence_expired after 3 benches", (
        "the specifics belong in result_detail, not in the outcome"
    )
    assert job.finished_at is not None

    # THE ASSERTION THAT WOULD HAVE CAUGHT IT: the reporting query the
    # distinction exists to serve. Counting infra failures must find the worst
    # ones, not silently exclude them.
    infra = store.conn.execute(
        "SELECT id, state FROM jobs WHERE outcome = 'infra_error' ORDER BY id"
    ).fetchall()
    assert [(r["id"], r["state"]) for r in infra] == [("job-poison", "dead_letter")]

    # ...and it is still distinguishable from an ordinary retryable infra error.
    assert store.get_job("job-ordinary").outcome is None
    assert [
        r["id"] for r in store.conn.execute("SELECT id FROM jobs WHERE state='dead_letter'")
    ] == ["job-poison"]


def test_dead_letter_is_not_a_permitted_outcome_at_all(store):
    """Belt and braces at the schema level: the CHECK constraint no longer
    accepts it, so no future write can reintroduce the ambiguity."""
    submit(store, "job-A", 1)
    with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
        store.conn.execute("UPDATE jobs SET outcome = 'dead_letter' WHERE id = 'job-A'")


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


def test_a_one_bench_fleet_cannot_ping_pong_a_job_forever(store, reap, config):
    """Poison detection counts DISTINCT benches, which is right — three failures
    across three machines means the job is the problem, not the fleet. But on a
    one-bench fleet that count never passes 1, so a job that keeps coming back
    returns to the same bench forever and never reaches a terminal state (I3).

    Presence expiry is the path that shows it plainly: a bench that keeps dying
    and rebooting requeues the job every time, and nothing here touches
    `consecutive_fails`, so quarantine attribution never intervenes to end it.

    The backstop is a LIVENESS bound, not an attribution rule — `tried_agents`
    still decides who is to blame, and one bench is still blamed once.
    """
    store.register_agent("bench-only", "b.local", inventory(1), now=T0)
    submit(store, "job-forever", 1)

    for cycle in range(config.max_total_attempts + 2):
        if store.get_job("job-forever").state == JobState.DEAD_LETTER:
            break
        now = T0 + cycle * 100
        store.register_agent("bench-only", "b.local", inventory(1), now=now)
        assert store.claim_all("job-forever", "bench-only", ["bench-only:vg-01"], now=now).ok
        reap(store, "bench-only", now=now + config.presence_ttl_s + 1)

    job = store.get_job("job-forever")
    assert job.state == JobState.DEAD_LETTER, "it would have gone round forever"
    assert job.outcome == Outcome.INFRA_ERROR, "never outcome='dead_letter' — that is a state"
    assert job.attempt >= config.max_total_attempts
    assert job.result_detail.endswith("attempts"), (
        "the record must say it ran out of attempts, not out of benches"
    )
    assert set(job.tried_agents) == {"bench-only"}, "one bench tried, one bench blamed"


def test_the_distinct_bench_rule_still_ends_it_first_on_a_real_fleet(store, reap, config):
    """The backstop must not preempt the attribution rule. Three benches, three
    failures: it dead-letters on DISTINCT benches at attempt 3, well before the
    total-attempts bound."""
    submit(store, "job-poison", 1)
    for i, bench_id in enumerate(["bench-a", "bench-b", "bench-c"]):
        store.register_agent(bench_id, f"{bench_id}.local", inventory(1), now=T0 + i)
        assert store.claim_all("job-poison", bench_id, [f"{bench_id}:vg-01"], now=T0 + i).ok
        reap(store, bench_id, now=T0 + i + config.presence_ttl_s + 1)

    job = store.get_job("job-poison")
    assert job.state == JobState.DEAD_LETTER
    assert job.attempt == 3 < config.max_total_attempts
    assert job.result_detail.endswith("benches"), "attribution, not exhaustion, ended this one"

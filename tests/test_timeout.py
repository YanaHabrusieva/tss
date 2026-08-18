"""Sweep 2: the job hung, the bench is fine (§3.5, I6).

The distinction this file exists for: a job that stops making progress on a
healthy, heartbeating bench, versus a job on a bench that died. They need
different responses — one job dies here, the whole bench empties there — and,
just as important, they must be TELLABLE APART AFTERWARDS. If both paths wrote
the same record, I6 would be an assertion about code rather than about data, and
the `hung` chaos profile in step 5 would prove nothing.
"""

from __future__ import annotations

import pytest

from tests.conftest import inventory, submit
from tss.core.directives import DirectiveQueue
from tss.core.invariants import check_all, check_i6
from tss.core.models import AgentState, JobState, Outcome
from tss.core.reaper import Reaper
from tss.core.store import DETAIL_PRESENCE, DETAIL_TIMEOUT

AGENT = "bench-sf-01"
T0 = 1_000_000.0
BUDGET = 600  # max_duration_s


@pytest.fixture
def directives():
    return DirectiveQueue()


@pytest.fixture
def reaper(store, config, directives):
    return Reaper(store, config, directives=directives)


def running_job(store, job_id="job-hang", *, agent_id=AGENT, devices=2, started_at=T0):
    """A job that has started and is holding a device on a live bench."""
    store.register_agent(agent_id, f"{agent_id}.local", inventory(devices), now=started_at)
    submit(store, job_id, 1)
    claim = store.claim_all(job_id, agent_id, [f"{agent_id}:vg-01"], now=started_at)
    assert claim.ok
    assert store.start_job(job_id, agent_id, claim.epoch, now=started_at) == "started"
    return claim


# ------------------------------------------------------------- the detection
def test_a_job_inside_its_budget_is_left_alone(store, reaper):
    running_job(store)

    assert store.timed_out_jobs(now=T0 + BUDGET - 1) == []
    assert reaper.sweep_timeouts(now=T0 + BUDGET - 1) == []
    assert store.get_job("job-hang").state == JobState.RUNNING


def test_a_job_past_its_budget_is_terminated_and_its_devices_freed(store, reaper, directives):
    claim = running_job(store, devices=2)
    # A second job on the SAME bench with a much longer budget — a soak test that
    # is behaving itself while its neighbour hangs.
    store.submit_job(
        "job-fine", "soak", [{"product": "vehicle_gateway"}], max_duration_s=10_000, now=T0
    )
    other = store.claim_all("job-fine", AGENT, [f"{AGENT}:vg-02"], now=T0)
    assert store.start_job("job-fine", AGENT, other.epoch, now=T0) == "started"

    ended = reaper.sweep_timeouts(now=T0 + BUDGET + 1)

    assert ended == ["job-hang"]
    hung = store.get_job("job-hang")
    assert hung.state == JobState.QUEUED, "first timeout is a retry, not a death"
    assert hung.epoch == claim.epoch + 1, "the epoch moves so the late report is fenced out"
    assert store.resources_held_by("job-hang") == []
    assert directives.peek(AGENT) == [{"cancel_job": "job-hang"}]

    # The bench and its other job are untouched: this is a JOB timeout.
    assert store.get_agent(AGENT).state == AgentState.ONLINE
    assert store.get_job("job-fine").state == JobState.RUNNING
    assert store.resources_held_by("job-fine") == [f"{AGENT}:vg-02"]
    assert check_all(store) == []


def test_the_bench_is_never_taken_offline_by_a_job_timeout(store, reaper, config):
    """I6's other half. The machine is heartbeating; only the job is stuck. A
    20-minute test must not look like a dead bench."""
    running_job(store)
    store.renew_presence(AGENT, now=T0 + BUDGET)  # still alive and beating

    reaper.sweep_timeouts(now=T0 + BUDGET + 1)

    assert store.get_agent(AGENT).state == AgentState.ONLINE
    assert store.expired_agents(now=T0 + BUDGET + 1) == []
    assert [e.kind for e in store.events(kind="agent.offline")] == []


def test_a_short_budget_is_respected(store, reaper):
    store.register_agent(AGENT, "b.local", inventory(1), now=T0)
    store.submit_job(
        "job-quick", "quick", [{"product": "vehicle_gateway"}], max_duration_s=5, now=T0
    )
    claim = store.claim_all("job-quick", AGENT, [f"{AGENT}:vg-01"], now=T0)
    store.start_job("job-quick", AGENT, claim.epoch, now=T0)

    assert reaper.sweep_timeouts(now=T0 + 4) == []
    assert reaper.sweep_timeouts(now=T0 + 6) == ["job-quick"]


# --------------------------------------------------------------- one retry
def test_a_job_that_hangs_twice_dead_letters_as_an_infra_error(store, reaper, config):
    """Retry once, then dead-letter — counted in timeouts, not in benches. A job
    that hangs on two machines is hanging because of itself; blaming three
    benches for it would take three machines out of rotation for a job that will
    hang on the fourth too."""
    running_job(store, agent_id="bench-a", devices=1, started_at=T0)
    assert reaper.sweep_timeouts(now=T0 + BUDGET + 1) == ["job-hang"]
    assert store.get_job("job-hang").state == JobState.QUEUED

    # It gets picked up by a second bench and hangs again.
    store.register_agent("bench-b", "b.local", inventory(1), now=T0 + 100)
    claim = store.claim_all("job-hang", "bench-b", ["bench-b:vg-01"], now=T0 + 100)
    store.start_job("job-hang", "bench-b", claim.epoch, now=T0 + 100)

    assert reaper.sweep_timeouts(now=T0 + 100 + BUDGET + 1) == ["job-hang"]

    job = store.get_job("job-hang")
    assert job.state == JobState.DEAD_LETTER
    assert job.outcome == Outcome.INFRA_ERROR, "never outcome='dead_letter' — it is a state"
    assert job.result_detail == "timeout after 2 attempts"
    assert len(job.tried_agents) == 2, "only two benches were tried; neither is blamed for three"
    assert store.resources_held_by("job-hang") == []
    assert check_all(store) == []

    # The reporting query that the FAILED-vs-INFRA_ERROR split exists to serve.
    infra = store.conn.execute("SELECT id FROM jobs WHERE outcome = 'infra_error'").fetchall()
    assert [r["id"] for r in infra] == ["job-hang"]


# ------------------------------------------------------------ I6, checkable
def test_the_two_termination_paths_are_distinguishable(store, reaper, config):
    """THE ASSERTION I6 RESTS ON.

    One job hangs while its bench keeps heartbeating; another job's bench dies.
    Both end up infra failures, and if both wrote the same `result_detail` there
    would be no way to prove which mechanism fired — the timeout sweep could
    quietly never run and presence expiry would cover for it.
    """
    # A hung job on a live bench, timed out twice so it lands terminal.
    running_job(store, "job-hung", agent_id="bench-live", devices=1, started_at=T0)
    reaper.sweep_timeouts(now=T0 + BUDGET + 1)
    store.renew_presence("bench-live", now=T0 + BUDGET + 2)  # it has been alive throughout
    claim = store.claim_all("job-hung", "bench-live", ["bench-live:vg-01"], now=T0 + BUDGET + 2)
    store.start_job("job-hung", "bench-live", claim.epoch, now=T0 + BUDGET + 2)
    reaper.sweep_timeouts(now=T0 + 2 * BUDGET + 3)

    # A job whose bench simply died, walked to death across three benches.
    submit(store, "job-orphan", 1)
    for i, bench in enumerate(["bench-x", "bench-y", "bench-z"]):
        store.register_agent(bench, f"{bench}.local", inventory(1), now=T0 + i)
        assert store.claim_all("job-orphan", bench, [f"{bench}:vg-01"], now=T0 + i).ok
        reaper.sweep_presence(now=T0 + config.presence_ttl_s + 2 + i)

    hung = store.get_job("job-hung")
    orphan = store.get_job("job-orphan")

    assert hung.state == JobState.DEAD_LETTER
    assert orphan.state == JobState.DEAD_LETTER
    assert hung.outcome == orphan.outcome == Outcome.INFRA_ERROR
    # ...and yet the records are not the same. This is I6.
    assert hung.result_detail.startswith(DETAIL_TIMEOUT)
    assert orphan.result_detail.startswith(DETAIL_PRESENCE)
    assert hung.result_detail != orphan.result_detail
    assert check_i6(store) == []


def test_the_checker_catches_a_record_that_names_the_wrong_path(store, reaper):
    """Proof the check has teeth: rewrite the record to claim presence expiry
    killed a job the timeout sweep killed, and I6 must fail."""
    running_job(store, agent_id="bench-a", devices=1)
    reaper.sweep_timeouts(now=T0 + BUDGET + 1)
    store.renew_presence("bench-a", now=T0 + BUDGET + 2)
    claim = store.claim_all("job-hang", "bench-a", ["bench-a:vg-01"], now=T0 + BUDGET + 2)
    store.start_job("job-hang", "bench-a", claim.epoch, now=T0 + BUDGET + 2)
    reaper.sweep_timeouts(now=T0 + 2 * BUDGET + 3)
    assert check_i6(store) == []

    store.conn.execute(
        "UPDATE jobs SET result_detail = 'presence_expired after 1 benches' WHERE id = 'job-hang'"
    )

    violations = check_i6(store)
    assert len(violations) == 1
    assert "indistinguishable" in violations[0]


def test_a_job_that_finishes_during_the_sweep_is_not_double_ended(store, reaper):
    """The sweep reads, then acts. In between, the agent's completion can land —
    and the guard on `state='running'` is what stops us ending a job twice."""
    claim = running_job(store, devices=1)
    stale = store.timed_out_jobs(now=T0 + BUDGET + 1)
    assert stale == [("job-hang", AGENT)]

    assert store.complete_job("job-hang", AGENT, claim.epoch, Outcome.PASSED, now=T0 + BUDGET) == (
        "accepted"
    )
    assert store.time_out_job("job-hang", now=T0 + BUDGET + 1) is None

    job = store.get_job("job-hang")
    assert job.state == JobState.PASSED
    assert job.outcome == Outcome.PASSED, "a real result is never overwritten by a sweep"
    assert check_all(store) == []


# ---------------------------------------------- post-review hardening (§3.5)
def test_the_timeout_sweep_pokes_the_scheduler(store, config):
    """Sweep 2 frees devices exactly as sweep 1 does, so it owes the queue the
    same wake-up. Without it those devices sit idle until the backstop tick —
    correct, and dead for a second, which is the shape of every latency bug this
    project has hit."""
    poked = []
    reaper = Reaper(store, config, on_reap=lambda: poked.append(1))
    running_job(store, devices=1)

    ended = reaper.sweep_timeouts(now=T0 + BUDGET + 1)

    assert ended == ["job-hang"]
    assert poked == [1], "devices came free and nobody was told"


def test_the_timeout_sweep_revalidates_its_deadline_inside_the_transaction(store, config):
    """The pre-read is advisory. Between selecting overdue jobs and acting on
    one, its deadline can move — a restart, a longer budget, or simply another
    writer getting there first. With one event loop this is unreachable; with the
    second writer that Postgres brings (§9) it is not."""
    claim = running_job(store, devices=1)
    assert store.timed_out_jobs(now=T0 + BUDGET + 1) == [("job-hang", AGENT)]

    # ...and now it is no longer overdue.
    store.conn.execute("UPDATE jobs SET started_at = ? WHERE id = 'job-hang'", (T0 + BUDGET,))

    assert store.time_out_job("job-hang", now=T0 + BUDGET + 1) is None
    job = store.get_job("job-hang")
    assert job.state == JobState.RUNNING, "a job inside its budget was killed anyway"
    assert job.epoch == claim.epoch, "and its fence moved for nothing"
    assert store.resources_held_by("job-hang") == [f"{AGENT}:vg-01"]

"""I8 — all-or-nothing. The claim commits every resource or none of them (§3.3).

Each test forces the claim to fail on the *last* resource of the set, in a
different way, and then asserts that every earlier resource in that transaction is
back to `free` with no owner, and that the job is untouched: still queued, same
epoch, same attempt count, nothing appended to `tried_agents`, no allocation
record, no event.

The failure this guards against is not a tidy error return. It is two devices
sitting `busy` with no owner and no lease to expire them, which is how a fleet
quietly loses hardware (§7.5).
"""

from __future__ import annotations

import sqlite3

import pytest

from tests.conftest import make_bench, submit
from tss.core.models import ResourceState
from tss.core.store import (
    REASON_JOB_NOT_QUEUED,
    REASON_RESOURCE_COUNT_MISMATCH,
    REASON_RESOURCE_UNAVAILABLE,
    SCHEMA_SQL,
)

AGENT = "bench-sf-01"
DEVICES = ["vg-01", "vg-02", "vg-03"]


@pytest.fixture
def bench(store):
    """One bench, three devices, one 3-device job queued and ready to claim."""
    resources = make_bench(store, AGENT, DEVICES)
    submit(store, "job-3dev", 3)
    return resources


def assert_nothing_held(store, resource_ids, job_id):
    """The whole point: no residue anywhere after a failed claim."""
    for rid in resource_ids:
        resource = store.get_resource(rid)
        assert resource.state == ResourceState.FREE, f"{rid} left {resource.state} after rollback"
        assert resource.current_job_id is None, f"{rid} still owned by {resource.current_job_id}"
        assert resource.last_assigned_at is None, f"{rid} kept a claim timestamp"

    job = store.get_job(job_id)
    assert job.state == "queued"
    assert job.agent_id is None
    assert job.epoch == 0, "a failed claim must not burn an epoch"
    assert job.attempt == 0, "a failed claim is not a dispatch"
    assert job.tried_agents == [], "a failed claim must not burn a retry slot"
    assert job.assigned_at is None
    assert store.allocation_records(job_id) == []
    assert store.events(job_id=job_id) == []


def test_claim_takes_the_whole_set_and_bumps_the_epoch_once(store, bench, claim):
    """Positive control: what success looks like, so the failures mean something."""
    result = claim(store, "job-3dev", AGENT, list(reversed(bench)))
    assert result.ok
    assert result.epoch == 1
    assert result.resource_ids == sorted(bench)

    assert store.resources_held_by("job-3dev") == sorted(bench)
    job = store.get_job("job-3dev")
    assert (job.state, job.epoch, job.attempt, job.tried_agents) == ("assigned", 1, 1, [AGENT])
    records = store.allocation_records("job-3dev")
    assert len(records) == 3
    assert {r["epoch"] for r in records} == {1}
    assert [e.kind for e in store.events(job_id="job-3dev")] == ["job.assigned"]


def test_rollback_when_the_last_resource_is_already_busy(store, bench, claim):
    """The ordinary lost race: someone took the last device first."""
    last = sorted(bench)[-1]
    submit(store, "job-decoy", 1)
    assert claim(store, "job-decoy", AGENT, [last]).ok

    result = claim(store, "job-3dev", AGENT, bench)

    assert not result.ok
    assert result.reason == REASON_RESOURCE_UNAVAILABLE
    assert result.blocked_by == last
    assert_nothing_held(store, sorted(bench)[:-1], "job-3dev")
    # The decoy keeps exactly what it had — the rollback is scoped to one claim.
    assert store.resources_held_by("job-decoy") == [last]


def test_rollback_when_the_last_update_raises(store, bench, claim):
    """A device fails mid-transaction — the case a release-on-failure loop misses.

    Injected at the database, not by monkeypatching the store, so the claim path
    under test is the real one. An exception between resource 2 and resource 3 is
    the same shape as the process dying there: if earlier resources survive as
    `busy`, they are held by nobody and nothing will ever free them.
    """
    last = sorted(bench)[-1]
    store.conn.execute(
        f"""CREATE TRIGGER injected_failure BEFORE UPDATE ON resources
            WHEN NEW.id = '{last}'
            BEGIN SELECT RAISE(ABORT, 'injected: device dropped off the bus'); END"""
    )

    with pytest.raises(sqlite3.Error, match="injected"):
        claim(store, "job-3dev", AGENT, bench)

    store.conn.execute("DROP TRIGGER injected_failure")
    assert_nothing_held(store, sorted(bench), "job-3dev")


def test_an_unexpected_database_error_is_not_swallowed_as_a_lost_race(store, bench, claim):
    """`sqlite3.OperationalError` covers both SQLITE_BUSY and "no such table". The
    first is a lost race; the second is a bug, and swallowing it would turn a
    schema mistake into a scheduler that silently never dispatches anything.

    The failure is injected at the very end of the transaction — after every
    resource has been taken and the job row updated — so the rollback has the most
    to undo."""
    store.conn.execute("DROP TABLE job_resources")

    with pytest.raises(sqlite3.OperationalError, match="job_resources"):
        claim(store, "job-3dev", AGENT, bench)

    store.conn.executescript(SCHEMA_SQL)
    for rid in sorted(bench):
        resource = store.get_resource(rid)
        assert resource.state == ResourceState.FREE, f"{rid} left {resource.state}"
        assert resource.current_job_id is None
    job = store.get_job("job-3dev")
    assert (job.state, job.epoch, job.attempt, job.tried_agents) == ("queued", 0, 0, [])


def test_rollback_when_the_job_is_no_longer_queued(store, bench, claim):
    """The second guard. The matcher read the job as queued; by the time the claim
    runs it has been cancelled. Without `AND state='queued'` the devices are taken
    for a job that will never run, and nothing frees them."""
    # Stands in for DELETE /v1/jobs, which arrives in step 3.
    store.conn.execute("UPDATE jobs SET state = 'cancelled' WHERE id = 'job-3dev'")

    result = claim(store, "job-3dev", AGENT, bench)

    assert not result.ok
    assert result.reason == REASON_JOB_NOT_QUEUED
    for rid in sorted(bench):
        resource = store.get_resource(rid)
        assert resource.state == ResourceState.FREE, f"{rid} taken for a cancelled job"
        assert resource.current_job_id is None
    assert store.allocation_records("job-3dev") == []


def test_partial_set_is_refused_outright(store, bench, claim):
    """I8 made structural: a 3-device job cannot be committed onto 2 devices, even
    if the caller asks for it. `resource_count = :n` is one more predicate in a
    guard that already had to be there."""
    result = claim(store, "job-3dev", AGENT, sorted(bench)[:2])

    assert not result.ok
    assert result.reason == REASON_RESOURCE_COUNT_MISMATCH
    assert_nothing_held(store, sorted(bench), "job-3dev")


def test_rollback_when_a_resource_is_on_another_bench(store, bench, claim):
    """Co-location, enforced by `AND agent_id = :agent_id` in the same guard. The
    devices on the intended bench must not be left holding a set that can never be
    completed."""
    other = make_bench(store, "bench-sf-02", ["vg-01"])[0]
    mixed = sorted(bench)[:2] + [other]

    result = claim(store, "job-3dev", AGENT, mixed)

    assert not result.ok
    assert result.reason == REASON_RESOURCE_UNAVAILABLE
    assert result.blocked_by == other
    assert_nothing_held(store, sorted(bench), "job-3dev")
    assert store.get_resource(other).state == ResourceState.FREE


def test_resource_updates_are_issued_in_sorted_order(store, bench, claim):
    """Lock ordering (§3.3). Invisible in SQLite — BEGIN IMMEDIATE serializes
    writers, so a cycle can never form — and an aborted transaction in Postgres on
    the day we port. Observed here with a trigger, because the ordering is a
    property of the statements issued, not of the final state."""
    store.conn.executescript(
        """
        CREATE TABLE claim_order_log (seq INTEGER PRIMARY KEY AUTOINCREMENT, resource_id TEXT);
        CREATE TRIGGER log_claim_order AFTER UPDATE ON resources
        BEGIN INSERT INTO claim_order_log (resource_id) VALUES (NEW.id); END;
        """
    )

    assert claim(store, "job-3dev", AGENT, list(reversed(sorted(bench)))).ok

    issued = [r["resource_id"] for r in store.conn.execute("SELECT * FROM claim_order_log")]
    assert issued == sorted(bench), "resource UPDATEs must be issued sorted by resource_id"


def test_the_claim_revalidates_the_agent_inside_the_transaction(store, bench, claim):
    """The matcher read the fleet a moment ago; the claim commits now. An agent
    that went offline, was quarantined, or let its lease lapse in between must
    not be handed devices — the pre-read is advisory (CLAUDE.md, ownership)."""
    import time as _time

    now = _time.time()
    store.conn.execute("UPDATE agents SET presence_expires_at = ? WHERE id = ?", (now - 1, AGENT))

    result = claim(store, "job-3dev", AGENT, bench)

    assert not result.ok, "devices were claimed on a bench whose lease had expired"
    assert_nothing_held(store, sorted(bench), "job-3dev")


def test_the_claim_refuses_a_quarantined_bench(store, bench, claim):
    store.conn.execute(
        "UPDATE agents SET state = 'quarantined', quarantined_at = 1 WHERE id = ?", (AGENT,)
    )

    result = claim(store, "job-3dev", AGENT, bench)

    assert not result.ok
    assert_nothing_held(store, sorted(bench), "job-3dev")

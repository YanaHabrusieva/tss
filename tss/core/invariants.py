"""Machine-checkable safety properties, read straight from the database (§3.8).

These are the invariants the DB alone can prove. I1 and I4 cannot be checked here
and are not faked: TSS's record of a resource's capabilities is exactly what the
agent claimed, and TSS's record of ownership can never show two owners because
the reaper clears the dead one. Checking TSS against itself proves nothing, so
those two are checked against the mock agents' ground truth by the chaos harness
in step 5.

Each function returns a list of human-readable violations — empty means it holds.
"""

from __future__ import annotations

import json

from tss.core.models import TERMINAL_JOB_STATES, AgentState, ResourceState
from tss.core.store import Store

#: Job states in which a job is holding hardware.
HOLDING_STATES = ("assigned", "running")


def check_i2(store: Store) -> list[str]:
    """No resource is held by two jobs.

    Structural by construction — `resources.current_job_id` is a single nullable
    column, so a device physically cannot reference two jobs, and the claim's
    `WHERE state='free'` guard is what keeps it honest. What is checkable is the
    durable record: two open allocations for one device means it happened.
    """
    holders: dict[str, list[str]] = {}
    for record in store.allocation_records():
        if record["released_at"] is None:
            holders.setdefault(record["resource_id"], []).append(record["job_id"])
    return [
        f"I2: {resource_id} is held by {sorted(jobs)}"
        for resource_id, jobs in holders.items()
        if len(jobs) > 1
    ]


def check_i5(store: Store) -> list[str]:
    """No resource of an OFFLINE agent is busy or holds a job.

    Stated negatively on purpose. "All free" would be wrong: a device that was
    broken, or that has been unplugged from the bench, is still broken or
    unplugged after the machine dies. The reap releases claims; it does not
    diagnose hardware.
    """
    violations = []
    offline = {a.id for a in store.agents() if a.state == AgentState.OFFLINE}
    for resource in store.list_resources():
        if resource.agent_id not in offline:
            continue
        if resource.state == ResourceState.BUSY:
            violations.append(f"I5: {resource.id} is busy on an offline agent")
        if resource.current_job_id is not None:
            violations.append(f"I5: {resource.id} still holds {resource.current_job_id}")
    return violations


def check_i8(store: Store) -> list[str]:
    """A job in assigned/running holds EXACTLY `resource_count` resources.

    I8 at rest, and it is a different check from the one in the claim. The
    `resource_count = :n` guard proves the job took the right number of devices
    at claim time; this proves it still has them. Nothing else would catch a
    release path that frees one device of a running job — which is precisely why
    `complete_job` frees them with a single statement keyed on `current_job_id`
    rather than a loop.
    """
    rows = store.conn.execute(
        f"""SELECT j.id AS job_id, j.state, j.resource_count,
                   (SELECT COUNT(*) FROM resources r WHERE r.current_job_id = j.id) AS held
              FROM jobs j
             WHERE j.state IN {HOLDING_STATES}"""
    ).fetchall()
    return [
        f"I8: {row['job_id']} ({row['state']}) requires {row['resource_count']} "
        f"resources but holds {row['held']}"
        for row in rows
        if row["held"] != row["resource_count"]
    ]


def check_i6(store: Store) -> list[str]:
    """A hung job is terminated by the job-timeout sweep, not by presence expiry.

    This is only checkable because the two paths leave different records. Every
    requeue and every dead-letter writes a `result_detail` that LEADS with the
    reason that ended the attempt — `timeout...` from sweep 2, `presence_expired...`
    from sweep 1 — and emits an event carrying the same reason. So the check is:
    does the terminal record agree with the last thing that actually happened to
    it?

    If both paths wrote the same string there would be nothing to compare, I6
    would be an assertion about the code rather than about the data, and the
    `hung` chaos profile in step 5 would prove nothing.
    """
    violations = []
    rows = store.conn.execute(
        """SELECT j.id, j.state, j.result_detail,
                  (SELECT e.detail FROM events e
                    WHERE e.job_id = j.id
                      AND e.kind IN ('job.requeued','job.dead_letter')
                    ORDER BY e.seq DESC LIMIT 1) AS last_cause
             FROM jobs j
            WHERE j.state IN ('infra_error','dead_letter')"""
    ).fetchall()
    for row in rows:
        if not row["last_cause"]:
            continue
        reason = json.loads(row["last_cause"]).get("reason")
        detail = row["result_detail"] or ""
        if reason and not detail.startswith(reason):
            violations.append(
                f"I6: {row['id']} ended via {reason!r} but its record says {detail!r} — "
                "the two termination paths are indistinguishable"
            )
    return violations


def check_i7(store: Store) -> list[str]:
    """A terminal job's outcome is never overwritten.

    Two halves. The event log must not show a job reaching a terminal state
    twice — that is the late "PASSED" landing on top of a CANCELLED, which is
    what the epoch bump on cancel exists to prevent. And the outcome column must
    agree with the state: an outcome on a job that is still queued, or a terminal
    job with no outcome at all, means something wrote half a transition.
    """
    violations = []
    for row in store.conn.execute(
        """SELECT job_id, COUNT(*) AS n, GROUP_CONCAT(kind) AS kinds
             FROM events
            WHERE kind IN ('job.completed','job.cancelled','job.dead_letter')
              AND job_id IS NOT NULL
            GROUP BY job_id HAVING n > 1"""
    ):
        violations.append(
            f"I7: {row['job_id']} reached a terminal state {row['n']} times ({row['kinds']})"
        )

    for row in store.conn.execute("SELECT id, state, outcome FROM jobs"):
        terminal = row["state"] in TERMINAL_JOB_STATES
        if terminal and row["outcome"] is None:
            violations.append(f"I7: {row['id']} is {row['state']} with no outcome recorded")
        if not terminal and row["outcome"] is not None:
            violations.append(
                f"I7: {row['id']} is {row['state']} but already carries outcome={row['outcome']!r}"
            )
    return violations


#: Every DB-checkable invariant, for the chaos watchdog and for tests.
DB_CHECKS = (check_i2, check_i5, check_i6, check_i7, check_i8)


def check_all(store: Store) -> list[str]:
    violations: list[str] = []
    for check in DB_CHECKS:
        violations.extend(check(store))
    return violations

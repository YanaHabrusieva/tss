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

from tss.core import matcher
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
    violations = [
        f"I8: {row['job_id']} ({row['state']}) requires {row['resource_count']} "
        f"resources but holds {row['held']}"
        for row in store.conn.execute(
            f"""SELECT j.id AS job_id, j.state, j.resource_count,
                       (SELECT COUNT(*) FROM resources r WHERE r.current_job_id = j.id) AS held
                  FROM jobs j
                 WHERE j.state IN {HOLDING_STATES}"""
        )
        if row["held"] != row["resource_count"]
    ]

    # ...and the converse, which is the half that catches an ORPHAN HOLD: a
    # device still pointing at a job that is not holding anything any more.
    # Counting from the job's side alone cannot see this — a release that frees
    # one device of a finished job leaves the remaining device busy, owned by a
    # terminal job, and every job-side count still balances. That device is lost
    # to the fleet with nothing left to free it.
    violations.extend(
        f"I8: {row['id']} is held by {row['current_job_id']}, which is {row['state']}"
        for row in store.conn.execute(
            f"""SELECT r.id, r.current_job_id, j.state
                  FROM resources r JOIN jobs j ON j.id = r.current_job_id
                 WHERE r.current_job_id IS NOT NULL AND j.state NOT IN {HOLDING_STATES}"""
        )
    )
    return violations


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


def check_i9(scheduler) -> list[str]:
    """At most one job reserves; its reservations are all on one FEASIBLE agent;
    and no reserved resource is claimed by another job.

    Checked against SCHEDULER STATE, not the database — unlike every other check
    in this file — because a reservation deliberately leaves no database trace
    (§3.4.1). That is the whole point: a reserved device is still `free` and
    owned by nobody, so there is nothing in the DB to inspect.

    Every clause is a bug someone would otherwise ship:

      one reserver          two reservers holding partial sets deadlock in
                            bookkeeping, exactly as two jobs holding partial
                            hardware deadlock in §7.5
      one feasible agent    reservations spread across benches idle devices
                            toward a set that can never be assembled, because
                            allocation is single-bench
      not claimed           the reservation is only real if the claim path
                            actually respects it
    """
    reservation = getattr(scheduler, "reservation", None)
    if reservation is None:
        return []

    violations = []
    store = scheduler.store
    job = store.get_job(reservation.job_id)
    if job is None or job.state != "queued":
        violations.append(
            f"I9: reserving for {reservation.job_id}, which is "
            f"{'gone' if job is None else job.state} — a reservation outlived its job"
        )

    resources = {r.id: r for r in store.list_resources()}
    for resource_id in sorted(reservation.resource_ids):
        resource = resources.get(resource_id)
        if resource is None:
            violations.append(f"I9: reserved {resource_id} does not exist")
            continue
        if resource.agent_id != reservation.agent_id:
            violations.append(
                f"I9: reserved {resource_id} is on {resource.agent_id}, "
                f"but the reservation targets {reservation.agent_id}"
            )
        # RESERVE IS NOT CLAIM: a reserved device must not be owned by anyone
        # ELSE. Phrased that way on purpose. The reserving job taking its own set
        # is the reservation succeeding, and a reservation is a decision snapshot
        # that the fleet keeps moving underneath — a device can legitimately go
        # unhealthy a moment after it was withheld, and the next pass recomputes.
        # Reporting that would be reporting the checker's staleness as the
        # scheduler's bug, and a checker that cries wolf gets ignored.
        if resource.current_job_id not in (None, reservation.job_id):
            violations.append(
                f"I9: reserved {resource_id} is claimed by {resource.current_job_id}, "
                f"not by the reserving job {reservation.job_id}"
            )

    if job is not None:
        # Feasibility counts everything still cabled to the bench, including
        # devices that are unhealthy right now: those get repaired, and the
        # scheduler re-evaluates every pass. What this catches is the reservation
        # that can NEVER be satisfied — two devices held for a three-device job.
        inventory = [
            r.model_copy(update={"state": ResourceState.FREE})
            for r in resources.values()
            if r.agent_id == reservation.agent_id and r.state != ResourceState.RETIRED
        ]
        if not matcher.could_ever_satisfy(job.requirements, inventory):
            violations.append(
                f"I9: reserving on {reservation.agent_id}, which could never satisfy "
                f"{job.id} — those devices idle forever, for nothing"
            )
    return violations


#: Every DB-checkable invariant, for the chaos watchdog and for tests.
DB_CHECKS = (check_i2, check_i5, check_i6, check_i7, check_i8)


def check_all(store: Store, scheduler=None) -> list[str]:
    """Every invariant this process can check. Pass the scheduler to include I9,
    which lives in memory rather than in the database."""
    violations: list[str] = []
    for check in DB_CHECKS:
        violations.extend(check(store))
    if scheduler is not None:
        violations.extend(check_i9(scheduler))
    return violations

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

from tss.core.models import AgentState, ResourceState
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


#: Every DB-checkable invariant, for the chaos watchdog and for tests.
DB_CHECKS = (check_i2, check_i5, check_i8)


def check_all(store: Store) -> list[str]:
    violations: list[str] = []
    for check in DB_CHECKS:
        violations.extend(check(store))
    return violations

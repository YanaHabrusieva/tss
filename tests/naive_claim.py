"""The foil: the allocation code you get if you don't think about it (§7.5).

This is kept in the repo deliberately. `just test-naive` runs the real test suite
against it and both allocation tests fail — which is the only way to know the
tests are worth anything. A test that passes before the fix proves nothing.

Two variants, because the second is the one that looks fine in review:

  naive_claim_all           check-then-act, one autocommitted UPDATE per resource,
                            no rollback of earlier resources, epoch computed in
                            Python, no guard on the job's state.

  naive_claim_all_cleanup   the same, plus "release what I already took" on the
                            tidy failure path. This is the version an AI will
                            offer when you point out the first one, and it is
                            still wrong: an exception (or a process death)
                            between resource 2 and resource 3 leaves two devices
                            held by nobody, with no lease to expire them — and it
                            still double-books under concurrency, because the gap
                            between SELECT and UPDATE is where the race lives.

Neither writes anything to production code paths; nothing in `tss/` imports this.
"""

from __future__ import annotations

import time
from collections.abc import Sequence

from tss.core.models import ClaimResult
from tss.core.store import REASON_RESOURCE_UNAVAILABLE, Store

_SELECT_RESOURCE = "SELECT state FROM resources WHERE id = ? AND agent_id = ?"
_TAKE_RESOURCE = (
    "UPDATE resources SET state = 'busy', current_job_id = ?, last_assigned_at = ? WHERE id = ?"
)
_GIVE_BACK = "UPDATE resources SET state = 'free', current_job_id = NULL WHERE id = ?"
_TAKE_JOB = (
    "UPDATE jobs SET state = 'assigned', agent_id = ?, epoch = ?, attempt = attempt + 1, "
    "assigned_at = ? WHERE id = ?"
)
_RECORD = "INSERT INTO job_resources (job_id, resource_id, epoch, claimed_at) VALUES (?, ?, ?, ?)"


def _claim(
    store: Store,
    job_id: str,
    agent_id: str,
    resource_ids: Sequence[str],
    *,
    cleanup: bool,
) -> ClaimResult:
    conn = store.conn
    now = time.time()
    taken: list[str] = []

    for resource_id in resource_ids:  # note: whatever order the caller passed
        row = conn.execute(_SELECT_RESOURCE, (resource_id, agent_id)).fetchone()
        # ...and here is the window. Another thread claims it between this check
        # and the UPDATE below, and both callers think they own the device.
        if row is None or row["state"] != "free":
            if cleanup:
                for held in taken:
                    conn.execute(_GIVE_BACK, (held,))
            return ClaimResult(
                ok=False,
                job_id=job_id,
                agent_id=agent_id,
                reason=REASON_RESOURCE_UNAVAILABLE,
                blocked_by=resource_id,
            )
        conn.execute(_TAKE_RESOURCE, (job_id, now, resource_id))
        taken.append(resource_id)

    # Read-then-write epoch, in Python, outside any transaction.
    epoch = int(conn.execute("SELECT epoch FROM jobs WHERE id = ?", (job_id,)).fetchone()["epoch"])
    epoch += 1
    conn.execute(_TAKE_JOB, (agent_id, epoch, now, job_id))  # no state='queued' guard
    for resource_id in taken:
        conn.execute(_RECORD, (job_id, resource_id, epoch, now))

    return ClaimResult(
        ok=True, job_id=job_id, agent_id=agent_id, epoch=epoch, resource_ids=list(taken)
    )


def naive_claim_all(
    store: Store, job_id: str, agent_id: str, resource_ids: Sequence[str]
) -> ClaimResult:
    return _claim(store, job_id, agent_id, resource_ids, cleanup=False)


def naive_claim_all_cleanup(
    store: Store, job_id: str, agent_id: str, resource_ids: Sequence[str]
) -> ClaimResult:
    return _claim(store, job_id, agent_id, resource_ids, cleanup=True)

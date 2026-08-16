"""The second foil: the fan-out you get without the DISTINCT (§3.5, §7.3 race 5).

This is the most likely bug in the whole reaper and it is completely silent —
nothing errors, no exception is logged, the job just mysteriously dead-letters
early with its retry budget burned on a single bench failure.

Two things in the real implementation independently prevent it, and this foil
lacks both, because a version that had either would not be the version anyone
actually writes:

  * `SELECT DISTINCT current_job_id` — a bench running one job across three
    devices appears three times if you iterate resources.
  * `WHERE state IN ('assigned','running')` on the requeue — without it, the
    second and third passes over the same job bump the epoch again.

Nothing in `tss/` imports this.
"""

from __future__ import annotations

import json
import time

from tss.core.models import ReapResult
from tss.core.store import Store

_UNGUARDED_REQUEUE = """
UPDATE jobs
   SET state = 'queued', agent_id = NULL, epoch = epoch + 1,
       assigned_at = NULL, started_at = NULL
 WHERE id = ?
"""


def naive_reap_agent(
    store: Store, agent_id: str, *, now: float | None = None, reason: str = "presence_expired"
) -> ReapResult:
    conn = store.conn
    now = time.time() if now is None else now
    conn.execute("BEGIN IMMEDIATE")

    requeued: list[str] = []
    # ...one row per RESOURCE, not per job.
    rows = conn.execute(
        """SELECT id, current_job_id FROM resources
            WHERE agent_id = ? AND current_job_id IS NOT NULL""",
        (agent_id,),
    ).fetchall()
    for row in rows:
        job_id = row["current_job_id"]
        conn.execute(_UNGUARDED_REQUEUE, (job_id,))
        tried = json.loads(
            conn.execute("SELECT tried_agents FROM jobs WHERE id = ?", (job_id,)).fetchone()[0]
        )
        tried.append(agent_id)
        conn.execute("UPDATE jobs SET tried_agents = ? WHERE id = ?", (json.dumps(tried), job_id))
        conn.execute(
            "INSERT INTO events (ts, kind, agent_id, job_id, detail) VALUES (?, ?, ?, ?, ?)",
            (now, "job.requeued", agent_id, job_id, json.dumps({"reason": reason})),
        )
        requeued.append(job_id)

    freed = [
        r["id"]
        for r in conn.execute(
            "SELECT id FROM resources WHERE agent_id = ? AND state != 'free'", (agent_id,)
        )
    ]
    conn.execute(
        "UPDATE resources SET state = 'free', current_job_id = NULL WHERE agent_id = ?", (agent_id,)
    )
    conn.execute("UPDATE agents SET state = 'offline' WHERE id = ?", (agent_id,))
    conn.execute(
        "INSERT INTO events (ts, kind, agent_id, detail) VALUES (?, ?, ?, ?)",
        (now, "agent.offline", agent_id, json.dumps({"reason": reason, "requeued": requeued})),
    )
    conn.execute("COMMIT")
    return ReapResult(agent_id=agent_id, freed_resources=freed, requeued_jobs=requeued)

"""The fourth foil: reserving by CLAIMING (§3.4.1, §7.5).

"Reserve" sounds like a weak word for "hold", so the obvious implementation is to
take the device now and wait for its siblings — mark it busy so nobody else can
have it. That is the partial hold, and it is §7.5's deadlock with a friendlier
name:

    job-A needs 2 VGs.  Takes bench-01:vg-01.  Waits for a second VG.
    job-B needs 2 VGs.  Takes bench-01:vg-02.  Waits for a second VG.
                        Neither can ever proceed. Both hold hardware forever.

Two things make this foil the shape people actually write. It has no
single-reserver rule — spreading the free devices among the waiting jobs looks
*fairer* than making one of them wait. And it advances each starving job by one
device per pass, because devices come free one at a time in a real fleet.

Note what does NOT catch it. The job stays `queued`, so I8 says nothing: I8 is
about jobs in assigned/running. `claim_all` would refuse a partial set outright
(`resource_count = :n`), so this writes the resource row directly — which is
exactly why the real reservation lives in memory and touches no rows at all.

Nothing in `tss/` imports this.
"""

from __future__ import annotations

import time

from tss.core import matcher
from tss.core.scheduler import Scheduler


class ClaimingReservationScheduler(Scheduler):
    """Reserves by taking hardware, one device at a time, for every starving job."""

    def pass_once(self, *, now: float | None = None):
        now = time.time() if now is None else now
        self._grab_for_starving_jobs(now)
        return super().pass_once(now=now)

    def _grab_for_starving_jobs(self, now: float) -> None:
        conn = self.store.conn
        online = {a.id for a in self.store.online_agents(now=now)}
        for job in self.store.queued_jobs():
            if now - job.submitted_at < self.config.starvation_threshold_s:
                continue
            held = self.store.resources_held_by(job.id)
            if len(held) >= job.resource_count:
                continue
            for resource in self.store.list_resources():
                if resource.agent_id not in online or resource.state != "free":
                    continue
                if not any(matcher.satisfies(resource, spec) for spec in job.requirements):
                    continue
                # ...and here the reservation takes the device. One per pass, so
                # the next starving job gets the next one that frees.
                conn.execute(
                    "UPDATE resources SET state = 'busy', current_job_id = ? WHERE id = ?",
                    (job.id, resource.id),
                )
                break

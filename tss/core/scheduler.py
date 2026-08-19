"""The scheduler — decides, never writes (§3.4).

It reads the queue and the free devices, asks the matcher which set goes where,
and calls `store.claim_all(...)` to commit it. Every write in this file goes
through the store; there is no SQL here.

THE LOST WAKEUP. A resource frees up in the middle of a pass, the notify lands
while that pass is still running, and if the pass clears the flag on its way out
it erases a wakeup for a change it never saw. The queue then sits still with free
devices and queued jobs and no reason for anyone to look again. Two defences,
both required:

  * `_wake.clear()` happens BEFORE reading state, never after. A notify that
    arrives during a pass survives it and the next iteration runs immediately.
  * a backstop tick (`scheduler_tick_s`, 1s). Even if a notify is lost some other
    way, the queue moves within a second. Belt and braces, because a stalled
    queue is invisible — nothing errors, the fleet just quietly stops working.

MeteorShower's `wait_for_allocation` carries this exact comment. It was a real
bug that was really hit.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass

from tss.core import matcher
from tss.core.config import DEFAULT, Config
from tss.core.models import (
    Assignment,
    CapabilitySpec,
    ClaimResult,
    Job,
    Resource,
    ResourceState,
)
from tss.core.store import BLOCKED_NO_CAPABLE_AGENT, Store

log = logging.getLogger("tss.scheduler")


@dataclass(frozen=True)
class Reservation:
    """Devices withheld from other jobs so a starving one can eventually run.

    RESERVE IS NOT CLAIM, and this distinction carries the whole safety argument
    (§3.4.1). The resources named here are still `free` in the database with no
    owner; nothing is written anywhere. A job that needs three devices and can
    see one free DOES NOT TAKE IT — it takes nothing, and the scheduler merely
    declines to offer that device to anyone else.

    That is wait-WITHOUT-hold, so no cycle can form. The moment a reservation
    marks a resource busy or sets `current_job_id`, it has re-invented the
    partial hold and §7.5's deadlock is back — and I8 will not catch it, because
    the job is not assigned yet.

    Held in scheduler memory and recomputed from scratch every pass, so a crash
    mid-wait leaves nothing to clean up, and the target follows the fleet: if
    another bench frees a full set first, the job takes that instead.
    """

    job_id: str
    agent_id: str
    resource_ids: frozenset[str]
    since: float


class Scheduler:
    def __init__(self, store: Store, config: Config = DEFAULT) -> None:
        self.store = store
        self.config = config
        self.passes = 0
        #: At most ONE reservation exists at a time (§3.4.1). Two jobs each
        #: holding partial sets is a deadlock you built yourself — in bookkeeping
        #: rather than in hardware, but with the same outcome.
        self.reservation: Reservation | None = None
        self._wake = asyncio.Event()
        self._agent_wake: dict[str, asyncio.Event] = {}
        self._task: asyncio.Task | None = None

    # ------------------------------------------------------------- the pass
    def pass_once(self, *, now: float | None = None) -> list[ClaimResult]:
        """One scheduling pass. Pure decision-making plus `store.claim_all`.

        Walks the queue oldest-first and skips a job no bench can satisfy rather
        than stopping at it. §3.4 step 6 says "stop when no agent can satisfy the
        head", but a literal head-of-line block would idle a bench that has a
        free, compatible device while a matching job waits — which §1.3 names as
        the utilization target the whole two-level model exists for. A job needing
        an asset gateway must not hold up a job needing a vehicle gateway that is
        sitting free. Starvation of big multi-device jobs is what reservation
        solves in step 5; it is not solved by blocking the queue.
        """
        now = time.time() if now is None else now
        jobs = self.store.queued_jobs()
        if not jobs:
            self._set_reservation(None, now)  # nothing to starve, nothing to withhold
            self.passes += 1
            return []

        online = {agent.id for agent in self.store.online_agents(now=now)}
        resources = self.store.list_resources()
        installed_by_agent: dict[str, list[Resource]] = defaultdict(list)
        free_by_agent: dict[str, list[Resource]] = defaultdict(list)
        for resource in resources:
            if resource.agent_id not in online:
                continue
            installed_by_agent[resource.agent_id].append(resource)
        for resource in matcher.offerable(resources):
            if resource.agent_id in online:
                free_by_agent[resource.agent_id].append(resource)

        # Reservations are computed FIRST (§3.4 step 2): the walk below has to
        # know which devices are being held for the starving job before it starts
        # handing them out.
        self._set_reservation(
            self._recompute_reservation(jobs, installed_by_agent, free_by_agent, now), now
        )
        reserved = self.reservation.resource_ids if self.reservation else frozenset()

        results: list[ClaimResult] = []
        for job in jobs:
            if not free_by_agent:
                break
            offer = free_by_agent
            if reserved and (self.reservation is None or job.id != self.reservation.job_id):
                offer = {
                    agent_id: [r for r in pool if r.id not in reserved]
                    for agent_id, pool in free_by_agent.items()
                }
                offer = {a: pool for a, pool in offer.items() if pool}
            for candidate in matcher.rank_matches(job.requirements, offer):
                result = self.store.claim_all(
                    job.id, candidate.agent_id, candidate.resource_ids, now=now
                )
                if not result.ok:
                    # A lost race, not an error: another pass or another process
                    # took one of these devices. Try the next bench (§3.4 step 5).
                    log.debug("claim for %s on %s: %s", job.id, candidate.agent_id, result.reason)
                    continue
                results.append(result)
                claimed = set(result.resource_ids)
                remaining = [r for r in free_by_agent[candidate.agent_id] if r.id not in claimed]
                if remaining:
                    free_by_agent[candidate.agent_id] = remaining
                else:
                    del free_by_agent[candidate.agent_id]
                self.wake_agent(candidate.agent_id)
                log.info(
                    "assigned %s -> %s %s (epoch %d)",
                    job.id,
                    candidate.agent_id,
                    result.resource_ids,
                    result.epoch,
                )
                break

        self.passes += 1
        return results

    # -------------------------------------------------------- reservations
    def _recompute_reservation(
        self,
        jobs: list[Job],
        installed_by_agent: dict[str, list[Resource]],
        free_by_agent: dict[str, list[Resource]],
        now: float,
    ) -> Reservation | None:
        """Three steps, and step 1 is the one that is easy to miss (§3.4.1).

        Recomputed from scratch every pass. That is what makes a reservation
        released "the instant it stops being needed" a property of the design
        rather than of some cleanup path: a job that dispatched, was cancelled or
        dead-lettered simply is not the oldest starving job any more, so nothing
        is withheld on the very next pass.
        """
        starving = None
        feasible: dict[str, list[Resource]] = {}
        # Every starving job is assessed, not just the one that ends up
        # reserving. An unsatisfiable job that happens to be younger than another
        # starving job would otherwise never be flagged and never dead-letter —
        # it would simply sit there, which is the failure mode §3.4.1 is about.
        for job in self._starving_jobs(jobs, free_by_agent, now):
            # STEP 1 — feasibility. Only benches whose total installed inventory
            # could ever satisfy this job, ignoring what is free right now.
            candidates = {
                agent_id: pool
                for agent_id, pool in installed_by_agent.items()
                if matcher.could_ever_satisfy(job.requirements, pool)
            }
            if not candidates:
                # Nothing to reserve TOWARD. Reserving here would idle devices
                # forever for a job no bench can run, and from the outside that
                # is indistinguishable from a broken scheduler.
                self._mark_unsatisfiable(job, now)
                continue
            self._clear_unsatisfiable(job)
            if starving is None:
                # The oldest starving job that something could actually run.
                starving, feasible = job, candidates
        if starving is None:
            return None

        # STEP 2 — pick exactly ONE target: closest to satisfying the job (most
        # matching devices already free), tie-broken on fewest busy. Reserving
        # across benches would idle devices to assemble a set that can never be
        # assembled, because allocation is single-bench (§1.2).
        def score(agent_id: str) -> tuple[int, int, str]:
            free_here = [
                r
                for r in free_by_agent.get(agent_id, [])
                if any(matcher.satisfies(r, spec) for spec in starving.requirements)
            ]
            busy_here = sum(1 for r in feasible[agent_id] if r.state == ResourceState.BUSY)
            return (-len(free_here), busy_here, agent_id)

        target = min(feasible, key=score)

        # STEP 3 — withhold that bench's free matching devices. Everything on
        # every other bench keeps flowing.
        held = frozenset(
            r.id
            for r in free_by_agent.get(target, [])
            if any(matcher.satisfies(r, spec) for spec in starving.requirements)
        )
        previous = self.reservation
        since = (
            previous.since
            if previous and previous.job_id == starving.id and previous.agent_id == target
            else now
        )
        if previous is None or previous.job_id != starving.id:
            log.info("%s is starving; reserving on %s", starving.id, target)
        return Reservation(job_id=starving.id, agent_id=target, resource_ids=held, since=since)

    def _set_reservation(self, reservation: Reservation | None, now: float) -> None:
        """Adopt the recomputed reservation, announcing only a TRANSITION.

        The reservation is recomputed from scratch every pass — that is what makes
        "released the instant it stops being needed" a property of the design
        rather than of a cleanup path — so the object is new every time even when
        nothing has changed. Comparing (job, bench) is what turns a per-pass
        recomputation into an event a human would recognise: it started, it moved,
        or it stopped.

        Deliberately keyed on the TARGET, not the held set. A device freeing on
        the same bench grows the set without changing the sentence the page is
        showing, and the live queue view carries the current devices anyway.
        """
        before, after = self.reservation, reservation
        self.reservation = reservation
        was = (before.job_id, before.agent_id) if before else None
        now_is = (after.job_id, after.agent_id) if after else None
        if was == now_is:
            return
        if before is not None and (after is None or after.job_id != before.job_id):
            # Ending is a transition too: a page told only about the start goes on
            # showing a reservation that stopped existing.
            self.store.record_reservation(job_id=before.job_id, agent_id=None, now=now)
        if after is not None:
            self.store.record_reservation(
                job_id=after.job_id,
                agent_id=after.agent_id,
                resource_ids=after.resource_ids,
                now=now,
            )

    def _starving_jobs(
        self, jobs: list[Job], free_by_agent: dict[str, list[Resource]], now: float
    ) -> list[Job]:
        """Queued jobs that have waited too long AND cannot run right now.

        Oldest first. Only one of these ever reserves — one reserver can always
        eventually be satisfied, because nothing else is permitted to take what
        it waits for, whereas two reservers holding partial sets deadlock each
        other in bookkeeping. The rest are here only so their feasibility gets
        assessed.
        """
        return [
            job
            for job in jobs  # already oldest-first
            if now - job.submitted_at >= self.config.starvation_threshold_s
            and not matcher.rank_matches(job.requirements, free_by_agent)
        ]

    def _mark_unsatisfiable(self, job: Job, now: float) -> None:
        """Flag it, say so once, and KEEP IT QUEUED — fleets get repaired.

        THE CLOCK STARTS AT FLAGGING, NOT AT SUBMISSION. Measured from
        `submitted_at`, a job that sat happily on a healthy fleet for longer than
        UNSATISFIABLE_TIMEOUT was dead-lettered the instant the last capable
        bench retired — punished on the way in for time during which nothing was
        wrong. The window is meant to be "nobody could run this for half an
        hour", so it runs from the moment nobody could, and `_clear_unsatisfiable`
        resets it when the fleet recovers.
        """
        if self.store.set_blocked_reason(job.id, BLOCKED_NO_CAPABLE_AGENT, now=now):
            log.warning("%s cannot run on this fleet as it stands: no capable bench", job.id)
            return  # the clock starts now; it cannot already have run out
        # `job` was read before the flag was written, so on every later pass this
        # is the stamp from the pass that first flagged it.
        blocked_since = job.blocked_since if job.blocked_since is not None else now
        expired = now - blocked_since > self.config.unsatisfiable_timeout_s
        if expired and self.store.dead_letter_unsatisfiable(job.id, now=now):
            log.warning("%s dead-lettered: no capable bench appeared", job.id)

    def _clear_unsatisfiable(self, job: Job) -> None:
        if job.blocked_reason == BLOCKED_NO_CAPABLE_AGENT:
            self.store.set_blocked_reason(job.id, None)

    def feasible_agents(
        self, requirements: Sequence[CapabilitySpec], *, now: float | None = None
    ) -> list[str]:
        """Benches that could EVER satisfy these requirements, free or not.

        STEP 1 OF §3.4.1, EXPOSED. This is the same filter a reservation is
        scoped by and the same one that decides a job is unsatisfiable; the
        submit path asks it at the door so the caller is told at once instead of
        discovering it when the starvation threshold eventually flags the job.
        Deliberately not a second implementation of the matching semantics — one
        `could_ever_satisfy` per bench, exactly as `_recompute_reservation` does
        it, or the answer at the door and the answer in the queue would drift.
        """
        now = time.time() if now is None else now
        online = {agent.id for agent in self.store.online_agents(now=now)}
        installed_by_agent: dict[str, list[Resource]] = defaultdict(list)
        for resource in self.store.list_resources():
            if resource.agent_id in online:
                installed_by_agent[resource.agent_id].append(resource)
        return sorted(
            agent_id
            for agent_id, pool in installed_by_agent.items()
            if matcher.could_ever_satisfy(requirements, pool)
        )

    def reservation_for(self, job_id: str) -> Reservation | None:
        reservation = self.reservation
        return reservation if reservation and reservation.job_id == job_id else None

    # ------------------------------------------------------------- wakeups
    def notify(self) -> None:
        """Something changed: a job was submitted, or a device came free."""
        self._wake.set()

    def wake_agent(self, agent_id: str) -> None:
        """Release a long-polling heartbeat immediately — this is what makes
        dispatch sub-second rather than one heartbeat interval."""
        event = self._agent_wake.get(agent_id)
        if event is not None:
            event.set()

    async def wait_for_assignment(
        self, agent_id: str, *, timeout: float | None = None
    ) -> Assignment | None:
        """Long-poll (§3.1). Block until this bench has work or the timeout.

        Same ordering rule as the main loop: clear the flag BEFORE reading, so an
        assignment committed between the read and the wait cannot be missed.
        """
        timeout = self.config.longpoll_timeout_s if timeout is None else timeout
        event = self._agent_wake.setdefault(agent_id, asyncio.Event())
        event.clear()

        assignment = self.store.pending_assignment(agent_id)
        if assignment is not None:
            return assignment

        with contextlib.suppress(TimeoutError, asyncio.TimeoutError):
            await asyncio.wait_for(event.wait(), timeout=timeout)
        return self.store.pending_assignment(agent_id)

    # ------------------------------------------------------------- the loop
    async def run(self) -> None:
        log.info("scheduler started (backstop tick=%ss)", self.config.scheduler_tick_s)
        while True:
            with contextlib.suppress(TimeoutError, asyncio.TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=self.config.scheduler_tick_s)
            # BEFORE reading state. Clearing after the pass would erase a notify
            # that arrived while the pass was running — see the module docstring.
            self._wake.clear()
            try:
                self.pass_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("scheduling pass failed; continuing")

    def start(self) -> asyncio.Task:
        self._task = asyncio.create_task(self.run(), name="tss-scheduler")
        return self._task

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None

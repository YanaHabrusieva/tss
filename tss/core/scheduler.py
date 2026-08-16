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

from tss.core import matcher
from tss.core.config import DEFAULT, Config
from tss.core.models import Assignment, ClaimResult, Resource
from tss.core.store import Store

log = logging.getLogger("tss.scheduler")


class Scheduler:
    def __init__(self, store: Store, config: Config = DEFAULT) -> None:
        self.store = store
        self.config = config
        self.passes = 0
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
            self.passes += 1
            return []

        online = {agent.id for agent in self.store.online_agents(now=now)}
        free_by_agent: dict[str, list[Resource]] = defaultdict(list)
        for resource in matcher.offerable(self.store.list_resources()):
            if resource.agent_id in online:
                free_by_agent[resource.agent_id].append(resource)

        results: list[ClaimResult] = []
        for job in jobs:
            if not free_by_agent:
                break
            for candidate in matcher.rank_matches(job.requirements, free_by_agent):
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

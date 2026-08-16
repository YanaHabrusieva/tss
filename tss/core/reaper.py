"""The reaper — presence expiry, and the fan-out requeue (§3.5).

Sweep 1 is here. Sweep 2 (hung jobs, `max_duration_s`) arrives in step 4, when
there are running jobs to hang. They are deliberately separate sweeps: presence
timeout and job timeout detect different things and must never be collapsed — a
test that legitimately takes 20 minutes must not look like a dead bench, and a
bench whose power supply died must not get 20 minutes of grace.

WHY A LEASE AND NOT AN "IS ALIVE" FLAG. A flag needs someone to *decide*
liveness, and every such decision is a special case: what if the agent is slow,
what if the network blipped, what if TSS restarted. A lease has no opinion — time
passes, it expires. On restart the first tick sweeps every stale lease with no
dedicated recovery path at all, which is the whole payoff for storing expiry as
wall-clock (§7.2).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time

from tss.core.config import DEFAULT, Config
from tss.core.models import ReapResult
from tss.core.store import Store

log = logging.getLogger("tss.reaper")


class Reaper:
    def __init__(self, store: Store, config: Config = DEFAULT) -> None:
        self.store = store
        self.config = config
        self.sweeps = 0  # visible in tests and logs; a stalled reaper is silent otherwise
        self._task: asyncio.Task | None = None

    def sweep_presence(self, *, now: float | None = None) -> list[ReapResult]:
        """One pass: every agent whose lease has run out, whatever its load."""
        now = time.time() if now is None else now
        results: list[ReapResult] = []
        for agent_id in self.store.expired_agents(now=now):
            result = self.store.reap_agent(agent_id, now=now, reason="presence_expired")
            results.append(result)
            log.warning(
                "reaped %s — %d device(s) freed, %d job(s) requeued%s",
                result.agent_id,
                len(result.freed_resources),
                len(result.requeued_jobs),
                f", {len(result.dead_lettered_jobs)} dead-lettered"
                if result.dead_lettered_jobs
                else "",
            )
        self.sweeps += 1
        return results

    async def run(self) -> None:
        """Wake every REAPER_INTERVAL, forever.

        The body is wrapped: a background loop that dies on one unhandled
        exception dies *silently*, and every failure story in this design runs
        through here. Nothing it can raise is worth stopping for — the next tick
        is two seconds away.
        """
        log.info(
            "reaper started (interval=%ss, ttl=%ss)",
            self.config.reaper_interval_s,
            self.config.presence_ttl_s,
        )
        while True:
            try:
                self.sweep_presence()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("reaper sweep failed; continuing")
            await asyncio.sleep(self.config.reaper_interval_s)

    def start(self) -> asyncio.Task:
        self._task = asyncio.create_task(self.run(), name="tss-reaper")
        return self._task

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None

"""The reaper — two sweeps, deliberately not one (§3.5).

    sweep 1  presence expiry: the MACHINE died. Free every device it held and
             requeue each distinct job once (the fan-out).
    sweep 2  job timeout: the machine is fine and heartbeating; this one JOB is
             not finishing. Kill that job, free its devices, leave the bench and
             its other jobs alone.

Collapsing them is the most common design mistake in this system. A test that
legitimately takes 20 minutes must not look like a dead bench, and a bench whose
power supply died must not get 20 minutes of grace. Keeping them apart is also
what makes I6 checkable at all: the two paths write different `result_detail`, so
afterwards you can prove which mechanism ended a job.

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
from collections.abc import Callable

from tss.core.config import DEFAULT, Config
from tss.core.directives import DirectiveQueue
from tss.core.models import ReapResult
from tss.core.store import Store

log = logging.getLogger("tss.reaper")


class Reaper:
    def __init__(
        self,
        store: Store,
        config: Config = DEFAULT,
        *,
        on_reap: Callable[[], None] | None = None,
        directives: DirectiveQueue | None = None,
    ) -> None:
        self.store = store
        self.config = config
        #: Where cancel hints go for hung jobs. Best-effort; the epoch is what
        #: actually fences the report (see core/directives.py).
        self.directives = directives
        #: Poke the scheduler (§3.5): a reap frees devices and requeues jobs, and
        #: the queue should be looked at now rather than on the backstop tick.
        self.on_reap = on_reap
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
        if results and self.on_reap is not None:
            self.on_reap()
        return results

    def sweep_timeouts(self, *, now: float | None = None) -> list[str]:
        """Sweep 2: jobs that are running and not finishing (§3.5).

        A column detects nothing. `max_duration_s` sitting in the schema with no
        loop scanning it means a hung job holds its devices until its bench
        happens to die — which for a healthy, heartbeating bench is never.

        Separate from sweep 1 on purpose, and the separation is what makes I6
        checkable: this path records `result_detail='timeout...'`, presence expiry
        records `presence_expired...`, so afterwards you can tell which mechanism
        fired. The agent is told to stop via a directive, but the epoch bump in
        `time_out_job` is what actually makes its eventual report harmless.
        """
        now = time.time() if now is None else now
        ended: list[str] = []
        for job_id, agent_id in self.store.timed_out_jobs(now=now):
            kind = self.store.time_out_job(job_id, now=now)
            if kind is None:
                continue  # it finished while we were looking at it
            ended.append(job_id)
            if self.directives is not None and agent_id:
                self.directives.cancel_job(agent_id, job_id)
            log.warning(
                "job %s exceeded its budget on %s -> %s",
                job_id,
                agent_id,
                "dead_letter" if kind == "job.dead_letter" else "requeued",
            )
        if ended and self.on_reap is not None:
            # This sweep frees devices exactly as sweep 1 does, so it owes the
            # queue the same wake-up. Without it they idle until the backstop
            # tick — correct, and dead for a second.
            self.on_reap()
        return ended

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
            # Both sweeps, each wrapped: one failing must not stop the other,
            # and neither may kill the loop. A background task that dies on an
            # unhandled exception dies silently, and every failure story in this
            # design runs through here.
            sweeps = (("presence", self.sweep_presence), ("timeout", self.sweep_timeouts))
            for name, sweep in sweeps:
                try:
                    sweep()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("reaper %s sweep failed; continuing", name)
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

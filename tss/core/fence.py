"""The inverse fence: jobs an agent stops mentioning (§3.5, §6).

The epoch fences what an agent DOES report — a result for a run TSS has given
away is rejected. Nothing looks at what it fails to report, and there are two
ordinary network events that produce exactly that:

  * the /start response is lost, so the bench never learns it owns the job;
  * the /complete response is lost, so the bench drops the job locally and stops
    mentioning it while TSS still believes it is running.

Either way the job holds devices on a machine that is not running it, and the
only thing that would ever notice is presence expiry — which fires when the
whole bench dies. This bench is healthy and heartbeating; nothing is wrong with
it except that TSS and it disagree about one job.

TWO CONSECUTIVE MISSES, not one. The beat in which TSS delivers an assignment
necessarily precedes the agent knowing about it, so a single miss is ordinary
and taking a job off a bench for it would be its own bug.

IN MEMORY, LIKE THE DIRECTIVE QUEUE. A restart resets the counters and the
condition simply re-detects: the agent keeps not reporting the job, and two beats
later we are back where we were. Persisting a counter would be durable state that
exists only to accelerate a check that costs two heartbeats.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterable

log = logging.getLogger("tss.fence")

MISSES_BEFORE_REQUEUE = 2


class SilentJobTracker:
    def __init__(self, threshold: int = MISSES_BEFORE_REQUEUE) -> None:
        self.threshold = threshold
        self._misses: dict[tuple[str, str], int] = {}
        self._lock = threading.Lock()

    def observe(self, agent_id: str, *, owned: Iterable[str], reported: Iterable[str]) -> list[str]:
        """One heartbeat's worth of evidence.

        `owned` is what TSS believes this bench holds (assigned or running);
        `reported` is what the bench says it is running. Returns the jobs that
        have now been unmentioned for `threshold` consecutive beats.
        """
        owned, reported = set(owned), set(reported)
        silent: list[str] = []
        with self._lock:
            # Anything no longer owned is no longer interesting: the job finished,
            # was cancelled, or moved on. Dropping it here is what makes the count
            # CONSECUTIVE rather than cumulative.
            for key in [k for k in self._misses if k[0] == agent_id and k[1] not in owned]:
                del self._misses[key]

            for job_id in sorted(owned):
                key = (agent_id, job_id)
                if job_id in reported:
                    self._misses.pop(key, None)
                    continue
                misses = self._misses.get(key, 0) + 1
                if misses >= self.threshold:
                    del self._misses[key]
                    silent.append(job_id)
                else:
                    self._misses[key] = misses
        if silent:
            log.warning("%s has not mentioned %s for %d beats", agent_id, silent, self.threshold)
        return silent

    def forget(self, agent_id: str) -> None:
        """Drop everything for a bench that has gone or re-registered."""
        with self._lock:
            for key in [k for k in self._misses if k[0] == agent_id]:
                del self._misses[key]

    @property
    def tracked(self) -> int:
        return len(self._misses)

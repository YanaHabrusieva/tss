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

AND ONLY ABOUT WORK THE BENCH WAS TOLD IT OWNS. Two misses was the wrong bound
on its own, because `pending_assignment` hands over ONE job per heartbeat while
this counted a miss against EVERY job the bench owned. A bench given two jobs in
one scheduler pass — ordinary, it has two free devices — spent its misses on the
job TSS had not got round to mentioning yet, and that job was taken back before
the bench ever heard of it. Silence is only evidence when there was something to
be silent about, so a job is not counted until it has actually been delivered.

Reporting a job is itself proof of delivery: after a TSS restart the deliveries
are gone but the bench keeps naming what it is running, and that is enough.

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
        #: (agent, job) pairs TSS has actually handed over. Nothing outside this
        #: set can be accused of anything.
        self._delivered: set[tuple[str, str]] = set()
        self._lock = threading.Lock()

    def delivered(self, agent_id: str, job_id: str) -> None:
        """TSS has just put this assignment in a response to this bench.

        Recorded on the way out, whether or not the reply arrives: a delivery
        that is lost in flight is exactly the case the fence exists to recover,
        and the bench really was told — TSS simply cannot know it did not hear.
        """
        with self._lock:
            self._delivered.add((agent_id, job_id))

    def observe(self, agent_id: str, *, owned: Iterable[str], reported: Iterable[str]) -> list[str]:
        """One heartbeat's worth of evidence.

        `owned` is what TSS believes this bench holds (assigned or running);
        `reported` is what the bench says it is running. Returns the jobs that
        have now been unmentioned for `threshold` consecutive beats — counting
        only jobs this bench has been told about.
        """
        owned, reported = set(owned), set(reported)
        silent: list[str] = []
        with self._lock:
            # Anything no longer owned is no longer interesting: the job finished,
            # was cancelled, or moved on. Dropping it here is what makes the count
            # CONSECUTIVE rather than cumulative.
            for key in [k for k in self._misses if k[0] == agent_id and k[1] not in owned]:
                del self._misses[key]
            for key in [k for k in self._delivered if k[0] == agent_id and k[1] not in owned]:
                self._delivered.discard(key)

            for job_id in sorted(owned):
                key = (agent_id, job_id)
                if job_id in reported:
                    # It is running it, so it was plainly told about it — which
                    # is what carries this across a TSS restart.
                    self._delivered.add(key)
                    self._misses.pop(key, None)
                    continue
                if key not in self._delivered:
                    # TSS has not mentioned this job yet. Its silence is ours.
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
            for key in [k for k in self._delivered if k[0] == agent_id]:
                self._delivered.discard(key)

    @property
    def tracked(self) -> int:
        return len(self._misses)

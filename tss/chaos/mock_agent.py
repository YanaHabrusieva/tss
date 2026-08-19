"""A misbehaving bench: the REAL daemon plus a failure profile (§3.7).

`MockAgent` subclasses `TestbedAgent` and overrides four things — how it runs a
job, what its health probes say, how it heartbeats, and whether it is alive at
all. Everything else is the production agent: the same registration payload, the
same long-poll, the same /start and /complete, the same 404/410/409 handling.

That is deliberate. A mock that reimplements the protocol tests the mock. Every
bug the chaos suite has any chance of finding lives in the interaction between
the real daemon and the real service, and a hand-written stand-in would quietly
paper over exactly the mismatches worth finding.

WHAT IT COUNTS IS EVIDENCE. `fenced_reports` and `lost_responses` are not
statistics — they are what the gate's floors read to prove the zombie was
actually fenced and the deaf bench actually lost a reply. A profile whose effect
nothing counts is a profile that can be neutered without the gate noticing.

GROUND TRUTH is the other half of the job. The invariant checker cannot learn
some things from TSS's database — TSS's record of a device's capabilities is
precisely what this agent claimed, so a liar's fleet looks perfectly healthy from
the inside. `ground_truth()` reports what is actually true out here: what this
bench is running, at what epoch, on which devices, and what its hardware really
is.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from tss.agent.daemon import TestbedAgent
from tss.agent.executor import ExecutionResult
from tss.chaos.profiles import Profile
from tss.core.models import qualify

log = logging.getLogger("tss.chaos.agent")


@dataclass(frozen=True)
class RunningJob:
    job_id: str
    epoch: int
    resource_ids: tuple[str, ...]
    started_at: float


@dataclass(frozen=True)
class AgentTruth:
    """What is actually true on this bench, whatever TSS believes."""

    agent_id: str
    profile: str
    alive: bool
    running: tuple[RunningJob, ...] = ()
    #: local_id -> the capabilities the hardware REALLY has. A liar declared
    #: something else, and this is the only place the difference is visible.
    capabilities: dict[str, dict[str, Any]] = field(default_factory=dict)


class MockAgent(TestbedAgent):
    def __init__(
        self,
        agent_id: str,
        inventory: list[dict],
        *,
        base_url: str,
        profile: Profile,
        seed: int,
        presence_ttl_s: float = 12.0,
        reaper_interval_s: float = 2.0,
    ) -> None:
        declared = inventory
        self.true_capabilities = {d["id"]: dict(d["capabilities"]) for d in inventory}
        if profile.lies_about_capabilities:
            # It declares the inventory it was given and the hardware is
            # something else entirely. TSS has no way to know — the inventory is
            # pushed by the machine that is supposed to know best (§3.1), so
            # every check TSS can make against its own records agrees with
            # itself. Only the bench knows, which is the entire reason the
            # checker reads ground truth.
            self.true_capabilities = {
                device_id: {"product": "decoy_module"} for device_id in self.true_capabilities
            }
        super().__init__(agent_id, declared, base_url=base_url, hostname=f"{agent_id}.mock")
        self.profile = profile
        self.rng = random.Random(f"{seed}:{agent_id}")
        self.presence_ttl_s = presence_ttl_s
        self.reaper_interval_s = reaper_interval_s
        self.alive = True
        self.crashes = 0
        self.dropped_beats = 0
        self.reregistrations = 0
        #: /complete answered 409 — the epoch fence rejecting a stale result.
        #: The zombie profile exists to produce these, and until something
        #: counted them the gate could not tell a fenced zombie from a zombie
        #: that never went silent.
        self.fenced_reports = 0
        #: replies discarded after the server had already acted on the request.
        self.lost_responses = 0
        #: the job ids whose /start reply this bench threw away. Named, not just
        #: counted: it is what lets the gate require that the inverse fence took
        #: back THESE jobs, rather than crediting the profile with any unrelated
        #: requeue that happened to land on a deaf bench.
        self.lost_start_jobs: set[str] = set()
        # The daemon uses an injected client as-is and does not close it, which
        # is what lets the response hook live for the whole run — including
        # across a crash and reboot.
        self._client = httpx.AsyncClient(
            timeout=30.0, event_hooks={"response": [self._on_response]}
        )
        self._devices: dict[str, tuple[str, ...]] = {}
        self._silent_until = 0.0
        self._vanish_at: float | None = None
        self._next_flap: float | None = None
        self._next_device_flap: float | None = None
        self._sick_device: str | None = None
        self._local_stop = asyncio.Event()

    async def aclose(self) -> None:
        """The daemon never closes an injected client, so this bench owns it."""
        with contextlib.suppress(Exception, asyncio.CancelledError):
            await self._client.aclose()

    # ------------------------------------------------------------- the wire
    async def _on_response(self, response: httpx.Response) -> None:
        """Every reply this bench receives, before the daemon sees it.

        Two jobs. It counts the epoch fence rejecting a stale result, which is
        the zombie profile's whole output and is invisible from TSS's side — the
        409 is the only trace, and TSS does not record one. And it is where the
        `deaf` profile loses a REPLY: the request has already landed and the
        server has already acted on it, so this is the lost-response case the
        inverse fence was built for, not the lost-request case the presence TTL
        already absorbs.
        """
        path = response.request.url.path
        if response.status_code == 409 and path.endswith("/complete"):
            self.fenced_reports += 1
            # THE ZOMBIE DEMO'S MONEY MOMENT, and until now a single INFO line in
            # a stream of them. This is the instant a partitioned bench's result
            # for a run TSS already gave away is refused — the whole reason the
            # epoch exists — so in verbose mode it gets a line nobody can miss.
            log.warning(
                "\n%s\n  FENCED: %s reported a result for %s and was REFUSED "
                "(stale epoch)\n  a partitioned bench came back and TSS did not "
                "believe it\n%s",
                "=" * 78,
                self.agent_id,
                path.rsplit("/", 2)[-2],
                "=" * 78,
            )
        if not self.profile.deaf_probability:
            return
        # /start ONLY, and that is the whole point rather than a simplification.
        # Losing the reply to /complete proves nothing: TSS has already committed
        # the result and the job is terminal, so there is nothing left for anyone
        # to take back — the bench merely forgets a job that is already finished.
        # Losing the reply to /start is the one that leaves TSS holding a RUNNING
        # job on a bench that does not know it owns it, which is the disagreement
        # no lease can detect and the only thing the inverse fence can fix.
        # Never heartbeats either: losing those is flaky_network's job, and here
        # it would just look like death.
        if not path.endswith("/start"):
            return
        if self.rng.random() >= self.profile.deaf_probability:
            return
        # Drain the body first so the connection is returned to the pool rather
        # than abandoned mid-response.
        with contextlib.suppress(Exception):
            await response.aread()
        self.lost_responses += 1
        self.lost_start_jobs.add(path.rsplit("/", 2)[-2])
        log.info("%s discarded the reply to %s", self.agent_id, path)
        raise httpx.ReadError("reply discarded (deaf profile)", request=response.request)

    # ------------------------------------------------------------ ground truth
    def ground_truth(self) -> AgentTruth:
        return AgentTruth(
            agent_id=self.agent_id,
            profile=self.profile.name,
            alive=self.alive,
            running=tuple(
                RunningJob(
                    job_id=job_id,
                    epoch=epoch,
                    resource_ids=self._devices.get(job_id, ()),
                    started_at=0.0,
                )
                for job_id, epoch in self.running.items()
            ),
            capabilities=dict(self.true_capabilities),
        )

    # --------------------------------------------------------------- behaviour
    async def execute_job(self, job_id: str, resource_ids: list[str], payload: dict):
        # Qualified ids: the assignment names devices the way the bench knows
        # them (§6), the checker compares against fleet-wide ids.
        self._devices[job_id] = tuple(qualify(self.agent_id, rid) for rid in resource_ids)
        try:
            if self.rng.random() < self.profile.crash_probability:
                # The whole machine goes. Not an exception TSS ever sees — the
                # bench simply stops existing, and its lease has to notice.
                self._die()
                raise asyncio.CancelledError

            if self.profile.never_completes:
                # Heartbeats keep flowing; this job never ends. Only sweep 2 can
                # end it, and if sweep 2 does not exist the devices are gone
                # forever.
                await asyncio.sleep(3600)

            if self.profile.silent_ttls:
                # The zombie: go quiet long enough to be reaped, keep working,
                # then report a result for a run TSS has already given away.
                #
                # Two derived margins, both of which used to be one hardcoded
                # 0.2. Expiry is noticed by the next SWEEP, not at the instant
                # the lease lapses, so the report has to land at least a reaper
                # interval late or there is no reap yet for the fence to reject
                # against. And the silence has to outlast the report itself: the
                # moment this bench heartbeats again it gets 410 presence_expired
                # and the daemon CANCELS the run — correctly, and the result is
                # then never sent, which is the one thing this profile exists to
                # send. So it stays partitioned one beat past its own report.
                silence = self.profile.silent_ttls * self.presence_ttl_s
                silence += 2 * self.reaper_interval_s
                self._silent_until = time.monotonic() + silence + self.heartbeat_interval_s
                await asyncio.sleep(silence)
                return ExecutionResult("passed", "zombie report", 0.0)

            low, high = self.profile.duration_multiplier
            duration = float(payload.get("duration_s", 0.2)) * self.rng.uniform(low, high)
            await asyncio.sleep(duration)
            if self.profile.fails_every_job:
                # A liar's hardware cannot run the test it was given.
                return ExecutionResult("infra_error", "device does not match request", duration)
            return ExecutionResult("passed", None, duration)
        finally:
            self._devices.pop(job_id, None)

    def resource_health(self) -> dict[str, str]:
        if not self.profile.flap_device_every_ttls:
            return {}
        now = time.monotonic()
        if self._next_device_flap is None:
            self._next_device_flap = now + self.profile.flap_device_every_ttls * self.presence_ttl_s
            return {}
        if now < self._next_device_flap:
            return {}
        self._next_device_flap = now + self.profile.flap_device_every_ttls * self.presence_ttl_s
        if self._sick_device is None:
            self._sick_device = sorted(self.true_capabilities)[0]
            return {self._sick_device: "unhealthy"}
        recovered, self._sick_device = self._sick_device, None
        return {recovered: "healthy"}

    async def heartbeat(self, client: httpx.AsyncClient):
        now = time.monotonic()
        if now < self._silent_until:
            return None  # partitioned: the request never leaves the bench
        if self.rng.random() < self.profile.heartbeat_drop_rate:
            self.dropped_beats += 1
            return None
        return await super().heartbeat(client)

    # -------------------------------------------------------------- lifecycle
    def _die(self) -> None:
        """The machine dies: it stops believing anything at all."""
        self.crashes += 1
        self.alive = False
        self.registered = False
        self.running.clear()
        self._devices.clear()
        for task in list(self._tasks.values()):
            task.cancel()
        self._tasks.clear()
        self._local_stop.set()
        log.info("%s crashed (%s)", self.agent_id, self.profile.name)

    async def run(self, stop: asyncio.Event | None = None) -> None:
        """The daemon's loop, wrapped in a machine that can die and come back."""
        stop = stop or asyncio.Event()
        while not stop.is_set():
            self._local_stop = asyncio.Event()
            self._arm_timers()
            watchdog = asyncio.create_task(self._misbehave(stop))
            try:
                await super().run(_either(stop, self._local_stop))
            finally:
                watchdog.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await watchdog

            if stop.is_set():
                return
            if not self.alive:
                # A dead bench reboots after a while, exactly as a real one does:
                # fresh, with everything free, and no memory of what it was
                # running (§6 — which is why re-registering requeues).
                await _sleep_or_stop(stop, self.profile.reboot_after_ttls * self.presence_ttl_s)
                self.alive = True
                self.registered = False
                log.info("%s rebooted", self.agent_id)

    def _arm_timers(self) -> None:
        now = time.monotonic()
        self._vanish_at = (
            now + self.profile.idle_death_after_ttls * self.presence_ttl_s
            if self.profile.idle_death_after_ttls
            else None
        )
        self._next_flap = (
            now + self.profile.flap_every_ttls * self.presence_ttl_s
            if self.profile.flap_every_ttls
            else None
        )

    async def _misbehave(self, stop: asyncio.Event) -> None:
        """Timed misbehaviour that is not tied to running a job."""
        while not stop.is_set() and not self._local_stop.is_set():
            await asyncio.sleep(0.05)
            now = time.monotonic()
            if self._vanish_at is not None and now >= self._vanish_at:
                # idle_death: it never took a job, and now it is simply gone.
                # Nothing about its allocation state can reveal this; only the
                # lease can.
                self._vanish_at = None
                self.alive = False
                self._local_stop.set()
                log.info("%s vanished while idle", self.agent_id)
                return
            if self._next_flap is not None and now >= self._next_flap:
                self._next_flap = now + self.profile.flap_every_ttls * self.presence_ttl_s
                self.reregistrations += 1
                self.registered = False  # forces a re-register on the next beat
                self.running.clear()
                self._devices.clear()
                for task in list(self._tasks.values()):
                    task.cancel()
                self._tasks.clear()


def _either(first: asyncio.Event, second: asyncio.Event) -> asyncio.Event:
    """An event that is set when either input is. Cheap, and good enough for a
    loop that polls it every heartbeat."""

    class _Either(asyncio.Event):
        def is_set(self) -> bool:
            return first.is_set() or second.is_set()

        async def wait(self) -> bool:
            while not self.is_set():
                await asyncio.sleep(0.02)
            return True

    return _Either()


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    with contextlib.suppress(TimeoutError, asyncio.TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)

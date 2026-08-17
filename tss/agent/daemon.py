"""The testbed daemon: register with inventory, then heartbeat forever (§3.1).

WHY THE AGENT PULLS. The obvious design is for TSS to push jobs to an agent's
HTTP endpoint. Testbeds sit inside office networks behind NAT, and a service in
one region cannot reliably dial into a bench in another. Pull inverts that — the
agent always opens the connection outbound, which works through NAT with no
network engineering at all. It also means readiness is self-reported: a bench
only asks for work when it is genuinely ready.

The loop: register, heartbeat, and run whatever comes back. Each assignment gets
its own task, so a bench with three free devices runs three jobs side by side —
the agent has capacity, not a busy flag (§1.2). Execution itself is simulated
(§1.4); see executor.py.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import socket
import sys
import time

import httpx

from tss.agent.executor import execute

log = logging.getLogger("tss.agent")

DEFAULT_URL = "http://127.0.0.1:8000"


class TestbedAgent:
    """One machine, several devices cabled to it."""

    def __init__(
        self,
        agent_id: str,
        inventory: list[dict],
        *,
        base_url: str = DEFAULT_URL,
        hostname: str | None = None,
        agent_version: str = "0.1.0",
    ) -> None:
        self.agent_id = agent_id
        self.inventory = inventory
        self.base_url = base_url.rstrip("/")
        self.hostname = hostname or socket.gethostname()
        self.agent_version = agent_version
        # Server-supplied; the agent does not get to pick its own heartbeat rate.
        self.heartbeat_interval_s = 3.0
        self.presence_ttl_s = 12.0
        self.longpoll_timeout_s = 8.0
        self.registered = False
        #: job_id -> epoch, for the jobs this bench believes it owns.
        self.running: dict[str, int] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        #: Set when a job ends: a device just freed, so ask for work now rather
        #: than sitting out the rest of the heartbeat interval.
        self._nudge = asyncio.Event()

    async def register(self, client: httpx.AsyncClient) -> None:
        response = await client.post(
            f"{self.base_url}/v1/agents/register",
            json={
                "agent_id": self.agent_id,
                "hostname": self.hostname,
                "agent_version": self.agent_version,
                "resources": self.inventory,
            },
        )
        response.raise_for_status()
        body = response.json()
        self.heartbeat_interval_s = body["heartbeat_interval_s"]
        self.presence_ttl_s = body["presence_ttl_s"]
        self.longpoll_timeout_s = body.get("longpoll_timeout_s", 8.0)
        self.registered = True
        log.info(
            "registered %s with %d device(s): %s",
            self.agent_id,
            len(self.inventory),
            ", ".join(d["id"] for d in self.inventory),
        )

    async def heartbeat(self, client: httpx.AsyncClient) -> dict | None:
        """One beat. Returns the response body, or None if we had to re-register.

        This call may block server-side for up to LONGPOLL_TIMEOUT while the
        bench has spare capacity — that is the point, and it is why dispatch is
        sub-second instead of one heartbeat interval.
        """
        response = await client.post(
            f"{self.base_url}/v1/agents/{self.agent_id}/heartbeat",
            json={
                # Every job we believe we own, so TSS can fence each one (§6).
                "running_jobs": [
                    {"job_id": job_id, "epoch": epoch} for job_id, epoch in self.running.items()
                ],
                "resource_health": self.resource_health(),
            },
            timeout=self.longpoll_timeout_s + 10,
        )
        if response.status_code == 409:
            # We lost ONE job — cancelled, timed out, or reassigned while we were
            # partitioned. Everything else on this bench is untouched, so this is
            # not a re-registration: drop that run and carry on.
            lost = response.json().get("job_id")
            self.abandon(lost, why="lease_lost")
            return None
        if response.status_code in (404, 410):
            # 404 unknown_agent / 410 presence_expired. Both mean the same thing
            # to us: TSS no longer believes in this bench. Re-register and come
            # back clean — we do not get our jobs back, and must not pretend to.
            reason = response.json().get("error", response.status_code)
            log.warning("%s -> re-registering", reason)
            self.registered = False
            self.running.clear()
            self._tasks.clear()
            await self.register(client)
            return None
        response.raise_for_status()
        return response.json()

    # --- the two seams a mock bench needs. Both are real agent responsibilities
    # (§11 lists executor.py and health.py separately); overriding them is how the
    # chaos fleet misbehaves without reimplementing the protocol.
    async def execute_job(self, job_id: str, resource_ids: list[str], payload: dict):
        """Run the workload. Subclasses make it slow, hang, or crash."""
        return await execute(job_id, resource_ids, payload)

    def resource_health(self) -> dict[str, str]:
        """What our own probes say about our devices. TSS never probes hardware
        it cannot reach (§3.1)."""
        return {}

    def abandon(self, job_id: str | None, *, why: str) -> None:
        """Stop running a job and release its devices locally — without reporting.

        The result of an abandoned run is worthless: TSS has already moved the
        epoch on, so a report would be fenced out anyway. Reporting it would only
        risk a race with whoever owns the job now.
        """
        if job_id is None:
            return
        task = self._tasks.pop(job_id, None)
        self.running.pop(job_id, None)
        if task is not None:
            task.cancel()
        log.warning("abandoning %s (%s) — releasing its devices locally", job_id, why)

    def handle_directives(self, directives: list) -> None:
        for directive in directives or []:
            if isinstance(directive, dict) and "cancel_job" in directive:
                self.abandon(directive["cancel_job"], why="cancel_job directive")
            else:
                log.info("ignoring unknown directive %r", directive)

    async def _run_job(self, client: httpx.AsyncClient, assignment: dict) -> None:
        """/start -> execute -> /complete. One task per job, so a bench with
        several free devices genuinely runs several jobs at once (§1.2)."""
        job_id, epoch = assignment["job_id"], assignment["epoch"]
        body = {"agent_id": self.agent_id, "epoch": epoch}
        try:
            started = await client.post(f"{self.base_url}/v1/jobs/{job_id}/start", json=body)
            if started.status_code != 200:
                # Fenced out before we even began — someone else owns this now.
                log.warning("start %s rejected (%s); abandoning", job_id, started.status_code)
                return
            result = await self.execute_job(
                job_id, assignment["resource_ids"], assignment["payload"]
            )
            done = await client.post(
                f"{self.base_url}/v1/jobs/{job_id}/complete",
                json={
                    **body,
                    "outcome": result.outcome,
                    "detail": result.detail,
                    "duration_s": result.duration_s,
                },
            )
            if done.status_code == 409:
                # The zombie case: our lease died while we were running. Release
                # the hardware locally and drop the result on the floor.
                log.warning("complete %s fenced out (stale epoch); abandoning", job_id)
        except asyncio.CancelledError:
            # A cancel directive, or shutdown. Do NOT report: this run was
            # abandoned, and its result is not ours to give.
            log.info("%s cancelled mid-run; not reporting", job_id)
            raise
        except httpx.HTTPError as exc:
            log.warning("reporting %s failed (%s)", job_id, exc.__class__.__name__)
        finally:
            self.running.pop(job_id, None)
            self._tasks.pop(job_id, None)
            self._nudge.set()

    async def run(self, stop: asyncio.Event | None = None) -> None:
        stop = stop or asyncio.Event()
        async with httpx.AsyncClient(timeout=30.0) as client:
            while not stop.is_set():
                began = time.monotonic()  # in-process duration only (§3.3)
                try:
                    if not self.registered:
                        await self.register(client)
                        continue  # start polling for work now, not one beat from now
                    else:
                        body = await self.heartbeat(client)
                        self.handle_directives((body or {}).get("directives", []))
                        assignment = (body or {}).get("assignment")
                        if assignment and assignment["job_id"] not in self.running:
                            job_id = assignment["job_id"]
                            self.running[job_id] = assignment["epoch"]
                            self._tasks[job_id] = asyncio.create_task(
                                self._run_job(client, assignment)
                            )
                            continue  # more capacity may be waiting — ask again now
                except httpx.HTTPError as exc:
                    # A flaky link is expected and survivable: presence TTL is 4
                    # beats precisely so a few losses do not look like death.
                    log.warning("heartbeat failed (%s); retrying", exc.__class__.__name__)

                # One beat per interval, no matter how long the poll blocked for.
                elapsed = time.monotonic() - began
                await self._wait_for_next_beat(stop, self.heartbeat_interval_s - elapsed)

            for task in list(self._tasks.values()):
                task.cancel()

    async def _wait_for_next_beat(self, stop: asyncio.Event, timeout: float) -> None:
        """Sit out the rest of the heartbeat interval — unless a job just ended.

        A finished job has freed a device on this bench, and TSS may already have
        assigned the next one. Waiting out the remaining interval before asking
        would put a heartbeat's worth of dead air between "device freed" and
        "agent has job", which is the number §1.3 puts a sub-second budget on. So
        the nudge cuts the wait short; the interval itself is what stops a bench
        at full capacity from spinning.
        """
        if timeout <= 0:
            self._nudge.clear()
            return
        waiters = [
            asyncio.create_task(stop.wait()),
            asyncio.create_task(self._nudge.wait()),
        ]
        try:
            _done, pending = await asyncio.wait(
                waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
        finally:
            # Cleared before the next heartbeat reads state, never after.
            self._nudge.clear()


def build_inventory(count: int, product: str, harness: str | None) -> list[dict]:
    """N identical devices, named vg-01..vg-0N — enough for the demo fleet."""
    caps: dict[str, str] = {"product": product}
    if harness:
        caps["harness"] = harness
    prefix = "vg" if product == "vehicle_gateway" else "ag"
    return [{"id": f"{prefix}-{i:02d}", "capabilities": dict(caps)} for i in range(1, count + 1)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TSS testbed agent")
    parser.add_argument("--id", required=True, help="agent id, e.g. bench-sf-01")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--devices", type=int, default=3, help="how many identical DUTs")
    parser.add_argument("--product", default="vehicle_gateway")
    parser.add_argument("--harness", default="j1939")
    parser.add_argument("--version", default="0.1.0")
    parser.add_argument(
        "--inventory",
        help="path to a JSON file of [{'id':..,'capabilities':{..}}] — overrides --devices",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s"
    )
    if args.inventory:
        with open(args.inventory) as fh:
            inventory = json.load(fh)
    else:
        inventory = build_inventory(args.devices, args.product, args.harness)

    agent = TestbedAgent(args.id, inventory, base_url=args.url, agent_version=args.version)
    try:
        asyncio.run(agent.run())
    except KeyboardInterrupt:
        log.info("stopping %s", args.id)
    return 0


if __name__ == "__main__":
    sys.exit(main())

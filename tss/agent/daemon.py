"""The testbed daemon: register with inventory, then heartbeat forever (§3.1).

WHY THE AGENT PULLS. The obvious design is for TSS to push jobs to an agent's
HTTP endpoint. Testbeds sit inside office networks behind NAT, and a service in
one region cannot reliably dial into a bench in another. Pull inverts that — the
agent always opens the connection outbound, which works through NAT with no
network engineering at all. It also means readiness is self-reported: a bench
only asks for work when it is genuinely ready.

Step 2 scope: presence only. There is no scheduler yet, so the heartbeat response
carries no assignment and the daemon runs nothing. Job execution lands in step 3.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import socket
import sys

import httpx

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
        self.registered = False

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
        self.registered = True
        log.info(
            "registered %s with %d device(s): %s",
            self.agent_id,
            len(self.inventory),
            ", ".join(d["id"] for d in self.inventory),
        )

    async def heartbeat(self, client: httpx.AsyncClient) -> str:
        """One beat. Returns what happened, for the caller's loop and for tests."""
        response = await client.post(
            f"{self.base_url}/v1/agents/{self.agent_id}/heartbeat",
            json={"running_jobs": [], "resource_health": {}},
        )
        if response.status_code in (404, 410):
            # 404 unknown_agent / 410 presence_expired. Both mean the same thing
            # to us: TSS no longer believes in this bench. Re-register and come
            # back clean — we do not get our jobs back, and must not pretend to.
            reason = response.json().get("error", response.status_code)
            log.warning("%s -> re-registering", reason)
            self.registered = False
            await self.register(client)
            return str(reason)
        response.raise_for_status()
        return "ok"

    async def run(self, stop: asyncio.Event | None = None) -> None:
        stop = stop or asyncio.Event()
        async with httpx.AsyncClient(timeout=30.0) as client:
            while not stop.is_set():
                try:
                    if not self.registered:
                        await self.register(client)
                    else:
                        await self.heartbeat(client)
                except httpx.HTTPError as exc:
                    # A flaky link is expected and survivable: presence TTL is 4
                    # beats precisely so a few losses do not look like death.
                    log.warning("heartbeat failed (%s); retrying", exc.__class__.__name__)
                    self.registered = self.registered and True
                with contextlib.suppress(TimeoutError, asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=self.heartbeat_interval_s)


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

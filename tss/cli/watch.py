"""`tss watch` — the live fleet view (§3.9).

PUSH-DRIVEN, NEVER POLLING. This is what is on screen during the demo, and the
best moment in it is a bench flipping to OFFLINE the instant it dies. A one-second
poll turns that into "watch this... wait... there", which is a different and much
worse sentence. Everything here arrives over `WS /v1/events`: a snapshot on
connect, then event lines immediately and state frames coalesced behind them.

BUILT FOR A PROJECTOR:
  * state is always spelled out in TEXT (OFFLINE, BUSY, RESERVING) — colour only
    ever reinforces it, because projector colour is unreliable and some of the
    audience will not see it at all;
  * high contrast, no dim-on-dark for anything that carries meaning;
  * survives a terminal resize, an empty fleet, and a service that is not running
    yet, because it will be resized, empty and disconnected at some point in
    front of people.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections import deque

from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

EVENT_LOG_SIZE = 200

#: Colour reinforces; the words carry the meaning on their own.
AGENT_STYLE = {
    "online": "bold green",
    "offline": "bold white on red",
    "quarantined": "bold black on yellow",
    "draining": "bold cyan",
}
EVENT_STYLE = {
    "agent.offline": "bold white on red",
    "agent.quarantined": "bold black on yellow",
    "resource.quarantined": "bold yellow",
    "job.requeued": "bold yellow",
    "job.dead_letter": "bold white on red",
    "job.timed_out": "bold yellow",
    "job.cancelled": "bold magenta",
    "job.unsatisfiable": "bold red",
    "agent.registered": "bold green",
    "job.completed": "green",
    "job.assigned": "cyan",
    "job.started": "cyan",
}


class FleetScreen:
    def __init__(self, url: str, *, show_all: bool = False) -> None:
        self.url = url.rstrip("/")
        self.show_all = show_all
        self.fleet: dict = {"agents": []}
        self.queue: dict = {"queued": [], "running": []}
        self.events: deque[tuple[float, str, str]] = deque(maxlen=EVENT_LOG_SIZE)
        self.connected = False
        self.status = "connecting..."

    # ------------------------------------------------------------- rendering
    def render(self) -> Group:
        return Group(self._benches(), self._queue(), self._events())

    def _benches(self) -> Panel:
        table = Table(box=None, pad_edge=False, header_style="bold", expand=True)
        table.add_column("BENCH", no_wrap=True)
        table.add_column("STATE", no_wrap=True)
        table.add_column("BEAT", justify="right", no_wrap=True)
        table.add_column("DEVICES", overflow="fold")
        table.add_column("LOAD", justify="right", no_wrap=True)

        agents = self.fleet.get("agents", [])
        for agent in agents:
            state = agent["state"]
            devices = [d for d in agent["resources"] if self.show_all or d["state"] != "retired"]
            busy = sum(1 for d in devices if d["state"] == "busy")
            capacity = sum(1 for d in devices if d["state"] != "retired")
            cell = Text()
            for i, device in enumerate(devices):
                if i:
                    cell.append("  ")
                cell.append(*_device(device))
            if not devices:
                cell.append("(no devices)")
            requeued = agent.get("requeued_on_last_reap") or []
            if requeued:
                cell.append(f"   [{len(requeued)} job(s) requeued]", style="bold yellow")
            table.add_row(
                Text(agent["id"], style="bold"),
                Text(state.upper(), style=AGENT_STYLE.get(state, "white")),
                f"{agent['seconds_since_beat']:.0f}s",
                cell,
                "—" if state == "offline" else f"{busy}/{capacity}",
            )
        if not agents:
            table.add_row(Text("no benches registered", style="bold"), "", "", "", "")
        return Panel(table, title=f"FLEET ({len(agents)} benches)", border_style="white")

    def _queue(self) -> Panel:
        table = Table(box=None, pad_edge=False, header_style="bold", expand=True)
        table.add_column("JOB", no_wrap=True)
        table.add_column("NAME", no_wrap=True)
        table.add_column("STATE", no_wrap=True)
        table.add_column("NEEDS", overflow="fold")
        table.add_column("WHERE / WHY", overflow="fold")
        table.add_column("TIME", justify="right", no_wrap=True)

        running = self.queue.get("running", [])
        queued = self.queue.get("queued", [])
        for job in running:
            devices = " ".join(r.split(":", 1)[-1] for r in job["resource_ids"])
            elapsed = job["elapsed_s"]
            table.add_row(
                job["job_id"],
                job["name"],
                Text(job["state"].upper(), style="bold cyan"),
                _needs(job),
                f"{job['agent_id']} {devices}",
                "—" if elapsed is None else f"{elapsed:.0f}s/{job['max_duration_s']}s",
            )
        for job in queued:
            table.add_row(
                job["job_id"],
                job["name"],
                Text("QUEUED", style="bold"),
                _needs(job),
                _why(job),
                f"{job['waited_s']:.0f}s",
            )
        if not running and not queued:
            table.add_row(Text("queue empty", style="bold"), "", "", "", "", "")
        return Panel(
            table,
            title=f"JOBS ({len(running)} running, {len(queued)} queued)",
            border_style="white",
        )

    def _events(self) -> Panel:
        lines = Text()
        for ts, kind, detail in list(self.events)[-12:]:
            lines.append(f"{time.strftime('%H:%M:%S', time.localtime(ts))} ", style="bold")
            lines.append(f"{kind:<20}", style=EVENT_STYLE.get(kind, "white"))
            lines.append(f" {detail}\n")
        if not self.events:
            lines.append("waiting for events...", style="bold")
        title = "EVENTS" if self.connected else f"EVENTS — {self.status}"
        return Panel(lines, title=title, border_style="green" if self.connected else "bold red")

    # ------------------------------------------------------------ the stream
    def apply(self, message: dict) -> None:
        if message.get("type") == "snapshot":
            self.fleet = message.get("fleet", {"agents": []})
            self.queue = message.get("queue", {"queued": [], "running": []})
        elif message.get("type") == "event":
            event = message["event"]
            self.events.append(
                (event.get("ts", time.time()), event.get("kind", "?"), _summarise(event))
            )

    async def run(self) -> None:
        import websockets

        ws_url = self.url.replace("http://", "ws://").replace("https://", "wss://")
        with Live(self.render(), refresh_per_second=12, screen=False) as live:
            while True:
                try:
                    async with websockets.connect(f"{ws_url}/v1/events") as socket:
                        self.connected = True
                        self.status = "connected"
                        live.update(self.render())
                        async for raw in socket:
                            self.apply(json.loads(raw))
                            live.update(self.render())
                except (OSError, websockets.exceptions.WebSocketException) as exc:
                    # Reconnect and RE-SNAPSHOT rather than showing stale state:
                    # a fleet view that is confidently out of date is worse than
                    # one that says it is disconnected.
                    self.connected = False
                    self.status = f"disconnected ({exc.__class__.__name__}) — retrying"
                    self.fleet = {"agents": []}
                    self.queue = {"queued": [], "running": []}
                    live.update(self.render())
                    await asyncio.sleep(1.0)


def _device(device: dict) -> tuple[str, str]:
    state = device["state"]
    local = device["local_id"]
    if state == "busy":
        return f"{local}:BUSY({device['current_job_id'] or '?'})", "bold black on cyan"
    if state == "unhealthy":
        return f"{local}:UNHEALTHY", "bold black on yellow"
    if state == "retired":
        return f"{local}:RETIRED", "bold white on grey30"
    return f"{local}:free", "green"


def _needs(job: dict) -> Text:
    specs = " + ".join(
        ",".join(f"{k}={v}" for k, v in spec.items()) for spec in job["requirements"]
    )
    if job["resource_count"] > 1:
        return Text(f"{job['resource_count']}x ONE BENCH: {specs}", style="bold magenta")
    return Text(specs)


def _why(job: dict) -> Text:
    if job.get("reserving_on"):
        return Text(f"RESERVING on {job['reserving_on']}", style="bold black on cyan")
    if job.get("blocked_reason") == "no_capable_agent":
        return Text("UNSATISFIABLE — no capable bench", style="bold white on red")
    if job.get("attempt"):
        return Text(
            f"waiting (attempt {job['attempt']}, {len(job['tried_agents'])} bench(es) tried)"
        )
    return Text("waiting for a free device")


def _summarise(event: dict) -> str:
    parts = [p for p in (event.get("agent_id"), event.get("job_id")) if p]
    detail = event.get("detail") or {}
    if event["kind"] == "agent.offline":
        requeued = detail.get("requeued") or []
        freed = len(detail.get("freed_resources", []))
        parts.append(f"{len(requeued)} job(s) requeued, {freed} device(s) freed")
    elif event["kind"] == "job.completed":
        parts.append(str(detail.get("outcome", "")))
    elif event["kind"] in ("job.requeued", "job.dead_letter"):
        parts.append(str(detail.get("reason", "")))
    elif event["kind"] == "job.assigned":
        parts.append(",".join(r.split(":", 1)[-1] for r in detail.get("resource_ids", [])))
    return "  ".join(p for p in parts if p)


def watch(url: str, *, show_all: bool = False) -> int:
    screen = FleetScreen(url, show_all=show_all)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(screen.run())
    return 0

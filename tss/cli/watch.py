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
  * jobs are `smoke-1 (2bb76)`: the engineer's name, plus enough id to tell three
    `smoke` jobs apart. Full ids stay on the wire and in the logs;
  * survives a terminal resize, an empty fleet, and a service that is not running
    yet, because it will be resized, empty and disconnected at some point in
    front of people.

IT OWNS THE SCREEN. `Live(screen=True)` puts the view on the alternate screen
buffer: it repaints in place, adds nothing to scrollback, and hands the terminal
back exactly as it found it on Ctrl-C. That is only half of it — a frame TALLER
than the terminal still loses its bottom rows, which are the event lines, which
are the whole point. So every frame is built to fit the height it is being drawn
into: the sections are given rows out of the budget the terminal actually has,
and whatever does not fit is said out loud (`+3 more benches`) rather than
silently dropped off the edge.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sys
import time
from collections import deque

from rich.console import Console, ConsoleOptions, Group, RenderResult
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from tss.core.models import job_label

EVENT_LOG_SIZE = 200
#: More than this and the feed is scrollback with extra steps — the eye does not
#: read it. The floor is what matters more: the feed never gets squeezed to
#: nothing, because a fleet view with no events is a screenshot.
EVENT_ROWS_MAX = 14
EVENT_ROWS_MIN = 3
#: Two panel borders and a header row each for FLEET and JOBS, two borders for
#: EVENTS. Everything else is content the sections bid for.
CHROME_ROWS = 8

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
    "job.submitted": "cyan",
    "job.reserving": "bold yellow",
}


def fit(height: int, benches: int, jobs: int) -> tuple[int, int, int]:
    """How many content rows FLEET, JOBS and EVENTS get out of `height`.

    Never more than `height - CHROME_ROWS` between them, so the frame cannot
    overflow the terminal it is drawn into. Every section keeps at least one row
    — a section that vanished entirely would read as "nothing here" rather than
    "no room here". Events take their floor first because a live view whose live
    part got squeezed out is a screenshot; after that FLEET and JOBS take turns,
    so a fifteen-bench fleet cannot push the queue off the screen; whatever is
    left over goes back to the feed.
    """
    body = max(height - CHROME_ROWS, 3)
    fleet_rows = jobs_rows = event_rows = 1
    for _ in range(body - 3):
        if event_rows < EVENT_ROWS_MIN:
            event_rows += 1
        elif fleet_rows < benches and fleet_rows <= jobs_rows:
            fleet_rows += 1
        elif jobs_rows < jobs:
            jobs_rows += 1
        elif fleet_rows < benches:
            fleet_rows += 1
        elif event_rows < EVENT_ROWS_MAX:
            event_rows += 1
        else:
            break
    return fleet_rows, jobs_rows, event_rows


def _truncate(items: list, rows: int, noun: str) -> tuple[list, str | None]:
    """The first `rows` items, or `rows - 1` of them and a line saying so."""
    if len(items) <= rows:
        return items, None
    return items[: rows - 1], f"+{len(items) - rows + 1} more {noun} — not enough rows"


class FleetScreen:
    def __init__(self, url: str, *, show_all: bool = False) -> None:
        self.url = url.rstrip("/")
        self.show_all = show_all
        self.fleet: dict = {"agents": []}
        self.queue: dict = {"queued": [], "running": []}
        self.events: deque[tuple[float, str, str]] = deque(maxlen=EVENT_LOG_SIZE)
        #: job_id -> name, learned from snapshots and kept after the job leaves
        #: the queue, so a `job.completed` line can still say what completed. The
        #: event itself carries only the id, and widening the event shape to
        #: carry a name would put presentation on the wire.
        self.names: dict[str, str] = {}
        self.connected = False
        self.status = "connecting..."

    # ------------------------------------------------------------- rendering
    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        """Re-fit on every repaint, which is what makes a resize come out clean:
        the frame is built against the height the console has NOW, not the height
        it had when the last event arrived."""
        yield self.render(options.max_height or console.size.height)

    def render(self, height: int) -> Group:
        agents = self.fleet.get("agents", [])
        jobs = len(self.queue.get("running", [])) + len(self.queue.get("queued", []))
        fleet_rows, jobs_rows, event_rows = fit(height, max(len(agents), 1), max(jobs, 1))
        return Group(self._benches(fleet_rows), self._queue(jobs_rows), self._events(event_rows))

    def _benches(self, rows: int) -> Panel:
        table = Table(box=None, pad_edge=False, header_style="bold", expand=True)
        # Every column is no_wrap so a bench is exactly one row — that is what
        # makes the row budget in `fit` mean anything. DEVICES is the one that
        # gives, with an ellipsis, because the alternative to a truncated device
        # list is a frame taller than the terminal.
        table.add_column("BENCH", no_wrap=True, min_width=12)
        table.add_column("STATE", no_wrap=True, min_width=11)
        table.add_column("BEAT", justify="right", no_wrap=True, min_width=5)
        table.add_column("DEVICES", overflow="ellipsis", no_wrap=True, ratio=1)
        table.add_column("LOAD", justify="right", no_wrap=True, min_width=5)

        agents = self.fleet.get("agents", [])
        shown, cut = _truncate(agents, rows, "benches")
        for agent in shown:
            state = agent["state"]
            devices = [d for d in agent["resources"] if self.show_all or d["state"] != "retired"]
            busy = sum(1 for d in devices if d["state"] == "busy")
            capacity = sum(1 for d in devices if d["state"] != "retired")
            cell = Text()
            for i, device in enumerate(devices):
                if i:
                    cell.append("  ")
                cell.append(*self._device(device))
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
        if cut:
            table.add_row(Text(cut, style="bold yellow"), "", "", "", "")
        return Panel(table, title=f"FLEET ({len(agents)} benches)", border_style="white")

    def _queue(self, rows: int) -> Panel:
        table = Table(box=None, pad_edge=False, header_style="bold", expand=True)
        table.add_column("JOB", no_wrap=True, min_width=18)
        table.add_column("STATE", no_wrap=True, min_width=8)
        table.add_column("NEEDS", overflow="ellipsis", no_wrap=True, ratio=2)
        table.add_column("WHERE / WHY", overflow="ellipsis", no_wrap=True, ratio=3)
        table.add_column("TIME", justify="right", no_wrap=True, min_width=11)

        running = self.queue.get("running", [])
        queued = self.queue.get("queued", [])
        # Running first: what is happening beats what is waiting when rows are
        # scarce, and a queue that scrolls off says so on its last line.
        shown, cut = _truncate(running + queued, rows, "jobs")
        for job in shown:
            label = job_label(job["name"], job["job_id"])
            if job in running:
                devices = " ".join(r.split(":", 1)[-1] for r in job["resource_ids"])
                elapsed = job["elapsed_s"]
                table.add_row(
                    label,
                    Text(job["state"].upper(), style="bold cyan"),
                    _needs(job),
                    f"{job['agent_id']} {devices}",
                    "—" if elapsed is None else f"{elapsed:.0f}s/{job['max_duration_s']}s",
                )
            else:
                table.add_row(
                    label,
                    Text("QUEUED", style="bold"),
                    _needs(job),
                    _why(job),
                    f"{job['waited_s']:.0f}s",
                )
        if not running and not queued:
            table.add_row(Text("queue empty", style="bold"), "", "", "", "")
        if cut:
            table.add_row(Text(cut, style="bold yellow"), "", "", "", "")
        return Panel(
            table,
            title=f"JOBS ({len(running)} running, {len(queued)} queued)",
            border_style="white",
        )

    def _events(self, rows: int) -> Panel:
        lines = Text()
        # No trailing newline: it would render as a blank row, and a blank row
        # the budget did not account for is exactly one row of overflow.
        for i, (ts, kind, detail) in enumerate(list(self.events)[-rows:]):
            if i:
                lines.append("\n")
            lines.append(f"{time.strftime('%H:%M:%S', time.localtime(ts))} ", style="bold")
            lines.append(f"{kind:<20}", style=EVENT_STYLE.get(kind, "white"))
            lines.append(f" {detail}")
        if not self.events:
            lines.append("waiting for events...", style="bold")
        title = "EVENTS" if self.connected else f"EVENTS — {self.status}"
        return Panel(lines, title=title, border_style="green" if self.connected else "bold red")

    def _device(self, device: dict) -> tuple[str, str]:
        state = device["state"]
        local = device["local_id"]
        if state == "busy":
            job_id = device["current_job_id"]
            held = job_label(self.names.get(job_id), job_id) if job_id else "?"
            return f"{local}:BUSY {held}", "bold black on cyan"
        if state == "unhealthy":
            # The bench's own report versus TSS's verdict: one the bench can
            # withdraw, the other only an operator or a new agent version clears
            # (§4.2). Same row, different word.
            word = "QUARANTINED" if device.get("quarantined_at") else "UNHEALTHY"
            return f"{local}:{word}", "bold black on yellow"
        if state == "retired":
            return f"{local}:RETIRED", "bold white on grey30"
        return f"{local}:free", "green"

    # ------------------------------------------------------------ the stream
    def apply(self, message: dict) -> None:
        if message.get("type") == "snapshot":
            self.fleet = message.get("fleet", {"agents": []})
            self.queue = message.get("queue", {"queued": [], "running": []})
            for job in self.queue.get("running", []) + self.queue.get("queued", []):
                self.names[job["job_id"]] = job["name"]
        elif message.get("type") == "event":
            event = message["event"]
            self.events.append(
                (event.get("ts", time.time()), event.get("kind", "?"), self._summarise(event))
            )

    def _summarise(self, event: dict) -> str:
        job_id = event.get("job_id")
        parts = [p for p in (event.get("agent_id"),) if p]
        if job_id:
            parts.append(job_label(self.names.get(job_id), job_id))
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
        elif event["kind"] == "job.submitted":
            parts.append("submitted" if detail.get("feasible", True) else "NO CAPABLE BENCH")
        elif event["kind"] == "job.reserving":
            held = ",".join(r.split(":", 1)[-1] for r in detail.get("resource_ids", []))
            parts.append(f"RESERVING {held}".strip() if detail.get("reserving") else "released")
        return "  ".join(p for p in parts if p)

    async def run(self, console: Console | None = None) -> None:
        import websockets

        ws_url = self.url.replace("http://", "ws://").replace("https://", "wss://")
        with Live(self, console=console, refresh_per_second=12, screen=True) as live:
            while True:
                try:
                    async with websockets.connect(f"{ws_url}/v1/events") as socket:
                        self.connected = True
                        self.status = "connected"
                        live.refresh()
                        async for raw in socket:
                            self.apply(json.loads(raw))
                            live.refresh()
                except (OSError, websockets.exceptions.WebSocketException) as exc:
                    # Reconnect and RE-SNAPSHOT rather than showing stale state:
                    # a fleet view that is confidently out of date is worse than
                    # one that says it is disconnected. Everything derived from
                    # the old connection goes with it, names included.
                    self.connected = False
                    self.status = f"disconnected ({exc.__class__.__name__}) — retrying"
                    self.fleet = {"agents": []}
                    self.queue = {"queued": [], "running": []}
                    self.names.clear()
                    live.refresh()
                    await asyncio.sleep(1.0)


def _needs(job: dict) -> Text:
    specs = " + ".join(
        ",".join(f"{k}={v}" for k, v in spec.items()) for spec in job["requirements"]
    )
    if job["resource_count"] > 1:
        return Text(f"{job['resource_count']}x ONE BENCH: {specs}", style="bold magenta")
    return Text(specs)


def _why(job: dict) -> Text:
    if job.get("reserving_on"):
        held = ", ".join(r.split(":", 1)[-1] for r in job.get("reserving_resource_ids", []))
        text = Text(f"RESERVING on {job['reserving_on']}", style="bold black on cyan")
        if held:
            text.append(f" ({held} held)", style="bold cyan")
        return text
    if job.get("blocked_reason") == "no_capable_agent":
        return Text("UNSATISFIABLE — no capable bench", style="bold white on red")
    if job.get("attempt"):
        return Text(
            f"waiting (attempt {job['attempt']}, {len(job['tried_agents'])} bench(es) tried)"
        )
    return Text("waiting for a free device")


def watch(url: str, *, show_all: bool = False) -> int:
    console = Console()
    if not console.is_terminal:
        # The alternate screen needs a terminal to give back. Piped or redirected
        # there is nothing to repaint, and the honest answer is to say so once
        # rather than emit a few hundred frames of escape codes into a file.
        Console(stderr=True).print(
            f"[bold red]tss watch needs a real terminal[/bold red] — run it in one, "
            f"or open the live view at {url.rstrip('/')}/ in a browser."
        )
        return 2
    screen = FleetScreen(url, show_all=show_all)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(screen.run(console=console))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(watch(sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"))

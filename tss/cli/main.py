"""`tss` — the one-shot CLI (§3.9).

    tss fleet     benches with their devices nested underneath
    tss queue     what is running and what is waiting

`tss why`, `tss watch` and the operator verbs arrive with the TUI in later steps.
"""

from __future__ import annotations

import argparse
import sys

import httpx
from rich.console import Console
from rich.table import Table
from rich.text import Text

DEFAULT_URL = "http://127.0.0.1:8000"

STATE_STYLE = {
    "online": "green",
    "offline": "bold red",
    "quarantined": "yellow",
    "draining": "cyan",
}


def _resources_cell(agent: dict, *, show_all: bool) -> Text:
    """`vg-01 BUSY job-8f21 · vg-02 free · ag-01 free` — the two-level view.

    Retired devices are hidden unless --all: they are gone from the bench, and a
    fleet view that fills up with devices nobody can ever repair is a fleet view
    people stop reading.
    """
    shown = [r for r in agent["resources"] if show_all or r["state"] != "retired"]
    cell = Text()
    for i, resource in enumerate(shown):
        if i:
            cell.append(" · ", style="dim")
        state = resource["state"]
        if state == "busy":
            cell.append(f"{resource['local_id']} BUSY", style="bold yellow")
            if resource["current_job_id"]:
                cell.append(f" {resource['current_job_id']}", style="dim")
        elif state == "unhealthy":
            cell.append(f"{resource['local_id']} UNHEALTHY", style="red")
        elif state == "retired":
            cell.append(f"{resource['local_id']} retired", style="dim strike")
        else:
            cell.append(f"{resource['local_id']} free", style="green")
    if not shown:
        cell.append("(no devices)", style="dim")
    hidden = len(agent["resources"]) - len(shown)
    if hidden:
        cell.append(f"   +{hidden} retired", style="dim")
    requeued = agent.get("requeued_on_last_reap") or []
    if requeued:
        cell.append(f"   ({len(requeued)} job{'s' if len(requeued) > 1 else ''} requeued)", "dim")
    return cell


def render_fleet(fleet: dict, console: Console, *, show_all: bool = False) -> None:
    table = Table(box=None, pad_edge=False, header_style="bold")
    table.add_column("BENCH")
    table.add_column("STATE")
    table.add_column("BEAT", justify="right")
    table.add_column("RESOURCES")
    table.add_column("LOAD", justify="right")

    for agent in fleet["agents"]:
        state = agent["state"]
        label = state.upper() if state != "online" else state
        busy = sum(1 for r in agent["resources"] if r["state"] == "busy")
        # Retired devices are not capacity, so they are not in the denominator.
        total = sum(1 for r in agent["resources"] if r["state"] != "retired")
        table.add_row(
            agent["id"],
            Text(label, style=STATE_STYLE.get(state, "")),
            f"{agent['seconds_since_beat']:.0f}s",
            _resources_cell(agent, show_all=show_all),
            "—" if state == "offline" else f"{busy}/{total}",
        )

    console.print(table)
    if not fleet["agents"]:
        console.print("[dim]no benches registered — start one with `just agent`[/dim]")


def render_queue(queue: dict, console: Console) -> None:
    """Queued and running jobs (§3.9).

    Elapsed-versus-budget, never an estimated start time: a confident "starts in
    ~3m" is a lie without historical durations, which the POC does not collect.
    """
    running = Table(box=None, pad_edge=False, header_style="bold", title_justify="left")
    running.add_column("JOB")
    running.add_column("NAME")
    running.add_column("STATE")
    running.add_column("BENCH")
    running.add_column("DEVICES")
    running.add_column("ELAPSED", justify="right")
    for job in queue["running"]:
        elapsed = job["elapsed_s"]
        running.add_row(
            job["job_id"],
            job["name"],
            Text(job["state"].upper(), style="yellow" if job["state"] == "running" else "cyan"),
            job["agent_id"] or "—",
            " · ".join(r.split(":", 1)[-1] for r in job["resource_ids"]) or "—",
            "—" if elapsed is None else f"{elapsed:.0f}s / {job['max_duration_s']}s",
        )

    queued = Table(box=None, pad_edge=False, header_style="bold")
    queued.add_column("JOB")
    queued.add_column("NAME")
    queued.add_column("NEEDS")
    queued.add_column("WAITED", justify="right")
    queued.add_column("WHY", overflow="fold")
    for job in queue["queued"]:
        needs = " + ".join(
            ",".join(f"{k}={v}" for k, v in spec.items()) for spec in job["requirements"]
        )
        why = job["blocked_reason"] or ""
        if job["attempt"]:
            why = (
                why + f" retry {job['attempt']}, tried {len(job['tried_agents'])} bench(es)"
            ).strip()
        queued.add_row(job["job_id"], job["name"], needs, f"{job['waited_s']:.0f}s", why)

    console.print(f"[bold]RUNNING[/bold] ({len(queue['running'])})")
    console.print(running if queue["running"] else Text("  nothing running", style="dim"))
    console.print(f"\n[bold]QUEUED[/bold] ({len(queue['queued'])})")
    console.print(queued if queue["queued"] else Text("  queue empty", style="dim"))


def _get(args: argparse.Namespace, path: str, console: Console) -> dict | None:
    try:
        response = httpx.get(f"{args.url.rstrip('/')}{path}", timeout=10.0)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        console.print(f"[red]cannot reach TSS at {args.url}[/red] ({exc.__class__.__name__})")
        console.print("[dim]is `just serve` running?[/dim]")
        return None
    return response.json()


def cmd_queue(args: argparse.Namespace) -> int:
    console = Console()
    queue = _get(args, "/v1/queue", console)
    if queue is None:
        return 1
    render_queue(queue, console)
    return 0


def cmd_fleet(args: argparse.Namespace) -> int:
    console = Console()
    fleet = _get(args, "/v1/fleet", console)
    if fleet is None:
        return 1
    render_fleet(fleet, console, show_all=args.all)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="tss", description="Test Scheduling Service")
    parser.add_argument("--url", default=DEFAULT_URL, help="TSS base URL")
    sub = parser.add_subparsers(dest="command", required=True)
    fleet_cmd = sub.add_parser("fleet", help="benches and their devices")
    fleet_cmd.add_argument(
        "--all",
        action="store_true",
        help="include retired devices (gone from the bench, kept for history)",
    )

    sub.add_parser("queue", help="queued and running jobs")

    args = parser.parse_args(argv)
    if args.command == "fleet":
        return cmd_fleet(args)
    if args.command == "queue":
        return cmd_queue(args)
    parser.error(f"unknown command {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())

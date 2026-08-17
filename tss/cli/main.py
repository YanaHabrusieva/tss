"""`tss` — the command line (§3.9).

    tss fleet          benches with their devices nested underneath
    tss queue          what is running and what is waiting
    tss watch          the same, live, pushed over a WebSocket
    tss why <job_id>   why that job is not running yet

Three surfaces, one data source. The operator verbs (drain, unquarantine) arrive
in step 7.
"""

from __future__ import annotations

import argparse
import contextlib
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
        if job["resource_count"] > 1:
            needs = f"{job['resource_count']}x on ONE bench: {needs}"
        why = Text()
        if job.get("reserving_on"):
            # The non-obvious answer to "why am I waiting" once jobs need several
            # devices — and nine times out of ten the thing that looks like a
            # scheduler bug is this, working correctly (§3.9).
            why.append(f"RESERVING on {job['reserving_on']}", style="bold cyan")
        elif job["blocked_reason"] == "no_capable_agent":
            why.append("UNSATISFIABLE — no bench in the fleet can run this", style="bold red")
        elif job["blocked_reason"]:
            why.append(job["blocked_reason"], style="yellow")
        if job["attempt"]:
            why.append(
                f"  retry {job['attempt']}, tried {len(job['tried_agents'])} bench(es)", "dim"
            )
        queued.add_row(job["job_id"], job["name"], needs, f"{job['waited_s']:.0f}s", why)

    console.print(f"[bold]RUNNING[/bold] ({len(queue['running'])})")
    console.print(running if queue["running"] else Text("  nothing running", style="dim"))
    console.print(f"\n[bold]QUEUED[/bold] ({len(queue['queued'])})")
    console.print(queued if queue["queued"] else Text("  queue empty", style="dim"))


def _get(args: argparse.Namespace, path: str, console: Console) -> dict | None:
    try:
        response = httpx.get(f"{args.url.rstrip('/')}{path}", timeout=10.0)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        detail = ""
        with contextlib.suppress(Exception):
            detail = exc.response.json().get("detail", "")
        console.print(f"[red]{exc.response.status_code}[/red] {detail or path}")
        return None
    except httpx.HTTPError as exc:
        console.print(f"[red]cannot reach TSS at {args.url}[/red] ({exc.__class__.__name__})")
        console.print("[dim]is `just serve` running?[/dim]")
        return None
    return response.json()


def render_why(why: dict, console: Console) -> None:
    """The customer feature (§3.9). Nobody builds this and every firmware
    engineer wants it.

    Only what TSS actually knows: elapsed against budget, which benches could
    ever run this, which cannot and why. No estimated start time — that needs
    duration history the POC does not keep (§13.7), and a fabricated ETA is the
    number that gets pulled on.
    """
    state = why["state"].upper()
    header = Text(f"{why['job_id']}  ", style="bold")
    header.append(state, style="bold cyan" if state in ("RUNNING", "ASSIGNED") else "bold")
    header.append(f"  {why['waited_s']:.0f}s waited")
    if why.get("reserving"):
        held = ", ".join(r.split(":", 1)[-1] for r in why["reserving"]["resource_ids"])
        header.append(
            f"  — RESERVING on {why['reserving']['agent_id']}" + (f" ({held})" if held else ""),
            style="bold black on cyan",
        )
    elif why.get("blocked_reason") == "no_capable_agent":
        header.append("  — UNSATISFIABLE (no capable bench)", style="bold white on red")
    console.print(header)

    specs = [
        ", ".join(f"{k}={v}" for k, v in spec.items()) or "any device"
        for spec in why["requirements"]
    ]
    if why["resource_count"] > 1:
        console.print(f"  needs: {why['resource_count']} devices, all on ONE bench")
        for spec in specs:
            console.print(f"           {spec}")
    else:
        console.print(f"  needs: {specs[0]}")
    if why["attempt"]:
        console.print(
            f"  attempt {why['attempt']}, tried {len(why['tried_agents'])} bench(es): "
            f"{', '.join(why['tried_agents'])}"
        )
    if why.get("outcome"):
        console.print(f"  outcome: [bold]{why['outcome']}[/bold]  {why.get('result_detail') or ''}")
        return
    if why["state"] in ("assigned", "running"):
        elapsed = why.get("elapsed_s")
        where = f"  running on {why['agent_id']}"
        if elapsed is not None:
            where += f", {elapsed:.0f}s elapsed of a {why['max_duration_s']}s budget"
        console.print(where)
        return

    if why["feasible"]:
        console.print("  feasible benches (could ever satisfy this):")
        for bench in why["feasible"]:
            console.print(f"    [bold]{bench['agent_id']}[/bold]")
            for device in bench["devices"]:
                console.print(f"      {_device_line(device)}")
    if why["infeasible"]:
        console.print("  not feasible:")
        for bench in why["infeasible"]:
            console.print(f"    [bold]{bench['agent_id']}[/bold]  {bench['why_not']}")
    if why.get("waiting_on"):
        console.print(f"  waiting on: [bold]{why['waiting_on']}[/bold]")
    if why.get("reserving"):
        console.print("  nothing else can take those devices while you wait")


def _device_line(device: dict) -> Text:
    line = Text(f"{device['local_id']:<8}")
    state = device["state"]
    if state == "busy":
        line.append("BUSY", style="bold cyan")
        line.append(f" {device['current_job_id']}")
        if device.get("elapsed_s") is not None and device.get("budget_s"):
            line.append(f" ({device['elapsed_s']:.0f}s / {device['budget_s']}s budget)")
    elif state == "free":
        line.append("free", style="green")
    else:
        line.append(state.upper(), style="bold yellow")
    if device.get("reserved_for_you"):
        line.append("  RESERVED FOR YOU", style="bold black on cyan")
    elif not device["matches"]:
        line.append("  (does not match)", style="dim")
    return line


def cmd_why(args: argparse.Namespace) -> int:
    console = Console()
    why = _get(args, f"/v1/jobs/{args.job_id}/why", console)
    if why is None:
        return 1
    render_why(why, console)
    return 0


def cmd_watch(args: argparse.Namespace) -> int:
    from tss.cli.watch import watch

    return watch(args.url, show_all=args.all)


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

    watch_cmd = sub.add_parser("watch", help="live fleet view (push-driven)")
    watch_cmd.add_argument("--all", action="store_true", help="include retired devices")

    why_cmd = sub.add_parser("why", help="why is this job not running?")
    why_cmd.add_argument("job_id")

    args = parser.parse_args(argv)
    if args.command == "fleet":
        return cmd_fleet(args)
    if args.command == "queue":
        return cmd_queue(args)
    if args.command == "watch":
        return cmd_watch(args)
    if args.command == "why":
        return cmd_why(args)
    parser.error(f"unknown command {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())

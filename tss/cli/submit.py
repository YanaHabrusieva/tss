"""`just submit` — the customer simulator.

    just submit NAME VG AG DURATION

Same argument order as `just add`: a name, how many vehicle gateways, how many
asset gateways, then how long. One mental model for standing up hardware and for
asking for it, which is the point — the two commands are read side by side during
a demo and a different order between them is a stumble every single time.

It prints ONE quiet line on the happy path. The exception is a job no bench in
the fleet can ever run: TSS queues it anyway, deliberately, because fleets get
repaired and extended (§3.4.1) — but the person who just typed the command should
not have to run `tss why` to discover that nothing can run it. That answer comes
back in the submit response and is printed loudly here.
"""

from __future__ import annotations

import argparse
import contextlib
import sys

import httpx
from rich.console import Console

from tss.core.models import job_label

DEFAULT_URL = "http://127.0.0.1:8000"
VEHICLE_GATEWAY = {"product": "vehicle_gateway"}
ASSET_GATEWAY = {"product": "asset_gateway"}


def requirements(vg: int, ag: int) -> list[dict]:
    return [dict(VEHICLE_GATEWAY)] * vg + [dict(ASSET_GATEWAY)] * ag


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="just submit", description="Submit a test job")
    parser.add_argument("name")
    parser.add_argument("vg", nargs="?", type=int, default=1, help="vehicle gateways")
    parser.add_argument("ag", nargs="?", type=int, default=0, help="asset gateways")
    parser.add_argument("duration", nargs="?", type=float, default=10.0, help="seconds")
    parser.add_argument(
        "--outcome",
        default="passed",
        choices=("passed", "failed", "infra_error"),
        help="CHAOS CONTROL — what the bench should report. Customers do not set this.",
    )
    parser.add_argument("--url", default=DEFAULT_URL)
    args = parser.parse_args(argv)
    console = Console()

    if args.vg + args.ag < 1:
        console.print(
            f"[red]just submit: {args.name} needs at least one device[/red] — "
            f"try: just submit {args.name} 1 0"
        )
        return 2

    body = {
        "name": args.name,
        "requirements": requirements(args.vg, args.ag),
        "payload": {"duration_s": args.duration, "outcome": args.outcome},
    }
    try:
        response = httpx.post(f"{args.url.rstrip('/')}/v1/jobs", json=body, timeout=20.0)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        detail = ""
        with contextlib.suppress(Exception):
            detail = exc.response.json().get("detail", "")
        console.print(f"[red]{exc.response.status_code}[/red] {detail}")
        return 1
    except httpx.HTTPError as exc:
        console.print(f"[red]cannot reach TSS at {args.url}[/red] ({exc.__class__.__name__})")
        console.print("[dim]is `just start` running?[/dim]")
        return 1

    created = response.json()
    label = job_label(args.name, created["job_id"])
    if created.get("feasible", True):
        console.print(f"[bold]{label}[/bold] queued — position {created['queue_position']}")
        return 0

    # The loud case. Still queued, and that is correct — but silence here means
    # the submitter walks away believing the fleet is working on it.
    console.print(
        f"[bold]{label}[/bold] queued — "
        f"[bold black on yellow]WARNING: {created['infeasible_reason']}[/bold black on yellow]"
    )
    console.print(
        "[yellow]It will wait for the fleet to change, and dead-letters if none "
        "appears in time.[/yellow]"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

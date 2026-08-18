"""The human surfaces: what a person reads, and whether it fits on the screen.

Two properties, both of which failed before this change:

  * a frame never claims more rows than the terminal has. `tss watch` used to
    build one frame for a fleet of any size and hand it to a non-alternate-screen
    `Live`, so on a short terminal the bottom of the frame — the event feed,
    which is the part that moves — scrolled off and every repaint stacked another
    stale copy in the scrollback;
  * a job is `smoke-1 (2bb76)` wherever a human reads it and `job-2bb76a1c`
    wherever a machine does. Two `smoke` jobs and a wall of hex are the same
    problem from opposite ends.
"""

from __future__ import annotations

import argparse
import io

import httpx
import pytest
from rich.console import Console

from tss.cli.main import _resolve_job, render_fleet, render_queue, render_why
from tss.cli.watch import CHROME_ROWS, EVENT_ROWS_MIN, FleetScreen, fit, watch
from tss.core.models import job_label, short_id

T0 = 1_700_000_000.0
VG = {"product": "vehicle_gateway"}


# ----------------------------------------------------------- the id helper
def test_short_id_is_the_suffix_not_the_prefix():
    """`job-` is a constant and carries nothing. The five characters after it
    are what tell two jobs apart."""
    assert short_id("job-2bb76a1c") == "2bb76"
    assert short_id("job-9d1f2abc") == "9d1f2"


def test_job_label_is_the_name_plus_enough_id():
    assert job_label("smoke-1", "job-2bb76a1c") == "smoke-1 (2bb76)"


def test_job_label_falls_back_to_the_short_id_when_the_name_is_unknown():
    """An event carries a job id and nothing else. A view that has not seen that
    job in a snapshot still has to print something a human can match against the
    screen — the short id alone, never a bare `None`."""
    assert job_label(None, "job-2bb76a1c") == "2bb76"
    assert job_label("", "job-2bb76a1c") == "2bb76"


def test_a_short_id_stays_short_for_an_id_with_no_prefix():
    assert short_id("2bb76a1c") == "2bb76"


# --------------------------------------------------------------- fitting
@pytest.mark.parametrize("height", [10, 14, 18, 24, 40, 60])
def test_the_row_budget_never_exceeds_the_terminal(height):
    fleet_rows, jobs_rows, event_rows = fit(height, benches=20, jobs=30)

    assert fleet_rows + jobs_rows + event_rows + CHROME_ROWS <= max(height, CHROME_ROWS + 3)
    assert min(fleet_rows, jobs_rows, event_rows) >= 1, "no section may vanish entirely"


def test_the_event_feed_keeps_its_floor_when_the_fleet_is_large():
    """The feed is the part that moves. A fifty-bench fleet must not squeeze the
    live half of a live view down to nothing."""
    _fleet_rows, _jobs_rows, event_rows = fit(40, benches=50, jobs=50)

    assert event_rows >= EVENT_ROWS_MIN


def test_the_fleet_cannot_push_the_queue_off_the_screen():
    fleet_rows, jobs_rows, _events = fit(30, benches=100, jobs=4)

    assert jobs_rows == 4, "the queue was starved by the bench list"
    assert fleet_rows > 1


def _screen(benches: int, jobs: int) -> FleetScreen:
    screen = FleetScreen("http://127.0.0.1:8000")
    screen.apply(
        {
            "type": "snapshot",
            "fleet": {
                "now": T0,
                "agents": [
                    {
                        "id": f"bench-{i:02d}",
                        "hostname": "b",
                        "state": "online",
                        "last_heartbeat_at": T0,
                        "presence_expires_at": T0 + 12,
                        "seconds_since_beat": 1.0,
                        "requeued_on_last_reap": [],
                        "resources": [
                            {
                                "id": f"bench-{i:02d}:vg-0{d}",
                                "local_id": f"vg-0{d}",
                                "state": "free",
                                "current_job_id": None,
                                "capabilities": VG,
                            }
                            for d in range(4)
                        ],
                    }
                    for i in range(benches)
                ],
            },
            "queue": {
                "now": T0,
                "queued": [
                    {
                        "job_id": f"job-{i:08x}",
                        "name": f"regression-{i}",
                        "state": "queued",
                        "requirements": [dict(VG)] * 2,
                        "resource_count": 2,
                        "agent_id": None,
                        "resource_ids": [],
                        "waited_s": 12.0,
                        "elapsed_s": None,
                        "max_duration_s": 600,
                        "attempt": 0,
                        "tried_agents": [],
                        "blocked_reason": None,
                        "reserving_on": None,
                        "submitted_at": T0,
                        "started_at": None,
                        "reserving_resource_ids": [],
                    }
                    for i in range(jobs)
                ],
                "running": [],
            },
        }
    )
    for i in range(30):
        screen.apply(
            {
                "type": "event",
                "event": {"ts": T0 + i, "kind": "job.assigned", "job_id": f"job-{i:08x}"},
            }
        )
    return screen


@pytest.mark.parametrize("height", [12, 16, 24, 30, 50])
def test_a_frame_fits_the_terminal_it_is_drawn_into(height):
    """The acceptance test for the alternate screen: whatever the fleet is doing,
    one frame is at most one screen. Deliberately over-supplied — twenty benches
    and forty jobs into a terminal that can hold neither."""
    screen = _screen(benches=20, jobs=40)
    console = Console(width=120, height=height, record=True, file=io.StringIO())

    console.print(screen.render(height))

    lines = console.export_text().rstrip("\n").split("\n")
    assert len(lines) <= height, f"{len(lines)} rows drawn into a {height}-row terminal"


def test_what_does_not_fit_is_said_out_loud():
    screen = _screen(benches=20, jobs=40)
    console = Console(width=120, height=16, record=True, file=io.StringIO())

    console.print(screen.render(16))

    rendered = console.export_text()
    assert "more benches" in rendered, "benches vanished with no trace"
    assert "more jobs" in rendered, "jobs vanished with no trace"


def test_the_same_screen_re_fits_when_the_terminal_changes_size():
    """A resize mid-run is a repaint at a new height, not a restart."""
    screen = _screen(benches=20, jobs=40)

    for height in (40, 12, 40):
        console = Console(width=120, height=height, record=True, file=io.StringIO())
        console.print(screen.render(height))
        assert len(console.export_text().rstrip("\n").split("\n")) <= height


# ------------------------------------------------------------ no terminal
def test_watch_without_a_tty_says_so_once_and_stops(capsys):
    """`tss watch > file` on the alternate screen has nothing to repaint and
    nothing to hand back. Better one line than a few hundred frames of escape
    codes into a pipe."""
    code = watch("http://127.0.0.1:8000")

    assert code == 2
    message = capsys.readouterr().err
    assert "terminal" in message
    assert "http://127.0.0.1:8000/" in message, "it must point at the web view instead"


# ---------------------------------------------------- names on every surface
def _fleet_with_a_busy_device() -> dict:
    return {
        "now": T0,
        "agents": [
            {
                "id": "bench-01",
                "hostname": "b",
                "state": "online",
                "last_heartbeat_at": T0,
                "presence_expires_at": T0 + 12,
                "seconds_since_beat": 1.0,
                "requeued_on_last_reap": [],
                "resources": [
                    {
                        "id": "bench-01:vg-01",
                        "local_id": "vg-01",
                        "state": "busy",
                        "current_job_id": "job-2bb76a1c",
                        "capabilities": VG,
                    }
                ],
            }
        ],
    }


def _console() -> Console:
    return Console(width=140, record=True, file=io.StringIO())


def test_the_fleet_names_the_job_holding_a_device():
    console = _console()

    render_fleet(_fleet_with_a_busy_device(), console, names={"job-2bb76a1c": "smoke-1"})

    rendered = console.export_text()
    assert "smoke-1 (2bb76)" in rendered
    assert "job-2bb76a1c" not in rendered, "a human surface printed a raw id"


def test_the_watch_fleet_names_the_job_holding_a_device():
    screen = FleetScreen("http://x")
    screen.apply(
        {
            "type": "snapshot",
            "fleet": _fleet_with_a_busy_device(),
            "queue": {
                "now": T0,
                "queued": [],
                "running": [
                    {
                        "job_id": "job-2bb76a1c",
                        "name": "smoke-1",
                        "state": "running",
                        "requirements": [dict(VG)],
                        "resource_count": 1,
                        "agent_id": "bench-01",
                        "resource_ids": ["bench-01:vg-01"],
                        "waited_s": 1.0,
                        "elapsed_s": 42.0,
                        "max_duration_s": 600,
                        "attempt": 0,
                        "tried_agents": [],
                        "blocked_reason": None,
                        "reserving_on": None,
                        "submitted_at": T0,
                        "started_at": T0,
                        "reserving_resource_ids": [],
                    }
                ],
            },
        }
    )
    console = _console()

    console.print(screen.render(40))

    rendered = console.export_text()
    assert "smoke-1 (2bb76)" in rendered
    assert "job-2bb76a1c" not in rendered


def test_an_event_about_a_finished_job_still_says_its_name():
    """The event carries an id and nothing else — widening the event shape to
    carry a name would put presentation on the wire. The view remembers what it
    learned from snapshots instead, which is why a `job.completed` line for a job
    that has already left the queue still reads as a name."""
    screen = FleetScreen("http://x")
    screen.apply(
        {
            "type": "snapshot",
            "fleet": {"now": T0, "agents": []},
            "queue": {
                "now": T0,
                "queued": [],
                "running": [
                    {
                        "job_id": "job-2bb76a1c",
                        "name": "smoke-1",
                        "state": "running",
                        "requirements": [dict(VG)],
                        "resource_count": 1,
                        "agent_id": "bench-01",
                        "resource_ids": ["bench-01:vg-01"],
                        "waited_s": 1.0,
                        "elapsed_s": 42.0,
                        "max_duration_s": 600,
                        "attempt": 0,
                        "tried_agents": [],
                        "blocked_reason": None,
                        "reserving_on": None,
                        "submitted_at": T0,
                        "started_at": T0,
                        "reserving_resource_ids": [],
                    }
                ],
            },
        }
    )
    # ...the job finishes and leaves the queue.
    screen.apply({"type": "snapshot", "fleet": {"now": T0, "agents": []}, "queue": {}})
    screen.apply(
        {
            "type": "event",
            "event": {
                "ts": T0,
                "kind": "job.completed",
                "job_id": "job-2bb76a1c",
                "detail": {"outcome": "passed"},
            },
        }
    )
    console = _console()

    console.print(screen.render(40))

    assert "smoke-1 (2bb76)" in console.export_text()


def test_the_queue_reads_as_names():
    queue = {
        "now": T0,
        "running": [],
        "queued": [
            {
                "job_id": "job-067fe123",
                "name": "gw2gw",
                "state": "queued",
                "requirements": [dict(VG)] * 2,
                "resource_count": 2,
                "agent_id": None,
                "resource_ids": [],
                "waited_s": 35.0,
                "elapsed_s": None,
                "max_duration_s": 600,
                "attempt": 0,
                "tried_agents": [],
                "blocked_reason": None,
                "reserving_on": "bench-sf-01",
                "submitted_at": T0,
                "started_at": None,
                "reserving_resource_ids": ["bench-sf-01:vg-02"],
            }
        ],
    }
    console = _console()

    render_queue(queue, console)

    rendered = console.export_text()
    assert "gw2gw (067fe)" in rendered
    assert "job-067fe123" not in rendered
    assert "vg-02 held" in rendered, "the reservation must name the device it is withholding"


def test_why_reads_as_a_name():
    console = _console()

    render_why(
        {
            "job_id": "job-067fe123",
            "name": "gw2gw",
            "state": "queued",
            "waited_s": 35.0,
            "requirements": [dict(VG)] * 2,
            "resource_count": 2,
            "blocked_reason": None,
            "reserving": None,
            "attempt": 0,
            "tried_agents": [],
            "feasible": [
                {
                    "agent_id": "bench-sf-01",
                    "agent_state": "online",
                    "feasible": True,
                    "why_not": None,
                    "devices": [
                        {
                            "local_id": "vg-01",
                            "state": "busy",
                            "matches": True,
                            "current_job_id": "job-2bb76a1c",
                            "elapsed_s": 11.0,
                            "budget_s": 600,
                            "reserved_for_you": False,
                        }
                    ],
                }
            ],
            "infeasible": [],
            "waiting_on": None,
        },
        console,
        names={"job-2bb76a1c": "smoke-1"},
    )

    rendered = console.export_text()
    assert "gw2gw (067fe)" in rendered
    assert "smoke-1 (2bb76)" in rendered, "the device holding you up must name its job too"


# ------------------------------------------- the label is also an argument
def test_why_accepts_the_label_that_is_on_the_screen(dispatch_server):
    """If the screen says `gw2gw (067fe)` and the CLI only takes
    `job-067fe123`, the screen has stopped being useful. Full ids still work —
    including for jobs long gone from the queue, which the short form cannot
    reach — and are what every machine reads."""
    base, _config = dispatch_server
    submitted = httpx.post(
        f"{base}/v1/jobs",
        json={"name": "gw2gw", "requirements": [dict(VG)] * 2, "payload": {"duration_s": 1}},
        timeout=10.0,
    )
    job_id = submitted.json()["job_id"]
    args = argparse.Namespace(url=base)
    console = _console()

    assert _resolve_job(args, job_id, console) == job_id, "the full id must pass through"
    assert _resolve_job(args, short_id(job_id), console) == job_id
    assert _resolve_job(args, "gw2gw", console) == job_id
    assert _resolve_job(args, "nope", console) is None


def test_an_ambiguous_label_names_the_candidates_instead_of_guessing(dispatch_server):
    base, _config = dispatch_server
    for _ in range(2):
        httpx.post(
            f"{base}/v1/jobs",
            json={"name": "smoke", "requirements": [dict(VG)], "payload": {"duration_s": 1}},
            timeout=10.0,
        )
    args = argparse.Namespace(url=base)
    console = _console()

    assert _resolve_job(args, "smoke", console) is None
    assert "ambiguous" in console.export_text()

"""`GET /` — the live fleet view in a browser (§3.9).

Three properties, and the first two are the ones that fail in the room rather
than in CI:

  * the page loads with NO external request. The demo machine may have no
    internet; a CDN script tag turns the whole view into a blank rectangle and
    there is nothing to say while it does;
  * the page never polls. The push is the demo — a bench dying and the screen
    going red are the same moment — and a "just in case" fetch loop quietly
    turns that into a second of dead air that nobody can explain;
  * the snapshot carries what the page derives its numbers from. It re-computes
    elapsed, waited and beat-age locally on a one-second timer, which is only
    possible because the snapshot hands it the absolute timestamps rather than
    numbers that were true when they were sent.
"""

from __future__ import annotations

import asyncio
import json
import re
import time

import httpx
import pytest
import websockets

from tests.conftest import RunningAgent
from tss.api.web import index_path


@pytest.fixture(scope="module")
def page() -> str:
    with open(index_path(), encoding="utf-8") as handle:
        return handle.read()


# ------------------------------------------------------------------ served
def test_the_root_serves_the_page(dispatch_server):
    base, _config = dispatch_server

    response = httpx.get(f"{base}/", timeout=10.0)

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "TSS" in response.text
    assert "/v1/events" in response.text


def test_the_page_is_served_from_the_package_not_the_checkout():
    """A path relative to the repo works right up until someone installs the
    wheel, and then `GET /` 404s. We run editable, so this is the only place
    that difference is visible before it matters."""
    assert index_path().endswith("tss/api/static/index.html")
    assert "/tss/api/static/" in index_path()


# ------------------------------------------------------- no external request
def test_the_page_asks_the_internet_for_nothing(page):
    """No CDN, no web font, no image, no stylesheet. One file."""
    assert not re.search(r"https?://", page), "an absolute URL is a request off the box"
    assert not re.search(r"\b(?:src|href)\s*=", page), "no tag may load anything"
    assert "@import" not in page
    assert not re.search(r"\burl\(", page), "no CSS may fetch a font or an image"
    assert "<img" not in page
    assert "<link" not in page
    assert "<script src" not in page


def test_the_socket_url_is_built_from_the_page_it_was_served_from(page):
    """Hard-coding 127.0.0.1:8000 works on the laptop it was written on and
    nowhere else."""
    assert "location.host" in page
    assert "location.protocol" in page


def test_the_page_never_polls(page):
    """The forbidden fallback. `tss watch` and this page share one contract —
    snapshot, then deltas — and a fetch loop 'just in case' is how a push-driven
    demo silently becomes a polling one that nobody notices until the timing
    reads wrong on stage."""
    assert "fetch(" not in page
    assert "XMLHttpRequest" not in page
    assert "EventSource" not in page
    assert page.count("setInterval") == 1, "the one timer is the local re-render"
    assert "setInterval(render" in page


def test_a_reconnect_throws_the_old_view_away(page):
    """Merging across a reconnect leaves benches on screen that may have been
    reaped while the socket was down."""
    assert re.search(r"function reset\(\)", page)
    close = page[page.index("socket.onclose") : page.index("socket.onclose") + 800]
    assert "reset()" in close, "onclose must drop the state, not keep it"


def test_every_state_is_a_word_not_just_a_colour(page):
    """Projector colour is unreliable and part of the audience will not see it
    at all. Colour reinforces; the word carries — and every state word is
    uppercase, so a state never reads like prose."""
    for word in (
        "ONLINE",
        "DRAINING",
        "OFFLINE",
        "QUARANTINED",
        "RESERVED",
        "FREED",
        "RETIRED",
        "FREE",
        "BUSY",
        "UNHEALTHY",
        "GAVE UP",
        "INFRA ERROR",
    ):
        assert word in page, f"{word} is not spelled out anywhere"


def test_a_job_name_is_never_uppercased(page):
    """The reason the uppercasing is per-word rather than a text-transform on the
    rows: those rows hold job names too. `BUSY · smoke-1` must never become
    `BUSY · SMOKE-1` — a name is what the engineer typed."""
    assert "function stateWord(" in page, "state words must be uppercased in code, per word"
    for selector in (".st{", ".jname{", ".dev{", ".ev{", ".jmeta{"):
        start = page.index(selector)
        block = page[start : page.index("}", start)]
        assert "text-transform" not in block, (
            f"{selector} uppercases a whole container, which would rewrite job names"
        )


def test_dead_letter_reads_as_gave_up_on_screen_only(page):
    """The state is `dead_letter` everywhere a machine reads it and GAVE UP where
    a person does — and the legend carries the mapping so nobody has to guess."""
    assert '"GAVE UP"' in page, "the display label must exist"
    assert "dead_letter in the API" in page, "the legend must name the real state"
    assert "dead_letter:" in page, "the mapping must key off the real state name"


def test_the_state_key_is_three_frames(page):
    """Bench, Device, Job — as in the design target."""
    assert page.count('class="lgroup"') == 3
    for title in ("<h2>Bench</h2>", "<h2>Device</h2>", "<h2>Job</h2>"):
        assert title in page


def test_the_empty_state_says_only_what_is_true(page):
    """It used to suggest a command. The page is read-only and the command it
    named was wrong for half the ways a fleet gets started."""
    assert "no benches registered</div>" in page
    assert "just add bench" not in page
    assert "just agent bench" not in page


def test_values_from_the_fleet_are_escaped_before_they_reach_the_dom(page):
    """Job names, bench ids and capability values are caller-supplied strings
    that get interpolated into HTML."""
    assert "function h(value)" in page
    assert "&amp;" in page and "&lt;" in page


# ---------------------------------------------------- the snapshot it needs
def test_the_snapshot_carries_what_the_page_ticks_from(dispatch_server):
    """Additive fields, all of them absolute wall-clock or facts the view models
    did not carry before. `elapsed_s` and `waited_s` are still there and still
    mean what they meant; these are what let a client move them forward without
    asking the server again — which is what keeps the page push-only."""
    base, _config = dispatch_server
    ws_url = base.replace("http://", "ws://")

    async def scenario():
        async with (
            httpx.AsyncClient(base_url=base, timeout=20.0) as client,
            RunningAgent(base, count=1),
        ):
            deadline = time.monotonic() + 10
            while not (await client.get("/v1/fleet")).json()["agents"]:
                assert time.monotonic() < deadline
                await asyncio.sleep(0.02)
            await client.post(
                "/v1/jobs",
                json={
                    "name": "smoke-1",
                    "requirements": [{"product": "vehicle_gateway"}],
                    "payload": {"duration_s": 5},
                },
            )
            async with websockets.connect(f"{ws_url}/v1/events") as socket:
                began = time.monotonic()
                while time.monotonic() - began < 8:
                    message = json.loads(await asyncio.wait_for(socket.recv(), timeout=5))
                    if message["type"] != "snapshot":
                        continue
                    # ASSIGNED has no started_at yet; wait for the real thing.
                    if any(j["state"] == "running" for j in message["queue"]["running"]):
                        return message
            raise AssertionError("no snapshot with a running job arrived")

    snapshot = asyncio.run(scenario())

    agent = snapshot["fleet"]["agents"][0]
    assert "quarantined_at" in agent, "the QUARANTINED card has nothing to date itself from"
    assert "consecutive_fails" in agent
    device = agent["resources"][0]
    # Both a bench's own fault report and TSS's verdict read `unhealthy` in
    # `state`; only the second one needs an operator to clear it (§4.2), and
    # without this the page cannot tell the two rows apart.
    assert "quarantined_at" in device
    assert "consecutive_fails" in device

    job = snapshot["queue"]["running"][0]
    assert job["started_at"] is not None, "no started_at, no elapsed bar between events"
    assert job["submitted_at"] > 0
    assert job["max_duration_s"] > 0
    for entry in snapshot["queue"]["queued"]:
        assert "reserving_resource_ids" in entry

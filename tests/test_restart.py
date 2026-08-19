"""Restart recovery, in a real process (§7.2).

THE HEADLINE OF THE RESILIENCE STORY AND THE ONE UNTESTED PILLAR. Everything TSS
needs to recover from its own death is already on disk: leases are wall-clock
expiries in a table, ownership is an epoch in a row, allocation is a column. No
recovery code exists, and that is the claim — a new process reads the same file
and carries on. A claim with no test is a hope.

WHY A SUBPROCESS AND NOT THE APP OBJECT. The claim is process-level, and the
things that only break at that level are the ones worth testing: an unclean WAL
left by a SIGKILL, an in-memory directive queue and fence tracker that vanish,
a scheduler and reaper that have to be started again from nothing. Tearing down
a FastAPI app inside the same interpreter tests none of that — and would leave
SQLite's own connection state intact, which is precisely what a crash does not.

SIGKILL, not SIGTERM: a clean shutdown flushes and closes, which is the case that
was never in doubt.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

from tss.core.models import JobState
from tss.core.store import Store

REPO = Path(__file__).resolve().parent.parent
AGENT = "bench-restart-01"
VG = {"product": "vehicle_gateway"}
#: Short enough that a reap happens inside a test, long enough that a loaded CI
#: box does not reap the bench while it is still being set up.
ENV = {
    "TSS_PRESENCE_TTL_S": "3",
    "TSS_HEARTBEAT_INTERVAL_S": "0.5",
    "TSS_REAPER_INTERVAL_S": "0.3",
    "TSS_LONGPOLL_TIMEOUT_S": "1",
    "TSS_SCHEDULER_TICK_S": "0.5",
}


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Service:
    """A real `python -m uvicorn` on a real port, killable."""

    def __init__(self, db_path: str, port: int) -> None:
        self.db_path, self.port = db_path, port
        self.base = f"http://127.0.0.1:{port}"
        self.proc: subprocess.Popen | None = None

    def start(self, *, timeout: float = 30.0) -> None:
        env = {k: v for k, v in os.environ.items() if not k.startswith("TSS_")}
        env.update(ENV, TSS_DB_PATH=self.db_path)
        self.proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "tss.api.app:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--log-level",
                "warning",
            ],
            cwd=REPO,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise AssertionError(f"the service exited at once:\n{self.proc.stdout.read()}")
            try:
                if httpx.get(f"{self.base}/v1/fleet", timeout=1.0).status_code == 200:
                    return
            except httpx.HTTPError:
                time.sleep(0.1)
        raise AssertionError("the service never came up")

    def kill(self) -> None:
        """SIGKILL: no shutdown hook, no flush, no close."""
        assert self.proc is not None
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait(timeout=15)
        self.proc = None

    def stop(self) -> None:
        if self.proc is not None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait(timeout=15)


def _beat(base: str, running=()) -> httpx.Response:
    return httpx.post(
        f"{base}/v1/agents/{AGENT}/heartbeat",
        json={
            "running_jobs": [{"job_id": j, "epoch": e} for j, e in running],
            "resource_health": {},
        },
        timeout=20.0,
    )


def _register(base: str) -> httpx.Response:
    return httpx.post(
        f"{base}/v1/agents/register",
        json={
            "agent_id": AGENT,
            "hostname": f"{AGENT}.local",
            "agent_version": "0.1.0",
            "resources": [{"id": "vg-01", "capabilities": VG}],
        },
        timeout=20.0,
    )


@pytest.mark.slow
def test_tss_recovers_from_being_killed(tmp_path):
    db_path = str(tmp_path / "restart.db")
    service = Service(db_path, _free_port())
    service.start()
    try:
        # --- a fleet doing real work -------------------------------------
        assert _register(service.base).status_code == 200
        submitted = httpx.post(
            f"{service.base}/v1/jobs",
            json={"name": "survivor", "requirements": [VG], "payload": {"duration_s": 600}},
            timeout=20.0,
        )
        job_id = submitted.json()["job_id"]

        deadline = time.monotonic() + 20
        assignment = None
        while time.monotonic() < deadline:
            assignment = _beat(service.base).json().get("assignment")
            if assignment:
                break
            time.sleep(0.1)
        assert assignment is not None, "the job was never dispatched"
        started = httpx.post(
            f"{service.base}/v1/jobs/{job_id}/start",
            json={"agent_id": AGENT, "epoch": assignment["epoch"]},
            timeout=20.0,
        )
        assert started.status_code == 200
        before = httpx.get(f"{service.base}/v1/jobs/{job_id}", timeout=20.0).json()
        assert before["state"] == JobState.RUNNING

        # --- the process dies, mid-job, holding a device -----------------
        service.kill()

        # --- and a new one opens the same file ---------------------------
        service.start()

        # The WAL a SIGKILL left behind is readable, and the state is intact.
        after = httpx.get(f"{service.base}/v1/jobs/{job_id}", timeout=20.0).json()
        assert after["state"] in (JobState.RUNNING, JobState.QUEUED)
        assert after["epoch"] >= before["epoch"], "a restart must not lose the fence"
        fleet = httpx.get(f"{service.base}/v1/fleet", timeout=20.0).json()
        assert [a["id"] for a in fleet["agents"]] == [AGENT], "the bench is gone from the fleet"

        # The lease was written before the crash and has been expiring ever
        # since — wall-clock, not a timer in a dead process. The new reaper
        # sweeps it with no recovery code of its own.
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            state = next(
                a["state"]
                for a in httpx.get(f"{service.base}/v1/fleet", timeout=20.0).json()["agents"]
                if a["id"] == AGENT
            )
            if state == "offline":
                break
            time.sleep(0.1)
        assert state == "offline", "a stale lease survived the restart"

        # The bench comes back to a service that has never heard of this run:
        # 410, re-register, and it does NOT get its job back.
        rejected = _beat(service.base, running=[(job_id, assignment["epoch"])])
        assert rejected.status_code == 410, f"expected presence_expired, got {rejected.status_code}"
        assert rejected.json()["error"] == "presence_expired"
        assert _register(service.base).status_code == 200

        # And the interrupted job is not stranded: requeued and re-dispatched.
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            job = httpx.get(f"{service.base}/v1/jobs/{job_id}", timeout=20.0).json()
            if job["epoch"] > before["epoch"]:
                break
            _beat(service.base)
            time.sleep(0.1)
        assert job["epoch"] > before["epoch"], "the job was never taken back from the dead bench"
        assert job["state"] in (JobState.QUEUED, JobState.ASSIGNED, JobState.RUNNING)
    finally:
        service.stop()

    # --- the database itself, read directly --------------------------------
    store = Store(db_path)
    store.init_schema()  # a build that can open it can also verify its version
    assert store.get_job(job_id) is not None
    assert json.loads(json.dumps(store.fleet().model_dump(mode="json")))["agents"], (
        "the fleet did not survive being read by a third process"
    )
    store.close()

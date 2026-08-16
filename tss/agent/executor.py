"""Running a job against a resource set — simulated (§1.4 non-goal).

Actually flashing firmware is out of scope, so the executor sleeps for the
payload's `duration_s` and reports the payload's `outcome`. That is enough to
exercise everything TSS cares about: the device is genuinely held for the
duration, several jobs genuinely run at once on one bench, and a job can be made
to fail, hang, or blow up on demand.

The payload knobs (all optional):

    {"duration_s": 2.0,          # how long the "test" takes
     "outcome": "passed",        # passed | failed | infra_error
     "detail": "..."}            # free text recorded with the result
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

log = logging.getLogger("tss.agent.executor")

DEFAULT_DURATION_S = 2.0


@dataclass(frozen=True)
class ExecutionResult:
    outcome: str
    detail: str | None
    duration_s: float


async def execute(job_id: str, resource_ids: list[str], payload: dict[str, Any]) -> ExecutionResult:
    """Hold the devices for a while, then report. Never raises."""
    duration = float(payload.get("duration_s", DEFAULT_DURATION_S))
    outcome = str(payload.get("outcome", "passed"))
    detail = payload.get("detail")

    log.info("running %s on %s (%.1fs)", job_id, ",".join(resource_ids), duration)
    try:
        await asyncio.sleep(duration)
    except asyncio.CancelledError:
        # A cancel directive, or the daemon shutting down. The job did not
        # finish, and saying it did would be the worst possible lie.
        log.warning("%s cancelled mid-run", job_id)
        raise
    except Exception as exc:  # pragma: no cover — the simulator has nothing to throw
        log.exception("%s blew up in the executor", job_id)
        return ExecutionResult("infra_error", f"executor_error: {exc}", duration)

    log.info("finished %s -> %s", job_id, outcome)
    return ExecutionResult(outcome, detail if isinstance(detail, str) else None, duration)

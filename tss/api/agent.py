"""Agent-facing HTTP surface (§3.2, §6).

High-frequency, machine-to-machine. Kept on its own router from the client
endpoints because these are what will need their own auth and rate limits, and
because splitting the service in two later should be a config change rather than
a refactor.

The handlers hold no logic: validate, call the store, serialize. Every scheduling
decision is the scheduler's and every write is the store's.
"""

from __future__ import annotations

import logging
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from tss.api.deps import get_config, get_directives, get_scheduler, get_store
from tss.core.config import Config
from tss.core.directives import DirectiveQueue
from tss.core.models import Assignment, InventoryItem, Outcome, PresenceStatus
from tss.core.scheduler import Scheduler
from tss.core.store import Store

log = logging.getLogger("tss.api.agent")

router = APIRouter(prefix="/v1/agents", tags=["agent"])
#: /start and /complete are agent-facing too, despite living under /v1/jobs (§6).
job_router = APIRouter(prefix="/v1/jobs", tags=["agent"])

StoreDep = Annotated[Store, Depends(get_store)]
ConfigDep = Annotated[Config, Depends(get_config)]
SchedulerDep = Annotated[Scheduler, Depends(get_scheduler)]
DirectivesDep = Annotated[DirectiveQueue, Depends(get_directives)]


class RegisterRequest(BaseModel):
    agent_id: str
    hostname: str
    agent_version: str | None = None
    #: Inventory is PUSHED by the machine that can actually see the hardware —
    #: never read from a central config file (§3.1).
    resources: list[InventoryItem] = Field(default_factory=list)


class RegisterResponse(BaseModel):
    heartbeat_interval_s: float
    presence_ttl_s: float
    longpoll_timeout_s: float


class RunningJob(BaseModel):
    job_id: str
    epoch: int


class HeartbeatRequest(BaseModel):
    #: A bench with four devices may be running two jobs; it reports every job it
    #: believes it owns and TSS fences each INDEPENDENTLY (§6). Losing one job
    #: must not disturb the others running on the same machine.
    running_jobs: list[RunningJob] = Field(default_factory=list)
    #: The agent probes its own hardware; TSS never probes a device it cannot
    #: reach (§3.1).
    resource_health: dict[str, str] = Field(default_factory=dict)


class HeartbeatResponse(BaseModel):
    #: One assignment at a time. A bench with spare capacity heartbeats again
    #: immediately after taking one, so filling four devices costs four fast
    #: round-trips rather than four heartbeat intervals.
    assignment: Assignment | None = None
    #: e.g. [{"cancel_job": "job-8f21"}] — best-effort hints. The epoch is what
    #: actually fences a run TSS has given up on; this just stops the bench
    #: wasting hardware time on it.
    directives: list[Any] = Field(default_factory=list)


class StartRequest(BaseModel):
    agent_id: str
    epoch: int


class CompleteRequest(BaseModel):
    agent_id: str
    epoch: int
    outcome: Outcome
    detail: str | None = None
    duration_s: float | None = None


@router.post("/register", response_model=RegisterResponse)
async def register(req: RegisterRequest, store: StoreDep, config: ConfigDep) -> RegisterResponse:
    """Register a bench and its inventory. Idempotent — re-registering replaces
    the inventory in place and never duplicates a device (§6)."""
    result = store.register_agent(
        req.agent_id,
        req.hostname,
        req.resources,
        agent_version=req.agent_version,
    )
    log.info(
        "registered %s (%s) with %d device(s)%s",
        result.agent_id,
        "new" if result.is_new else "re-register",
        len(result.resource_ids),
        f" — requeued {len(result.requeued_jobs)} job(s)" if result.requeued_jobs else "",
    )
    return RegisterResponse(
        heartbeat_interval_s=config.heartbeat_interval_s,
        presence_ttl_s=config.presence_ttl_s,
        longpoll_timeout_s=config.longpoll_timeout_s,
    )


@router.post("/{agent_id}/heartbeat")
async def heartbeat(
    agent_id: str,
    req: HeartbeatRequest,
    store: StoreDep,
    scheduler: SchedulerDep,
    directives: DirectivesDep,
):
    """The workhorse: renew presence, report device health, collect work.

    The 404 and 410 are what make recovery reachable. A reaped agent's row still
    exists, so an unguarded renewal would hand it a cheerful 200 and leave it
    sitting in OFFLINE forever — gone from the fleet and never coming back, which
    is exactly what a bench does after a few dropped beats.

    LONG-POLL (§3.1). An agent with spare capacity blocks here for up to
    LONGPOLL_TIMEOUT and returns the instant the scheduler assigns it something —
    one endpoint, one connection per agent, sub-second dispatch. An agent at full
    capacity gets an immediate reply; it only needs to renew presence. Presence is
    renewed BEFORE the block, and LONGPOLL_TIMEOUT + HEARTBEAT_INTERVAL <
    PRESENCE_TTL (§7.1), so an agent's own long-poll can never let its lease lapse.
    """
    status, _agent = store.renew_presence(agent_id)

    if status is PresenceStatus.UNKNOWN_AGENT:
        return JSONResponse(
            status_code=404, content={"error": "unknown_agent", "action": "register"}
        )
    if status is PresenceStatus.EXPIRED:
        # It does NOT get its jobs back: they were requeued when the lease died.
        return JSONResponse(
            status_code=410, content={"error": "presence_expired", "action": "register"}
        )

    if req.resource_health:
        store.report_resource_health(agent_id, req.resource_health)

    # Fence what it believes it owns, one job at a time. A bench running two jobs
    # can lose one — cancelled, timed out, or reassigned after a network blip —
    # and must go on running the other; that is why the 409 names a job.
    lost = store.fence_running_jobs(agent_id, [(j.job_id, j.epoch) for j in req.running_jobs])
    if lost is not None:
        log.info("%s reported %s, which it no longer owns", agent_id, lost)
        return JSONResponse(
            status_code=409,
            content={"error": "lease_lost", "action": "abandon_job", "job_id": lost},
        )

    pending = directives.drain(agent_id)
    if pending:
        return HeartbeatResponse(assignment=store.pending_assignment(agent_id), directives=pending)

    assignment = store.pending_assignment(agent_id)
    if assignment is None and store.has_free_resources(agent_id):
        assignment = await scheduler.wait_for_assignment(agent_id)

    # Drained again after the long poll: a cancel may have been queued while we
    # were blocked here, and holding it for another heartbeat interval would let
    # the bench keep running something nobody wants.
    return HeartbeatResponse(assignment=assignment, directives=directives.drain(agent_id))


@job_router.post("/{job_id}/start")
async def start_job(job_id: str, req: StartRequest, store: StoreDep):
    """The agent has begun. Fenced by the epoch it was issued (§3.5)."""
    outcome = store.start_job(job_id, req.agent_id, req.epoch)
    if outcome != "started":
        return _fenced_out(outcome, job_id)
    return {"accepted": True}


@job_router.post("/{job_id}/complete")
async def complete_job(job_id: str, req: CompleteRequest, store: StoreDep, scheduler: SchedulerDep):
    """The result, and the release of every device the job held.

    THE ZOMBIE (§3.5). An agent that was isolated, had its job requeued, and has
    now come back reports a result for a run that was abandoned. Its epoch is
    stale, so it gets a 409 and abandons the job instead of overwriting a result
    someone else produced. Without this, an engineer ships on a result from a run
    that TSS had already given up on.
    """
    outcome = store.complete_job(job_id, req.agent_id, req.epoch, req.outcome, detail=req.detail)
    if outcome in ("accepted", "requeued", "dead_lettered"):
        # Devices came free — the queue should be looked at right now, not on the
        # next backstop tick.
        scheduler.notify()
        return {"accepted": True, "result": outcome}
    return _fenced_out(outcome, job_id)


def _fenced_out(reason: str, job_id: str) -> JSONResponse:
    if reason == "unknown_job":
        return JSONResponse(status_code=404, content={"error": "unknown_job", "job_id": job_id})
    return JSONResponse(
        status_code=409,
        content={"error": "stale_epoch", "action": "abandon_job", "job_id": job_id},
    )

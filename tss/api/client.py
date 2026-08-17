"""Human- and CI-facing HTTP surface (§3.2, §6).

Low frequency, low stakes, separate router — these will want different auth and
different rate limits from the agent endpoints, and splitting the service in two
later should be a config change rather than a refactor.
"""

from __future__ import annotations

import time
import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from tss.api.deps import get_config, get_directives, get_scheduler, get_store
from tss.core.config import Config
from tss.core.directives import DirectiveQueue
from tss.core.models import CapabilitySpec, FleetView, Job, JobState
from tss.core.scheduler import Scheduler
from tss.core.store import Store

router = APIRouter(prefix="/v1", tags=["client"])

StoreDep = Annotated[Store, Depends(get_store)]
SchedulerDep = Annotated[Scheduler, Depends(get_scheduler)]
DirectivesDep = Annotated[DirectiveQueue, Depends(get_directives)]
ConfigDep = Annotated[Config, Depends(get_config)]


class SubmitRequest(BaseModel):
    name: str
    #: A LIST of tag-subsets, one per device needed (§3.4). Modelling it this way
    #: from the first commit is what lets "a heavy-duty VG AND an AG on the same
    #: bench" be expressible later without reshaping the schema.
    requirements: list[CapabilitySpec]
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = 100
    max_duration_s: int | None = None


class SubmitResponse(BaseModel):
    job_id: str
    queue_position: int


class ReservationView(BaseModel):
    """Devices being withheld for this job. They are still `free` in the database
    and owned by nobody — reserve is not claim (§3.4.1)."""

    agent_id: str
    resource_ids: list[str] = Field(default_factory=list)
    since: float


class JobStatus(Job):
    reserving: ReservationView | None = None


class QueueEntry(BaseModel):
    job_id: str
    name: str
    state: JobState
    requirements: list[CapabilitySpec]
    resource_count: int
    agent_id: str | None = None
    resource_ids: list[str] = Field(default_factory=list)
    waited_s: float
    elapsed_s: float | None = None
    max_duration_s: int
    attempt: int
    tried_agents: list[str] = Field(default_factory=list)
    blocked_reason: str | None = None
    reserving_on: str | None = None


class QueueView(BaseModel):
    now: float
    queued: list[QueueEntry] = Field(default_factory=list)
    running: list[QueueEntry] = Field(default_factory=list)


@router.post("/jobs", response_model=SubmitResponse, status_code=201)
async def submit_job(
    req: SubmitRequest, store: StoreDep, scheduler: SchedulerDep, config: ConfigDep
) -> SubmitResponse:
    if not req.requirements:
        raise HTTPException(status_code=400, detail="a job must require at least one resource")
    if len(req.requirements) > config.max_resources_per_job:
        # Matching is a backtracking search over specs x devices. It is trivial
        # at the sizes real HIL tests use, and this bound keeps it that way
        # rather than letting one absurd job pin a scheduler pass.
        raise HTTPException(
            status_code=400,
            detail=(
                f"a job may require at most {config.max_resources_per_job} devices; "
                f"got {len(req.requirements)}"
            ),
        )

    job_id = f"job-{uuid.uuid4().hex[:8]}"
    store.submit_job(
        job_id,
        req.name,
        req.requirements,
        payload=req.payload,
        priority=req.priority,
        max_duration_s=req.max_duration_s,
    )
    # Wake the scheduler now rather than waiting for the backstop tick.
    scheduler.notify()
    return SubmitResponse(job_id=job_id, queue_position=store.queue_position(job_id))


@router.get("/jobs/{job_id}", response_model=JobStatus)
async def get_job(job_id: str, store: StoreDep, scheduler: SchedulerDep) -> JobStatus:
    """A job, plus why it is not moving.

    `blocked_reason` and `reserving` are the two answers to "why am I waiting"
    that are genuinely non-obvious once jobs need several devices — and the
    second one lives only in scheduler memory, because a reservation
    deliberately leaves no trace in the database (§3.4.1).
    """
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"unknown job {job_id}")
    reservation = scheduler.reservation_for(job_id)
    return JobStatus(
        **job.model_dump(),
        reserving=(
            None
            if reservation is None
            else ReservationView(
                agent_id=reservation.agent_id,
                resource_ids=sorted(reservation.resource_ids),
                since=reservation.since,
            )
        ),
    )


@router.delete("/jobs/{job_id}")
async def cancel_job(
    job_id: str, store: StoreDep, scheduler: SchedulerDep, directives: DirectivesDep
):
    """Cancel a job (§6).

    Queued dies quietly. Running is fenced first: the epoch bump inside
    `store.cancel_job` is what stops the agent's late "PASSED" from overwriting
    CANCELLED — I7, and the reason cancel is not simply a state change. The
    directive is a courtesy that stops the bench wasting hardware time; it is not
    what makes the outcome correct.
    """
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"unknown job {job_id}")
    agent_id = job.agent_id

    result = store.cancel_job(job_id)
    if result == "unknown_job":
        raise HTTPException(status_code=404, detail=f"unknown job {job_id}")
    if result == "already_terminal":
        raise HTTPException(
            status_code=409,
            detail=f"{job_id} already finished as {job.state} — a result is never overwritten",
        )

    if result == "cancelled_running" and agent_id:
        directives.cancel_job(agent_id, job_id)
    scheduler.notify()  # its devices just came free
    return {"cancelled": True, "job_id": job_id, "was_running": result == "cancelled_running"}


@router.get("/queue", response_model=QueueView)
async def queue(store: StoreDep, scheduler: SchedulerDep) -> QueueView:
    """Queued and running jobs with wait times (§3.9).

    Elapsed-versus-budget is shown; an estimated start time is not. A confident
    "starts in ~3m" would be a lie without historical duration data, which the
    POC does not have (§13.6).
    """
    now = time.time()
    view = QueueView(now=now)
    for job in store.jobs_in_flight():
        entry = QueueEntry(
            job_id=job.id,
            name=job.name,
            state=job.state,
            requirements=job.requirements,
            resource_count=job.resource_count,
            agent_id=job.agent_id,
            resource_ids=store.resources_held_by(job.id),
            waited_s=max(0.0, (job.assigned_at or now) - job.submitted_at),
            elapsed_s=None if job.started_at is None else max(0.0, now - job.started_at),
            max_duration_s=job.max_duration_s,
            attempt=job.attempt,
            tried_agents=job.tried_agents,
            blocked_reason=job.blocked_reason,
            reserving_on=(
                r.agent_id if (r := scheduler.reservation_for(job.id)) is not None else None
            ),
        )
        if job.state == JobState.QUEUED:
            view.queued.append(entry)
        else:
            view.running.append(entry)
    return view


@router.get("/fleet", response_model=FleetView)
async def fleet(store: StoreDep) -> FleetView:
    """Benches, each with the devices cabled to it (§3.9) — including benches
    that died while holding nothing, which is the case a bench-level view
    misses."""
    return store.fleet()

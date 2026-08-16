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

from tss.api.deps import get_scheduler, get_store
from tss.core.models import CapabilitySpec, FleetView, Job, JobState
from tss.core.scheduler import Scheduler
from tss.core.store import Store

router = APIRouter(prefix="/v1", tags=["client"])

StoreDep = Annotated[Store, Depends(get_store)]
SchedulerDep = Annotated[Scheduler, Depends(get_scheduler)]


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


class QueueView(BaseModel):
    now: float
    queued: list[QueueEntry] = Field(default_factory=list)
    running: list[QueueEntry] = Field(default_factory=list)


@router.post("/jobs", response_model=SubmitResponse, status_code=201)
async def submit_job(
    req: SubmitRequest, store: StoreDep, scheduler: SchedulerDep
) -> SubmitResponse:
    if not req.requirements:
        raise HTTPException(status_code=400, detail="a job must require at least one resource")
    if len(req.requirements) > 1:
        # Multi-device is off until step 5, where reservation and the invariant
        # checker land together. Rejecting is honest; accepting and scheduling
        # only the first requirement would be a silent partial allocation, which
        # is the one thing this system exists to prevent.
        raise HTTPException(
            status_code=400,
            detail=(
                "multi-device jobs are not enabled yet (step 5): "
                f"got {len(req.requirements)} requirements, expected 1"
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


@router.get("/jobs/{job_id}")
async def get_job(job_id: str, store: StoreDep) -> Job:
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"unknown job {job_id}")
    return job


@router.get("/queue", response_model=QueueView)
async def queue(store: StoreDep) -> QueueView:
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

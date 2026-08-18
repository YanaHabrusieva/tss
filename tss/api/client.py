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
from pydantic import BaseModel, Field, field_validator

from tss.api.deps import get_config, get_directives, get_scheduler, get_store
from tss.core import matcher
from tss.core.config import Config
from tss.core.directives import DirectiveQueue
from tss.core.models import (
    AgentState,
    CapabilitySpec,
    FleetView,
    Job,
    JobState,
    Outcome,
    ResourceState,
    local_of,
)
from tss.core.scheduler import Scheduler
from tss.core.store import Store

router = APIRouter(prefix="/v1", tags=["client"])

StoreDep = Annotated[Store, Depends(get_store)]
SchedulerDep = Annotated[Scheduler, Depends(get_scheduler)]
DirectivesDep = Annotated[DirectiveQueue, Depends(get_directives)]
ConfigDep = Annotated[Config, Depends(get_config)]


#: One day. Long enough for any HIL test anyone has described; short enough that a
#: typo cannot pin a device until someone notices.
MAX_JOB_DURATION_S = 86_400


class SubmitRequest(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    #: A LIST of tag-subsets, one per device needed (§3.4). Modelling it this way
    #: from the first commit is what lets "a heavy-duty VG AND an AG on the same
    #: bench" be expressible later without reshaping the schema.
    requirements: list[CapabilitySpec]
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=100, ge=0, le=1000)
    #: `0` would start, be killed by the next timeout sweep, and be recorded as
    #: `infra_error` — the fleet blamed for the caller's input, which is the one
    #: thing the FAILED/INFRA_ERROR split exists to prevent (§4.3).
    max_duration_s: int | None = Field(default=None, gt=0, le=MAX_JOB_DURATION_S)

    @field_validator("requirements")
    @classmethod
    def _scalar_spec_values(cls, requirements: list[CapabilitySpec]) -> list[CapabilitySpec]:
        """A spec value must be a scalar, and must not be null.

        `{"product": null}` matches EVERY device that lacks the key, because the
        subset test compares `capabilities.get(k) == v` and `.get` returns None
        for an absent key. A typo'd null silently widens the match to the whole
        fleet instead of narrowing it — the job runs on the wrong hardware and
        nothing anywhere reports a problem.
        """
        for spec in requirements:
            for key, value in spec.items():
                if value is None:
                    raise ValueError(
                        f"requirement {key!r} is null, which would match every device "
                        "that lacks that capability"
                    )
                if not isinstance(value, str | int | float | bool):
                    raise ValueError(
                        f"requirement {key!r} must be a scalar, got {type(value).__name__}"
                    )
        return requirements

    @field_validator("payload")
    @classmethod
    def _sane_duration(cls, payload: dict[str, Any]) -> dict[str, Any]:
        """The bench reads `duration_s` to decide how long to hold the hardware.
        A non-numeric one reaches the agent, kills the task, and the job burns
        its whole budget twice before dead-lettering as an infra error."""
        if "duration_s" not in payload:
            return payload
        duration = payload["duration_s"]
        if isinstance(duration, bool) or not isinstance(duration, int | float):
            raise ValueError(f"payload.duration_s must be a number, got {duration!r}")
        if duration < 0:
            raise ValueError(f"payload.duration_s must not be negative, got {duration!r}")
        return payload


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


def queue_view(store: Store, scheduler: Scheduler) -> QueueView:
    """Queued and running jobs with wait times (§3.9).

    Elapsed-versus-budget is shown; an estimated start time is not. A confident
    "starts in ~3m" would be a lie without historical duration data, which the
    POC deliberately does not collect (§13.7) — and a number you cannot support
    is exactly the thing that gets pulled on.
    """
    now = time.time()
    view = QueueView(now=now)
    for job in store.jobs_in_flight():
        reservation = scheduler.reservation_for(job.id)
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
            reserving_on=reservation.agent_id if reservation else None,
        )
        if job.state == JobState.QUEUED:
            view.queued.append(entry)
        else:
            view.running.append(entry)
    return view


@router.get("/queue", response_model=QueueView)
async def queue(store: StoreDep, scheduler: SchedulerDep) -> QueueView:
    return queue_view(store, scheduler)


class DeviceLine(BaseModel):
    local_id: str
    state: ResourceState
    matches: bool
    current_job_id: str | None = None
    elapsed_s: float | None = None
    budget_s: int | None = None
    reserved_for_you: bool = False


class BenchAssessment(BaseModel):
    agent_id: str
    agent_state: AgentState
    feasible: bool
    #: Why this bench can never satisfy the job, in words. Named for the human
    #: reading it, not for the matcher.
    why_not: str | None = None
    devices: list[DeviceLine] = Field(default_factory=list)


class WhyView(BaseModel):
    job_id: str
    name: str
    state: JobState
    waited_s: float
    requirements: list[CapabilitySpec]
    resource_count: int
    blocked_reason: str | None = None
    reserving: ReservationView | None = None
    agent_id: str | None = None
    elapsed_s: float | None = None
    max_duration_s: int = 600
    outcome: Outcome | None = None
    result_detail: str | None = None
    attempt: int = 0
    tried_agents: list[str] = Field(default_factory=list)
    feasible: list[BenchAssessment] = Field(default_factory=list)
    infeasible: list[BenchAssessment] = Field(default_factory=list)
    waiting_on: str | None = None


@router.get("/jobs/{job_id}/why", response_model=WhyView)
async def why(job_id: str, store: StoreDep, scheduler: SchedulerDep) -> WhyView:
    """ "Why is my test stuck?" — answered in the tool (§3.9).

    The number-one support question for any shared-hardware system, and the
    default answer is a Slack message to whoever owns the fleet. On a
    multi-device fleet it also removes a category of FALSE bug report: "the
    scheduler is broken, there are free VGs and my job isn't running" is, nine
    times out of ten, the reservation logic working exactly as designed.

    Everything here is something TSS actually knows. Elapsed-versus-budget, yes;
    an estimated start time, no — that needs duration history this POC does not
    keep (§13.7).
    """
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"unknown job {job_id}")

    now = time.time()
    reservation = scheduler.reservation_for(job_id)
    reserved_ids = set(reservation.resource_ids) if reservation else set()
    view = WhyView(
        job_id=job.id,
        name=job.name,
        state=job.state,
        waited_s=max(0.0, (job.assigned_at or now) - job.submitted_at),
        requirements=job.requirements,
        resource_count=job.resource_count,
        blocked_reason=job.blocked_reason,
        agent_id=job.agent_id,
        elapsed_s=None if job.started_at is None else max(0.0, now - job.started_at),
        max_duration_s=job.max_duration_s,
        outcome=job.outcome,
        result_detail=job.result_detail,
        attempt=job.attempt,
        tried_agents=job.tried_agents,
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
    if job.is_terminal:
        return view

    by_agent: dict[str, list] = {}
    for resource in store.list_resources():
        by_agent.setdefault(resource.agent_id, []).append(resource)

    soonest: tuple[float, str] | None = None
    for agent in store.agents():
        devices = by_agent.get(agent.id, [])
        installed = matcher.installed(devices)
        feasible = agent.state == AgentState.ONLINE and matcher.could_ever_satisfy(
            job.requirements, devices
        )
        assessment = BenchAssessment(
            agent_id=agent.id,
            agent_state=agent.state,
            feasible=feasible,
            why_not=None if feasible else _why_not(agent, job, installed, devices),
        )
        for resource in sorted(devices, key=lambda r: r.id):
            matches = any(matcher.satisfies(resource, spec) for spec in job.requirements)
            if resource.state == ResourceState.RETIRED and not matches:
                continue
            holder = store.get_job(resource.current_job_id) if resource.current_job_id else None
            elapsed = (
                None if holder is None or holder.started_at is None else now - holder.started_at
            )
            assessment.devices.append(
                DeviceLine(
                    local_id=local_of(resource.id),
                    state=resource.state,
                    matches=matches,
                    current_job_id=resource.current_job_id,
                    elapsed_s=elapsed,
                    budget_s=holder.max_duration_s if holder else None,
                    reserved_for_you=resource.id in reserved_ids,
                )
            )
            if feasible and matches and holder is not None and elapsed is not None:
                remaining = holder.max_duration_s - elapsed
                if soonest is None or remaining < soonest[0]:
                    soonest = (remaining, f"{local_of(resource.id)} on {agent.id}")

        (view.feasible if feasible else view.infeasible).append(assessment)

    if job.state == JobState.QUEUED:
        if not view.feasible:
            view.waiting_on = "nothing — no bench in the fleet can run this"
        elif soonest is not None:
            view.waiting_on = (
                f"{soonest[1]} to free (~{max(0, int(soonest[0]))}s of its budget left)"
            )
        else:
            view.waiting_on = "the next scheduling pass"
    return view


def _why_not(agent, job, installed, devices) -> str:
    """One sentence a human can act on."""
    if agent.state == AgentState.OFFLINE:
        return "OFFLINE — its presence lease expired"
    if agent.state == AgentState.QUARANTINED:
        stamp = (
            f" since {time.strftime('%H:%M', time.localtime(agent.quarantined_at))}"
            if (agent.quarantined_at)
            else ""
        )
        return f"QUARANTINED{stamp} (repeated failures across its devices)"
    if agent.state == AgentState.DRAINING:
        return "DRAINING — finishing what it has, accepting nothing new"
    matching = [r for r in installed if any(matcher.satisfies(r, s) for s in job.requirements)]
    broken = [
        r
        for r in devices
        if r.state in (ResourceState.UNHEALTHY, ResourceState.RETIRED)
        and any(matcher.satisfies(r, s) for s in job.requirements)
    ]
    if len(matching) < job.resource_count:
        detail = f"only {len(matching)} healthy matching device(s), needs {job.resource_count}"
        if broken:
            detail += f" — {', '.join(f'{local_of(r.id)} is {r.state}' for r in broken)}"
        return detail
    return "its devices cannot be assigned to these requirements together"


@router.post("/agents/{agent_id}/drain")
async def drain(agent_id: str, store: StoreDep, scheduler: SchedulerDep):
    """Finish current jobs, accept no more (§4.1).

    Every non-automatic transition needs a control surface, and this is the one
    deploys need: without it, upgrading an agent means killing running tests.
    """
    result = store.drain_agent(agent_id)
    if result == "unknown_agent":
        raise HTTPException(status_code=404, detail=f"unknown agent {agent_id}")
    if result.startswith("not_online"):
        raise HTTPException(
            status_code=409, detail=f"{agent_id} is {result.split(':', 1)[1]}, not online"
        )
    scheduler.notify()  # its devices are out of the pool now
    # ...and release the bench's in-flight long-poll so it hears about this on
    # THIS beat rather than up to LONGPOLL_TIMEOUT later. The drain directive is
    # derived from state rather than queued, so unlike a cancel nothing wakes the
    # agent as a side effect of pushing it — a bench would keep taking work it is
    # not supposed to take for another eight seconds.
    scheduler.wake_agent(agent_id)
    return {"draining": True, "agent_id": agent_id}


@router.post("/agents/{agent_id}/unquarantine")
async def unquarantine_agent(agent_id: str, store: StoreDep, scheduler: SchedulerDep):
    """Let a machine back in. A state with no way out is a slow fleet-drain
    dressed up as a health feature (§4.2)."""
    result = store.unquarantine_agent(agent_id)
    if result == "unknown_agent":
        raise HTTPException(status_code=404, detail=f"unknown agent {agent_id}")
    scheduler.notify()
    return {"unquarantined": True, "agent_id": agent_id}


@router.post("/resources/{resource_id:path}/unquarantine")
async def unquarantine_resource(resource_id: str, store: StoreDep, scheduler: SchedulerDep):
    """Let one device back in. One dead J-Link costs you one device, not the
    bench — and clearing it costs one command, not a re-registration."""
    result = store.unquarantine_resource(resource_id)
    if result == "unknown_resource":
        raise HTTPException(status_code=404, detail=f"unknown resource {resource_id}")
    if result == "retired":
        raise HTTPException(
            status_code=409,
            detail=(
                f"{resource_id} is retired — it is not on the bench any more. "
                "Re-register the agent with it attached instead."
            ),
        )
    scheduler.notify()
    return {"unquarantined": True, "resource_id": resource_id}


@router.get("/fleet", response_model=FleetView)
async def fleet(store: StoreDep) -> FleetView:
    """Benches, each with the devices cabled to it (§3.9) — including benches
    that died while holding nothing, which is the case a bench-level view
    misses."""
    return store.fleet()

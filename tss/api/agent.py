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

from tss.api.deps import get_config, get_store
from tss.core.config import Config
from tss.core.models import InventoryItem, PresenceStatus
from tss.core.store import Store

log = logging.getLogger("tss.api.agent")

router = APIRouter(prefix="/v1/agents", tags=["agent"])

StoreDep = Annotated[Store, Depends(get_store)]
ConfigDep = Annotated[Config, Depends(get_config)]


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
    #: believes it owns and TSS fences each independently (§6). Fencing arrives
    #: with completion in step 4 — for now the field is accepted and recorded.
    running_jobs: list[RunningJob] = Field(default_factory=list)
    #: The agent probes its own hardware; TSS never probes a device it cannot
    #: reach (§3.1).
    resource_health: dict[str, str] = Field(default_factory=dict)


class HeartbeatResponse(BaseModel):
    #: Always null until the scheduler exists (step 3), along with the long-poll
    #: that makes dispatch sub-second. An agent still heartbeats to hold presence.
    assignment: dict[str, Any] | None = None
    directives: list[Any] = Field(default_factory=list)


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
async def heartbeat(agent_id: str, req: HeartbeatRequest, store: StoreDep):
    """The workhorse: renew presence, report device health, collect work.

    The 404 and 410 are what make recovery reachable. A reaped agent's row still
    exists, so an unguarded renewal would hand it a cheerful 200 and leave it
    sitting in OFFLINE forever — gone from the fleet and never coming back, which
    is exactly what a bench does after a few dropped beats.
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

    return HeartbeatResponse()

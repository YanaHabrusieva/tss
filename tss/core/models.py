"""Domain types: Agent, Resource, Job, Assignment (§4, §5).

Two levels, and the distinction is the whole design (§1.2):

    AGENT     the machine. Heartbeats, holds the presence lease. No `busy` state —
              capacity is counted, not flagged.
    RESOURCE  a device cabled to that machine. The unit of allocation.
    JOB       claims a *set* of resources, all on one agent, all-or-nothing.

All timestamps are absolute unix floats in UTC wall-clock (§3.3).
"""

from __future__ import annotations

import json
import sqlite3
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class AgentState(StrEnum):
    ONLINE = "online"
    OFFLINE = "offline"
    QUARANTINED = "quarantined"
    DRAINING = "draining"


class ResourceState(StrEnum):
    FREE = "free"
    BUSY = "busy"
    UNHEALTHY = "unhealthy"


class JobState(StrEnum):
    QUEUED = "queued"
    ASSIGNED = "assigned"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    INFRA_ERROR = "infra_error"
    CANCELLED = "cancelled"
    DEAD_LETTER = "dead_letter"


#: A job is done when it lands in one of these; I3 (liveness) is stated over this set,
#: and I7 says none of them is ever overwritten.
TERMINAL_JOB_STATES: frozenset[JobState] = frozenset(
    {
        JobState.PASSED,
        JobState.FAILED,
        JobState.INFRA_ERROR,
        JobState.CANCELLED,
        JobState.DEAD_LETTER,
    }
)


class Outcome(StrEnum):
    PASSED = "passed"
    FAILED = "failed"  # the firmware misbehaved — the engineer's problem
    INFRA_ERROR = "infra_error"  # the rig misbehaved — our problem (§4.3)
    CANCELLED = "cancelled"
    DEAD_LETTER = "dead_letter"


#: One entry per device a job needs. A spec matches a resource if the resource's
#: capabilities satisfy *every* key in the spec — a subset test, per resource (§3.4).
CapabilitySpec = dict[str, Any]


class Agent(BaseModel):
    id: str
    hostname: str
    state: AgentState
    presence_expires_at: float
    last_heartbeat_at: float
    consecutive_fails: int = 0
    quarantined_at: float | None = None
    registered_at: float
    agent_version: str | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Agent:
        return cls(**dict(row))


class Resource(BaseModel):
    id: str
    agent_id: str
    capabilities: dict[str, Any]
    state: ResourceState
    current_job_id: str | None = None
    last_assigned_at: float | None = None
    consecutive_fails: int = 0
    quarantined_at: float | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Resource:
        d = dict(row)
        d["capabilities"] = json.loads(d["capabilities"])
        return cls(**d)

    def satisfies(self, spec: CapabilitySpec) -> bool:
        """Subset test (§3.4): every key in the spec must match this device."""
        return all(self.capabilities.get(k) == v for k, v in spec.items())


class Job(BaseModel):
    id: str
    name: str
    requirements: list[CapabilitySpec]
    resource_count: int
    payload: dict[str, Any] = Field(default_factory=dict)
    state: JobState
    agent_id: str | None = None
    epoch: int = 0  # fencing token; only ever self-incremented in SQL (§3.5)
    attempt: int = 0  # total dispatches — history only
    tried_agents: list[str] = Field(default_factory=list)  # drives retry AND poison
    priority: int = 100
    max_duration_s: int = 600
    submitted_at: float
    assigned_at: float | None = None
    started_at: float | None = None
    finished_at: float | None = None
    blocked_reason: str | None = None
    outcome: Outcome | None = None
    result_detail: str | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Job:
        d = dict(row)
        d["requirements"] = json.loads(d["requirements"])
        d["payload"] = json.loads(d["payload"])
        d["tried_agents"] = json.loads(d["tried_agents"])
        return cls(**d)

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_JOB_STATES


class Assignment(BaseModel):
    """What the agent is handed. The epoch is the fence (§3.5) — it comes back on
    every /start and /complete, and a stale one is rejected."""

    job_id: str
    epoch: int
    agent_id: str
    resource_ids: list[str]
    payload: dict[str, Any] = Field(default_factory=dict)
    max_duration_s: int = 600


class ClaimResult(BaseModel):
    """Outcome of the N-way all-or-nothing claim (§3.3).

    `ok=False` is not an error — it is a lost race, and the scheduler's answer is
    to try the next agent. `reason` exists for events and diagnosis only.
    """

    ok: bool
    job_id: str
    agent_id: str
    epoch: int | None = None
    resource_ids: list[str] = Field(default_factory=list)
    reason: str | None = None
    blocked_by: str | None = None  # the resource we lost the race on, if any

    def __bool__(self) -> bool:
        return self.ok


class Event(BaseModel):
    seq: int | None = None
    ts: float
    kind: str
    agent_id: str | None = None
    resource_id: str | None = None
    job_id: str | None = None
    detail: dict[str, Any] | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Event:
        d = dict(row)
        d["detail"] = json.loads(d["detail"]) if d["detail"] else None
        return cls(**d)

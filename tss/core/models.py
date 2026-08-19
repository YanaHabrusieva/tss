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

from pydantic import BaseModel, Field, field_validator


class AgentState(StrEnum):
    ONLINE = "online"
    OFFLINE = "offline"
    QUARANTINED = "quarantined"
    DRAINING = "draining"


class ResourceState(StrEnum):
    FREE = "free"
    BUSY = "busy"
    #: Present but broken — a dropped J-Link, an unresponsive DUT. Someone can
    #: walk over and fix it, and the bench keeps working on its other devices.
    UNHEALTHY = "unhealthy"
    #: Gone — it vanished from the inventory when the agent re-registered. Never
    #: deleted (`job_resources` records which devices a past attempt ran on),
    #: never matched, and hidden from the default fleet view. Parking these in
    #: `unhealthy` instead would fill the fleet view with ghosts nobody can ever
    #: repair (§4.2).
    RETIRED = "retired"


#: States a device can never be allocated from. The claim's own `state='free'`
#: guard is what enforces it; this is for the matcher and the fleet view.
UNSCHEDULABLE_RESOURCE_STATES: frozenset[ResourceState] = frozenset(
    {ResourceState.UNHEALTHY, ResourceState.RETIRED}
)


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
    """Whose problem is it? `JobState` says what happened; this says who owns it.

    Deliberately NOT a copy of `JobState`: there is no `dead_letter` here. A
    dead-lettered job is `state='dead_letter'`, `outcome='infra_error'` — it only
    ever gets there down the infra retry path, since FAILED is a real result and
    never retries (§4.2). Repeating the state in the outcome would discard the
    one distinction the data model exists to preserve, on the jobs that failed
    worst: a dashboard counting `infra_error` would silently exclude the most
    severe infra failures in the fleet.
    """

    PASSED = "passed"
    FAILED = "failed"  # the firmware misbehaved — the engineer's problem
    INFRA_ERROR = "infra_error"  # the rig misbehaved — our problem (§4.3)
    CANCELLED = "cancelled"


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
    #: The bench reported this device faulty while it was busy; the fault lands
    #: when the device is released (§4.2).
    fault_reported_at: float | None = None

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
    #: When this job became unsatisfiable — NOT when it was submitted. The
    #: dead-letter window runs from here and resets if the fleet recovers.
    blocked_since: float | None = None
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


#: Characters an id may not contain. `:` is the separator `qualify()` uses, so a
#: device locally named "bench-01:vg-01" on agent "bench-01" would qualify to the
#: same row as a plain "vg-01" — two physical devices silently merged into one.
#: A `:` in an AGENT id also misroutes `tss unquarantine`, which uses it to tell a
#: device id from a bench id. `/` would land inside URL paths.
FORBIDDEN_IN_IDS = (":", "/")


def check_identifier(value: str, *, what: str) -> str:
    """Agent and device ids are structural: they become row keys and URL paths."""
    if not value or not value.strip():
        raise ValueError(f"{what} must not be empty")
    if value != value.strip():
        raise ValueError(f"{what} must not have leading or trailing whitespace: {value!r}")
    for character in FORBIDDEN_IN_IDS:
        if character in value:
            raise ValueError(f"{what} must not contain {character!r}: {value!r}")
    return value


class InventoryItem(BaseModel):
    """One device, as the agent reports it (§6). `id` is bench-local ("vg-01");
    TSS qualifies it to "bench-sf-04:vg-01" so ids are unique fleet-wide."""

    id: str
    capabilities: dict[str, Any] = Field(default_factory=dict)

    @field_validator("id")
    @classmethod
    def _valid_id(cls, value: str) -> str:
        return check_identifier(value, what="device id")


class PresenceStatus(StrEnum):
    """Outcome of a heartbeat's presence renewal (§3.5)."""

    RENEWED = "renewed"
    UNKNOWN_AGENT = "unknown_agent"  # -> 404, agent registers
    EXPIRED = "presence_expired"  # -> 410, agent re-registers. It does NOT get its jobs back.


class Registration(BaseModel):
    """What a register call did. Re-registering is not free: a bench that comes
    back has lost its hardware state, so everything it held is requeued (§6)."""

    agent_id: str
    is_new: bool
    requeued_jobs: list[str] = Field(default_factory=list)
    dead_lettered_jobs: list[str] = Field(default_factory=list)
    resource_ids: list[str] = Field(default_factory=list)
    #: Devices that were on this bench last time and are not in the new inventory.
    retired_resource_ids: list[str] = Field(default_factory=list)
    quarantine_retained: bool = False


class ReapResult(BaseModel):
    """One dead bench, swept. `requeued_jobs` is per *job*, not per resource —
    a job spanning three devices appears once (§3.5)."""

    agent_id: str
    freed_resources: list[str] = Field(default_factory=list)
    requeued_jobs: list[str] = Field(default_factory=list)
    dead_lettered_jobs: list[str] = Field(default_factory=list)


class ResourceView(BaseModel):
    id: str
    local_id: str
    state: ResourceState
    current_job_id: str | None = None
    capabilities: dict[str, Any] = Field(default_factory=dict)
    #: Set only when TSS quarantined this device after repeated failures. Both
    #: that and an agent-reported fault read `unhealthy` in `state`, and they are
    #: not the same thing to the person looking at the screen: one is the bench's
    #: own report and it can withdraw it, the other is TSS's verdict and only an
    #: operator or a new agent version clears it (§4.2).
    quarantined_at: float | None = None
    consecutive_fails: int = 0


class AgentView(BaseModel):
    """One bench and the devices cabled to it — the two-level fleet view (§3.9)."""

    id: str
    hostname: str
    state: AgentState
    agent_version: str | None = None
    last_heartbeat_at: float
    presence_expires_at: float
    seconds_since_beat: float
    resources: list[ResourceView] = Field(default_factory=list)
    #: Filled in for benches that were reaped, so the fleet view can say what the
    #: machine took down with it.
    requeued_on_last_reap: list[str] = Field(default_factory=list)
    #: "quarantined since 14:02" — the useful half of a quarantine is when it
    #: started, because that is what tells you whether anyone has looked at it.
    quarantined_at: float | None = None
    consecutive_fails: int = 0

    @property
    def busy(self) -> int:
        return sum(1 for r in self.resources if r.state == ResourceState.BUSY)

    @property
    def total(self) -> int:
        """Capacity — retired devices are gone, so they are not capacity."""
        return sum(1 for r in self.resources if r.state != ResourceState.RETIRED)


class FleetView(BaseModel):
    now: float
    agents: list[AgentView] = Field(default_factory=list)


#: How many hex characters of a job id a human needs to tell two jobs apart on
#: one screen. Five is ~1M values: plenty for a fleet-sized queue, short enough
#: to read aloud in a demo.
SHORT_ID_CHARS = 5


def short_id(job_id: str) -> str:
    """`job-2bb76a1c` -> `2bb76`. The suffix, because the prefix is a constant."""
    _, _, suffix = job_id.partition("-")
    return (suffix or job_id)[:SHORT_ID_CHARS]


def job_label(name: str | None, job_id: str) -> str:
    """How a job is named on every HUMAN surface: `smoke-1 (2bb76)`.

    Machines keep the full id everywhere — API payloads, events on the wire,
    logs, and as accepted CLI arguments — because that is what is unique and
    what you paste back in. Humans get the name they chose, with just enough id
    to disambiguate the three `smoke` jobs on screen.
    """
    return f"{name} ({short_id(job_id)})" if name else short_id(job_id)


def qualify(agent_id: str, local_id: str) -> str:
    """Bench-local device name -> fleet-wide resource id."""
    return local_id if local_id.startswith(f"{agent_id}:") else f"{agent_id}:{local_id}"


def local_of(resource_id: str) -> str:
    """Fleet-wide resource id -> the name the bench knows it by."""
    _, _, local = resource_id.partition(":")
    return local or resource_id


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

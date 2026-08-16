"""SQLite state store — the single source of truth (§3.3).

ALL writes to TSS state live in this module. The scheduler decides; the store
commits. API handlers and the reaper never write SQL directly.

THE CLOCK RULE. Every persisted timestamp is an absolute unix float in UTC
wall-clock time (`time.time()`). `time.monotonic()` measures durations inside one
process and must never back a column: it resets on restart, which would break the
zero-code restart recovery in §7.2 (stale presence leases are simply expired when
the process comes back). Mixing the two is a bug that only shows up after an NTP
step or a restart.

CONNECTIONS. One `sqlite3.Connection` per thread, created lazily. The connection
object is not thread-safe and, more importantly, a transaction belongs to a
connection — two threads sharing one would share a transaction, which is exactly
the thing the N-way claim must not do.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
import threading
import time
from collections.abc import Sequence
from typing import Any

from tss.core.config import DEFAULT, Config
from tss.core.models import (
    Agent,
    AgentState,
    CapabilitySpec,
    ClaimResult,
    Event,
    Job,
    JobState,
    Resource,
    ResourceState,
)

# --- claim failure reasons. None of these is an error; all are lost races. ------
REASON_RESOURCE_UNAVAILABLE = "resource_unavailable"
REASON_JOB_NOT_QUEUED = "job_not_queued"
REASON_RESOURCE_COUNT_MISMATCH = "resource_count_mismatch"
REASON_UNKNOWN_JOB = "unknown_job"
REASON_DB_BUSY = "db_busy"

_BUSY_ERRCODES = frozenset({5, 6})  # SQLITE_BUSY, SQLITE_LOCKED

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS agents (
    id                  TEXT PRIMARY KEY,      -- "bench-sf-04"
    hostname            TEXT NOT NULL,
    state               TEXT NOT NULL CHECK (state IN
                          ('online','offline','quarantined','draining')),
    presence_expires_at REAL NOT NULL,         -- unix float, UTC. ALWAYS set, whatever the load.
    last_heartbeat_at   REAL NOT NULL,
    consecutive_fails   INTEGER NOT NULL DEFAULT 0,   -- failures spanning >=2 distinct resources
    quarantined_at      REAL,
    registered_at       REAL NOT NULL,
    agent_version       TEXT
);
CREATE INDEX IF NOT EXISTS idx_agents_presence
    ON agents(presence_expires_at) WHERE state != 'offline';

CREATE TABLE IF NOT EXISTS resources (
    id                TEXT PRIMARY KEY,        -- "bench-sf-04:vg-01"
    agent_id          TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    capabilities      TEXT NOT NULL,           -- JSON: {"product":"vehicle_gateway",...}
    state             TEXT NOT NULL CHECK (state IN ('free','busy','unhealthy')),
    current_job_id    TEXT REFERENCES jobs(id),  -- single column => I2 is structural
    last_assigned_at  REAL,                    -- drives LRU (§3.4)
    consecutive_fails INTEGER NOT NULL DEFAULT 0,
    quarantined_at    REAL
);
CREATE INDEX IF NOT EXISTS idx_res_dispatch ON resources(agent_id, state, last_assigned_at);
CREATE INDEX IF NOT EXISTS idx_res_job
    ON resources(current_job_id) WHERE current_job_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS jobs (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    requirements   TEXT NOT NULL,              -- JSON LIST of tag-subsets, one per device
    resource_count INTEGER NOT NULL,           -- len(requirements); denormalized so I8 is 1 query
    payload        TEXT NOT NULL,
    state          TEXT NOT NULL CHECK (state IN
                     ('queued','assigned','running','passed','failed',
                      'infra_error','cancelled','dead_letter')),
    agent_id       TEXT REFERENCES agents(id), -- co-location: ALL resources on this one agent
    epoch          INTEGER NOT NULL DEFAULT 0, -- fencing token; only ever self-incremented
    attempt        INTEGER NOT NULL DEFAULT 0, -- total dispatches (history only)
    tried_agents   TEXT NOT NULL DEFAULT '[]', -- JSON list — drives retry AND poison detection
    priority       INTEGER NOT NULL DEFAULT 100,
    max_duration_s INTEGER NOT NULL DEFAULT 600,
    submitted_at   REAL NOT NULL,
    assigned_at    REAL,
    started_at     REAL,
    finished_at    REAL,
    blocked_reason TEXT,                       -- 'no_capable_agent' — surfaced by `tss why`
    outcome        TEXT CHECK (outcome IS NULL OR outcome IN
                     ('passed','failed','infra_error','cancelled','dead_letter')),
    result_detail  TEXT                        -- 'timeout', 'agent_lost', 'no_capable_agent'
);
CREATE INDEX IF NOT EXISTS idx_jobs_queue   ON jobs(state, priority, submitted_at);
CREATE INDEX IF NOT EXISTS idx_jobs_timeout ON jobs(state, started_at) WHERE state = 'running';

CREATE TABLE IF NOT EXISTS job_resources (     -- durable allocation record; survives release
    job_id      TEXT NOT NULL REFERENCES jobs(id),
    resource_id TEXT NOT NULL REFERENCES resources(id),
    epoch       INTEGER NOT NULL,
    claimed_at  REAL NOT NULL,
    released_at REAL,
    PRIMARY KEY (job_id, resource_id, epoch)
);

CREATE TABLE IF NOT EXISTS events (            -- append-only audit log
    seq       INTEGER PRIMARY KEY AUTOINCREMENT,
    ts        REAL NOT NULL,
    kind      TEXT NOT NULL,                   -- job.requeued, agent.offline, ...
    agent_id  TEXT, resource_id TEXT, job_id TEXT,
    detail    TEXT
);
"""

# The N-way claim, §3.3. Both guards are load-bearing; see claim_all().
_CLAIM_RESOURCE_SQL = """
UPDATE resources
   SET state            = 'busy',
       current_job_id   = :job_id,
       last_assigned_at = :now
 WHERE id = :resource_id
   AND state = 'free'
   AND agent_id = :agent_id
"""

_CLAIM_JOB_SQL = """
UPDATE jobs
   SET state        = 'assigned',
       agent_id     = :agent_id,
       epoch        = epoch + 1,
       attempt      = attempt + 1,
       tried_agents = json_insert(tried_agents, '$[#]', :agent_id),
       assigned_at  = :now
 WHERE id = :job_id
   AND state = 'queued'
   AND resource_count = :n
"""

_INSERT_JOB_RESOURCE_SQL = """
INSERT INTO job_resources (job_id, resource_id, epoch, claimed_at)
     VALUES (:job_id, :resource_id, :epoch, :now)
"""

_INSERT_EVENT_SQL = """
INSERT INTO events (ts, kind, agent_id, resource_id, job_id, detail)
     VALUES (:ts, :kind, :agent_id, :resource_id, :job_id, :detail)
"""


class Store:
    """Owns the database. Every write in TSS goes through a method on this class."""

    def __init__(self, path: str, config: Config = DEFAULT) -> None:
        self.path = str(path)
        self.config = config
        self._local = threading.local()

    # ------------------------------------------------------------------ setup
    @property
    def conn(self) -> sqlite3.Connection:
        """This thread's connection, opened on first use with the §3.3 pragmas."""
        conn: sqlite3.Connection | None = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(
                self.path,
                # We manage transactions by hand: the claim needs BEGIN IMMEDIATE,
                # not whatever the driver decides to wrap around a statement.
                isolation_level=None,
                timeout=self.config.busy_timeout_ms / 1000,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode = WAL")  # readers don't block the scheduler
            conn.execute(f"PRAGMA busy_timeout = {self.config.busy_timeout_ms}")
            conn.execute("PRAGMA foreign_keys = ON")  # otherwise the FKs are decorative
            conn.execute("PRAGMA synchronous = NORMAL")  # safe under WAL
            self._local.conn = conn
        return conn

    def init_schema(self) -> None:
        self.conn.executescript(SCHEMA_SQL)

    def close(self) -> None:
        """Close this thread's connection. Each thread closes its own."""
        conn: sqlite3.Connection | None = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    # ------------------------------------------------- the critical operation
    def claim_all(
        self,
        job_id: str,
        agent_id: str,
        resource_ids: Sequence[str],
        *,
        now: float | None = None,
    ) -> ClaimResult:
        """Claim N resources for one job: all of them, or none of them (§3.3).

        One transaction covering every resource plus the job row. Each statement
        carries a WHERE guard and a rowcount check; ANY zero rolls back ALL of it.
        Never a loop of individual claims with cleanup on failure — a crash
        mid-loop strands devices with no owner and no lease to expire them. The
        database's rollback is the point, and we get it for free, including on
        crash.

        Three things here look like style and are not:

        * The resource UPDATEs are issued **sorted by resource_id**. SQLite hides
          lock-order deadlocks (BEGIN IMMEDIATE serializes writers) so this is
          invisible until the Postgres port, where a cycle aborts a transaction.
        * The job UPDATE's `state='queued'` guard is as necessary as the resource
          guards: without it a cancelled or already-claimed job takes the devices
          anyway. `resource_count = :n` is I8 made structural — a partial set can
          never be committed, even if the caller asks for one.
        * The epoch is self-incremented in SQL. A read-then-write epoch is itself
          a race, in the mechanism that exists to prevent races.

        Returns ClaimResult(ok=False) on a lost race — including SQLITE_BUSY,
        which is what a *second connection* gets instead of rowcount 0. Both are
        the same thing to the caller: try the next agent.
        """
        if not resource_ids:
            raise ValueError("claim_all requires at least one resource")
        ordered = sorted(resource_ids)  # lock ordering — see docstring
        if len(set(ordered)) != len(ordered):
            raise ValueError(f"duplicate resource ids in claim: {resource_ids!r}")
        now = time.time() if now is None else now
        conn = self.conn

        try:
            conn.execute("BEGIN IMMEDIATE")  # take the write lock up front; no upgrade
        except sqlite3.OperationalError as exc:
            if not _is_busy(exc):
                raise
            return ClaimResult(ok=False, job_id=job_id, agent_id=agent_id, reason=REASON_DB_BUSY)

        try:
            for resource_id in ordered:
                cur = conn.execute(
                    _CLAIM_RESOURCE_SQL,
                    {
                        "job_id": job_id,
                        "now": now,
                        "resource_id": resource_id,
                        "agent_id": agent_id,
                    },
                )
                if cur.rowcount != 1:
                    # Busy, unhealthy, unknown, or on another bench. Everything
                    # already updated in this transaction goes back with it.
                    self._rollback(conn)
                    return ClaimResult(
                        ok=False,
                        job_id=job_id,
                        agent_id=agent_id,
                        reason=REASON_RESOURCE_UNAVAILABLE,
                        blocked_by=resource_id,
                    )

            cur = conn.execute(
                _CLAIM_JOB_SQL,
                {
                    "agent_id": agent_id,
                    "now": now,
                    "job_id": job_id,
                    "n": len(ordered),
                },
            )
            if cur.rowcount != 1:
                reason = self._diagnose_job_guard(conn, job_id, len(ordered))
                self._rollback(conn)
                return ClaimResult(ok=False, job_id=job_id, agent_id=agent_id, reason=reason)

            row = conn.execute("SELECT epoch FROM jobs WHERE id = ?", (job_id,)).fetchone()
            epoch = int(row["epoch"])

            conn.executemany(
                _INSERT_JOB_RESOURCE_SQL,
                [
                    {"job_id": job_id, "resource_id": rid, "epoch": epoch, "now": now}
                    for rid in ordered
                ],
            )
            # The audit row is written inside the same transaction as the state
            # change it records (§3.6); publication to subscribers happens after
            # commit. Otherwise a crash between the two leaves the log and the
            # live stream telling different stories.
            self._insert_event(
                conn,
                kind="job.assigned",
                ts=now,
                agent_id=agent_id,
                job_id=job_id,
                detail={"epoch": epoch, "resource_ids": ordered},
            )
            conn.execute("COMMIT")
        except sqlite3.OperationalError as exc:
            self._rollback(conn)
            if not _is_busy(exc):
                raise
            return ClaimResult(ok=False, job_id=job_id, agent_id=agent_id, reason=REASON_DB_BUSY)
        except BaseException:
            # Anything at all — including a KeyboardInterrupt between two
            # resource UPDATEs — must not leave devices held by a transaction
            # that never commits.
            self._rollback(conn)
            raise

        return ClaimResult(
            ok=True,
            job_id=job_id,
            agent_id=agent_id,
            epoch=epoch,
            resource_ids=ordered,
        )

    @staticmethod
    def _diagnose_job_guard(conn: sqlite3.Connection, job_id: str, n: int) -> str:
        """Why the job UPDATE matched no row. Diagnosis only — all are lost races."""
        row = conn.execute(
            "SELECT state, resource_count FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
        if row is None:
            return REASON_UNKNOWN_JOB
        if row["state"] != JobState.QUEUED:
            return REASON_JOB_NOT_QUEUED
        if row["resource_count"] != n:
            return REASON_RESOURCE_COUNT_MISMATCH
        return REASON_JOB_NOT_QUEUED

    @staticmethod
    def _rollback(conn: sqlite3.Connection) -> None:
        # "cannot rollback - no transaction is active" means the driver already
        # unwound it; anything else here would mask the error we are unwinding for.
        with contextlib.suppress(sqlite3.OperationalError):
            conn.execute("ROLLBACK")

    # ------------------------------------------------------------- fleet writes
    # Minimal creation paths for step 1. Idempotent registration with inventory
    # replacement, and the requeue-on-re-register rule (§6), arrive in step 2.
    def create_agent(
        self,
        agent_id: str,
        hostname: str,
        *,
        agent_version: str | None = None,
        state: AgentState = AgentState.ONLINE,
        now: float | None = None,
        presence_ttl_s: float | None = None,
    ) -> None:
        now = time.time() if now is None else now
        ttl = self.config.presence_ttl_s if presence_ttl_s is None else presence_ttl_s
        self.conn.execute(
            """INSERT INTO agents
                   (id, hostname, state, presence_expires_at, last_heartbeat_at,
                    registered_at, agent_version)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (agent_id, hostname, str(state), now + ttl, now, now, agent_version),
        )

    def add_resource(
        self,
        resource_id: str,
        agent_id: str,
        capabilities: dict[str, Any],
        *,
        state: ResourceState = ResourceState.FREE,
    ) -> None:
        self.conn.execute(
            """INSERT INTO resources (id, agent_id, capabilities, state)
               VALUES (?, ?, ?, ?)""",
            (resource_id, agent_id, json.dumps(capabilities), str(state)),
        )

    def submit_job(
        self,
        job_id: str,
        name: str,
        requirements: list[CapabilitySpec],
        *,
        payload: dict[str, Any] | None = None,
        priority: int = 100,
        max_duration_s: int | None = None,
        now: float | None = None,
    ) -> Job:
        """Queue a job. `resource_count` is len(requirements) — denormalized so
        that I8 ("holds exactly what it required") is one cheap query (§5)."""
        if not requirements:
            raise ValueError("a job must require at least one resource")
        now = time.time() if now is None else now
        max_duration_s = (
            self.config.default_max_duration_s if max_duration_s is None else max_duration_s
        )
        self.conn.execute(
            """INSERT INTO jobs
                   (id, name, requirements, resource_count, payload, state,
                    priority, max_duration_s, submitted_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                job_id,
                name,
                json.dumps(requirements),
                len(requirements),
                json.dumps(payload or {}),
                str(JobState.QUEUED),
                priority,
                max_duration_s,
                now,
            ),
        )
        job = self.get_job(job_id)
        assert job is not None
        return job

    def append_event(
        self,
        kind: str,
        *,
        agent_id: str | None = None,
        resource_id: str | None = None,
        job_id: str | None = None,
        detail: dict[str, Any] | None = None,
        now: float | None = None,
    ) -> None:
        """Standalone audit row. Events that accompany a state change are written
        inside that change's transaction instead — see claim_all()."""
        self._insert_event(
            self.conn,
            kind=kind,
            ts=time.time() if now is None else now,
            agent_id=agent_id,
            resource_id=resource_id,
            job_id=job_id,
            detail=detail,
        )

    @staticmethod
    def _insert_event(
        conn: sqlite3.Connection,
        *,
        kind: str,
        ts: float,
        agent_id: str | None = None,
        resource_id: str | None = None,
        job_id: str | None = None,
        detail: dict[str, Any] | None = None,
    ) -> None:
        conn.execute(
            _INSERT_EVENT_SQL,
            {
                "ts": ts,
                "kind": kind,
                "agent_id": agent_id,
                "resource_id": resource_id,
                "job_id": job_id,
                "detail": json.dumps(detail) if detail is not None else None,
            },
        )

    # ------------------------------------------------------------------- reads
    def get_agent(self, agent_id: str) -> Agent | None:
        row = self.conn.execute("SELECT * FROM agents WHERE id = ?", (agent_id,)).fetchone()
        return Agent.from_row(row) if row else None

    def get_resource(self, resource_id: str) -> Resource | None:
        row = self.conn.execute("SELECT * FROM resources WHERE id = ?", (resource_id,)).fetchone()
        return Resource.from_row(row) if row else None

    def get_job(self, job_id: str) -> Job | None:
        row = self.conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return Job.from_row(row) if row else None

    def list_resources(self, agent_id: str | None = None) -> list[Resource]:
        if agent_id is None:
            rows = self.conn.execute("SELECT * FROM resources ORDER BY id").fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM resources WHERE agent_id = ? ORDER BY id", (agent_id,)
            ).fetchall()
        return [Resource.from_row(r) for r in rows]

    def resources_held_by(self, job_id: str) -> list[str]:
        """Live ownership, from the single column that makes I2 structural."""
        rows = self.conn.execute(
            "SELECT id FROM resources WHERE current_job_id = ? ORDER BY id", (job_id,)
        ).fetchall()
        return [r["id"] for r in rows]

    def allocation_records(self, job_id: str | None = None) -> list[dict[str, Any]]:
        """The durable allocation history (`job_resources`), which — unlike
        `resources.current_job_id` — cannot be overwritten by a later claimer.
        That is what makes it the honest place to check for double-booking."""
        sql = "SELECT * FROM job_resources"
        params: tuple[Any, ...] = ()
        if job_id is not None:
            sql += " WHERE job_id = ?"
            params = (job_id,)
        sql += " ORDER BY job_id, resource_id, epoch"
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def events(self, *, kind: str | None = None, job_id: str | None = None) -> list[Event]:
        clauses: list[str] = []
        params: list[Any] = []
        if kind is not None:
            clauses.append("kind = ?")
            params.append(kind)
        if job_id is not None:
            clauses.append("job_id = ?")
            params.append(job_id)
        sql = "SELECT * FROM events"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY seq"
        return [Event.from_row(r) for r in self.conn.execute(sql, params).fetchall()]


def _is_busy(exc: sqlite3.OperationalError) -> bool:
    """SQLITE_BUSY / SQLITE_LOCKED — a lost race, not an error (§3.3).

    Narrow on purpose: `OperationalError` also covers "no such table", and
    swallowing that would turn a schema bug into a silent scheduling stall.
    """
    code = getattr(exc, "sqlite_errorcode", None)
    if code is not None:
        return code in _BUSY_ERRCODES
    text = str(exc).lower()
    return "locked" in text or "busy" in text

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
from collections.abc import Callable, Sequence
from typing import Any

from tss.core.config import DEFAULT, Config
from tss.core.models import (
    TERMINAL_JOB_STATES,
    Agent,
    AgentState,
    AgentView,
    Assignment,
    CapabilitySpec,
    ClaimResult,
    Event,
    FleetView,
    InventoryItem,
    Job,
    JobState,
    Outcome,
    PresenceStatus,
    ReapResult,
    Registration,
    Resource,
    ResourceState,
    ResourceView,
    local_of,
    qualify,
)

# --- claim failure reasons. None of these is an error; all are lost races. ------
REASON_RESOURCE_UNAVAILABLE = "resource_unavailable"
REASON_JOB_NOT_QUEUED = "job_not_queued"
REASON_RESOURCE_COUNT_MISMATCH = "resource_count_mismatch"
REASON_UNKNOWN_JOB = "unknown_job"
REASON_DB_BUSY = "db_busy"
#: offline, quarantined, draining, or the lease lapsed between decision and claim
REASON_AGENT_NOT_LIVE = "agent_not_live"

# --- result_detail tokens. Every terminal record LEADS with one of these, so the
# record says which path ended the job — that is what makes I6 checkable (§3.8).
DETAIL_TIMEOUT = "timeout"  # sweep 2: the job hung, the bench was fine
DETAIL_PRESENCE = "presence_expired"  # sweep 1: the machine died
DETAIL_CANCELLED = "cancelled_by_client"
DETAIL_DRAINED = "agent_draining"  # taken back because the bench had not started it
#: the bench is alive and heartbeating but has stopped mentioning this job
DETAIL_UNREPORTED = "unreported_by_agent"
#: `blocked_reason` on a queued job nobody in the fleet can run (§3.4.1).
BLOCKED_NO_CAPABLE_AGENT = "no_capable_agent"
DETAIL_REREGISTERED = "agent_reregistered"

_BUSY_ERRCODES = frozenset({5, 6})  # SQLITE_BUSY, SQLITE_LOCKED

#: Bump on ANY change to SCHEMA_SQL. Stamped into `PRAGMA user_version` when a
#: database is created and checked when one is opened.
#:
#:   1  pre-guard: unstamped, and `outcome` still accepted 'dead_letter'
#:   2  'dead_letter' removed from the outcome CHECK — it is a state, not an
#:      outcome, and a dead letter's outcome is 'infra_error'
#:   3  indexes on events(job_id) and events(kind, agent_id, seq) — the table is
#:      append-only and unbounded, so the fleet view, the timeout sweep and I6
#:      were scanning more of it every hour the service stayed up
#:
#: This is NOT a migration system and is not trying to be one; migrations are out
#: of scope for the POC. It exists because the failure it prevents is silent:
#: `CREATE TABLE IF NOT EXISTS` leaves an older database's constraints exactly as
#: they were, so a stale file goes on cheerfully accepting writes this build
#: forbids, and the first sign of trouble is a report that quietly disagrees with
#: the code. Refusing to open is loud, immediate, and one `rm` from fixed.
SCHEMA_VERSION = 3


class SchemaVersionError(RuntimeError):
    """The database on disk was written by a different schema than this build."""


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
    state             TEXT NOT NULL CHECK (state IN ('free','busy','unhealthy','retired')),
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
                     ('passed','failed','infra_error','cancelled')),
                                               -- no 'dead_letter': that is a STATE.
                                               -- A dead letter's outcome is infra_error.
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
-- This table is append-only and never pruned (retention is out of scope, README),
-- so every unindexed scan of it degrades linearly for as long as the service
-- runs. Three hot paths read it: `tss why` and the job views by job_id, the
-- fleet view by (kind, agent_id) for the last agent.offline, and check_i6 the
-- same way per terminal job — the last one on every pass of the chaos watchdog.
CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id) WHERE job_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_kind_agent ON events(kind, agent_id, seq);
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
       assigned_at  = :now,
       blocked_reason = NULL   -- it just ran; whatever blocked it no longer does
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
        #: Where committed events go for live subscribers (§3.6). Set by the app;
        #: None in tests and in the chaos checker's read-only connections.
        self.publish: Callable[[list[Event]], None] | None = None

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
            try:
                self._check_schema_version(conn)
            except BaseException:
                conn.close()
                raise
            self._local.conn = conn
        return conn

    def _check_schema_version(self, conn: sqlite3.Connection) -> None:
        """Refuse to touch a database this build did not write (SCHEMA_VERSION).

        An unstamped file with tables in it is a database from before the guard
        existed, which is exactly the stale case worth catching. An unstamped
        file with no tables is simply new — `init_schema` will stamp it.
        """
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        if version == SCHEMA_VERSION:
            return
        created = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'jobs'"
        ).fetchone()
        if version == 0 and created is None:
            return  # fresh database
        raise SchemaVersionError(
            f"{self.path} has schema version {version}, but this build expects "
            f"{SCHEMA_VERSION}. TSS has no migrations — that is deliberate for the "
            f"POC, and refusing to open is the point: an older file keeps its old "
            f"CHECK constraints and would silently accept writes this build "
            f"forbids. Delete it and let it be recreated:\n"
            f"    rm -f {self.path} {self.path}-wal {self.path}-shm"
        )

    def init_schema(self) -> None:
        conn = self.conn  # runs the version guard first
        conn.executescript(SCHEMA_SQL)
        # Stamped last, so a half-created database stays unstamped rather than
        # claiming to be a version it never finished becoming.
        conn.execute(f"PRAGMA user_version = {int(SCHEMA_VERSION)}")

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

            # The matcher read the fleet a moment ago; this commits now. A bench
            # that went offline, was quarantined or drained, or simply let its
            # lease lapse in between must not be handed devices — so the
            # condition the decision rested on is checked again here, inside the
            # transaction that acts on it.
            if not conn.execute(
                """SELECT 1 FROM agents
                    WHERE id = :agent_id AND state = 'online'
                      AND presence_expires_at > :now""",
                {"agent_id": agent_id, "now": now},
            ).fetchone():
                self._rollback(conn)
                return ClaimResult(
                    ok=False, job_id=job_id, agent_id=agent_id, reason=REASON_AGENT_NOT_LIVE
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
            self._commit(conn)
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

    def _pending_events(self) -> list[Event]:
        pending = getattr(self._local, "pending_events", None)
        if pending is None:
            pending = []
            self._local.pending_events = pending
        return pending

    def _commit(self, conn: sqlite3.Connection) -> None:
        """Commit, THEN publish. Never the other way round (§3.6)."""
        conn.execute("COMMIT")
        published, self._local.pending_events = self._pending_events(), []
        if published and self.publish is not None:
            self.publish(published)

    def _rollback(self, conn: sqlite3.Connection) -> None:
        # "cannot rollback - no transaction is active" means the driver already
        # unwound it; anything else here would mask the error we are unwinding for.
        self._local.pending_events = []  # nothing happened, so nothing is announced
        with contextlib.suppress(sqlite3.OperationalError):
            conn.execute("ROLLBACK")

    # ------------------------------------------------- registration and presence
    def register_agent(
        self,
        agent_id: str,
        hostname: str,
        inventory: Sequence[InventoryItem],
        *,
        agent_version: str | None = None,
        now: float | None = None,
    ) -> Registration:
        """Register a bench and its devices. Idempotent (§6).

        Re-registering an existing id replaces its inventory in place — never a
        duplicate device. And it is not a no-op: a restarted agent has lost its
        hardware state, so every job it held is requeued and every resource reset
        to `free`, in this one transaction. Pretending it still owns those jobs
        orphans them forever, with no lease left to expire them.

        Quarantine survives a re-registration at the same `agent_version`: a
        bench that was restarted but not fixed must stay out of rotation, or you
        re-break the fleet one reboot at a time (§4.2).
        """
        now = time.time() if now is None else now
        ttl = self.config.presence_ttl_s
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT state, agent_version, quarantined_at FROM agents WHERE id = ?", (agent_id,)
            ).fetchone()
            requeued: list[str] = []
            dead_lettered: list[str] = []
            quarantine_retained = False

            if existing is None:
                conn.execute(
                    """INSERT INTO agents
                           (id, hostname, state, presence_expires_at, last_heartbeat_at,
                            registered_at, agent_version)
                       VALUES (?, ?, 'online', ?, ?, ?, ?)""",
                    (agent_id, hostname, now + ttl, now, now, agent_version),
                )
            else:
                # Per JOB, not per resource — see reap_agent().
                for job_id in self._distinct_jobs_on(conn, agent_id):
                    kind = self._requeue_job(
                        conn, job_id, agent_id=agent_id, now=now, reason=DETAIL_REREGISTERED
                    )
                    if kind == "job.requeued":
                        requeued.append(job_id)
                    elif kind == "job.dead_letter":
                        dead_lettered.append(job_id)

                # Keyed off `quarantined_at`, NOT the current state. A
                # quarantined bench that is rebooted has its lease expire on the
                # way down, so the reaper moves it to `offline` before it comes
                # back — and reading `state` here would find `offline`, clear the
                # quarantine, and hand work straight back to a machine nobody
                # fixed. The mark is what survives; §4.1's rule is that only a
                # NEW agent_version clears it.
                quarantine_retained = (
                    existing["state"] == AgentState.QUARANTINED
                    or existing["quarantined_at"] is not None
                ) and existing["agent_version"] == agent_version
                conn.execute(
                    """UPDATE agents
                          SET hostname            = :hostname,
                              state               = :state,
                              presence_expires_at = :expires,
                              last_heartbeat_at   = :now,
                              registered_at       = :now,
                              agent_version       = :version,
                              quarantined_at      = :quarantined_at
                        WHERE id = :agent_id""",
                    {
                        "hostname": hostname,
                        "state": (
                            AgentState.QUARANTINED if quarantine_retained else AgentState.ONLINE
                        ).value,
                        "expires": now + ttl,
                        "now": now,
                        "version": agent_version,
                        # Keep the ORIGINAL timestamp: "quarantined since 14:02"
                        # is the useful fact, and restarting must not reset it.
                        "quarantined_at": (
                            (existing["quarantined_at"] or now) if quarantine_retained else None
                        ),
                        "agent_id": agent_id,
                    },
                )

            resource_ids = [qualify(agent_id, item.id) for item in inventory]
            retired = self._replace_inventory(
                conn,
                agent_id,
                inventory,
                keep=set(resource_ids),
                # A new build is the one thing that clears a device verdict too:
                # somebody deployed a fix, so the old evidence is about old code.
                keep_quarantine=existing is not None and existing["agent_version"] == agent_version,
            )

            self._insert_event(
                conn,
                kind="agent.registered",
                ts=now,
                agent_id=agent_id,
                detail={
                    "is_new": existing is None,
                    "resources": [local_of(r) for r in resource_ids],
                    "requeued": requeued,
                    "dead_lettered": dead_lettered,
                    "retired": [local_of(r) for r in retired],
                    "agent_version": agent_version,
                },
            )
            self._commit(conn)
        except BaseException:
            self._rollback(conn)
            raise

        return Registration(
            agent_id=agent_id,
            is_new=existing is None,
            requeued_jobs=requeued,
            dead_lettered_jobs=dead_lettered,
            resource_ids=resource_ids,
            retired_resource_ids=retired,
            quarantine_retained=quarantine_retained,
        )

    @staticmethod
    def _replace_inventory(
        conn: sqlite3.Connection,
        agent_id: str,
        inventory: Sequence[InventoryItem],
        *,
        keep: set[str],
        keep_quarantine: bool = True,
    ) -> list[str]:
        """Upsert what the agent reports; retire what it no longer does.

        ONE RULE: a device that vanished from the inventory becomes `retired`,
        always — whether or not it has allocation history. Deleting the ones
        without history and retiring the ones with it would be two rules for one
        situation, and the fleet view would then depend on whether a device
        happened to have run something before it was unplugged.

        Never `unhealthy`: that means present-but-broken, and someone is expected
        to go fix it. A retired device is not there to fix (§4.2).
        """
        retired: list[str] = []
        current = {
            row["id"]
            for row in conn.execute("SELECT id FROM resources WHERE agent_id = ?", (agent_id,))
        }
        for resource_id in sorted(current - keep):
            conn.execute(
                "UPDATE resources SET state = 'retired', current_job_id = NULL WHERE id = ?",
                (resource_id,),
            )
            retired.append(resource_id)

        for item in inventory:
            # A device TSS quarantined keeps its verdict across a re-register,
            # for the same reason an agent does (§4.2): rebooting the bench is
            # the first thing anyone tries, and a restart is not a repair. Only a
            # new agent_version or `tss unquarantine` clears it.
            conn.execute(
                """INSERT INTO resources (id, agent_id, capabilities, state)
                        VALUES (:id, :agent_id, :capabilities, 'free')
                   ON CONFLICT(id) DO UPDATE
                          SET capabilities   = excluded.capabilities,
                              agent_id       = excluded.agent_id,
                              state          = CASE
                                  WHEN resources.quarantined_at IS NOT NULL AND :keep_quarantine
                                      THEN resources.state
                                  ELSE 'free'
                              END,
                              quarantined_at = CASE
                                  WHEN :keep_quarantine THEN resources.quarantined_at
                                  ELSE NULL
                              END,
                              consecutive_fails = CASE
                                  WHEN :keep_quarantine THEN resources.consecutive_fails
                                  ELSE 0
                              END,
                              current_job_id = NULL""",
                {
                    "id": qualify(agent_id, item.id),
                    "agent_id": agent_id,
                    "capabilities": json.dumps(item.capabilities),
                    "keep_quarantine": 1 if keep_quarantine else 0,
                },
            )
        return retired

    def renew_presence(
        self, agent_id: str, *, now: float | None = None
    ) -> tuple[PresenceStatus, Agent | None]:
        """Push the presence lease forward — the whole of "I'm alive" (§3.5).

        The `state != 'offline'` guard is the point. A heartbeat that lands
        microseconds after the reaper ran must NOT resurrect the lease: the agent
        would go on believing it owns resources that have already been freed and
        handed to someone else. It gets 410 instead, re-registers, and comes back
        clean — without its jobs.
        """
        now = time.time() if now is None else now
        conn = self.conn
        cur = conn.execute(
            """UPDATE agents
                  SET presence_expires_at = :expires,
                      last_heartbeat_at   = :now
                WHERE id = :agent_id
                  AND state != 'offline'""",
            {"expires": now + self.config.presence_ttl_s, "now": now, "agent_id": agent_id},
        )
        if cur.rowcount == 1:
            return PresenceStatus.RENEWED, self.get_agent(agent_id)
        agent = self.get_agent(agent_id)
        if agent is None:
            return PresenceStatus.UNKNOWN_AGENT, None
        return PresenceStatus.EXPIRED, agent

    def report_resource_health(
        self, agent_id: str, health: dict[str, str], *, now: float | None = None
    ) -> list[str]:
        """Apply the agent's own device probes (§3.1, §4.2).

        Device health and machine health are different things: one dead J-Link
        costs you one device, not the bench. A `busy` device is left alone — its
        job has to end before the device can change state, and that path arrives
        with completion in step 3. A `retired` device is left alone too: the
        transitions below are guarded to free<->unhealthy, so a stale health
        report cannot bring a device that is no longer on the bench back into the
        pool.
        """
        now = time.time() if now is None else now
        changed: list[str] = []
        for local, status in sorted(health.items()):
            resource_id = qualify(agent_id, local)
            if status == ResourceState.UNHEALTHY:
                sql, kind = (
                    "UPDATE resources SET state = 'unhealthy' WHERE id = ? AND state = 'free'",
                    "resource.unhealthy",
                )
            elif status in (ResourceState.FREE, "healthy"):
                # Only a fault the AGENT reported is the agent's to withdraw. A
                # device TSS quarantined after repeated failures carries
                # `quarantined_at`, and a bench announcing itself healthy is not
                # an appeal against that — `tss unquarantine` is.
                sql, kind = (
                    """UPDATE resources SET state = 'free'
                        WHERE id = ? AND state = 'unhealthy' AND quarantined_at IS NULL""",
                    "resource.healthy",
                )
            else:
                continue
            if self.conn.execute(sql, (resource_id,)).rowcount == 1:
                changed.append(resource_id)
                self.append_event(kind, agent_id=agent_id, resource_id=resource_id, now=now)
        return changed

    # --------------------------------------------------- dispatch and results
    def pending_assignment(self, agent_id: str) -> Assignment | None:
        """The oldest job claimed for this agent that it has not started yet.

        Re-delivered on every heartbeat until the agent calls /start, which is
        what recovers an assignment lost in flight. The agent ignores a job it is
        already running, and /start is fenced by the epoch, so a duplicate
        delivery is harmless (§13.1).
        """
        row = self.conn.execute(
            """SELECT * FROM jobs
                WHERE agent_id = ? AND state = 'assigned'
                ORDER BY assigned_at, id LIMIT 1""",
            (agent_id,),
        ).fetchone()
        if row is None:
            return None
        job = Job.from_row(row)
        return Assignment(
            job_id=job.id,
            epoch=job.epoch,
            agent_id=agent_id,
            # The bench knows its devices by their local names (§6).
            resource_ids=[local_of(r) for r in self.resources_held_by(job.id)],
            payload=job.payload,
            max_duration_s=job.max_duration_s,
        )

    def start_job(self, job_id: str, agent_id: str, epoch: int, *, now: float | None = None) -> str:
        """The agent says it has begun. Returns 'started' or a rejection reason.

        Fenced: the epoch and the agent must both match, and the job must still
        be `assigned`. An agent that was reaped and is only now getting round to
        starting the job must not move a job someone else owns.
        """
        now = time.time() if now is None else now
        cur = self.conn.execute(
            """UPDATE jobs SET state = 'running', started_at = :now
                WHERE id = :job_id AND agent_id = :agent_id AND epoch = :epoch
                  AND state = 'assigned'""",
            {"now": now, "job_id": job_id, "agent_id": agent_id, "epoch": epoch},
        )
        if cur.rowcount == 1:
            self.append_event("job.started", agent_id=agent_id, job_id=job_id, now=now)
            return "started"
        return self._diagnose_fence(job_id, agent_id, epoch)

    def complete_job(
        self,
        job_id: str,
        agent_id: str,
        epoch: int,
        outcome: Outcome,
        *,
        detail: str | None = None,
        now: float | None = None,
    ) -> str:
        """Record a result and release the hardware — one transaction (§6).

        EVERY resource the job holds is freed by a single statement keyed on
        `current_job_id`, so a release cannot free some of a job's devices and
        leave the rest held. That is I8's other half: the `resource_count = :n`
        guard covers claim time, and this covers release time.

        `infra_error` is not a result. The rig broke, not the firmware, so the
        job goes back to the queue (or dead-letters after MAX_DISTINCT_AGENTS
        benches) rather than being reported to the engineer as a failure (§4.3).
        """
        now = time.time() if now is None else now
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                """SELECT state FROM jobs
                    WHERE id = :job_id AND agent_id = :agent_id AND epoch = :epoch
                      AND state IN ('assigned','running')""",
                {"job_id": job_id, "agent_id": agent_id, "epoch": epoch},
            ).fetchone()
            if row is None:
                self._rollback(conn)
                return self._diagnose_fence(job_id, agent_id, epoch)

            if outcome is Outcome.INFRA_ERROR:
                kind = self._requeue_job(
                    conn,
                    job_id,
                    agent_id=agent_id,
                    now=now,
                    reason=f"infra_error:{detail}" if detail else "infra_error",
                )
                accepted = "requeued" if kind == "job.requeued" else "dead_lettered"
            else:
                conn.execute(
                    """UPDATE jobs
                          SET state = :state, outcome = :outcome, result_detail = :detail,
                              finished_at = :now, epoch = epoch + 1
                        WHERE id = :job_id AND epoch = :epoch
                          AND state IN ('assigned','running')""",
                    {
                        "state": str(outcome),
                        "outcome": str(outcome),
                        "detail": detail,
                        "now": now,
                        "job_id": job_id,
                        "epoch": epoch,
                    },
                )
                conn.execute(
                    """UPDATE job_resources SET released_at = :now
                        WHERE job_id = :job_id AND epoch = :epoch AND released_at IS NULL""",
                    {"now": now, "job_id": job_id, "epoch": epoch},
                )
                self._insert_event(
                    conn,
                    kind="job.completed",
                    ts=now,
                    agent_id=agent_id,
                    job_id=job_id,
                    detail={"outcome": str(outcome), "detail": detail, "epoch": epoch},
                )
                accepted = "accepted"

            held = [
                row["id"]
                for row in conn.execute(
                    "SELECT id FROM resources WHERE current_job_id = ? ORDER BY id", (job_id,)
                )
            ]
            # Every device this job holds, in one statement. Not a loop.
            conn.execute(
                """UPDATE resources SET state = 'free', current_job_id = NULL
                    WHERE current_job_id = :job_id AND state = 'busy'""",
                {"job_id": job_id},
            )
            # Blame AFTER the release: quarantining a device means moving it out
            # of `free`, and doing that first would only have it freed back.
            self._attribute_failure(conn, agent_id, held, outcome=outcome, now=now)
            self._commit(conn)
        except BaseException:
            self._rollback(conn)
            raise
        return accepted

    def _attribute_failure(
        self,
        conn: sqlite3.Connection,
        agent_id: str,
        resource_ids: Sequence[str],
        *,
        outcome: Outcome,
        now: float,
    ) -> None:
        """Blame the device or blame the machine — never both, never neither (§4.2).

        Failures clustered on ONE resource mean a bad device: quarantine that
        device and the bench keeps working on its others. Failures spanning
        SEVERAL resources on one machine mean a bad machine: quarantine the
        machine. Conflating them is how one unplugged cable costs you a third of
        the fleet, or how a broken bench keeps being handed work forever.

        A pass clears the slate: `consecutive_fails` means consecutive.
        """
        if outcome not in (Outcome.INFRA_ERROR, Outcome.FAILED):
            # A pass clears the slate: `consecutive_fails` means consecutive.
            placeholders = ",".join("?" * len(resource_ids))
            conn.execute(
                f"UPDATE resources SET consecutive_fails = 0 WHERE id IN ({placeholders})",
                tuple(resource_ids),
            )
            conn.execute("UPDATE agents SET consecutive_fails = 0 WHERE id = ?", (agent_id,))
            return
        if outcome is Outcome.FAILED:
            # The firmware misbehaved. That is the engineer's problem and says
            # nothing at all about the hardware (§4.3).
            return

        threshold = self.config.quarantine_threshold
        for resource_id in resource_ids:
            conn.execute(
                "UPDATE resources SET consecutive_fails = consecutive_fails + 1 WHERE id = ?",
                (resource_id,),
            )
        conn.execute(
            "UPDATE agents SET consecutive_fails = consecutive_fails + 1 WHERE id = ?", (agent_id,)
        )
        row = conn.execute(
            "SELECT consecutive_fails FROM agents WHERE id = ?", (agent_id,)
        ).fetchone()
        if row is None or row["consecutive_fails"] < threshold:
            return

        # Threshold reached. Now the attribution: it is the SPREAD of the
        # failures that says whether the device or the machine is at fault.
        implicated = [
            r["id"]
            for r in conn.execute(
                """SELECT id FROM resources
                    WHERE agent_id = ? AND consecutive_fails > 0 ORDER BY id""",
                (agent_id,),
            )
        ]
        # Blame assigned; the count starts again from here either way.
        conn.execute("UPDATE agents SET consecutive_fails = 0 WHERE id = ?", (agent_id,))

        if len(implicated) >= 2:
            # `draining` is excluded: an operator's acknowledged order outranks
            # automated blame. A bench draining for maintenance whose last jobs
            # fail — often WHY it is being drained — would otherwise have the
            # drain silently converted into a quarantine, and the reason would
            # read as a verdict nobody chose.
            quarantined = conn.execute(
                """UPDATE agents SET state = 'quarantined', quarantined_at = :now
                    WHERE id = :agent_id
                      AND state NOT IN ('offline','quarantined','draining')""",
                {"now": now, "agent_id": agent_id},
            )
            # The event follows the WRITE, not the intent. Emitting it anyway
            # would put a quarantine in the audit log and on the live stream for
            # a bench that is still draining — the log has to be able to explain
            # the state, and here it would contradict it.
            if quarantined.rowcount != 1:
                return
            self._insert_event(
                conn,
                kind="agent.quarantined",
                ts=now,
                agent_id=agent_id,
                detail={"consecutive_fails": threshold, "devices": implicated},
            )
            return

        for resource_id in implicated:
            marked = conn.execute(
                """UPDATE resources SET state = 'unhealthy', quarantined_at = :now
                    WHERE id = :id AND state NOT IN ('retired','busy')""",
                {"now": now, "id": resource_id},
            )
            if marked.rowcount != 1:
                continue
            self._insert_event(
                conn,
                kind="resource.quarantined",
                ts=now,
                agent_id=agent_id,
                resource_id=resource_id,
                detail={"consecutive_fails": threshold},
            )

    def _diagnose_fence(self, job_id: str, agent_id: str, epoch: int) -> str:
        """Why a fenced write matched nothing. Everything here means the same
        thing to the agent: you do not own this job any more, abandon it."""
        job = self.get_job(job_id)
        if job is None:
            return "unknown_job"
        if job.epoch != epoch or job.agent_id != agent_id:
            return "stale_epoch"
        if job.is_terminal:
            return "already_terminal"
        return "wrong_state"

    # ------------------------------------------------------------- the reaper
    def expired_agents(self, *, now: float | None = None) -> list[str]:
        """Sweep 1's query (§3.5). A lease has no opinion: time passes, it expires.

        Note what is *not* here: any notion of how loaded the bench is. Presence
        belongs to the machine, always, so a bench unplugged while idle expires
        exactly like one that died mid-job — the hole that fails Pillar 4 if you
        only lease busy agents.
        """
        now = time.time() if now is None else now
        rows = self.conn.execute(
            """SELECT id FROM agents
                WHERE presence_expires_at < ? AND state != 'offline'
                ORDER BY id""",
            (now,),
        ).fetchall()
        return [r["id"] for r in rows]

    def reap_agent(
        self, agent_id: str, *, now: float | None = None, reason: str = DETAIL_PRESENCE
    ) -> ReapResult:
        """Mark a bench offline, free every device, requeue each job it held.

        THE FAN-OUT. Jobs are collected with SELECT DISTINCT and requeued once
        each. A bench running one job across three devices appears three times if
        you iterate resources — requeue per resource and you bump the epoch three
        times and burn the job's whole retry budget on a single bench failure.
        Nothing errors; the job just mysteriously dead-letters early.

        `tried_agents` is NOT appended here. The bench was recorded when it
        claimed the job (§3.3); appending again on the way out would count one
        bench twice.

        The sweep releases claims and nothing else — see the resource UPDATE
        below for why `unhealthy` and `retired` are left alone.
        """
        now = time.time() if now is None else now
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            # Taking the agent offline is also this reap's claim on the work: a
            # second sweep racing us finds rowcount 0 and does nothing.
            # REVALIDATE THE TRIGGER CONDITION. `expired_agents` was a SELECT;
            # the bench can heartbeat between it and this UPDATE, and reaping it
            # then frees devices from under a machine that is alive and running
            # on them. The pre-read is advisory.
            reaped = conn.execute(
                """UPDATE agents SET state = 'offline'
                    WHERE id = :agent_id AND state != 'offline'
                      AND presence_expires_at < :now""",
                {"agent_id": agent_id, "now": now},
            )
            if reaped.rowcount != 1:
                self._rollback(conn)
                return ReapResult(agent_id=agent_id)

            requeued: list[str] = []
            dead_lettered: list[str] = []
            for job_id in self._distinct_jobs_on(conn, agent_id):
                kind = self._requeue_job(conn, job_id, agent_id=agent_id, now=now, reason=reason)
                if kind == "job.requeued":
                    requeued.append(job_id)
                elif kind == "job.dead_letter":
                    dead_lettered.append(job_id)

            freed = [
                row["id"]
                for row in conn.execute(
                    "SELECT id FROM resources WHERE agent_id = ? AND state = 'busy' ORDER BY id",
                    (agent_id,),
                )
            ]
            # A reap releases CLAIMS. That is all it knows about.
            #
            # I5 is the negative statement — no resource of an offline agent is
            # busy or holds a job — and `state = 'busy'` is exactly what it takes
            # to satisfy it. `unhealthy` and `retired` are left as they were: TSS
            # never infers device health, the agent reports it. Marking a broken
            # device free because its machine died would be TSS deciding the
            # J-Link got fixed, and no exception for `retired` is needed once the
            # update only touches what was claimed.
            conn.execute(
                """UPDATE resources SET state = 'free', current_job_id = NULL
                    WHERE agent_id = ? AND state = 'busy'""",
                (agent_id,),
            )
            self._insert_event(
                conn,
                kind="agent.offline",
                ts=now,
                agent_id=agent_id,
                detail={
                    "reason": reason,
                    "requeued": requeued,
                    "dead_lettered": dead_lettered,
                    "freed_resources": [local_of(r) for r in freed],
                },
            )
            self._commit(conn)
        except BaseException:
            self._rollback(conn)
            raise

        return ReapResult(
            agent_id=agent_id,
            freed_resources=freed,
            requeued_jobs=requeued,
            dead_lettered_jobs=dead_lettered,
        )

    # ---------------------------------------------------- sweep 2: hung jobs
    def timed_out_jobs(self, *, now: float | None = None) -> list[tuple[str, str]]:
        """Sweep 2's query (§3.5): running past its budget.

        Nothing here looks at presence. The agent is alive and heartbeating; the
        JOB is what has stopped making progress. Collapsing this with presence
        expiry is the most common design mistake in this system — a test that
        legitimately takes 20 minutes must not look like a dead bench, and a
        bench whose power supply died must not get 20 minutes of grace.
        """
        now = time.time() if now is None else now
        rows = self.conn.execute(
            """SELECT id, agent_id FROM jobs
                WHERE state = 'running' AND ? > started_at + max_duration_s
                ORDER BY id""",
            (now,),
        ).fetchall()
        return [(r["id"], r["agent_id"]) for r in rows]

    def time_out_job(self, job_id: str, *, now: float | None = None) -> str | None:
        """Terminate one hung job. Returns the event kind, or None if it finished
        while we were looking at it.

        Retry ONCE, then dead-letter — counted in `job.timed_out` events, not in
        `tried_agents`: a job that hangs on two different benches is hanging
        because of itself, and blaming three benches for it would take three
        machines out of rotation for a job that will hang on the fourth too.
        """
        now = time.time() if now is None else now
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                """SELECT agent_id, epoch, max_duration_s, started_at FROM jobs
                    WHERE id = :job_id AND state = 'running'
                      AND :now > started_at + max_duration_s""",
                {"job_id": job_id, "now": now},
            ).fetchone()
            if row is None:
                self._rollback(conn)
                return None
            agent_id = row["agent_id"]

            prior = int(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM events WHERE job_id = ? AND kind = 'job.timed_out'",
                    (job_id,),
                ).fetchone()["n"]
            )
            self._insert_event(
                conn,
                kind="job.timed_out",
                ts=now,
                agent_id=agent_id,
                job_id=job_id,
                detail={
                    "elapsed_s": round(now - float(row["started_at"]), 3),
                    "max_duration_s": row["max_duration_s"],
                    "prior_timeouts": prior,
                },
            )
            # The epoch bump inside here is what fences out the report the agent
            # will eventually send for a run we have already given up on.
            kind = self._requeue_job(
                conn,
                job_id,
                agent_id=agent_id,
                now=now,
                reason=DETAIL_TIMEOUT,
                force_dead_letter=prior >= 1,
            )
            if kind is None:
                self._rollback(conn)
                return None
            # THIS job's devices only — the bench is fine and its other jobs are
            # still running on it (§7.2).
            conn.execute(
                """UPDATE resources SET state = 'free', current_job_id = NULL
                    WHERE current_job_id = :job_id AND state = 'busy'""",
                {"job_id": job_id},
            )
            self._commit(conn)
        except BaseException:
            self._rollback(conn)
            raise
        return kind

    def jobs_owned_by(self, agent_id: str) -> list[str]:
        """What TSS believes this bench is holding: assigned or running."""
        return [
            row["id"]
            for row in self.conn.execute(
                """SELECT id FROM jobs
                    WHERE agent_id = ? AND state IN ('assigned','running')
                    ORDER BY id""",
                (agent_id,),
            )
        ]

    def requeue_job(
        self, job_id: str, *, agent_id: str, reason: str, now: float | None = None
    ) -> str | None:
        """Take one job back from a bench that is not running it — the inverse
        fence's write (§3.5). Same transaction shape as the timeout sweep: bump
        the epoch, close the allocation record, free that job's devices only."""
        now = time.time() if now is None else now
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            kind = self._requeue_job(conn, job_id, agent_id=agent_id, now=now, reason=reason)
            if kind is None:
                self._rollback(conn)
                return None
            conn.execute(
                """UPDATE resources SET state = 'free', current_job_id = NULL
                    WHERE current_job_id = :job_id AND state = 'busy'""",
                {"job_id": job_id},
            )
            self._commit(conn)
        except BaseException:
            self._rollback(conn)
            raise
        return kind

    # ---------------------------------------------------------- operator verbs
    def drain_agent(self, agent_id: str, *, now: float | None = None) -> str:
        """Finish what you are running, accept nothing new (§4.1).

        Needed for deploys: without it, upgrading an agent means killing running
        tests. The matcher stops offering work the moment the state changes,
        because `online_agents` only ever returns `online` ones.

        THERE IS NO DRAINING -> OFFLINE TRANSITION, deliberately. The agent
        finishes its jobs, reports them normally — a drained bench is not fenced,
        so its results are accepted — and exits. Its lease then expires and the
        ordinary presence sweep takes it offline with nothing left to requeue.
        One mechanism, the same one that handles every other way a bench stops
        being there.
        """
        now = time.time() if now is None else now
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                "UPDATE agents SET state = 'draining' WHERE id = ? AND state = 'online'",
                (agent_id,),
            )
            if cur.rowcount != 1:
                self._rollback(conn)
                agent = self.get_agent(agent_id)
                return "unknown_agent" if agent is None else f"not_online:{agent.state}"

            # An assignment this bench has not started yet is limbo: it will be
            # redelivered to a daemon that is draining and discarded, and the job
            # then waits out presence expiry having burned an attempt for
            # nothing. Take those back here, in the same transaction that turns
            # the tap off — a job that never started is not "current work".
            requeued = []
            for job_id in self._distinct_jobs_on(conn, agent_id):
                row = conn.execute("SELECT state FROM jobs WHERE id = ?", (job_id,)).fetchone()
                if row is None or row["state"] != JobState.ASSIGNED:
                    continue
                if self._requeue_job(
                    conn, job_id, agent_id=agent_id, now=now, reason=DETAIL_DRAINED
                ):
                    requeued.append(job_id)
            if requeued:
                placeholders = ",".join("?" * len(requeued))
                conn.execute(
                    f"""UPDATE resources SET state = 'free', current_job_id = NULL
                         WHERE current_job_id IN ({placeholders}) AND state = 'busy'""",
                    tuple(requeued),
                )

            finishing = self._distinct_jobs_on(conn, agent_id)
            self._insert_event(
                conn,
                kind="agent.draining",
                ts=now,
                agent_id=agent_id,
                detail={"finishing": finishing, "requeued": requeued},
            )
            self._commit(conn)
        except BaseException:
            self._rollback(conn)
            raise
        return "draining"

    def unquarantine_agent(self, agent_id: str, *, now: float | None = None) -> str:
        """Let a machine back in (§4.2).

        A state with no way out is a slow fleet-drain dressed up as a health
        feature, which is why this exists at all. Clearing the failure count is
        the point: the bench starts again from zero rather than being one bad job
        away from going straight back out.
        """
        now = time.time() if now is None else now
        agent = self.get_agent(agent_id)
        if agent is None:
            return "unknown_agent"
        cur = self.conn.execute(
            """UPDATE agents
                  SET state = CASE WHEN state = 'quarantined' THEN 'online' ELSE state END,
                      quarantined_at = NULL,
                      consecutive_fails = 0
                WHERE id = ?""",
            (agent_id,),
        )
        if cur.rowcount != 1:
            return "unknown_agent"
        self.append_event(
            "agent.unquarantined", agent_id=agent_id, detail={"was": str(agent.state)}, now=now
        )
        return "unquarantined"

    def unquarantine_resource(self, resource_id: str, *, now: float | None = None) -> str:
        """Let one device back in — the bench itself was never the problem."""
        now = time.time() if now is None else now
        resource = self.get_resource(resource_id)
        if resource is None:
            return "unknown_resource"
        if resource.state == ResourceState.RETIRED:
            # Retired is not a health state: this device is not on the bench any
            # more, and un-quarantining it would put a ghost back in the pool.
            return "retired"
        self.conn.execute(
            """UPDATE resources
                  SET state = CASE WHEN state = 'unhealthy' THEN 'free' ELSE state END,
                      quarantined_at = NULL,
                      consecutive_fails = 0
                WHERE id = ?""",
            (resource_id,),
        )
        self.append_event(
            "resource.unquarantined",
            agent_id=resource.agent_id,
            resource_id=resource_id,
            detail={"was": str(resource.state)},
            now=now,
        )
        return "unquarantined"

    # ------------------------------------------------------ unsatisfiable jobs
    def set_blocked_reason(
        self, job_id: str, reason: str | None, *, now: float | None = None
    ) -> bool:
        """Annotate a queued job with why it is not moving. Returns True if this
        changed anything, so the caller can emit the event exactly once.

        The job stays QUEUED. Fleets change — a bench gets repaired,
        un-quarantined or added — and a job that cannot run on today's fleet may
        run on tomorrow's (§3.4.1).
        """
        now = time.time() if now is None else now
        cur = self.conn.execute(
            """UPDATE jobs SET blocked_reason = :reason
                WHERE id = :job_id AND state = 'queued'
                  AND (blocked_reason IS NOT :reason)""",
            {"reason": reason, "job_id": job_id},
        )
        if cur.rowcount != 1:
            return False
        if reason is not None:
            self.append_event(
                "job.unsatisfiable", job_id=job_id, detail={"reason": reason}, now=now
            )
        return True

    def dead_letter_unsatisfiable(self, job_id: str, *, now: float | None = None) -> bool:
        """Give up on a job no bench could ever run (§3.4.1, UNSATISFIABLE_TIMEOUT).

        Not a retry path and not a bench's fault, but still infrastructure's
        problem rather than the engineer's: nobody in the fleet has the hardware
        this job asked for. Hence `outcome='infra_error'` with the specifics in
        result_detail — never `outcome='dead_letter'`, which is a state.
        """
        now = time.time() if now is None else now
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                """UPDATE jobs
                      SET state = 'dead_letter', outcome = 'infra_error',
                          result_detail = :detail, finished_at = :now, epoch = epoch + 1
                    WHERE id = :job_id AND state = 'queued'""",
                {
                    "job_id": job_id,
                    "now": now,
                    "detail": f"{BLOCKED_NO_CAPABLE_AGENT} after "
                    f"{int(self.config.unsatisfiable_timeout_s)}s",
                },
            )
            if cur.rowcount != 1:
                self._rollback(conn)
                return False
            self._insert_event(
                conn,
                kind="job.dead_letter",
                ts=now,
                job_id=job_id,
                detail={"reason": BLOCKED_NO_CAPABLE_AGENT},
            )
            self._commit(conn)
        except BaseException:
            self._rollback(conn)
            raise
        return True

    # --------------------------------------------------------------- cancel
    def cancel_job(self, job_id: str, *, now: float | None = None) -> str:
        """Client cancel (§6). Queued dies quietly; running is fenced first.

        The epoch bump is the whole point on the running path: it is what stops
        the agent's late "PASSED" from overwriting CANCELLED. Without it, a job
        an engineer cancelled reappears as a passing result minutes later (I7).
        """
        now = time.time() if now is None else now
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT state, agent_id FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if row is None:
                self._rollback(conn)
                return "unknown_job"
            if row["state"] in TERMINAL_JOB_STATES:
                self._rollback(conn)
                return "already_terminal"

            was_running = row["state"] in (JobState.ASSIGNED, JobState.RUNNING)
            cur = conn.execute(
                """UPDATE jobs
                      SET state = 'cancelled', outcome = 'cancelled',
                          result_detail = :detail, finished_at = :now,
                          epoch = epoch + 1
                    WHERE id = :job_id
                      AND state IN ('queued','assigned','running')""",
                {"job_id": job_id, "now": now, "detail": DETAIL_CANCELLED},
            )
            if cur.rowcount != 1:
                self._rollback(conn)
                return "already_terminal"

            conn.execute(
                """UPDATE job_resources SET released_at = :now
                    WHERE job_id = :job_id AND released_at IS NULL""",
                {"now": now, "job_id": job_id},
            )
            conn.execute(
                """UPDATE resources SET state = 'free', current_job_id = NULL
                    WHERE current_job_id = :job_id AND state = 'busy'""",
                {"job_id": job_id},
            )
            self._insert_event(
                conn,
                kind="job.cancelled",
                ts=now,
                agent_id=row["agent_id"],
                job_id=job_id,
                detail={"was_running": was_running},
            )
            self._commit(conn)
        except BaseException:
            self._rollback(conn)
            raise
        return "cancelled_running" if was_running else "cancelled"

    # -------------------------------------------------------------- fencing
    def fence_running_jobs(self, agent_id: str, reported: Sequence[tuple[str, int]]) -> str | None:
        """Check what the agent believes it owns against the epoch (§6).

        Returns the first job it has lost, or None. Each job is checked
        INDEPENDENTLY: a bench running two jobs can lose one — cancelled, timed
        out, or reassigned after a blip — and must keep the other. Returning the
        job id is what lets the agent abandon exactly that run.
        """
        for job_id, epoch in reported:
            job = self.get_job(job_id)
            if job is None:
                return job_id
            if job.agent_id != agent_id or job.epoch != epoch or job.is_terminal:
                return job_id
        return None

    @staticmethod
    def _distinct_jobs_on(conn: sqlite3.Connection, agent_id: str) -> list[str]:
        """Per JOB, not per resource. The DISTINCT is the whole point (§3.5)."""
        rows = conn.execute(
            """SELECT DISTINCT current_job_id FROM resources
                WHERE agent_id = ? AND current_job_id IS NOT NULL
                ORDER BY current_job_id""",
            (agent_id,),
        ).fetchall()
        return [r["current_job_id"] for r in rows]

    def _requeue_job(
        self,
        conn: sqlite3.Connection,
        job_id: str,
        *,
        agent_id: str,
        now: float,
        reason: str,
        force_dead_letter: bool = False,
    ) -> str | None:
        """Take one job back. Returns the event kind emitted, or None if there was
        nothing to take back.

        Guarded on `state IN ('assigned','running')`: a job that has already
        reached a terminal state is never resurrected (I7), and a job that is
        already queued is never requeued a second time — the second line of
        defence behind the DISTINCT above.
        """
        row = conn.execute(
            "SELECT state, epoch, attempt, tried_agents FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
        if row is None or row["state"] not in (JobState.ASSIGNED, JobState.RUNNING):
            return None

        epoch = int(row["epoch"])
        conn.execute(
            """UPDATE job_resources SET released_at = :now
                WHERE job_id = :job_id AND epoch = :epoch AND released_at IS NULL""",
            {"now": now, "job_id": job_id, "epoch": epoch},
        )

        # Poison detection keys off distinct benches, not the attempt count: a
        # counter cannot tell "this job kills every bench it touches" from "these
        # three benches are broken". `force_dead_letter` is the timeout sweep's
        # separate rule — a job that hangs twice is hanging on its own, not
        # because of the bench, so it gets one retry rather than three (§7.2).
        benches_tried = len(set(json.loads(row["tried_agents"])))
        # A one-bench fleet never raises the DISTINCT count past 1, so an
        # always-failing job would return to the same bench forever and never
        # reach a terminal state (I3). A liveness bound, not an attribution rule:
        # `tried_agents` still decides who is to blame.
        attempts_exhausted = int(row["attempt"]) >= self.config.max_total_attempts
        if (
            force_dead_letter
            or attempts_exhausted
            or benches_tried >= self.config.max_distinct_agents
        ):
            # `state` says what happened; `outcome` says whose problem it is.
            # Dead-lettering only ever happens on the infra retry path — FAILED
            # is a real result and never retries (§4.2) — so the outcome is
            # always `infra_error`, and the specifics go in result_detail.
            # Writing outcome='dead_letter' would repeat the state and throw away
            # the FAILED-vs-INFRA_ERROR distinction on the jobs that failed
            # worst: a dashboard counting infra_error would silently exclude the
            # most severe infra failures in the fleet.
            sql = """UPDATE jobs
                        SET state = 'dead_letter', outcome = 'infra_error',
                            result_detail = :detail, agent_id = NULL,
                            epoch = epoch + 1, finished_at = :now
                      WHERE id = :job_id AND state IN ('assigned','running')"""
            kind = "job.dead_letter"
        else:
            sql = """UPDATE jobs
                        SET state = 'queued', agent_id = NULL, epoch = epoch + 1,
                            assigned_at = NULL, started_at = NULL,
                            result_detail = :detail
                      WHERE id = :job_id AND state IN ('assigned','running')"""
            kind = "job.requeued"

        # The detail always LEADS with the reason, so the record says which path
        # ended this attempt. That is what makes I6 checkable at all: a hung job
        # and a job on a dead bench are indistinguishable afterwards if both
        # write the same string (§3.8).
        if force_dead_letter or attempts_exhausted:
            detail = f"{reason} after {int(row['attempt'])} attempts"
        elif benches_tried >= self.config.max_distinct_agents:
            detail = f"{reason} after {benches_tried} benches"
        else:
            detail = reason
        params = {"job_id": job_id, "now": now, "reason": reason, "detail": detail}
        if conn.execute(sql, params).rowcount != 1:
            return None
        self._insert_event(
            conn,
            kind=kind,
            ts=now,
            agent_id=agent_id,
            job_id=job_id,
            detail={"reason": reason, "epoch": epoch + 1, "benches_tried": benches_tried},
        )
        return kind

    # ------------------------------------------------------------- fleet writes
    # Minimal creation paths kept from step 1 for tests that want a bench without
    # going through registration.
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
        # Autocommit: the row is already durable, so this is "after commit" too.
        published, self._local.pending_events = self._pending_events(), []
        if published and self.publish is not None:
            self.publish(published)

    def _insert_event(
        self,
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
        # Held back until COMMIT. Publishing here would announce a state change
        # that a rollback may yet undo, and the live stream would then disagree
        # with the audit log — which is the one thing §3.6 rules out.
        self._pending_events().append(
            Event(
                ts=ts,
                kind=kind,
                agent_id=agent_id,
                resource_id=resource_id,
                job_id=job_id,
                detail=detail,
            )
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

    def agents(self) -> list[Agent]:
        return [Agent.from_row(r) for r in self.conn.execute("SELECT * FROM agents ORDER BY id")]

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

    def queued_jobs(self, *, limit: int | None = None) -> list[Job]:
        """The queue, oldest-first within priority (§5's idx_jobs_queue)."""
        sql = "SELECT * FROM jobs WHERE state = 'queued' ORDER BY priority, submitted_at, id"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        return [Job.from_row(r) for r in self.conn.execute(sql)]

    def jobs_in_flight(self) -> list[Job]:
        """Everything not yet terminal: queued, assigned and running."""
        return [
            Job.from_row(r)
            for r in self.conn.execute(
                """SELECT * FROM jobs
                    WHERE state IN ('queued','assigned','running')
                    ORDER BY priority, submitted_at, id"""
            )
        ]

    def queue_position(self, job_id: str) -> int:
        """1-based position among queued jobs; 0 once it is no longer queued."""
        row = self.conn.execute(
            """SELECT COUNT(*) AS ahead FROM jobs
                WHERE state = 'queued'
                  AND (priority, submitted_at, id) <= (
                        SELECT priority, submitted_at, id FROM jobs WHERE id = ?)""",
            (job_id,),
        ).fetchone()
        return int(row["ahead"])

    def online_agents(self, *, now: float | None = None) -> list[Agent]:
        """Benches that may be offered work: online, lease still valid.

        `draining` and `quarantined` are deliberately excluded — one is finishing
        up before a deploy, the other is suspect. And an agent whose lease has run
        out but which the reaper has not swept yet is not offered work either:
        it is about to be taken apart.
        """
        now = time.time() if now is None else now
        rows = self.conn.execute(
            """SELECT * FROM agents
                WHERE state = 'online' AND presence_expires_at > ?
                ORDER BY id""",
            (now,),
        ).fetchall()
        return [Agent.from_row(r) for r in rows]

    def has_free_resources(self, agent_id: str) -> bool:
        """Spare capacity — which is what decides whether a heartbeat long-polls."""
        row = self.conn.execute(
            "SELECT 1 FROM resources WHERE agent_id = ? AND state = 'free' LIMIT 1",
            (agent_id,),
        ).fetchone()
        return row is not None

    def fleet(self, *, now: float | None = None) -> FleetView:
        """The two-level fleet view (§3.9): benches, each with its devices.

        An OFFLINE bench also carries what it took down with it, read back out of
        the `agent.offline` event so the view can say "3 devices free, 2 jobs
        requeued" instead of just going blank.
        """
        now = time.time() if now is None else now
        by_agent: dict[str, list[ResourceView]] = {}
        for resource in self.list_resources():
            by_agent.setdefault(resource.agent_id, []).append(
                ResourceView(
                    id=resource.id,
                    local_id=local_of(resource.id),
                    state=resource.state,
                    current_job_id=resource.current_job_id,
                    capabilities=resource.capabilities,
                )
            )

        agents: list[AgentView] = []
        for row in self.conn.execute("SELECT * FROM agents ORDER BY id"):
            agent = Agent.from_row(row)
            requeued: list[str] = []
            if agent.state == AgentState.OFFLINE:
                last = self.conn.execute(
                    """SELECT detail FROM events
                        WHERE kind = 'agent.offline' AND agent_id = ?
                        ORDER BY seq DESC LIMIT 1""",
                    (agent.id,),
                ).fetchone()
                if last and last["detail"]:
                    requeued = json.loads(last["detail"]).get("requeued", [])
            agents.append(
                AgentView(
                    id=agent.id,
                    hostname=agent.hostname,
                    state=agent.state,
                    agent_version=agent.agent_version,
                    last_heartbeat_at=agent.last_heartbeat_at,
                    presence_expires_at=agent.presence_expires_at,
                    seconds_since_beat=max(0.0, now - agent.last_heartbeat_at),
                    resources=by_agent.get(agent.id, []),
                    requeued_on_last_reap=requeued,
                )
            )
        return FleetView(now=now, agents=agents)

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

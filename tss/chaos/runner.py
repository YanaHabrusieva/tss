"""The chaos harness: spin up a fleet, flood it with jobs, watch it not break.

    python -m tss.chaos.runner --agents 15 --jobs 100 --multi-pct 30 --seed 42
    python -m tss.chaos.runner --seeds 5            # the merge gate

WHY THIS IS THE HIGHEST-LEVERAGE THING IN THE PROJECT. It is the demo, and it is
also the only way to find the concurrency bugs, because they are timing-dependent
and will not show up in hand-testing. "I ran chaos and it seemed fine" is an
anecdote; "nine invariants, 100 jobs, 30% of them multi-device, a 30% crash rate,
zero violations across every seed CI runs" is a threshold you can defend.

EVERY RUN LOGS ITS SEED, first line and again in any failure output. A violation
you cannot replay is a violation you cannot fix.

WHAT A SEED ACTUALLY REPRODUCES: the workload and the fleet — which benches get
which profile, what inventory each has, every job spec, every crash and dropped
beat drawn from a profile's probabilities. It does NOT reproduce the schedule.
The interleavings ride on real asyncio scheduling, real sockets and a real
SQLite, so a replay re-runs the same SCENARIO, not the same execution. That is
enough to make a violation investigable and not enough to guarantee it recurs on
the first try — which is why a failing run also dumps its event log.

NO SILENT CAPS. If the run drops jobs, truncates, or hits its deadline with work
outstanding, the summary says so loudly. A run that quietly covered less than it
claims reads as "all green" when it is not.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import random
import sys
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass, field

import httpx

from tss.api.app import create_app
from tss.chaos import invariants as chaos_invariants
from tss.chaos.mock_agent import MockAgent
from tss.chaos.profiles import fleet_profiles
from tss.core.config import Config
from tss.core.store import DETAIL_UNREPORTED, Store

log = logging.getLogger("tss.chaos")

#: Everything scaled down so a full run takes seconds instead of minutes. The
#: RATIOS are the ones that matter and they are preserved: TTL is 4 heartbeats,
#: TTL > longpoll + heartbeat, and the job budget is a few multiples of a job.
CHAOS_CONFIG = Config(
    heartbeat_interval_s=0.3,
    presence_ttl_s=1.2,
    reaper_interval_s=0.2,
    longpoll_timeout_s=0.8,
    scheduler_tick_s=0.3,
    starvation_threshold_s=3.0,
    unsatisfiable_timeout_s=25.0,
    default_max_duration_s=3,
)

#: How often the watcher runs the safety checks.
WATCH_INTERVAL_S = 0.25
#: The floor on how many of those a finished run must have completed, as a
#: fraction of duration/interval. Deliberately loose — each check costs real time
#: and the loop cannot hit its interval exactly, so the honest path lands near
#: 0.9 and has ~2x headroom. A watcher that died half way cannot reach it.
WATCH_FLOOR_FRACTION = 0.5

PRODUCTS = (
    {"product": "vehicle_gateway", "harness": "j1939"},
    {"product": "vehicle_gateway", "harness": "obd2"},
    {"product": "asset_gateway"},
)


@dataclass
class RunReport:
    seed: int
    agents: int
    jobs_requested: int
    jobs_submitted: int = 0
    multi_pct: int = 0
    profile_counts: dict[str, int] = field(default_factory=dict)
    outcomes: dict[str, int] = field(default_factory=dict)
    violations: list[str] = field(default_factory=list)
    #: Checks that ran to COMPLETION. A check that raised is not a check.
    safety_checks: int = 0
    #: Exceptions the watcher swallowed to stay alive. Never invisible: a
    #: checker that is throwing is a checker whose green means nothing.
    watch_errors: int = 0
    #: Evidence the chaos actually happened. A "clean" run in which nothing ever
    #: crashed, expired or timed out proves nothing at all.
    events: dict[str, int] = field(default_factory=dict)
    fleet: dict[str, int] = field(default_factory=dict)
    unfinished: list[str] = field(default_factory=list)
    deadline_hit: bool = False
    duration_s: float = 0.0
    notes: list[str] = field(default_factory=list)
    #: Where a failing run wrote its event log. Empty on a clean run.
    evidence_path: str = ""

    #: What the run must have PRODUCED, not merely survived. Filled in from the
    #: profiles actually in the fleet.
    floors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations and not self.unfinished and not self.floors

    def check_floors(self, profiles: Sequence, *, watch_interval_s: float = 0.25) -> list[str]:
        """Did the chaos actually happen?

        Nothing above this line would notice if it had not. Neuter
        `crash_probability` and every invariant still holds, every job still
        finishes, and the gate goes green — because "nothing broke" is trivially
        true of a fleet where nothing was broken. A run that dead-lettered all
        100 jobs also satisfies I3 and every safety check.

        So the floors are per profile PRESENT IN THIS FLEET: a `--profile zombie`
        run must not be asked to produce crashes. What is asked of every fleet
        that could manage it is one passing job — the happy path is a property
        too, and a gate that only proves the failure paths work is half a gate.
        """
        present = {p.name for p in profiles}
        failures = []

        def require(condition: bool, what: str, why: str) -> None:
            if not condition:
                failures.append(f"FLOOR: {what} — {why}")

        # THE WATCHER ITSELF. Every floor below asks whether the fleet did
        # something; this one asks whether anybody was looking. One exception out
        # of check_safety used to kill the task, and the run then finished green
        # having checked nothing since — the most expensive kind of quiet.
        # WATCH_FLOOR_FRACTION of nominal, because a check costs time and the
        # loop cannot hit its interval exactly; a watcher that died in the first
        # half of the run cannot reach it.
        if self.duration_s > 0:
            nominal = self.duration_s / watch_interval_s
            require(
                self.safety_checks >= nominal * WATCH_FLOOR_FRACTION,
                f"only {self.safety_checks} safety checks in {self.duration_s:.0f}s "
                f"(expected at least {nominal * WATCH_FLOOR_FRACTION:.0f})",
                "the watcher stopped checking part-way through and the run finished "
                "green on the strength of the checks it never ran",
            )

        if any(p.can_pass_a_job for p in profiles):
            require(
                self.outcomes.get("passed", 0) > 0,
                "no job passed",
                "a fleet that finishes nothing satisfies every safety invariant",
            )
        if "crasher" in present:
            require(
                self.fleet.get("crashes", 0) > 0,
                "no bench crashed",
                "the crasher profile is in the fleet and never fired",
            )
        if "flaky_network" in present:
            require(
                self.fleet.get("dropped_beats", 0) > 0,
                "no heartbeat was dropped",
                "flaky_network is in the fleet and never dropped one",
            )
        if "hung" in present:
            require(
                self.events.get("timed_out", 0) > 0,
                "no job timed out",
                "hung is in the fleet, so sweep 2 should have fired",
            )
        if any(p.can_die for p in profiles):
            require(
                self.events.get("offline", 0) > 0,
                "no bench was reaped",
                "profiles that stop heartbeating are in the fleet",
            )
        if any(p.can_strand_a_job for p in profiles):
            require(
                self.events.get("requeued", 0) > 0,
                "no job was requeued",
                "benches died holding jobs and nothing came back",
            )
        if "flapper" in present:
            require(
                self.fleet.get("reregistrations", 0) > 0,
                "no bench re-registered",
                "flapper is in the fleet; registration idempotency was never exercised",
            )
        if "resource_flap" in present:
            require(
                self.events.get("resource.unhealthy", 0) > 0,
                "no device was reported unhealthy",
                "resource_flap is in the fleet; device health never diverged from "
                "machine health, which is the whole distinction it tests",
            )
        if "zombie" in present:
            require(
                self.fleet.get("fenced_reports", 0) > 0,
                "no stale report was fenced",
                "zombie is in the fleet; if none of its reports was ever rejected it "
                "never outlived its lease, and the epoch fence was never exercised",
            )
        if "deaf" in present:
            require(
                self.events.get("inverse_fence_on_deaf", 0) > 0,
                "the inverse fence never fired on a bench that lost a reply",
                "deaf is in the fleet; a bench discarded a reply and TSS never took back "
                "the job it was left holding but not running",
            )
        return failures

    def render(self) -> str:
        lines = [
            f"seed={self.seed}  agents={self.agents}  jobs={self.jobs_submitted}"
            f"/{self.jobs_requested}  multi={self.multi_pct}%  {self.duration_s:.1f}s",
            f"  profiles: {_counts(self.profile_counts)}",
            f"  outcomes: {_counts(self.outcomes)}",
            f"  safety checks run: {self.safety_checks}"
            + (f"   !! {self.watch_errors} watcher error(s)" if self.watch_errors else ""),
            f"  chaos:    {_counts(self.fleet)}",
            f"  events:   {_counts(self.events)}",
        ]
        for note in self.notes:
            lines.append(f"  !! {note}")
        # A failed floor names itself exactly as a violation does: the run did
        # not do what it claimed, and that is the same kind of red.
        lines.extend(f"  !! {floor}" for floor in self.floors)
        if self.unfinished:
            lines.append(f"  !! I3 (liveness): {len(self.unfinished)} job(s) never finished")
            lines.extend(f"       {v}" for v in self.unfinished[:10])
        if self.violations:
            lines.append(f"  !! {len(self.violations)} invariant violation(s):")
            lines.extend(f"       {v}" for v in _first_of_each(self.violations))
        if self.ok:
            lines.append("  OK — no invariant violated")
        else:
            lines.append(f"  FAILED — replay with: just chaos-seed {self.seed}")
            if self.evidence_path:
                lines.append(f"       evidence: {self.evidence_path} (every event this run wrote)")
            lines.append(
                "       the seed fixes the workload and the fleet, not the interleaving — "
                "a replay re-runs the scenario, not the schedule"
            )
        return "\n".join(lines)


def _counts(counter: dict[str, int]) -> str:
    return ", ".join(f"{k}={v}" for k, v in sorted(counter.items())) or "none"


def _first_of_each(violations: list[str], per_kind: int = 3) -> list[str]:
    """Show a few of each invariant rather than 400 copies of one."""
    seen: dict[str, int] = {}
    out = []
    for violation in violations:
        kind = violation.split(":", 1)[0]
        seen[kind] = seen.get(kind, 0) + 1
        if seen[kind] <= per_kind:
            out.append(violation)
    for kind, count in sorted(seen.items()):
        if count > per_kind:
            out.append(f"...and {count - per_kind} more {kind} violation(s)")
    return out


def derive_deadline(jobs: int, agents: int) -> float:
    """How long a run of this size is allowed to take before it is called stuck.

    A fixed 90s was right for the gate's own shape and wrong for everything else:
    `--jobs 500` on a loaded CI box hit it and went red for being big, which
    teaches people to raise the number rather than read it. Scaled, the headroom
    is roughly 3x the honest path at every size — the gate's own 100/15 lands
    near 28s against a 95s deadline.
    """
    return 30.0 + 0.5 * jobs + 1.0 * agents


class ChaosRun:
    def __init__(
        self,
        *,
        seed: int,
        agents: int,
        jobs: int,
        multi_pct: int,
        profile_mix: str,
        db_path: str,
        config: Config = CHAOS_CONFIG,
        deadline_s: float | None = None,
        products: tuple[dict, ...] = PRODUCTS,
    ) -> None:
        self.seed = seed
        self.rng = random.Random(seed)
        self.n_agents = agents
        self.n_jobs = jobs
        self.multi_pct = multi_pct
        self.profile_mix = profile_mix
        self.config = config
        self.deadline_s = derive_deadline(jobs, agents) if deadline_s is None else deadline_s
        self.products = products
        self.store = Store(db_path, config)
        self.app = create_app(config, self.store)
        self.report = RunReport(seed=seed, agents=agents, jobs_requested=jobs, multi_pct=multi_pct)
        self.submitted: list[str] = []
        self.fleet: list[MockAgent] = []
        self._base_url = ""

    # ------------------------------------------------------------------ fleet
    def _build_fleet(self) -> None:
        profiles = fleet_profiles(self.n_agents, self.profile_mix)
        for index, profile in enumerate(profiles, start=1):
            agent_id = f"bench-{index:02d}"
            inventory = []
            for device in range(1, self.rng.randint(2, 4) + 1):
                caps = dict(self.rng.choice(self.products))
                prefix = "vg" if caps["product"] == "vehicle_gateway" else "ag"
                inventory.append({"id": f"{prefix}-{device:02d}", "capabilities": caps})
            self.fleet.append(
                MockAgent(
                    agent_id,
                    inventory,
                    base_url=self._base_url,
                    profile=profile,
                    seed=self.seed,
                    presence_ttl_s=self.config.presence_ttl_s,
                    reaper_interval_s=self.config.reaper_interval_s,
                )
            )
            self.report.profile_counts[profile.name] = (
                self.report.profile_counts.get(profile.name, 0) + 1
            )

    def _job_spec(self) -> dict:
        """A single- or multi-device job. Multi-device is what makes contention
        real: a fleet running only single-device jobs never exercises the N-way
        claim, the reservation logic or the fan-out."""
        multi = self.rng.randint(1, 100) <= self.multi_pct
        count = self.rng.choice((2, 2, 3)) if multi else 1
        product = dict(self.rng.choice(self.products))
        # Ask for the product, and sometimes pin the harness too — overlapping
        # specs are what the backtracking matcher exists for.
        requirements = []
        for _ in range(count):
            spec = {"product": product["product"]}
            if "harness" in product and self.rng.random() < 0.5:
                spec["harness"] = product["harness"]
            requirements.append(spec)
        return {
            "name": f"chaos-{len(self.submitted):03d}",
            "requirements": requirements,
            "payload": {"duration_s": round(self.rng.uniform(0.1, 0.6), 2)},
            "max_duration_s": self.config.default_max_duration_s,
        }

    # -------------------------------------------------------------------- run
    async def _submit_jobs(self, client: httpx.AsyncClient, stop: asyncio.Event) -> None:
        for _ in range(self.n_jobs):
            if stop.is_set():
                self.report.notes.append(
                    f"submission stopped early: {len(self.submitted)}/{self.n_jobs} jobs submitted"
                )
                return
            try:
                response = await client.post("/v1/jobs", json=self._job_spec())
                response.raise_for_status()
                self.submitted.append(response.json()["job_id"])
            except httpx.HTTPError as exc:
                self.report.notes.append(f"submission failed: {exc!r}")
            await asyncio.sleep(self.rng.uniform(0.0, 0.05))
        self.report.jobs_submitted = len(self.submitted)

    async def _watch(self, stop: asyncio.Event) -> None:
        """The safety checks, continuously, while everything is in flight.

        THE BODY IS GUARDED, and that is not defensive habit. Unguarded, one
        exception out of `check_safety` or `ground_truth` — a transient
        SQLITE_BUSY on the checker's own connection is enough — ended the task,
        the teardown swallowed it as just another cancelled task, and the run
        reported green having checked nothing from that moment on. The floor in
        `check_floors` is the other half: surviving is not the same as still
        working, so the count has to be defended too.
        """
        checker_store = Store(self.store.path, self.config)
        try:
            while not stop.is_set():
                try:
                    truth = [agent.ground_truth() for agent in self.fleet]
                    violations = chaos_invariants.check_safety(
                        checker_store, truth, self.app.state.scheduler
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # Counted, logged with the seed, and the loop lives. NOT
                    # counted as a safety check: a check that raised is not a
                    # check, and letting it inflate the count would defeat the
                    # floor that exists to catch exactly this.
                    self.report.watch_errors += 1
                    log.exception("seed=%s  safety check raised; continuing", self.seed)
                    await asyncio.sleep(WATCH_INTERVAL_S)
                    continue
                self.report.safety_checks += 1
                for violation in violations:
                    if violation not in self.report.violations:
                        log.error("seed=%s  %s", self.seed, violation)
                        self.report.violations.append(violation)
                await asyncio.sleep(WATCH_INTERVAL_S)
        finally:
            checker_store.close()

    async def _drain(self, client: httpx.AsyncClient, stop: asyncio.Event) -> None:
        """Wait for every job to reach a terminal state, or for the deadline."""
        began = time.monotonic()
        while time.monotonic() - began < self.deadline_s:
            await asyncio.sleep(0.3)
            if not self.submitted:
                continue
            remaining = self.store.conn.execute(
                "SELECT COUNT(*) AS n FROM jobs WHERE state IN ('queued','assigned','running')"
            ).fetchone()["n"]
            if remaining == 0 and len(self.submitted) == self.n_jobs:
                return
        self.report.deadline_hit = True
        self.report.notes.append(
            f"deadline of {self.deadline_s:.0f}s reached with work still outstanding — "
            "the run below covers less than it asked for"
        )

    async def _go(self) -> None:
        stop = asyncio.Event()
        async with httpx.AsyncClient(base_url=self._base_url, timeout=30.0) as client:
            agents = [asyncio.create_task(agent.run(stop)) for agent in self.fleet]
            watcher = asyncio.create_task(self._watch(stop))
            submitter = asyncio.create_task(self._submit_jobs(client, stop))
            try:
                await self._drain(client, stop)
            finally:
                stop.set()
                submitter.cancel()
                watcher.cancel()
                for task in agents:
                    task.cancel()
                for task in [*agents, watcher, submitter]:
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await task
                for agent in self.fleet:
                    await agent.aclose()

    def execute(self) -> RunReport:
        began = time.monotonic()
        print(
            f"chaos: seed={self.seed} agents={self.n_agents} jobs={self.n_jobs} "
            f"multi={self.multi_pct}% profile={self.profile_mix}",
            flush=True,
        )
        import uvicorn

        server = uvicorn.Server(
            uvicorn.Config(self.app, host="127.0.0.1", port=0, log_level="error", lifespan="on")
        )
        thread = threading.Thread(target=server.run, name="tss-chaos", daemon=True)
        thread.start()
        deadline = time.monotonic() + 20
        while not server.started:
            if time.monotonic() > deadline:  # pragma: no cover
                raise RuntimeError("TSS did not start")
            time.sleep(0.02)
        self._base_url = f"http://127.0.0.1:{server.servers[0].sockets[0].getsockname()[1]}"
        self._build_fleet()

        try:
            asyncio.run(self._go())
        finally:
            server.should_exit = True
            thread.join(timeout=20)

        self.report.jobs_submitted = len(self.submitted)
        if self.report.jobs_submitted < self.n_jobs:
            self.report.notes.append(
                f"only {self.report.jobs_submitted}/{self.n_jobs} jobs were submitted"
            )
        self.report.unfinished = chaos_invariants.check_liveness(
            self.store, submitted=self.submitted
        )
        # One last safety sweep now everything has settled.
        truth = [agent.ground_truth() for agent in self.fleet]
        for violation in chaos_invariants.check_safety(self.store, truth, self.app.state.scheduler):
            if violation not in self.report.violations:
                self.report.violations.append(violation)

        for row in self.store.conn.execute("SELECT state, COUNT(*) AS n FROM jobs GROUP BY state"):
            self.report.outcomes[row["state"]] = row["n"]
        for row in self.store.conn.execute(
            """SELECT kind, COUNT(*) AS n FROM events
                WHERE kind IN ('agent.offline','job.requeued','job.timed_out','job.dead_letter',
                               'agent.quarantined','resource.quarantined','job.unsatisfiable',
                               'resource.unhealthy')
                GROUP BY kind"""
        ):
            self.report.events[row["kind"].replace("job.", "").replace("agent.", "")] = row["n"]
        # The inverse fence leaves its own kind of requeue, told apart by the
        # reason: a presence reap says presence_expired, this one says the bench
        # never mentioned the job.
        #
        # AND THEN ATTRIBUTED TO THE EXACT JOBS WHOSE REPLY WAS LOST. The reason
        # alone is not enough, and finding that out is what this floor was for:
        # a fleet of four perfectly healthy benches produces these too, because
        # `pending_assignment` hands over one job per beat while the tracker
        # counts misses against every job the bench owns — so a bench handed two
        # jobs in one scheduler pass has the second taken back before TSS ever
        # told it. Real, separate, and reported. Here it means the untargeted
        # count is noise the `deaf` profile could hide inside — so the floor reads
        # only the recovery of a job whose /start reply a bench actually threw
        # away, which no unrelated requeue can supply.
        unreported = self.store.conn.execute(
            "SELECT job_id, COUNT(*) AS n FROM events "
            "WHERE kind = 'job.requeued' AND json_extract(detail, '$.reason') = ? "
            "GROUP BY job_id",
            (DETAIL_UNREPORTED,),
        ).fetchall()
        deafened = {job for a in self.fleet for job in a.lost_start_jobs}
        self.report.events["requeued_unreported"] = sum(r["n"] for r in unreported)
        self.report.events["inverse_fence_on_deaf"] = sum(
            r["n"] for r in unreported if r["job_id"] in deafened
        )
        self.report.fleet = {
            "crashes": sum(a.crashes for a in self.fleet),
            "dropped_beats": sum(a.dropped_beats for a in self.fleet),
            "reregistrations": sum(a.reregistrations for a in self.fleet),
            "fenced_reports": sum(a.fenced_reports for a in self.fleet),
            "lost_responses": sum(a.lost_responses for a in self.fleet),
        }
        # Set BEFORE the floors run: the watcher floor is measured against it.
        self.report.duration_s = time.monotonic() - began
        self.report.floors = self.report.check_floors(
            fleet_profiles(self.n_agents, self.profile_mix), watch_interval_s=WATCH_INTERVAL_S
        )
        for floor in self.report.floors:
            log.error("seed=%s  %s", self.seed, floor)
        # ITEM 4: a red run carries its own evidence. The database lives in a
        # temporary directory and is gone the moment the run returns, so the one
        # artifact worth keeping is written out while it still exists.
        if not self.report.ok:
            self.report.evidence_path = self._dump_events()
        self.store.close()
        return self.report

    def _dump_events(self) -> str:
        """Every event this run recorded, as JSONL, named for the seed."""
        path = f"chaos-seed-{self.seed}-events.jsonl"
        try:
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(json.dumps({"seed": self.seed, "report": self.report.render()}) + "\n")
                for event in self.store.events():
                    handle.write(event.model_dump_json() + "\n")
        except OSError as exc:  # pragma: no cover — a full disk on a red run
            log.error("seed=%s  could not write the event dump: %r", self.seed, exc)
            return ""
        return path


def run_seeds(seeds: list[int], **kwargs) -> list[RunReport]:
    import tempfile

    reports = []
    for seed in seeds:
        with tempfile.TemporaryDirectory() as tmp:
            report = ChaosRun(seed=seed, db_path=f"{tmp}/chaos.db", **kwargs).execute()
        print(report.render(), flush=True)
        reports.append(report)
    return reports


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TSS chaos harness")
    parser.add_argument("--agents", type=int, default=15)
    parser.add_argument("--jobs", type=int, default=100)
    parser.add_argument("--multi-pct", type=int, default=30)
    parser.add_argument("--profile", default="mixed", help="'mixed' or one profile name")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--seeds", type=int, help="run this many seeds, starting at --seed")
    parser.add_argument(
        "--deadline", type=float, default=None, help="default: derived from --jobs and --agents"
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.ERROR,
        format="%(levelname)-7s %(name)s: %(message)s",
    )
    seeds = [args.seed + i for i in range(args.seeds)] if args.seeds else [args.seed]
    reports = run_seeds(
        seeds,
        agents=args.agents,
        jobs=args.jobs,
        multi_pct=args.multi_pct,
        profile_mix=args.profile,
        deadline_s=args.deadline,
    )

    failed = [r for r in reports if not r.ok]
    seeds_run = ", ".join(str(r.seed) for r in reports)
    print()
    print(
        _verdict(
            passed=not failed, seeds=seeds_run, clean=len(reports) - len(failed), total=len(reports)
        )
    )
    if failed:
        print("failing seeds: " + ", ".join(str(r.seed) for r in failed))
        print("replay one with: just chaos-seed <seed>")
        return 1
    return 0


def _verdict(*, passed: bool, seeds: str, clean: int, total: int) -> str:
    """The last thing on screen, and on a projector the only thing anyone reads.

    A run that ends in a wall of counters and one quiet "5/5 seed(s) clean" makes
    the audience look to the presenter to find out whether it worked. The word
    and the seed, in a box.
    """
    rule = "=" * 78
    word = "PASS" if passed else "FAIL"
    summary = f"{clean}/{total} seed(s) clean  ·  seeds: {seeds}"
    return f"{rule}\n  {word}  —  {summary}\n{rule}"


if __name__ == "__main__":
    sys.exit(main())

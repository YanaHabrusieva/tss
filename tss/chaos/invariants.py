"""I1, I3 and I4 — the ones the database cannot prove (§3.8).

The checker reads TWO sources, and that is the whole point.

TSS's record of a device's capabilities is exactly what the agent claimed, so a
fleet full of liars looks perfectly healthy from the inside. TSS's record of
ownership can never show two owners, because the reaper clears the dead one on
its way past. Checking TSS against itself proves nothing at all; these three are
checked against what the mock agents are actually doing.

The other six (I2, I5, I6, I7, I8, I9) are database and scheduler properties and
live in `core/invariants.py`. This module composes both.
"""

from __future__ import annotations

from collections.abc import Sequence

from tss.chaos.mock_agent import AgentTruth
from tss.core import invariants as db_invariants
from tss.core import matcher
from tss.core.models import TERMINAL_JOB_STATES, Resource, ResourceState
from tss.core.store import Store


def check_i1(store: Store, truth: Sequence[AgentTruth]) -> list[str]:
    """At most one agent is AUTHORIZED to own a job, and exactly one result is
    ever accepted for it.

    Note what this does NOT say. It does not say "at most one agent believes it
    owns the job", because that is false by design and the zombie profile
    demonstrates it deliberately: between the reap and its return, agent-7
    sincerely believes it owns a job agent-3 is also running. Execution is
    at-least-once (§7.4). An invariant your own demo breaks teaches everyone to
    ignore the checker.

    Authorization is the epoch. An agent is authorized iff the (job_id, epoch) it
    is running matches the job's CURRENT epoch — which is exactly what the
    fencing token means and exactly what /complete tests.

    The second half is the stronger check, because it is not sampled. A periodic
    checker can miss a transient overlap; it cannot miss a second accepted
    result, because the event log keeps every one of them forever.
    """
    violations = []

    authorized: dict[str, list[str]] = {}
    for agent in truth:
        if not agent.alive:
            continue
        for job in agent.running:
            current = store.get_job(job.job_id)
            if current is not None and current.epoch == job.epoch:
                authorized.setdefault(job.job_id, []).append(agent.agent_id)
    for job_id, agents in sorted(authorized.items()):
        if len(agents) > 1:
            violations.append(
                f"I1: {job_id} is authorized on {sorted(agents)} at the same epoch — "
                "two agents hold the current fence"
            )

    for row in store.conn.execute(
        """SELECT job_id, COUNT(*) AS n FROM events
            WHERE kind = 'job.completed' AND job_id IS NOT NULL
            GROUP BY job_id HAVING n > 1"""
    ):
        violations.append(
            f"I1: {row['job_id']} had {row['n']} results accepted — exactly one is allowed"
        )
    return violations


def check_i3(store: Store, *, submitted: Sequence[str], deadline_reached: bool) -> list[str]:
    """Every submitted job reaches a terminal state within the run deadline.

    LIVENESS, not safety: an in-flight job legitimately has no terminal state, so
    this cannot be asserted continuously. It is a completion check with a
    deadline, run once at the end, with DEAD_LETTER and CANCELLED counting as
    terminal — a job that walked three benches and was called poison did reach a
    conclusion, and that is what the invariant is about.
    """
    if not deadline_reached:
        return []
    stuck = []
    for job_id in submitted:
        job = store.get_job(job_id)
        if job is None:
            stuck.append(f"I3: {job_id} was submitted and then vanished")
        elif job.state not in TERMINAL_JOB_STATES:
            held = store.resources_held_by(job.id)
            stuck.append(
                f"I3: {job_id} never finished — {job.state}"
                f"{f' on {job.agent_id}' if job.agent_id else ''}"
                f"{f' holding {held}' if held else ''}"
                f", {job.attempt} attempt(s), blocked_reason={job.blocked_reason!r}"
            )
    return stuck


def check_i4(store: Store, truth: Sequence[AgentTruth]) -> list[str]:
    """A job never runs on devices lacking its required capabilities — checked
    against the HARDWARE, not against TSS's copy of what the agent claimed.

    This is the check a liar defeats if you read it from the database. TSS
    matched the job against declared capabilities, so from TSS's point of view
    everything is consistent; only the bench knows what is really plugged in.

    Both halves are checked: what is running right now (from ground truth) and
    what has run (from `job_resources`, which survives the release).
    """
    violations = []
    real_caps = {
        f"{agent.agent_id}:{local_id}": caps
        for agent in truth
        for local_id, caps in agent.capabilities.items()
    }

    def mismatch(job_id: str, resource_ids: Sequence[str], when: str) -> str | None:
        job = store.get_job(job_id)
        if job is None:
            return None
        devices = [
            Resource(
                id=rid,
                agent_id=rid.split(":", 1)[0],
                capabilities=real_caps.get(rid, {}),
                state=ResourceState.FREE,
            )
            for rid in resource_ids
            if rid in real_caps
        ]
        if not resource_ids or len(devices) != len(resource_ids):
            # Nothing to compare: either the job has been handed devices we have
            # not seen it start on yet, or a bench has since left the fleet.
            return None
        if matcher.match_on_agent(job.requirements, devices) is None:
            actual = {d.id: d.capabilities for d in devices}
            return (
                f"I4: {job_id} {when} on hardware that cannot satisfy it — "
                f"needs {job.requirements}, devices really are {actual}"
            )
        return None

    for agent in truth:
        for running in agent.running:
            problem = mismatch(running.job_id, running.resource_ids, "is running")
            if problem:
                violations.append(problem)

    # Grouped by (job, epoch): each key is ONE allocation on ONE bench. Lumping a
    # job's attempts together would compare a set of devices that never existed
    # at the same time — across benches, so it would not even be co-located.
    history: dict[tuple[str, int], list[str]] = {}
    for record in store.allocation_records():
        history.setdefault((record["job_id"], record["epoch"]), []).append(record["resource_id"])
    for (job_id, epoch), resource_ids in sorted(history.items()):
        problem = mismatch(job_id, sorted(set(resource_ids)), f"ran at epoch {epoch}")
        if problem:
            violations.append(problem)
    return violations


def check_safety(store: Store, truth: Sequence[AgentTruth], scheduler=None) -> list[str]:
    """Everything that must hold at every instant. Run continuously."""
    violations = list(db_invariants.check_all(store, scheduler))
    violations.extend(check_i1(store, truth))
    violations.extend(check_i4(store, truth))
    return violations


def check_liveness(store: Store, *, submitted: Sequence[str]) -> list[str]:
    """I3, once, at the end."""
    return check_i3(store, submitted=submitted, deadline_reached=True)

"""The matcher — pure functions, no I/O (§3.4).

Nothing here touches the store, the clock, or the network. Given a job's
requirements and a pile of `Resource` objects, it says which devices could run
it and in what order they should be preferred. The scheduler decides; the store
commits; this module only knows how to compare a device to a requirement.

That boundary is what makes the interesting part unit-testable without a
database — and the interesting part is not the SQL, it is "does this device
satisfy this spec, and which of the ones that do should we use".
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from tss.core.models import (
    UNSCHEDULABLE_RESOURCE_STATES,
    CapabilitySpec,
    Resource,
    ResourceState,
)

#: A device that has never run anything sorts oldest — it is the least-recently
#: used thing on the bench, and `NULL` in `last_assigned_at` means exactly that.
NEVER_USED = float("-inf")


def satisfies(resource: Resource, spec: CapabilitySpec) -> bool:
    """Subset test (§3.4): the resource must satisfy EVERY key in the spec.

    `{"product": "vehicle_gateway"}` matches a j1939 VG; adding
    `{"harness": "obd2"}` does not. Extra capabilities on the device are fine —
    the spec is a filter, not an equality check.
    """
    return all(resource.capabilities.get(key) == value for key, value in spec.items())


def installed(resources: Sequence[Resource]) -> list[Resource]:
    """Devices the matcher will consider at all.

    `unhealthy` is present-but-broken and `retired` is gone from the bench — both
    are in UNSCHEDULABLE_RESOURCE_STATES and neither can ever be offered, however
    the fleet's load changes. Everything else is capacity, busy or not, which is
    what the feasibility filter needs to know in step 5.
    """
    return [r for r in resources if r.state not in UNSCHEDULABLE_RESOURCE_STATES]


def offerable(resources: Sequence[Resource]) -> list[Resource]:
    """...and of those, the ones that are free right now."""
    return [r for r in installed(resources) if r.state is ResourceState.FREE]


def lru_key(resource: Resource) -> float:
    return NEVER_USED if resource.last_assigned_at is None else resource.last_assigned_at


def co_located(resources: Sequence[Resource]) -> bool:
    """§1.2: every resource for one job comes from a single agent.

    ONE predicate, deliberately — not an assumption threaded through the claim,
    the reaper and the reservation logic. Co-location is policy (physics: the
    devices are cabled to each other; and failure surface), NOT the thing that
    prevents deadlock — all-or-nothing does that. If it ever needs relaxing, this
    function is the change.
    """
    return len({r.agent_id for r in resources}) <= 1


@dataclass(frozen=True)
class Match:
    """One way to satisfy a job: N devices, all on one bench."""

    agent_id: str
    resources: tuple[Resource, ...]

    @property
    def resource_ids(self) -> list[str]:
        return [r.id for r in self.resources]

    @property
    def preference(self) -> tuple[float, ...]:
        """LRU (§3.4). Smaller sorts first: the set whose devices have gone
        longest without work.

        Why not first-fit: first-fit sends every job to the same device while its
        siblings idle. That unit wears out first and — worse — if it is subtly
        broken, every job fails and you conclude the *firmware* is broken. LRU
        spreads the load, so a bad device shows up as "one resource failing"
        rather than "everything failing".
        """
        return tuple(sorted(lru_key(r) for r in self.resources))


def match_on_agent(
    requirements: Sequence[CapabilitySpec], resources: Sequence[Resource]
) -> Match | None:
    """Satisfy every requirement from one bench's free devices, or return None.

    Each spec consumes a DISTINCT resource — a job needing two vehicle gateways
    must get two devices, not the same one twice.

    WHAT THIS APPROXIMATES: **maximum bipartite matching** between requirement
    specs and free devices. The greedy walk below is exact at N=1, which is all
    the API currently accepts. At N>1 it can fail where a perfect matching
    exists, whenever two specs overlap on the same devices:

        specs   [{product: vg}, {product: vg, harness: j1939}]
        devices vg-01 {harness: j1939}   vg-02 {harness: obd2}

    Greedy hands vg-01 to the first spec (it matches, and it may sort first by
    LRU), then finds nothing for the second — and reports "no match" for a job
    that vg-01 + vg-02 could have run. That is a job left queued in front of
    hardware that could run it, which is the utilization failure the whole
    two-level model exists to avoid.

    At N <= 4 devices per job, plain backtracking is enough and Hopcroft-Karp is
    not worth the code. Step 5 turns multi-device on; this is what to reach for
    then. Leaving it greedy until then keeps the exact case exact rather than
    half-building the general one.
    """
    pool = offerable(resources)
    if not co_located(pool):
        return None
    pool.sort(key=lru_key)

    chosen: list[Resource] = []
    taken: set[str] = set()
    for spec in requirements:
        for candidate in pool:
            if candidate.id not in taken and satisfies(candidate, spec):
                chosen.append(candidate)
                taken.add(candidate.id)
                break
        else:
            return None
    return Match(agent_id=chosen[0].agent_id, resources=tuple(chosen))


def rank_matches(
    requirements: Sequence[CapabilitySpec], by_agent: Mapping[str, Sequence[Resource]]
) -> list[Match]:
    """Every bench that can satisfy the job, best first.

    A list rather than a single winner because the claim can still lose a race:
    the scheduler walks these in order until one commits (§3.4 step 5).
    """
    matches = []
    for agent_id in sorted(by_agent):
        match = match_on_agent(requirements, by_agent[agent_id])
        if match is not None:
            matches.append(match)
    return sorted(matches, key=lambda m: (m.preference, m.agent_id))

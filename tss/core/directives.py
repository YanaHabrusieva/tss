"""Directives waiting to be handed to an agent on its next heartbeat (§6).

In memory, process-local, and deliberately NOT durable — the same reasoning as
reservations in §3.4.1. A `cancel_job` directive is an optimisation: it stops a
bench wasting hardware time on a run TSS has already given up on. It is not what
makes the outcome correct. **The epoch is.** Every path that queues a cancel has
already bumped the job's epoch in the same transaction, so if this queue is lost
to a restart the worst that happens is the agent finishes a run nobody wants and
gets a 409 when it reports.

Building this durably would mean a table, a delivery-acknowledgement protocol and
a redelivery policy, to make a hint slightly more reliable than the fencing token
that already guarantees the result.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any


class DirectiveQueue:
    def __init__(self, on_push: Callable[[str], None] | None = None) -> None:
        self._pending: dict[str, list[dict[str, Any]]] = {}
        self._lock = threading.Lock()
        #: Release that bench's long-poll so it hears about this now. Without it a
        #: cancel waits out LONGPOLL_TIMEOUT — up to 8 seconds of a bench running
        #: a test nobody wants, which is exactly the waste the directive exists
        #: to prevent.
        self._on_push = on_push

    def cancel_job(self, agent_id: str, job_id: str) -> None:
        self.push(agent_id, {"cancel_job": job_id})

    def push(self, agent_id: str, directive: dict[str, Any]) -> None:
        if not agent_id:
            return
        with self._lock:
            queue = self._pending.setdefault(agent_id, [])
            if directive not in queue:  # a hint is worth sending once
                queue.append(directive)
        if self._on_push is not None:
            self._on_push(agent_id)

    def drain(self, agent_id: str) -> list[dict[str, Any]]:
        """Take everything queued for this bench. Delivery is fire-and-forget: if
        the response is lost, the epoch still fences the eventual report."""
        with self._lock:
            return self._pending.pop(agent_id, [])

    def peek(self, agent_id: str) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._pending.get(agent_id, []))

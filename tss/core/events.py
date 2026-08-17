"""In-process pub/sub (§3.6).

Reaper, scheduler and store publish; the WebSocket endpoint and the TUI
subscribe. Without it the fleet view polls `GET /v1/fleet` every second — laggy,
and at 1,000 agents genuinely expensive. With it the dashboard updates the
instant something happens, which is the difference between a demo where killing
a bench produces an immediate visible change and one where it appears on the
next tick.

THE DURABLE LOG AND THE LIVE STREAM ARE WRITTEN TOGETHER. The `events` row goes
in inside the same transaction as the state change it records; publication
happens only after that transaction commits (see `Store._commit`). A crash
between the two would otherwise leave the audit log and the live stream telling
different stories — and the audit log is what answers "why did my job move?"
after the fact, so it is the one that must not be a guess.

WHY IT IS AN ABSTRACTION over `asyncio.Queue`: at Stage 2 (§9) this becomes Redis
pub/sub or NATS, because a WebSocket client on replica A stops seeing events from
replica B the moment there is more than one replica. Keeping the interface to
`publish(event)` / `subscribe() -> AsyncIterator[Event]` means the call sites
never change.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Iterable

from tss.core.models import Event

log = logging.getLogger("tss.events")

#: Per-subscriber buffer. A subscriber that stops reading (a wedged WebSocket, a
#: paused TUI) must never block the scheduler — it loses events instead, and the
#: client recovers by re-snapshotting on reconnect.
QUEUE_SIZE = 512


class EventBus:
    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[Event]] = set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self.published = 0
        self.dropped = 0

    def bind(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Remember which loop the subscribers live on.

        Publishers are not all on it: the store is called from API handlers, from
        the reaper, and in tests from ordinary threads. Anything off-loop has to
        hand the event over with `call_soon_threadsafe` rather than touching an
        `asyncio.Queue` directly.
        """
        self._loop = loop or asyncio.get_running_loop()

    # ------------------------------------------------------------- publishing
    def publish(self, event: Event) -> None:
        if not self._subscribers:
            return
        if self._loop is not None and not self._on_loop():
            self._loop.call_soon_threadsafe(self._deliver, event)
            return
        self._deliver(event)

    def publish_all(self, events: Iterable[Event]) -> None:
        for event in events:
            self.publish(event)

    def _on_loop(self) -> bool:
        try:
            return asyncio.get_running_loop() is self._loop
        except RuntimeError:
            return False

    def _deliver(self, event: Event) -> None:
        self.published += 1
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                # Never block a scheduling pass on a slow reader.
                self.dropped += 1
                log.warning("subscriber is not keeping up; dropped %s", event.kind)

    # ------------------------------------------------------------ subscribing
    @contextlib.asynccontextmanager
    async def subscription(self) -> AsyncIterator[asyncio.Queue[Event]]:
        queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=QUEUE_SIZE)
        self._subscribers.add(queue)
        try:
            yield queue
        finally:
            self._subscribers.discard(queue)

    async def subscribe(self) -> AsyncIterator[Event]:
        """Stream events until the consumer stops reading."""
        async with self.subscription() as queue:
            while True:
                yield await queue.get()

    @property
    def subscribers(self) -> int:
        return len(self._subscribers)

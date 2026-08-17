"""`WS /v1/events` — the live stream behind `tss watch` (§3.6, §3.9).

SNAPSHOT FIRST, THEN DELTAS. A stream-only client starts blank and fills in as
things happen, which is exactly wrong for a fleet view: a bench that is quietly
running two jobs and never emits another event would simply not appear. So the
first frame is the whole fleet and the whole queue. If the connection drops the
client reconnects and re-snapshots rather than showing stale state.

WHY THE SNAPSHOT ALSO COMES BACK AFTER EVENTS. Each event says what happened,
which is what the event log needs — but reconstructing exact device states from
a stream of events means a state machine in every client, and a client whose
model drifts shows a fleet view that is confidently wrong. That is worse than
being slightly late. So state frames are re-sent, coalesced: a burst of twenty
events produces one refresh, not twenty.

This is a POC-scale decision and worth naming as one. At 1,000 benches the
snapshot gets too big to resend and you switch to real deltas — at which point
you also need the client state machine, and the sequence numbers to detect that
it has drifted. Here, correctness is free and the fleet is forty devices.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect

from tss.api.deps import get_bus, get_scheduler, get_store
from tss.core.events import EventBus
from tss.core.scheduler import Scheduler
from tss.core.store import Store

log = logging.getLogger("tss.api.ws")

router = APIRouter(tags=["events"])

StoreDep = Annotated[Store, Depends(get_store)]
BusDep = Annotated[EventBus, Depends(get_bus)]
SchedulerDep = Annotated[Scheduler, Depends(get_scheduler)]

#: Coalescing window. Long enough that a fan-out requeue of six jobs is one
#: refresh; short enough to stay under human perception (§1.3 budgets 1s for
#: dispatch, and this is meant to feel instant next to that).
REFRESH_DEBOUNCE_S = 0.08


def snapshot(store: Store, scheduler: Scheduler) -> dict:
    from tss.api.client import queue_view

    return {
        "type": "snapshot",
        "fleet": store.fleet().model_dump(mode="json"),
        "queue": queue_view(store, scheduler).model_dump(mode="json"),
    }


@router.websocket("/v1/events")
async def events(
    websocket: WebSocket, store: StoreDep, bus: BusDep, scheduler: SchedulerDep
) -> None:
    await websocket.accept()
    try:
        await websocket.send_json(snapshot(store, scheduler))
    except (WebSocketDisconnect, RuntimeError):
        return

    async with bus.subscription() as queue:
        pending_refresh = False
        try:
            while True:
                try:
                    event = await asyncio.wait_for(
                        queue.get(), timeout=REFRESH_DEBOUNCE_S if pending_refresh else None
                    )
                except TimeoutError:
                    # The burst has stopped; send one state frame for all of it.
                    await websocket.send_json(snapshot(store, scheduler))
                    pending_refresh = False
                    continue

                await websocket.send_json({"type": "event", "event": event.model_dump(mode="json")})
                pending_refresh = True
        except (WebSocketDisconnect, RuntimeError, ConnectionError):
            return
        finally:
            with contextlib.suppress(Exception):
                await websocket.close()

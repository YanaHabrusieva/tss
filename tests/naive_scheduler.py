"""The third foil: the scheduler loop that clears its wakeup flag too late.

The bug is one line out of order. A device frees mid-pass, the notify sets the
flag while the pass is still running, and the pass clears it on the way out —
erasing a wakeup for a change it never looked at. The queue then sits there with
a free device and a queued job and no reason for anyone to look again.

It is invisible: nothing errors, no exception is logged, throughput just quietly
drops. In production the 1s backstop tick hides it too, which is why the test
turns the backstop off — otherwise both versions pass and the test proves
nothing.

Nothing in `tss/` imports this.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from tss.core.scheduler import Scheduler

log = logging.getLogger("tss.scheduler.naive")


class NaiveScheduler(Scheduler):
    async def run(self) -> None:
        while True:
            with contextlib.suppress(TimeoutError, asyncio.TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=self.config.scheduler_tick_s)
            try:
                self.pass_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("scheduling pass failed; continuing")
            # ...and here the wakeup that arrived DURING the pass is thrown away.
            self._wake.clear()

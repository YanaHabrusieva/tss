"""Request-scoped access to the process-wide store, scheduler, bus and config.

Typed on `HTTPConnection` rather than `Request` deliberately: it is the base of
both `Request` and `WebSocket`, and `WS /v1/events` needs the same store and
scheduler the HTTP routes use. Asking for a `Request` works right up until the
first WebSocket route, which then fails at connect time with a 500 whose
traceback says nothing about the real cause.
"""

from __future__ import annotations

from starlette.requests import HTTPConnection

from tss.core.config import Config
from tss.core.directives import DirectiveQueue
from tss.core.events import EventBus
from tss.core.fence import SilentJobTracker
from tss.core.scheduler import Scheduler
from tss.core.store import Store


def get_store(connection: HTTPConnection) -> Store:
    return connection.app.state.store


def get_config(connection: HTTPConnection) -> Config:
    return connection.app.state.config


def get_scheduler(connection: HTTPConnection) -> Scheduler:
    return connection.app.state.scheduler


def get_directives(connection: HTTPConnection) -> DirectiveQueue:
    return connection.app.state.directives


def get_bus(connection: HTTPConnection) -> EventBus:
    return connection.app.state.bus


def get_silent_jobs(connection: HTTPConnection) -> SilentJobTracker:
    return connection.app.state.silent_jobs

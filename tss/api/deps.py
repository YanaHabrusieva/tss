"""Request-scoped access to the process-wide store and config."""

from __future__ import annotations

from fastapi import Request

from tss.core.config import Config
from tss.core.directives import DirectiveQueue
from tss.core.scheduler import Scheduler
from tss.core.store import Store


def get_store(request: Request) -> Store:
    return request.app.state.store


def get_config(request: Request) -> Config:
    return request.app.state.config


def get_scheduler(request: Request) -> Scheduler:
    return request.app.state.scheduler


def get_directives(request: Request) -> DirectiveQueue:
    return request.app.state.directives

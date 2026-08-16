"""Request-scoped access to the process-wide store and config."""

from __future__ import annotations

from fastapi import Request

from tss.core.config import Config
from tss.core.store import Store


def get_store(request: Request) -> Store:
    return request.app.state.store


def get_config(request: Request) -> Config:
    return request.app.state.config

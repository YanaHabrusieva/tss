"""The FastAPI application: wiring only.

Everything that matters happens in `core/`. This file exists to hold one store,
one config, and one reaper for the process, and to start and stop them.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from tss.api import agent, client
from tss.core.config import Config
from tss.core.reaper import Reaper
from tss.core.store import Store

log = logging.getLogger("tss")


def create_app(config: Config | None = None, store: Store | None = None) -> FastAPI:
    config = config or Config.from_env()
    store = store or Store(config.db_path, config)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store.init_schema()
        reaper = Reaper(store, config)
        app.state.reaper = reaper
        reaper.start()
        log.info("TSS up — db=%s ttl=%ss", store.path, config.presence_ttl_s)
        try:
            yield
        finally:
            await reaper.stop()
            store.close()

    app = FastAPI(title="TSS", version="0.1.0", lifespan=lifespan)
    app.state.store = store
    app.state.config = config
    app.include_router(agent.router)
    app.include_router(client.router)
    return app


#: `uvicorn tss.api.app:app` — see `just serve`.
app = create_app()

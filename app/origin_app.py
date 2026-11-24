"""Standalone local origin service (used by docker compose and manual runs).

Two instances run as two services.  ``DRILL_ORIGIN_SEED`` defaults to the same
value for both, so their ``v1`` baselines are byte-identical; the scenario
setup decides which side switches to the one-block variant ``v2``.
"""

from __future__ import annotations

import os

from fastapi import FastAPI

from .origin import OriginStore, make_origin_router

DEFAULT_LAYER_BYTES = int(os.environ.get("DRILL_LAYER_BYTES", 8 * 1024 * 1024))
DEFAULT_ORIGIN_CHUNK_BYTES = int(
    os.environ.get("DRILL_ORIGIN_CHUNK_BYTES", 64 * 1024)
)
ORIGIN_SEED = int(os.environ.get("DRILL_ORIGIN_SEED", 20261124))


def create_origin_app(
    size: int = DEFAULT_LAYER_BYTES,
    seed: int = ORIGIN_SEED,
    chunk_size: int = DEFAULT_ORIGIN_CHUNK_BYTES,
) -> FastAPI:
    app = FastAPI(title="镜像层源站", version="2.0.0")
    store = OriginStore(size=size, chunk_size=chunk_size, seed=seed)
    app.state.origin_store = store
    app.include_router(make_origin_router(store))

    @app.get("/healthz")
    async def healthz() -> dict:
        return {
            "ok": True,
            "size": store.size,
            "seed": seed,
            "version": store.current_name(),
        }

    return app


app = create_origin_app()

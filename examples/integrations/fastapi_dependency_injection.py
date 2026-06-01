"""FastAPI Dependency Injection -- KubeMQ FastStream Integrations.

Demonstrates using FastAPI's ``Depends()`` pattern to inject the
KubeMQ broker into route handlers. This decouples route logic from
global broker state and makes testing easier.

Usage:
    python examples/integrations/fastapi_dependency_injection.py

    Then in another terminal:
        curl -X POST http://localhost:8000/publish \
             -H 'Content-Type: application/json' \
             -d '{"message": "hello via DI"}'

        curl http://localhost:8000/health

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream fastapi uvicorn

Expected output:
    Starting FastAPI DI + KubeMQ app on http://localhost:8000
    [KubeMQ] Received: {'message': 'hello via DI'}
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    import uvicorn
    from fastapi import Depends, FastAPI, HTTPException
except ImportError as _err:
    raise SystemExit(
        "This example requires 'fastapi' and 'uvicorn'.\nInstall with:  pip install fastapi uvicorn"
    ) from _err

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")
CHANNEL = "example.integrations.di"

broker = KubeMQBroker(KUBEMQ_ADDRESS)
faststream_app = FastStream(broker)


# -- Dependency injection --------------------------------------------------


async def get_broker() -> KubeMQBroker:
    """FastAPI dependency that provides the KubeMQ broker instance."""
    return broker


# -- KubeMQ subscriber -----------------------------------------------------


@broker.subscriber(events=CHANNEL)
async def handle_kubemq_message(msg: dict) -> None:
    """Background subscriber processes KubeMQ messages."""
    logger.info("[KubeMQ] Received: %s", msg)
    print(f"[KubeMQ] Received: {msg}")


# -- Lifespan ---------------------------------------------------------------


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Start/stop the FastStream broker with the FastAPI app."""
    await faststream_app.start()
    logger.info("FastStream broker started")
    try:
        yield
    finally:
        await faststream_app.stop()
        logger.info("FastStream broker stopped")


web_app = FastAPI(
    lifespan=lifespan,
    title="FastAPI DI + KubeMQ",
    description="FastAPI dependency injection with KubeMQ broker",
)


# -- Routes using Depends(get_broker) --------------------------------------


@web_app.post("/publish")
async def publish_endpoint(
    payload: dict,
    b: KubeMQBroker = Depends(get_broker),  # noqa: B008
) -> dict:
    """Publish a message using an injected broker instance."""
    try:
        await b.publish(payload, events=CHANNEL)
        logger.info("Published via DI to %s", CHANNEL)
        return {"status": "published", "channel": CHANNEL}
    except Exception as exc:
        logger.error("Publish failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@web_app.get("/health")
async def health(b: KubeMQBroker = Depends(get_broker)) -> dict:  # noqa: B008
    """Health check using an injected broker instance."""
    try:
        is_healthy = await b.ping(timeout=2.0)
    except Exception:
        is_healthy = False
    return {
        "broker": "connected" if is_healthy else "disconnected",
        "address": KUBEMQ_ADDRESS,
    }


if __name__ == "__main__":
    print("Starting FastAPI DI + KubeMQ app on http://localhost:8000")
    print("POST to /publish to send a KubeMQ message (broker injected via Depends)")
    uvicorn.run(web_app, host="0.0.0.0", port=8000)

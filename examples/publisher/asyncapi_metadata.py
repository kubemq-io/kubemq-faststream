"""AsyncAPI Metadata on Publishers -- title, description, include_in_schema.

Demonstrate how to set ``title=``, ``description=``, and
``include_in_schema=`` on ``@broker.publisher()`` calls.  These
parameters control how the publisher appears in the generated
AsyncAPI documentation schema.

Usage:
    python examples/publisher/asyncapi_metadata.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    [handler] Enriched order: {'order_id': 1, 'enriched': True}
    [internal] Internal log: {'order_id': 1, 'enriched': True}
    Demo complete
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

broker = KubeMQBroker(KUBEMQ_ADDRESS)
app = FastStream(broker)

INPUT_CHANNEL = "example.publisher.asyncapi.input"
OUTPUT_CHANNEL = "example.publisher.asyncapi.output"
INTERNAL_CHANNEL = "example.publisher.asyncapi.internal"


@broker.subscriber(events=OUTPUT_CHANNEL)
async def output_handler(msg: dict) -> None:
    """Consume enriched orders from the documented output channel."""
    print(f"[handler] Enriched order: {msg}")


@broker.subscriber(events=INTERNAL_CHANNEL)
async def internal_handler(msg: dict) -> None:
    """Consume internal log messages (hidden from AsyncAPI schema)."""
    print(f"[internal] Internal log: {msg}")


# Publisher with full AsyncAPI metadata -- visible in generated docs
@broker.publisher(
    events=OUTPUT_CHANNEL,
    title="Order Enrichment Output",
    description="Publishes enriched order data after processing",
    include_in_schema=True,
)
# Publisher excluded from AsyncAPI schema -- internal use only
@broker.publisher(
    events=INTERNAL_CHANNEL,
    title="Internal Logger",
    description="Internal logging channel",
    include_in_schema=False,
)
@broker.subscriber(events=INPUT_CHANNEL)
async def enrich_order(msg: dict) -> dict:
    """Enrich the order and publish to both documented and internal channels."""
    return {**msg, "enriched": True}


@app.after_startup
async def run_demo() -> None:
    """Publish a raw order to the input channel."""
    try:
        await broker.publish(
            {"order_id": 1},
            events=INPUT_CHANNEL,
        )
    except Exception as exc:
        print(f"Failed to publish: {exc}")

    await asyncio.sleep(2)
    print("Demo complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

"""AsyncAPI Schema Configuration -- subscriber and publisher metadata.

Demonstrate ``title=``, ``description=``, and ``include_in_schema=``
on both ``@broker.subscriber()`` and ``@broker.publisher()`` calls.
These parameters control how channels appear in the auto-generated
AsyncAPI documentation schema.  Setting ``include_in_schema=False``
hides internal channels from the public API documentation.

Usage:
    python examples/config/asyncapi_schema.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    [public] Received order: {'order_id': 1, 'item': 'widget'}
    [processed] Enriched order: {'order_id': 1, 'item': 'widget', 'processed': True}
    [internal] Audit log: {'order_id': 1, 'item': 'widget', 'processed': True}
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

INPUT_CHANNEL = "example.config.asyncapi.orders"
OUTPUT_CHANNEL = "example.config.asyncapi.processed"
AUDIT_CHANNEL = "example.config.asyncapi.audit"


# Subscriber with full AsyncAPI metadata -- visible in docs
@broker.subscriber(
    events=OUTPUT_CHANNEL,
    title="Processed Orders",
    description="Receives enriched/processed order data",
    include_in_schema=True,
)
async def processed_handler(msg: dict) -> None:
    """Consume processed orders (documented in AsyncAPI schema)."""
    print(f"[processed] Enriched order: {msg}")


# Subscriber hidden from AsyncAPI schema -- internal use
@broker.subscriber(
    events=AUDIT_CHANNEL,
    title="Audit Log",
    description="Internal audit trail for compliance",
    include_in_schema=False,
)
async def audit_handler(msg: dict) -> None:
    """Consume audit log entries (hidden from AsyncAPI schema)."""
    print(f"[internal] Audit log: {msg}")


# Publisher with AsyncAPI metadata, publishing to both channels
@broker.publisher(
    events=OUTPUT_CHANNEL,
    title="Order Processing Output",
    description="Publishes enriched orders after processing",
    include_in_schema=True,
)
@broker.publisher(
    events=AUDIT_CHANNEL,
    title="Audit Publisher",
    description="Internal audit log publisher",
    include_in_schema=False,
)
@broker.subscriber(
    events=INPUT_CHANNEL,
    title="Order Intake",
    description="Receives raw orders for processing",
    include_in_schema=True,
)
async def process_order(msg: dict) -> dict:
    """Process the order, auto-publish to output and audit channels."""
    print(f"[public] Received order: {msg}")
    return {**msg, "processed": True}


@app.after_startup
async def run_demo() -> None:
    """Publish a raw order to the input channel."""
    try:
        await broker.publish(
            {"order_id": 1, "item": "widget"},
            events=INPUT_CHANNEL,
        )
    except Exception as exc:
        print(f"Failed to publish: {exc}")

    await asyncio.sleep(2)
    print("Demo complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

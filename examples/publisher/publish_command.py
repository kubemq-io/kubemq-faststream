"""Wire-level publishing with KubeMQPublishCommand.

``KubeMQPublishCommand`` gives fine-grained control over routing
metadata — pattern, headers, metadata string, correlation ID, and
custom message ID.  This is the low-level alternative to the
convenience ``broker.publish()`` method.

Usage:
    python examples/publisher/publish_command.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker, KubeMQPattern, KubeMQPublishCommand

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)

CHANNEL = "example.publisher.command"


@broker.subscriber(events=CHANNEL)
async def handle(msg: dict) -> None:
    """Receive the message published via KubeMQPublishCommand."""
    print(f"[Handler] Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Build a KubeMQPublishCommand manually and publish it."""
    cmd = KubeMQPublishCommand(
        {"action": "deploy", "version": "2.1.0"},
        destination=CHANNEL,
        pattern=KubeMQPattern.EVENTS,
        metadata="deployment-event",
        headers={"env": "staging", "region": "us-east-1"},
        correlation_id="deploy-corr-001",
        message_id="custom-msg-001",
    )

    if broker.config.producer is None:
        raise RuntimeError("Broker not connected")
    await broker.config.producer.publish(cmd)
    print("Published via KubeMQPublishCommand with custom metadata and headers")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

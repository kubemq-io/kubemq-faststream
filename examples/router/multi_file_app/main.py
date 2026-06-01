"""Multi-file application composing routers from separate modules.

Demonstrates a production-like project structure where event and queue
handlers live in dedicated modules and are composed into the main app
via ``broker.include_router()``.

Usage:
    python examples/router/multi_file_app/main.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

sys.path.insert(0, str(Path(__file__).resolve().parent))

from events_router import events_router  # noqa: E402
from queues_router import queues_router  # noqa: E402

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
broker.include_router(events_router)
broker.include_router(queues_router)
app = FastStream(broker)


@app.after_startup
async def run_demo() -> None:
    """Publish messages to channels managed by each router module."""
    await broker.publish(
        {"type": "info", "text": "System started"},
        events="example.multifile.events.notifications",
    )
    await broker.publish(
        {"severity": "high", "text": "Disk space low"},
        events="example.multifile.events.alerts",
    )
    await broker.publish(
        {"task": "generate-report", "params": {"format": "pdf"}},
        queues="example.multifile.queues.tasks",
    )
    await broker.publish(
        {"to": "user@example.com", "subject": "Welcome"},
        queues="example.multifile.queues.emails",
    )
    print("Published to events and queues via separate router modules")

    await asyncio.sleep(3)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

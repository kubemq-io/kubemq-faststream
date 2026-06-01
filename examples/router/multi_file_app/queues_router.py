"""Queues router module for the multi-file application.

Defines queue subscribers under an "example.multifile.queues." prefix.
Imported by main.py and included in the broker.

Usage:
    python examples/router/multi_file_app/main.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

from kubemq_faststream import KubeMQRouter

queues_router = KubeMQRouter(prefix="example.multifile.queues.")


@queues_router.subscriber(queues="tasks")
async def process_task(msg: dict) -> None:
    """Process queued tasks with auto-ack."""
    print(f"[Queues] Task processed: {msg}")


@queues_router.subscriber(queues="emails")
async def send_email(msg: dict) -> None:
    """Process email send requests."""
    print(f"[Queues] Email sent to: {msg.get('to', 'unknown')}")

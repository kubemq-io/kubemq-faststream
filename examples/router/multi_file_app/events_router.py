"""Events router module for the multi-file application.

Defines event subscribers under an "example.multifile.events." prefix.
Imported by main.py and included in the broker.

Usage:
    python examples/router/multi_file_app/main.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

from kubemq_faststream import KubeMQRouter

events_router = KubeMQRouter(prefix="example.multifile.events.")


@events_router.subscriber(events="notifications")
async def on_notification(msg: dict) -> None:
    """Handle notification events."""
    print(f"[Events] Notification: {msg}")


@events_router.subscriber(events="alerts")
async def on_alert(msg: dict) -> None:
    """Handle alert events."""
    print(f"[Events] Alert: {msg}")

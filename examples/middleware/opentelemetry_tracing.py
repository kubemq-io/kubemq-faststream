"""OpenTelemetry distributed tracing for KubeMQ messages.

Integrates FastStream's built-in TelemetryMiddleware to propagate
trace context through KubeMQ messages, enabling end-to-end
distributed tracing across services.

Usage:
    python examples/middleware/opentelemetry_tracing.py

Requires:
    KubeMQ broker on localhost:50000
    pip install opentelemetry-sdk opentelemetry-api
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

try:
    from faststream.opentelemetry import TelemetryMiddleware
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
except ImportError as _err:
    raise SystemExit(
        "This example requires 'opentelemetry-sdk' and 'opentelemetry-api'.\n"
        "Install with:  pip install opentelemetry-sdk opentelemetry-api"
    ) from _err

resource = Resource.create({"service.name": "kubemq-faststream-demo"})
provider = TracerProvider(resource=resource)
provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(provider)

telemetry_middleware = TelemetryMiddleware(tracer_provider=provider)

broker = KubeMQBroker(
    "kubemq://localhost:50000",
    middlewares=(telemetry_middleware,),
)
app = FastStream(broker)

CHANNEL = "example.middleware.otel"


@broker.subscriber(events=CHANNEL)
async def handle_message(msg: dict) -> None:
    """Process messages — spans are created automatically."""
    print(f"[OTel] Traced message: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish messages that carry trace context."""
    for i in range(3):
        await broker.publish({"trace_id": i, "action": "otel-demo"}, events=CHANNEL)

    await asyncio.sleep(2)
    print("Published 3 traced messages — check console for span output")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

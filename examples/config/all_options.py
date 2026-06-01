"""Comprehensive reference for every KubeMQBroker constructor parameter.

Lists all available configuration options with their default values
and descriptions, then starts a minimal app to verify the config.

Usage:
    python examples/config/all_options.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker(
    # --- Connection ---
    "kubemq://localhost:50000",  # Broker URL (kubemq:// or kubemq+tls://)
    client_id="all-options-demo",  # Client identifier (default: hostname)
    auth_token=None,  # JWT auth token (default: None)
    # --- TLS ---
    tls_enabled=False,  # Enable TLS (default: False)
    tls_cert_file=None,  # Client cert for mTLS (default: None)
    tls_key_file=None,  # Client key for mTLS (default: None)
    tls_ca_file=None,  # CA cert to verify server (default: None)
    # --- Message limits ---
    max_send_size=4_194_304,  # Max outgoing message size in bytes (4 MB)
    max_receive_size=4_194_304,  # Max incoming message size in bytes (4 MB)
    # --- Timeouts ---
    default_cq_timeout=30,  # Default command/query timeout in seconds
    graceful_timeout=15.0,  # Seconds to wait for handlers on shutdown
    # --- Keepalive ---
    keepalive_time_ms=30_000,  # gRPC keepalive ping interval (ms)
    keepalive_timeout_ms=10_000,  # gRPC keepalive ping timeout (ms)
)
app = FastStream(broker)


@broker.subscriber(events="example.config.all")
async def handle_message(msg: dict) -> None:
    print(f"Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    cfg = broker.config.broker_config
    print(f"URL:              {cfg.url}")
    print(f"Client ID:        {cfg.client_id}")
    print(f"TLS enabled:      {cfg.tls_enabled}")
    print(f"Max send size:    {cfg.max_send_size}")
    print(f"Max receive size: {cfg.max_receive_size}")
    print(f"CQ timeout:       {cfg.default_cq_timeout}s")
    print(f"Graceful timeout: {cfg.graceful_timeout}s")

    await broker.publish({"config": "loaded"}, events="example.config.all")
    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

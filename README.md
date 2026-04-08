# kubemq-faststream

KubeMQ broker adapter for the [FastStream](https://github.com/airtai/faststream) async messaging framework.

Provides native KubeMQ support for all five messaging patterns: **Events**, **Events Store**, **Queues**, **Commands**, and **Queries** -- fully integrated with FastStream's lifecycle, dependency injection, middleware, and testing infrastructure.

## Installation

```bash
uv add kubemq-faststream
```

Or with pip:

```bash
pip install kubemq-faststream
```

**Requirements**: Python 3.11+, a running [KubeMQ](https://kubemq.io/) broker.

## Quick Start

```python
import asyncio
from faststream import FastStream
from kubemq_faststream import KubeMQBroker

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)

@broker.subscriber(queues="orders")
async def handle_order(order: dict) -> None:
    print(f"Processing order: {order}")

@app.after_startup
async def publish() -> None:
    await broker.publish({"id": "ORD-001", "item": "Widget"}, queues="orders")

if __name__ == "__main__":
    asyncio.run(app.run())
```

## Configuration

### URL Formats

| Format | Description |
|---|---|
| `kubemq://host:port` | Plain gRPC connection |
| `kubemq+tls://host:port` | gRPC with TLS |
| `host:port` | Plain gRPC (bare format) |

### Constructor Options

```python
broker = KubeMQBroker(
    "kubemq://localhost:50000",
    client_id="my-service",           # Client identifier (default: hostname)
    auth_token="my-token",            # Authentication token
    tls_enabled=False,                # Enable TLS (also set via URL scheme)
    tls_cert_file="/path/cert.pem",   # Client certificate for mTLS
    tls_key_file="/path/key.pem",     # Client key for mTLS
    tls_ca_file="/path/ca.pem",       # CA certificate
    max_send_size=4_194_304,          # Max outbound message size (bytes)
    max_receive_size=4_194_304,       # Max inbound message size (bytes)
    default_cq_timeout=30,            # Default command/query timeout (seconds)
    keepalive_time_ms=30_000,         # gRPC keepalive ping interval
    keepalive_timeout_ms=10_000,      # gRPC keepalive ping timeout
    graceful_timeout=15.0,            # Graceful shutdown timeout (seconds)
)
```

### Environment Variables

All constructor parameters can be overridden by environment variables. Env vars take precedence when set.

| Environment Variable | Constructor Parameter | Default |
|---|---|---|
| `KUBEMQ_ADDRESS` | `url` | `kubemq://localhost:50000` |
| `KUBEMQ_CLIENT_ID` | `client_id` | System hostname |
| `KUBEMQ_AUTH_TOKEN` | `auth_token` | None (no auth) |
| `KUBEMQ_TLS_ENABLED` | `tls_enabled` | `false` |
| `KUBEMQ_TLS_CERT_FILE` | `tls_cert_file` | None |
| `KUBEMQ_TLS_KEY_FILE` | `tls_key_file` | None |
| `KUBEMQ_TLS_CA_FILE` | `tls_ca_file` | None |
| `KUBEMQ_MAX_SEND_SIZE` | `max_send_size` | `4194304` |
| `KUBEMQ_MAX_RECEIVE_SIZE` | `max_receive_size` | `4194304` |
| `KUBEMQ_DEFAULT_CQ_TIMEOUT` | `default_cq_timeout` | `30` |

## Messaging Patterns

### Events (Fire-and-Forget Pub/Sub)

Events are broadcast to all subscribers. No persistence, no acknowledgement.

```python
from kubemq_faststream import KubeMQBroker

broker = KubeMQBroker("kubemq://localhost:50000")

@broker.subscriber(events="notifications")
async def on_notification(msg: dict) -> None:
    print(f"Received: {msg}")

# Publish
await broker.publish({"type": "alert", "text": "Hello"}, events="notifications")
```

**Group subscribers**: Add `group="my-group"` so only one subscriber in the group receives each event (load balancing).

### Events Store (Persistent Pub/Sub with Replay)

Events Store persists messages on the broker. Subscribers can replay from any position.

```python
from kubemq_faststream import KubeMQBroker, StartPosition

broker = KubeMQBroker("kubemq://localhost:50000")

@broker.subscriber(
    events_store="audit-log",
    start_position=StartPosition.START_FROM_FIRST,
)
async def on_audit(msg: dict) -> None:
    print(f"Audit event: {msg}")
```

**Start positions**:

| StartPosition | Description |
|---|---|
| `START_FROM_NEW` | Only new messages (default) |
| `START_FROM_FIRST` | Replay from the first stored message |
| `START_FROM_LAST` | Start from the last stored message |
| `START_AT_SEQUENCE` | Start at a specific sequence number (requires `start_value`) |
| `START_AT_TIME` | Start at a Unix timestamp (requires `start_value`) |
| `START_AT_TIME_DELTA` | Start from N seconds ago (requires `start_value`) |

### Queues (Point-to-Point with Ack/Nack)

Queues provide reliable point-to-point messaging with transactional settlement.

```python
from faststream.middlewares import AckPolicy
from kubemq_faststream import KubeMQBroker

broker = KubeMQBroker("kubemq://localhost:50000")

@broker.subscriber(queues="tasks", ack_policy=AckPolicy.ACK)
async def process_task(task: dict) -> None:
    print(f"Processing: {task}")
    # Message is auto-acked on success, nacked on exception

# Publish single message
await broker.publish({"type": "email"}, queues="tasks")

# Publish batch
await broker.publish_batch(
    {"type": "email"},
    {"type": "sms"},
    {"type": "push"},
    queues="tasks",
)
```

### Commands (Fire-and-Wait RPC)

Commands implement request-reply with void response. The sender blocks until the handler confirms execution.

```python
from kubemq_faststream import KubeMQBroker

broker = KubeMQBroker("kubemq://localhost:50000")

@broker.subscriber(commands="device.restart")
async def restart_device(msg: dict) -> None:
    device_id = msg["device_id"]
    # ... perform restart ...
    print(f"Device {device_id} restarted")

# Send command and wait for completion
await broker.request(
    {"device_id": "sensor-01"},
    commands="device.restart",
    timeout=10,
)
```

### Queries (Cacheable RPC)

Queries implement request-reply with a data response. Supports server-side response caching.

```python
from kubemq_faststream import KubeMQBroker

broker = KubeMQBroker("kubemq://localhost:50000")

@broker.subscriber(queries="product.lookup")
async def lookup_product(msg: dict) -> dict:
    return {"name": "Widget", "price": 29.99}

# Send query and get response
result = await broker.request(
    {"product_id": "SKU-100"},
    queries="product.lookup",
    timeout=10,
    cache_key="product:SKU-100",  # Optional: cache the response
    cache_ttl=60,                  # Cache TTL in seconds
)
```

## AckPolicy

FastStream's `AckPolicy` controls message acknowledgement for queue subscribers:

| AckPolicy | Behavior |
|---|---|
| `AckPolicy.ACK` | Ack on success, nack (requeue) on handler error. **Default.** |
| `AckPolicy.NACK_ON_ERROR` | Nack (requeue) on handler error. |
| `AckPolicy.REJECT_ON_ERROR` | Reject on handler error (server-configured disposition). |
| `AckPolicy.ACK_FIRST` | Ack immediately before handler runs (at-most-once). |
| `AckPolicy.MANUAL` | No automatic ack -- handler must call `msg.ack()` / `msg.nack()`. |

**Note**: Events and Events Store are fire-and-forget; ack/nack operations are no-ops for these patterns.

## Router Composition

Use `KubeMQRouter` to organize handlers into modular groups with prefix propagation:

```python
from kubemq_faststream import KubeMQBroker, KubeMQRouter

# Define routers with prefixes
orders = KubeMQRouter(prefix="orders.")
inventory = KubeMQRouter(prefix="inventory.")

@orders.subscriber(events="created")
async def on_order_created(msg: dict) -> None:
    # Listens on channel "orders.created"
    print(f"New order: {msg}")

@inventory.subscriber(queues="restock")
async def on_restock(msg: dict) -> None:
    # Listens on channel "inventory.restock"
    print(f"Restock: {msg}")

# Compose into broker
broker = KubeMQBroker("kubemq://localhost:50000")
broker.include_router(orders)
broker.include_router(inventory)
```

## Publisher Decorators

Use `@broker.publisher()` to automatically publish handler return values:

```python
from kubemq_faststream import KubeMQBroker

broker = KubeMQBroker("kubemq://localhost:50000")

@broker.publisher(events="order.processed")
@broker.subscriber(queues="orders")
async def process_order(order: dict) -> dict:
    # Return value is auto-published to "order.processed"
    return {"order_id": order["id"], "status": "completed"}
```

## Testing with TestKubeMQBroker

Use `TestKubeMQBroker` for unit tests without a live broker. It provides an in-memory message router that delivers published messages to matching subscribers.

```python
import pytest
from kubemq_faststream import KubeMQBroker, TestKubeMQBroker

broker = KubeMQBroker("kubemq://localhost:50000")

@broker.subscriber(queues="tasks")
async def handle_task(msg: dict) -> None:
    assert msg["type"] == "test"

@pytest.mark.asyncio
async def test_task_handling():
    async with TestKubeMQBroker(broker) as br:
        await br.publish({"type": "test"}, queues="tasks")
```

Features of `TestKubeMQBroker`:
- No real broker connection needed
- Messages are routed in-memory to matching subscribers
- Full FastStream pipeline (parser, decoder, middleware) runs
- Supports all five messaging patterns
- `request()` works for commands and queries

## Health Check

```python
broker = KubeMQBroker("kubemq://localhost:50000")
async with broker:
    is_healthy = await broker.ping(timeout=5.0)
    print(f"Broker healthy: {is_healthy}")
```

## Examples

See the [`examples/`](examples/) directory for runnable scripts:

| Example | Description |
|---|---|
| `events_pubsub.py` | Basic event publish + subscribe |
| `events_store_replay.py` | Events store with replay from first |
| `queues_worker.py` | Queue producer + consumer with ack |
| `commands_rpc.py` | Command sender + handler |
| `queries_cache.py` | Query with server-side caching |
| `router_composition.py` | Multi-router app with prefixes |
| `full_app.py` | Complete app using all patterns |

## License

MIT

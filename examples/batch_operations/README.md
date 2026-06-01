# Batch Operations Examples

Batch publish and request operations across all KubeMQ patterns. Send multiple messages or requests in a single API call for higher throughput.

## Prerequisites

- KubeMQ broker running (default: `localhost:50000`)
- `pip install kubemq-faststream`

## Files

| File | Description |
|------|-------------|
| [events_batch.py](events_batch.py) | Batch publish multiple events via `publish_events_batch()` |
| [queues_batch_send.py](queues_batch_send.py) | Batch send multiple queue messages via `publish_batch()` |
| [queues_batch_receive.py](queues_batch_receive.py) | Batch queue subscriber with ack semantics deep-dive |
| [commands_batch.py](commands_batch.py) | Batch command requests via `request_batch()` |
| [queries_batch.py](queries_batch.py) | Batch query requests via `request_batch()` |
| [mixed_batch_pipeline.py](mixed_batch_pipeline.py) | End-to-end pipeline: events batch -> queue batch -> commands batch |

## See Also

- [events/](../events/) -- Single event publish and subscribe
- [queues/](../queues/) -- Single queue send and receive
- [commands/](../commands/) -- Single command request
- [queries/](../queries/) -- Single query request

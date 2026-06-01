# Events Examples

Fire-and-forget event pub/sub using KubeMQ FastStream. Events are delivered to all active subscribers with no persistence or acknowledgement.

## Prerequisites

- KubeMQ broker running (default: `localhost:50000`)
- `pip install kubemq-faststream`

## Files

| File | Description |
|------|-------------|
| [basic_pubsub.py](basic_pubsub.py) | Simple event publish and subscribe on one channel |
| [consumer_group.py](consumer_group.py) | Multiple subscribers with `group` for load-balanced delivery |
| [metadata_tags.py](metadata_tags.py) | Publishing events with metadata string and header tags |
| [multiple_channels.py](multiple_channels.py) | One app subscribing to multiple event channels |
| [wildcard_subscribe.py](wildcard_subscribe.py) | Wildcard channel subscriptions matching multiple channels |
| [message_id.py](message_id.py) | Custom `message_id` on published events |
| [loop_publish.py](loop_publish.py) | Publishing multiple events in a loop (non-batch) |
| [batch_publish.py](batch_publish.py) | Batch publishing via `publish_events_batch()` |

## See Also

- [events_store/](../events_store/) -- Persistent events with replay positions
- [batch_operations/](../batch_operations/) -- Batch publish across all patterns

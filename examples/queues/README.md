# Queues Examples

Queue messaging with guaranteed delivery, acknowledgement policies, dead-letter queues, TTL, delayed delivery, and batch receive.

## Prerequisites

- KubeMQ broker running (default: `localhost:50000`)
- `pip install kubemq-faststream`

## Files

| File | Description |
|------|-------------|
| [basic_send_receive.py](basic_send_receive.py) | Simplest queue send and receive with auto-ack |
| [ack_nack_reject.py](ack_nack_reject.py) | Ack strategies: `ACK`, `NACK_ON_ERROR`, `REJECT_ON_ERROR`, `MANUAL` |
| [ack_first_policy.py](ack_first_policy.py) | `ACK_FIRST` policy -- acknowledge before processing |
| [ack_all.py](ack_all.py) | Acknowledge all pending queue messages with `ack_all_queue_messages()` |
| [requeue.py](requeue.py) | Requeue a message to the same or different channel |
| [expiration_policy.py](expiration_policy.py) | Queue message TTL -- messages expire after N seconds |
| [delay_policy.py](delay_policy.py) | Delayed delivery -- messages become visible after N seconds |
| [max_receive_dlq.py](max_receive_dlq.py) | Dead-letter queue with `max_receive_count` |
| [batch_receive.py](batch_receive.py) | Batch queue subscriber with `BatchQueuesSubscriber` |
| [peek_messages.py](peek_messages.py) | Peek queue messages without consuming via `peek_queue_messages()` |

## See Also

- [batch_operations/](../batch_operations/) -- Batch send and receive across patterns
- [advanced_patterns/](../advanced_patterns/) -- DLQ processing, retry with backoff

# Advanced Patterns Examples

Production-grade messaging patterns for resilience, ordering, and complex workflows. Includes retry with backoff, dead-letter processing, saga orchestration, circuit breaker, event sourcing, and more.

## Prerequisites

- KubeMQ broker running (default: `localhost:50000`)
- `pip install kubemq-faststream`

## Files

| File | Description |
|------|-------------|
| [retry_with_backoff.py](retry_with_backoff.py) | Retry with exponential backoff using queue parameters |
| [dead_letter_processing.py](dead_letter_processing.py) | DLQ pipeline with `max_receive_count` and dead-letter consumer |
| [idempotent_consumer.py](idempotent_consumer.py) | Idempotent consumer tracking `message_id` to prevent duplicates |
| [priority_routing.py](priority_routing.py) | Priority-based routing to different queue channels |
| [fan_out_fan_in.py](fan_out_fan_in.py) | Fan-out work to multiple queues, fan-in aggregated results |
| [saga_pattern.py](saga_pattern.py) | Saga orchestrator with compensating actions on failure |
| [event_sourcing.py](event_sourcing.py) | Events Store as an append-only event log with replay |
| [circuit_breaker.py](circuit_breaker.py) | Circuit breaker state machine wrapping a queue subscriber |
| [ordered_processing.py](ordered_processing.py) | Ordered queue processing with single consumer and `max_messages=1` |
| [correlation_tracking.py](correlation_tracking.py) | Cross-pattern correlation ID tracking through a multi-step pipeline |

## See Also

- [patterns/](../patterns/) -- Foundational messaging patterns
- [queues/](../queues/) -- Queue fundamentals (ack policies, DLQ, TTL)
- [events_store/](../events_store/) -- Events Store basics for event sourcing

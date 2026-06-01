# Patterns Examples

Distributed messaging patterns built on KubeMQ's five messaging primitives. Demonstrates fan-out, scatter-gather, pipeline chains, request-reply, competing consumers, and work distribution.

## Prerequisites

- KubeMQ broker running (default: `localhost:50000`)
- `pip install kubemq-faststream`

## Files

| File | Description |
|------|-------------|
| [request_reply.py](request_reply.py) | Request-reply pattern using commands and queries side-by-side |
| [competing_consumers.py](competing_consumers.py) | Load-balanced queue processing with consumer groups |
| [fan_out.py](fan_out.py) | Broadcast vs load-balanced message distribution |
| [scatter_gather.py](scatter_gather.py) | Query multiple handlers and collect aggregated responses |
| [pipeline_chain.py](pipeline_chain.py) | Pipeline chain: event -> queue -> command across patterns |
| [work_distribution.py](work_distribution.py) | Multiple queue consumers with group-based load balancing |
| [multi_pattern_app.py](multi_pattern_app.py) | Production-like app using all 5 patterns with cross-pattern communication |

## See Also

- [advanced_patterns/](../advanced_patterns/) -- Production-grade patterns (saga, circuit breaker, event sourcing)

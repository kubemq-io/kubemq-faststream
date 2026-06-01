# Queries Examples

Query RPC (request/reply) patterns. Queries return response data and support server-side response caching.

## Prerequisites

- KubeMQ broker running (default: `localhost:50000`)
- `pip install kubemq-faststream`

## Files

| File | Description |
|------|-------------|
| [basic_query.py](basic_query.py) | Send a query and receive response data |
| [cache_ttl.py](cache_ttl.py) | Server-side response caching with `cache_key` and `cache_ttl` |
| [cache_hit_miss.py](cache_hit_miss.py) | Detecting cache hits and misses in query responses |
| [multiple_handlers.py](multiple_handlers.py) | Multiple query handlers with group-based load balancing |
| [query_timeout.py](query_timeout.py) | Query timeout when the handler is too slow |
| [no_reply_query.py](no_reply_query.py) | Fire-and-forget query with `no_reply=True` |

## See Also

- [commands/](../commands/) -- Command RPC (status-only, no response data)
- [patterns/](../patterns/) -- Scatter-gather and request-reply patterns
- [batch_operations/](../batch_operations/) -- Batch query requests

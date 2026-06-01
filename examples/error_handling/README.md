# Error Handling Examples

Error scenarios and recovery strategies for KubeMQ FastStream applications. Covers handler exceptions, connection errors, timeouts, reconnection, retries, graceful shutdown, and malformed messages.

## Prerequisites

- KubeMQ broker running (default: `localhost:50000`)
- `pip install kubemq-faststream`

## Files

| File | Description |
|------|-------------|
| [handler_exception.py](handler_exception.py) | Unhandled exception inside a subscriber handler |
| [connection_error.py](connection_error.py) | Clear error when the broker is unreachable (fail-fast) |
| [timeout.py](timeout.py) | Command/query timeout when the handler is too slow |
| [malformed_message.py](malformed_message.py) | Deserialization error on unexpected message format |
| [reconnection.py](reconnection.py) | Automatic reconnection when the broker restarts |
| [retry_policy.py](retry_policy.py) | Application-level retry with exponential backoff |
| [graceful_shutdown.py](graceful_shutdown.py) | Signal handling (SIGTERM/SIGINT) with in-flight message draining |

## See Also

- [middleware/](../middleware/) -- Observability middleware for error tracking
- [advanced_patterns/](../advanced_patterns/) -- Retry with backoff, circuit breaker

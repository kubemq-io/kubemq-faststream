# Publisher Examples

Publisher decorator patterns for automatic message routing. Use `@broker.publisher()` to auto-publish handler return values, attach headers, chain outputs, and configure AsyncAPI metadata.

## Prerequisites

- KubeMQ broker running (default: `localhost:50000`)
- `pip install kubemq-faststream`

## Files

| File | Description |
|------|-------------|
| [basic_publisher.py](basic_publisher.py) | `@broker.publisher()` decorator -- handler return value auto-published |
| [publish_command.py](publish_command.py) | Wire-level publishing with `KubeMQPublishCommand` |
| [publish_to_pattern.py](publish_to_pattern.py) | Cross-pattern publishing (events, queues, commands) from one app |
| [dynamic_routing.py](dynamic_routing.py) | Publish to different channels dynamically at runtime |
| [publisher_headers.py](publisher_headers.py) | Attach default headers dict to all published messages |
| [publisher_chain.py](publisher_chain.py) | Chain handler output to multiple publishers |
| [asyncapi_metadata.py](asyncapi_metadata.py) | AsyncAPI metadata: `title`, `description`, `include_in_schema` |

## See Also

- [router/](../router/) -- Router-based publisher registration
- [patterns/](../patterns/) -- Multi-pattern communication flows

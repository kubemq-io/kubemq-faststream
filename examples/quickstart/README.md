# Quickstart Examples

Minimal examples to get started with KubeMQ FastStream as quickly as possible. Each script is self-contained and demonstrates core concepts in under 30 lines.

## Prerequisites

- KubeMQ broker running (default: `localhost:50000`)
- `pip install kubemq-faststream`

## Files

| File | Description |
|------|-------------|
| [hello_world.py](hello_world.py) | Minimal event publish and subscribe -- the simplest possible app |
| [five_patterns.py](five_patterns.py) | Side-by-side comparison of all 5 KubeMQ patterns (events, events store, queues, commands, queries) |
| [full_app.py](full_app.py) | Single application using all 5 messaging patterns together |

## Running

```bash
python examples/quickstart/hello_world.py
```

## See Also

- [events/](../events/) -- Deep-dive into event pub/sub
- [queues/](../queues/) -- Queue messaging with ack policies
- [commands/](../commands/) -- Command RPC patterns
- [queries/](../queries/) -- Query RPC with caching

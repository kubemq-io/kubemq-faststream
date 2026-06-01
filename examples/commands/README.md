# Commands Examples

Command RPC (request/reply) patterns. Commands are one-way requests that return an executed/error status but no response data.

## Prerequisites

- KubeMQ broker running (default: `localhost:50000`)
- `pip install kubemq-faststream`

## Files

| File | Description |
|------|-------------|
| [basic_command.py](basic_command.py) | Send a command and receive the execution result |
| [timeout_handling.py](timeout_handling.py) | Command timeout when the handler is too slow |
| [multiple_handlers.py](multiple_handlers.py) | Multiple command handlers with group-based load balancing |
| [command_response.py](command_response.py) | Command response inspection -- executed vs error status |
| [no_reply_command.py](no_reply_command.py) | Fire-and-forget command with `no_reply=True` |

## See Also

- [queries/](../queries/) -- Query RPC with response data and caching
- [patterns/](../patterns/) -- Request-reply and competing consumers
- [batch_operations/](../batch_operations/) -- Batch command requests

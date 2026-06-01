# Events Store Examples

Persistent events with configurable replay start positions. Events Store retains messages so subscribers can replay from a specific point in the stream.

## Prerequisites

- KubeMQ broker running (default: `localhost:50000`)
- `pip install kubemq-faststream`

## Files

| File | Description |
|------|-------------|
| [start_from_new.py](start_from_new.py) | `START_FROM_NEW` position -- only receive events published after subscribing |
| [basic_positions.py](basic_positions.py) | `START_FROM_FIRST` and `START_FROM_LAST` start positions |
| [start_at_sequence.py](start_at_sequence.py) | Resume from a specific sequence number |
| [start_at_time.py](start_at_time.py) | Replay from a specific timestamp (`START_AT_TIME`, `START_AT_TIME_DELTA`) |
| [consumer_group_replay.py](consumer_group_replay.py) | Start positions combined with consumer groups for shared replay |

## See Also

- [events/](../events/) -- Fire-and-forget events (non-persistent)
- [advanced_patterns/](../advanced_patterns/) -- Event sourcing pattern using Events Store

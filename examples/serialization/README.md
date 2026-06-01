# Serialization Examples

Message serialization formats for KubeMQ FastStream. Demonstrates the default JSON codec, binary msgpack, protocol buffers, and custom encoder/decoder implementations.

## Prerequisites

- KubeMQ broker running (default: `localhost:50000`)
- `pip install kubemq-faststream`
- Msgpack example: `pip install msgpack`
- Protobuf example: `pip install protobuf`

## Files

| File | Description |
|------|-------------|
| [json_default.py](json_default.py) | Default JSON serialization with dicts, dataclasses, and Pydantic models |
| [msgpack_serializer.py](msgpack_serializer.py) | Msgpack serialization for compact binary messages |
| [custom_serializer.py](custom_serializer.py) | Custom encoder/decoder for application-specific formats |
| [protobuf_messages.py](protobuf_messages.py) | Protobuf serialization for strongly-typed message contracts |

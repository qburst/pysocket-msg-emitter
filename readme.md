# PySocket-msg-emitter

PySocket-msg-emitter is a Python library for emitting events and messages over different channels, with support for Redis and Kafka.

## Installation

You can install PySocket-msg-emitter using pip:

```bash
pip install PySocket-msg-emitter
```

## Motivation
The motivation behind creating MyEmitter was to provide a reliable and up-to-date solution for emitting events to Socket.IO clients in Python projects. Given the lack of maintenance and updates for the socket.io-emitter package, there was a need for a modern alternative that supports current Python versions and integrates well with popular libraries and frameworks.


## Features
Emit events to specific rooms or namespaces
Support for binary and JSON payloads
Integration with Redis for scalable event broadcasting
Easy-to-use API for seamless integration into existing projects

## Usage

### Basic Usage

```python
from PySocket-msg-emitter import Emitter

# Create an Emitter instance
emitter = Emitter()

# Add rooms
emitter.in_room("room1", "room2")

# Emit an event
emitter.emit("event_name", "Hello, world!")
```

### Configuration

You can configure the emitter with custom parameters:

```python
from PySocket-msg-emitter import Emitter

# Create an Emitter instance with custom configuration
emitter = Emitter(
    host="localhost",
    port=6379,
    namespace="/",
    key="socket.io",
    password="your_redis_password"
)
```

### Namespaces

You can specify a namespace for your emitter:

```python
from PySocket-msg-emitter import Emitter

# Create an Emitter instance with a specific namespace
emitter = Emitter(namespace="/my_namespace")
```

### Rooms

You can add rooms to emit events to:

```python
from PySocket-msg-emitter import Emitter

# Create an Emitter instance
emitter = Emitter()

# Add rooms
emitter.in_room("room1", "room2")

# Emit an event to specific rooms
emitter.emit("event_name", "Hello, world!")
```

## Supported Channels

Currently, PySocket-msg-emitter supports emitting events over Redis and Kafka channels.

## Contributing

Contributions are welcome! If you encounter any issues, have feature requests, or would like to contribute code, please open an issue or submit a pull request on the GitHub repository.

## Credits
This project was inspired by the socket.io-emitter (https://pypi.org/project/socket.io-emitter/) package, which provided a similar functionality but was last updated 6 years ago and is no longer maintained.

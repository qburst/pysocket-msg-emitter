from redisemitter import Emitter  # Assuming your Emitter class is in redisemitter.py


def main():

    emitter = Emitter(
        host="localhost",
        port=6379,
        namespace="/QB_space",
        key="socket.io",
    )

    # Emit a message to room1
    emitter.in_room("room1").emit("my_event", b"binary data to room1")

    # Emit a message to multiple rooms
    emitter.in_room(["room2", "room3", "room1"]).emit(
        "message", "Hello 1 hello", "hello world.............", "to multiple rooms"
    )

    # # Emit a message to room3
    emitter.in_room("room3").emit("my_event", b"only binary data to room3")

    # Broadcast message to all rooms
    emitter.emit(
        "broadcast_event", "Helloooooooooooooollooooooooooooooo", "hii", "to all room"
    )
    emitter.in_room("room1").emit("my_event", {"key": "value", "data": [1, 2, 3]})
    emitter.emit("broadcast_event", {"key": "value", "data": [2, 3, 4]})


if __name__ == "__main__":
    main()

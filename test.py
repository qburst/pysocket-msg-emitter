from redisemitter import Emitter  # Assuming your Emitter class is in redisemitter.py


def main():

    emitter = Emitter(
        host="localhost",
        port=6379,
        namespace=["QB_space"],
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
    #----------------------------------------------------------------------------------------#
    #                                                                                        #
    # Test for emitting messages to multiple namespaces and rooms according to the parameters#
    #                                                                                        #
    #----------------------------------------------------------------------------------------#

    # Creates Emitter connected to default namespace '/'
    emitter1 = Emitter(
        host = "localhost",
        port = 6379,
        key = "socket.io",
    )

    # creates Emitter connected with one namespace
    emitter1 = Emitter(
        host = "localhost",
        port = 6379,
        namespace = ["namespace1"],
        key = "socket.io",
    )

    # creates Emitter connected with two namespaces
    # we can provide as many namespaces
    emitter2 = Emitter(
        host = "localhost",
        port = 6379,
        namespace = ["namespace1", "namespace2"],
        key = "socket.io",
    )

    # Adding another namespace to existing Emitter
    emitter2.of_namespace('namespace3')

    # Removing a namespace from existing Emitter
    emitter2.off_namespace('namespace2')

    # Sends message to all namespaces connected
    emitter.emit('message', 'This message is send to all rooms')

    # Sends message to a specified namespace
    emitter.emit('message', 'Message to Orange', namespace = ['namespace1'])

    # Sends message to a list of specified namespaces
    emitter.emit('message', 'Message to Orange', namespace = ['namespace1', 'namespace2'])

    # Sends message to a specified rooms in all connected namespaces
    emitter.emit('message', 'Message to Orange', room = ['room1'])

    # Sends message to a specified rooms in one or more specified namespaces
    emitter.emit(
        'message',
        'Message to Orange',
        namespace = ['namespace1', 'namespace2'],
        room = ['room1']
    )


if __name__ == "__main__":
    main()


import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from pysocket_msg_emitter.emitter import Emitter


def main():
    # Initialize Emitter for Kafka
    kafka_emitter = Emitter(
        engine="kafka",
        host="localhost",
        port=9092,
        key="socket.io_emitter",
    )

    # Emit a message to room1 via Kafka
    kafka_emitter.of_namespace("/QB_space").in_room("room1").emit(
        "my_event", {"data": "Hello from Kafka to room1"}
    )

    # Emit a message to multiple rooms via Kafka
    # kafka_emitter.of_namespace("/QB_space").in_room("room2", "room3").emit(
    #     "another_event", {"data": "Message for rooms 2 and 3 from Kafka"}
    # )

    # Broadcast a message to all rooms via Kafka
    # kafka_emitter.of_namespace("/QB_space").emit(
    #     "broadcast_event", {"data": "Broadcast message to all rooms from Kafka"}
    # )

    # Initialize Unified Emitter for Redis
    redis_emitter = Emitter(
        engine="redis",
        host="localhost",
        port=6379,
        key="socket.io",
        password=None,  # Assume no password is set for Redis
    )

    # Emit a message to room1 via Redis
    redis_emitter.of_namespace("/QB_space").in_room("room1").emit(
        "my_event", {"data": "Hello from Redis to room1"}
    )
    # redis_emitter.of_namespace("/QB_space").in_room("room1").emit(
    #     "my_event", b"binary data to room1"
    # )
    # redis_emitter.of_namespace("/QB_space").in_room("room1").emit(
    #     "my_event", "only text data to room1"
    # )

    # # Emit a message to multiple rooms via Redis
    # redis_emitter.of_namespace("/QB_space").in_room("room4", "room5").emit(
    #     "another_event", {"data": "Message for rooms 4 and 5 from Redis"}
    # )

    # # Broadcast a message to all rooms via Redis
    # redis_emitter.of_namespace("/QB_space").emit(
    #     "broadcast_event", {"data": "Broadcast message to all rooms from Redis"}
    # )

    print("Messages have been emitted to both Redis and Kafka.")



if __name__ == "__main__":
    main()

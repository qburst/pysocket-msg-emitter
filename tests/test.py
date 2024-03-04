import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from pysocket_msg_emitter.emitter import Emitter


def main():

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
    kafka_emitter.of_namespace("/QB_space").in_room("room1").emit(
        "my_event", " text Hello from Kafka to room1"
    )

    # Emit a message to multiple rooms via Kafka
    kafka_emitter.of_namespace("/QB_space").in_room("room1", "room3", "room2").emit(
        "my_event", {"data": " Json Message for rooms 1 ,2 and 3 from Kafka"}
    )
    kafka_emitter.of_namespace("/QB_space").in_room("room1", "room3", "room2").emit(
        "my_event", " Text Message for rooms 1 , 2 and 3 from Kafka"
    )

    # Broadcast a message to all rooms via Kafka
    kafka_emitter.of_namespace("/QB_space").emit(
        "broadcast_event_kafka", {"data": "Broadcast message to all rooms from Kafka"}
    )

    kafka_emitter.of_namespace("/QB_space").in_room("room1").emit(
        "binary_event", b"binary data from kafka"
    )

    redis_emitter = Emitter(
        engine="redis",
        host="localhost",
        port=6379,
        key="socket.io_emitter",
        password=None,
    )

    # Emit a message to room1 via Redis
    redis_emitter.of_namespace("/QB_space").in_room("room1").emit(
        "my_event", {"data": "Hello from Redis to room1"}
    )
    redis_emitter.of_namespace("/QB_space").in_room("room1").emit(
        "binary_event", b"binary data to room1"
    )
    redis_emitter.of_namespace("/QB_space").in_room("room1").emit(
        "my_event", "only text data to room1"
    )

    redis_emitter.of_namespace("/QB_space").in_room("room1", "room2").emit(
        "my_event", " Text Message for rooms 1 and 2 from Redis"
    )
    # Emit a message to multiple rooms via Redis
    redis_emitter.of_namespace("/QB_space").in_room("room1", "room2").emit(
        "my_event", {"data": " Json Message for rooms 1 and 2 from Redis"}
    )

    # Broadcast a message to all rooms via Redis
    redis_emitter.of_namespace("/QB_space").emit(
        "broadcast_event", {"data": "Broadcast message to all rooms from Redis"}
    )

    print("Messages have been emitted to both Redis and Kafka.")

    # Using postgresql as engine of emitter
    postgresql_emitter = Emitter(
        engine = 'postgresql',
        host = 'localhost',
        port = 5432,
        key = 'socket.io_emitter',
        db_user = 'postgres',
        db_password = 'postgres',
        db_name = 'SocketMessageBroker'
    )

if __name__ == "__main__":
    main()

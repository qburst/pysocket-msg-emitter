from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit, join_room
import redis
import json
import threading
from flask_cors import CORS
from kafka import KafkaConsumer, KafkaAdminClient
import base64

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes
app.config["SECRET_KEY"] = "secret!"
socketio = SocketIO(app, cors_allowed_origins="*", logger=True, engineio_logger=True)

redis_client = redis.StrictRedis(host="localhost", port=6379, db=0)
pubsub = redis_client.pubsub()

bootstrap_servers = "localhost:9092"  # Removed duplicate variable assignment
consumer = KafkaConsumer(
    bootstrap_servers=[bootstrap_servers],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="socketio_group",  # Ensure consumers are part of the same group
    auto_offset_reset="latest",  # Start reading at the latest message
)


def messageListener():
    topics = None

    if topics is None:
        topics = fetch_topics(bootstrap_servers)
        print("topics--->", topics)

    if not topics:
        print("No topics found.")
        return

    consumer.subscribe(topics)
    for data in consumer:
        # room = ''
        print("Received message:", data.value)
        message = data.value

        if message:
            rooms = message.get("rooms", [])
            if not rooms:
                print("hi")
            if rooms:
                topic = data.topic
                room = topic.split(".")[-1]
                print("room:", room)

                try:
                    # decoded_message = json.loads(data)
                    event_type = message.get("type")
                    event = message.get("event")
                    args = message.get("args", [])
                    # namespace = message.get("namespace", "/")
                    # rooms = message.get("rooms", [])
                    print("yfuyggu", event)
                    # Process rooms if specified
                    if room:

                        if event == "my_event":

                            socketio.emit(
                                "kafka_event",
                                {"data": args},
                                room=room,
                                namespace="/QB_space",
                            )
                        elif event == "binary_event":
                            socketio.emit(
                                "kafkabinary_event",
                                {"data": args},
                                room=room,
                                namespace="/QB_space",
                            )
                    else:
                        print("USGvdsgvihdsvihfvhsdfhvbshjdfvinside", event)
                        # Broadcast to all clients in the specified namespace
                        print(f"Broadcasting {event} in namespace: /QB_space")
                        socketio.emit(event, {"data": args}, namespace="/QB_space")
                except Exception as e:
                    print(f"Error processing message from Redis: {e}")


def fetch_topics(bootstrap_servers):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topics = admin_client.list_topics()
    # print("topic list:", topics)
    return [topic for topic in topics if topic.startswith("socket.io_emitter")]


def listen_to_redis_channel():
    print("redis func--------")
    pubsub.psubscribe("socket.io_emitter#/QB_space#*")
    for message in pubsub.listen():

        if message["type"] == "pmessage":
            channel = message["channel"].decode("utf-8")
            room = ""

            if len(channel.split("#")) == 4:
                room = channel.split("#")[2]

            data = message["data"]
            # print("receiveddata", data)
            if data:

                try:
                    decoded_message = json.loads(data)
                    event_type = decoded_message.get("type")
                    event = decoded_message.get("event")
                    args = decoded_message.get("args", [])
                    namespace = decoded_message.get("namespace", "/")
                    print("chkjkhg", event)

                    # Process rooms if specified
                    if room:
                        if event == "binary_event":

                            # Handle binary event
                            binary_data = base64.b64decode(args[0]["data"]).decode(
                                "utf-8"
                            )
                            socketio.emit(
                                "binary_event",
                                {"data": binary_data},
                                room=room,
                                namespace=namespace,
                            )
                        elif event == "my_event":
                            # Handle text event
                            socketio.emit(
                                "text_event",
                                {"data": args},
                                room=room,
                                namespace=namespace,
                            )
                            # Add handling for JSON events if needed

                    # Handle broadcast events separately
                    else:
                        if event_type == "event":
                            if args and isinstance(args[0], dict):  # JSON data
                                # Emit JSON broadcast event
                                socketio.emit(
                                    "json_broadcast_event",
                                    args[0],
                                    namespace=namespace,
                                )
                            else:
                                # Emit text broadcast event
                                socketio.emit(
                                    "broadcast",
                                    {"data": args},
                                    namespace=namespace,
                                )

                        elif event_type == "binary_event":
                            if args[0].get("_is_binary"):
                                # Emit binary broadcast event
                                binary_data = base64.b64decode(args[0]["data"]).decode(
                                    "utf-8"
                                )
                                socketio.emit(
                                    "binary_broadcast_event",
                                    {"data": binary_data},
                                    namespace=namespace,
                                )

                except json.JSONDecodeError as e:
                    print(f"Error decoding message from Redis: {e}")
                except Exception as e:
                    print(f"Error processing message from Redis: {e}")


@app.route("/")
def index():
    return render_template("index.html")


@socketio.on("connect", namespace="/QB_space")
def test_connect():
    print(f"Client connected, emitting test_event, SID: {request.sid}")
    # emit("broadcast", {"data": "Broadcast: Hello from server!"}, broadcast=True)
    emit("test_event", {"data": "Hello World!...Client Connected"})


@socketio.on("disconnect", namespace="/QB_space")
def test_disconnect():
    print(f"Client disconnected, SID: {request.sid}")


@socketio.on("join", namespace="/QB_space")
def on_join(data):
    room = data["room"]
    join_room(room)
    print(f"Socket {request.sid} joined room: {room}")


@socketio.on("some_event", namespace="/QB_space")
def handle_some_event(json):
    print(f"Received 'some_event' with data: {json} from SID: {request.sid}")
    return "Acknowledged"


@socketio.on("join", namespace="/QB_space")
def on_join(data):
    room = data["room"]
    join_room(room)
    print(f"Socket {request.sid} joined room: {room}")
    emit("join_success", {"message": f"Joined room: {room}"}, room=request.sid)


if __name__ == "__main__":
    kafka_thread = threading.Thread(target=messageListener)
    kafka_thread.start()
    redis_thread = threading.Thread(target=listen_to_redis_channel)
    redis_thread.start()

    socketio.run(app, debug=False, host="0.0.0.0")

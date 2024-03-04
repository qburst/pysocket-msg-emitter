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

bootstrap_servers = "localhost:9092"
consumer = KafkaConsumer(
    bootstrap_servers=[bootstrap_servers],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="socketio_group",  # Ensure consumers are part of the same group
    auto_offset_reset="latest",  # Start reading at the latest message
)


conn = psycopg2.connect(
    dbname = "SocketMessageBroker",
    user = "postgres",
    password = "postgres",
    host = "localhost",
    port = 5432
)
cur = conn.cursor()
cur.execute("LISTEN test_channel;")
conn.commit()


def messageListener():
    topics = None

    if topics is None:
        topics = fetch_topics(bootstrap_servers)
        print("topics: ", topics)

    if not topics:
        print("No topics found.")
        return

    consumer.subscribe(topics)
    for data in consumer:
        print("Received message:", data.value)
        message = data.value

        if message:
            rooms = message.get("rooms", [])
            event = message.get("event")
            msg = message.get("msg", [])
            namespace = message.get("namespace", "/")

            try:     
                if rooms:
                    topic = data.topic
                    room = topic.split(".")[-1]
                    print("room:", room)

                    # Process rooms if specified
                    if room:
                        if event == "my_event":
                            socketio.emit(
                                "kafka_event",
                                {"data": msg},
                                room=room,
                                namespace=namespace,
                            )

                        elif event == "binary_event":
                            socketio.emit(
                                "kafkabinary_event",
                                {"data": msg},
                                room=room,
                                namespace=namespace,
                            )
        
                else:
                    if event == "broadcast_event_kafka":
                                socketio.emit(
                                    "broadcast_event_kafka",
                                    {"data": msg},
                                    namespace = namespace,
                                )
            except Exception as e:
                print(f"Error processing message from Redis: {e}")


def fetch_topics(bootstrap_servers):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topics = admin_client.list_topics()
    return [topic for topic in topics if topic.startswith("socket.io_emitter")]


def listen_to_redis_channel():
    pubsub.psubscribe("socket.io_emitter#/QB_space#*")
    for message in pubsub.listen():

        if message["type"] == "pmessage":
            channel = message["channel"].decode("utf-8")
            room = ""

            if len(channel.split("#")) == 4:
                room = channel.split("#")[2]

            data = message["data"]
            if data:

                try:
                    decoded_message = json.loads(data)
                    event_type = decoded_message.get("type")
                    event = decoded_message.get("event")
                    msg = decoded_message.get("msg", [])
                    namespace = decoded_message.get("namespace", "/")

                    # Process rooms if specified
                    if room:
                        if event == "binary_event":

                            # Handle binary event
                            binary_data = base64.b64decode(msg[0]["data"]).decode(
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
                                {"data": msg},
                                room=room,
                                namespace=namespace,
                            )

                    # Handle broadcast events separately
                    else:
                        if event_type == "event":
                            if msg and isinstance(msg[0], dict):  # JSON data
                                # Emit JSON broadcast event
                                socketio.emit(
                                    "json_broadcast_event",
                                    msg[0],
                                    namespace=namespace,
                                )
                            else:
                                # Emit text broadcast event
                                socketio.emit(
                                    "broadcast",
                                    {"data": msg},
                                    namespace=namespace,
                                )

                        elif event_type == "binary_event":
                            if msg[0].get("_is_binary"):
                                # Emit binary broadcast event
                                binary_data = base64.b64decode(msg[0]["data"]).decode(
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

def listen_db_notifications():

    while True:
        conn.poll()
        # print(conn.poll())
        if conn.notifies:
            print("-----------------------------")
            notify = conn.notifies.pop(0)
            print("Received notification on channel {}: {}".format(notify.channel, notify.payload))
            cur.execute("SELECT * FROM messages;")
            rows = cur.fetchall()
            for row in rows:
                print(row)
                namespace = row[1]
                room = row[2]
                r_text_arg = row[3]
                r_json_arg = row[4]
                try:
                    if r_json_arg.get('is_binary', False):
                        binary_data = base64.b64decode(r_json_arg["data"]).decode("utf-8")
                        if room:
                            send_room_event("binary_event", binary_data, room, namespace)
                        else:
                            send_broadcast('binary_broadcast_event',binary_data, namespace)
                    elif r_json_arg:
                        if room:
                            send_room_event("json_event", r_json_arg,room,namespace)
                        else:
                            send_broadcast('json_broadcast_event',r_json_arg,namespace)
                    else:
                        if room:
                            send_room_event('text_event', r_text_arg,room,namespace)
                        else:
                            send_broadcast('broadcast',r_text_arg,namespace)

                    cur.execute("DELETE FROM messages where id="+str(row[0])+";")
                    conn.commit()
                except:
                    pass

            cur.close()

@app.route("/")
def index():
    return render_template("index.html")


@socketio.on("connect", namespace="/QB_space")
def test_connect():
    print(f"Client connected, emitting test_event, SID: {request.sid}")
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
    postgresql_thread = threading.Thread(target=listen_db_notifications)
    postgresql_thread.start()

    socketio.run(app, debug=False, host="0.0.0.0")

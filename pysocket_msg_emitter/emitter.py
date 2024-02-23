import json
import base64
import redis
from kafka import KafkaProducer


class Emitter:
    def __init__(
        self,
        engine,
        host=None,
        port=None,
        namespace=[],
        key="socket.io_emitter",
        password=None,
    ):
        self.engine = engine
        self.host = host
        self.port = port
        self.key = key
        self.rooms = []
        # self.namespace = "/"
        # if namespace:
        #     self.namespace = self._flatten_namespaces(namespace)
        # else:
        self.namespace = []

        self.password = password

        if self.engine == "kafka":
            self._init_kafka()
        elif self.engine == "redis":
            self._init_redis()
        else:
            raise ValueError("Please use 'redis' or 'kafka'.")

    def _init_kafka(self):
        if self.host is None:
            self.host = "localhost"
        if self.port is None:
            self.port = "9092"

        bootstrap_server = f"{self.host}:{self.port}"

        print("bootstrap: ", bootstrap_server)

        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda m: json.dumps(m).encode("utf-8"),
        )

    def _init_redis(self):
        if self.host is None:
            self.host = "localhost"
        if self.port is None:
            self.port = 6379

        self.redis_client = redis.StrictRedis(
            host=self.host, port=self.port, password=self.password
        )

    def in_room(self, *rooms):
        """Add rooms to the list of rooms to emit to."""
        self.rooms.extend(rooms)
        return self

    def of_namespace(self, *namespaces):
        """Includes the namespace to the namespace list"""
        self.namespace.extend(namespaces)
        return self

    def off_namespace(self, namespace):
        """Removes namespace given from the namespace list"""
        try:
            self.namespace.remove("/" + namespace)
        finally:
            return self

    def emit(self, event, *args, **kwargs):
        """Emit an event with optional arguments to the specified rooms."""
        if self.engine == "kafka":
            self._emit_kafka(event, *args, **kwargs)
        elif self.engine == "redis":
            self._emit_redis(event, *args, **kwargs)

    def _emit_kafka(self, event, *args, **kwargs):
        if not self.namespace:
            self.namespace = ["/"]
        namespaces = list(set(self._flatten_list(self.namespace)))
        print("namespaces:", namespaces)
        for __namespace in namespaces:
            message, rooms = self._create_message(event, __namespace, *args, **kwargs)
            # for __namespace in namespaces:
            if __namespace.startswith("/"):
                __namespace = __namespace.split("/")[-1]
                # print("np without / :", _namespace)
            topic = f"{self.key}.{__namespace}"
            if rooms:
                for room in rooms:
                    room_topic = f"{topic}.{room}"
                    print("topic:", room_topic)
                    # print(message)
                    self.producer.send(room_topic, message)
                    self.producer.flush()
            else:
                self.producer.send(topic, message)
                print(message)
                self.producer.flush()
        self.rooms = []

    def _emit_redis(self, event, *args, **kwargs):
        if not self.namespace:
            self.namespace = ["/"]
        namespaces = list(set(self._flatten_list(self.namespace)))
        print("namespaces:", namespaces)
        for __namespace in namespaces:
            message, rooms = self._create_message(event, __namespace, *args, **kwargs)
            # for __namespace in namespaces:
            channel = f"{self.key}#{__namespace}#"
            if rooms:
                for room in rooms:
                    room_channel = f"{channel}{room}#"
                    print("channel:", room_channel)
                    self.redis_client.publish(room_channel, json.dumps(message))
            else:
                self.redis_client.publish(channel, json.dumps(message))
        self.rooms = []

    def _create_message(self, event, __namespace, *args, **kwargs):
        # flattened_rooms = list(set(self._flatten_list(self.rooms)))
        # Initialize the message with potential text or base64-encoded binary data

        # try:
        #     if kwargs['namespace']:
        #         namespaces = self._flatten_namespaces(kwargs['namespace'])
        # except:
        #     namespaces = self.namespace
        #     print(namespaces)

        try:
            if kwargs["room"]:
                rooms = kwargs["room"]
        except:
            rooms = list(set(self._flatten_list(self.rooms)))

        # for __namespace in namespaces:
        #     message = {
        #         "type": "event",  # Default type, might be overridden by binary or json
        #         "event": event,
        #         "args": [],
        #         "rooms": rooms,
        #         "namespace": __namespace
        #     }

        message = {
            "type": "event",
            "event": event,
            "args": [],
            "rooms": rooms,
            "namespace": __namespace,
        }
        # print(message)

        for arg in args:
            if isinstance(arg, bytes):
                # Base64-encode binary data and indicate that it's binary
                encoded_binary = base64.b64encode(arg).decode("utf-8")
                message["args"].append({"_is_binary": True, "data": encoded_binary})
            elif isinstance(arg, dict) or isinstance(arg, list):
                # Directly append JSON-serializable structures
                message["args"].append(arg)
            else:
                # Convert everything else to a string to ensure JSON serialization
                message["args"].append(str(arg))

        return message, rooms

    def _flatten_list(self, room_list):
        """Flatten a list of rooms."""
        flattened_rooms = []
        for item in room_list:
            if isinstance(item, list):
                flattened_rooms.extend(item)
            else:
                flattened_rooms.append(item)
        return flattened_rooms

    def _flatten_namespaces(self, namespaces):
        """Makes given string list to namespaces list"""
        return ["/" + np for np in namespaces]

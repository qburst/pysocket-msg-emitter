import json
import base64


class Emitter:
    def __init__(
        self,
        engine,
        host=None,
        port=None,
        key="socket.io_emitter",
        password=None,
    ):
        self.engine = engine
        self.host = host
        self.port = port
        self.key = key
        self.rooms = []
        self.namespace = []
        self.password = password

        if self.engine == "kafka":
            from kafka import KafkaProducer
            self._init_kafka(KafkaProducer)
        elif self.engine == "redis":
            import redis
            self._init_redis(redis.StrictRedis)
        else:
            raise ValueError("Please use 'redis' or 'kafka'.")

    def _init_kafka(self, KafkaProducer):
        if self.host is None:
            self.host = "localhost"
        if self.port is None:
            self.port = "9092"

        bootstrap_server = f"{self.host}:{self.port}"

        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda m: json.dumps(m).encode("utf-8"),
        )

    def _init_redis(self, StrictRedis):
        if self.host is None:
            self.host = "localhost"
        if self.port is None:
            self.port = 6379

        self.redis_client = StrictRedis(
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

    def emit(self, event, msg):
        """Emit an event with optional arguments to the specified rooms."""
        if not self.namespace:
            self.namespace = ["/"]
        namespaces_lst = list(set(self._flatten_list(self.namespace)))
        namespaces = self._flatten_namespaces(namespaces_lst)

        if self.engine == "kafka":
            self._emit_kafka(event, msg, namespaces)
        elif self.engine == "redis":
            self._emit_redis(event, msg, namespaces)

    def _emit_kafka(self, event, msg, namespaces):
        for namespace in namespaces:
            message, rooms = self._create_message(event, namespace, msg)
            if namespace.startswith("/"):
                namespace = namespace.split("/")[-1]
            topic = f"{self.key}.{namespace}"
            if rooms:
                for room in rooms:
                    room_topic = f"{topic}.{room}"
                    print("topic:", room_topic)
                    self.producer.send(room_topic, message)
                    self.producer.flush()
            else:
                self.producer.send(topic, message)
                self.producer.flush()
        self.rooms, self.namespace = []

    def _emit_redis(self, event, msg, namespaces):
        for namespace in namespaces:
            message, rooms = self._create_message(event, namespace, msg)
            channel = f"{self.key}#{namespace}#"
            if rooms:
                for room in rooms:
                    room_channel = f"{channel}{room}#"
                    print("channel:", room_channel)
                    self.redis_client.publish(room_channel, json.dumps(message))
            else:
                self.redis_client.publish(channel, json.dumps(message))
        self.rooms, self.namespace = []

    def _create_message(self, event, namespace, msg):
        rooms = list(set(self._flatten_list(self.rooms)))

        message = {
            "type": "event",
            "event": event,
            "msg": [],
            "rooms": rooms,
            "namespace": namespace,
        }

        if msg:
            if isinstance(msg, bytes):
                # Base64-encode binary data and indicate that it's binary
                encoded_binary = base64.b64encode(msg).decode("utf-8")
                message["msg"].append({"_is_binary": True, "data": encoded_binary})
            elif isinstance(msg, dict) or isinstance(msg, list):
                # Directly append JSON-serializable structures
                message["msg"].append(msg)
            else:
                # Convert everything else to a string to ensure JSON serialization
                message["msg"].append(str(msg))

        return message, rooms

    def _flatten_list(self, list_items):
        """Flatten the list of items."""
        flattened_list = []
        for item in list_items:
            if isinstance(item, list):
                flattened_list.extend(item)
            else:
                flattened_list.append(item)
        return flattened_list

    def _flatten_namespaces(self, namespaces):
        """Makes given string list to namespaces list"""
        return ["/" + ns if not ns.startswith("/") else ns for ns in namespaces]

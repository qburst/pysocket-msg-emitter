import json
import redis
import base64


class Emitter:
    def __init__(
            self,
            host="localhost",
            port=6379,
            namespace=[],
            key="socket.io",
            password=None
    ):

        self.host = host
        self.port = port

        # Receives namespace to be connected with, if not provided then uses default namespace
        if namespace:
            self.namespace = self._flatten_namespaces(namespace)
        else:
            self.namespace = ['/']
        self.password = password
        self.key = key
        self.rooms = []
        self.redis_client = self._create_client()

    def in_room(self, *rooms):
        """Add rooms to the list of rooms to emit to."""
        self.rooms.extend(rooms)
        return self

    def of_namespace(self, namespace):
        """Includes the namespace to the namespace list"""
        self.namespace.append("/"+namespace)
        return self

    def off_namespace(self, namespace):
        """Removes namespace given from the namespace list"""
        try:
            self.namespace.remove("/"+namespace)
        finally:
            return self

    def emit(self, event, *args, **kwargs):
        """Emit an event with optional arguments to the specified rooms."""
        # Prepare the message with initial values
        try:
            if kwargs['namespace']:
                namespaces = self._flatten_namespaces(kwargs['namespace'])
        except:
            namespaces = self.namespace
            print(namespaces)
        try:
            if kwargs['room']:
                rooms = kwargs['room']
        except:
            rooms = list(set(self._flatten_rooms(self.rooms)))

        for __namespace in namespaces:
            message = {
                "type": "event",  # Default type, might be overridden by binary or json
                "event": event,
                "args": [],
                "rooms": rooms,
                "namespace": __namespace
            }

            # Process each argument
            for arg in args:
                if isinstance(arg, (bytes, bytearray)):
                    # Handle binary data
                    message["type"] = "binary_event"
                    encoded_arg = base64.b64encode(arg).decode("utf-8")
                    message["args"].append({"_is_binary": True, "data": encoded_arg})
                elif isinstance(arg, dict):
                    # Handle JSON data
                    message["args"].append(json.dumps(arg))  # Serialize JSON data
                else:
                    # Handle text or other types as is
                    message["args"].append(arg)

            # Determine the channel based on whether specific rooms are targeted
            channel = f"{self.key}#{__namespace}#"
            if rooms:
                for room in rooms:
                    room_channel = f"{channel}{room}#"
                    self.redis_client.publish(room_channel, json.dumps(message))
            else:
                self.redis_client.publish(channel, json.dumps(message))

            # Clear the rooms after emitting the message
            self.rooms.clear()

    def _create_client(self):
        """Create a Redis client."""
        return redis.StrictRedis(host = self.host, port = self.port, password = self.password)

    def _flatten_rooms(self, room_list):
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
        return ["/"+np for np in namespaces]

    def _has_bin(self, *args):
        """Check if any argument is binary data."""

        for arg in args:
            if isinstance(arg, (bytes, bytearray)):
                return True
        return False

    def _encode_args(self, args):
        """Encode binary data to base64."""
        encoded_args = []
        for arg in args:
            if isinstance(arg, (bytes, bytearray)):
                encoded_arg = base64.b64encode(arg).decode("utf-8")
                encoded_args.append({"_is_binary": True, "data": encoded_arg})
            else:
                encoded_args.append(arg)
        return encoded_args

import json
import base64


class Emitter:
    def __init__(
        self,
        engine,
        host=None,
        port=None,
        key="socket.io_emitter",
        db_user = None,
        db_password = None,
        password=None,
        db_name = None,
    ):
        self.engine = engine
        self.host = host
        self.port = port
        self.key = key
        self.rooms = []
        self.namespace = []
        self.password = password
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name


        if self.engine == "kafka":
            from kafka import KafkaProducer
            self._init_kafka(KafkaProducer)
        elif self.engine == "redis":
            import redis
            self._init_redis(redis.StrictRedis)
        elif self.engine == "postgresql":
            import psycopg2
            self._init_postgresql(psycopg2)
        else:
            raise ValueError("Please use 'redis', 'kafka' or postgresql.")

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

    def _init_postgresql(self, psycopg2):
        # Define SQL command to create the table for the message queue
        create_table_sql = """
                    CREATE TABLE IF NOT EXISTS messages (
                        id SERIAL PRIMARY KEY,
                        r_namespace TEXT NULL,
                        r_room TEXT NULL,
                        r_str_arg TEXT NULL,
                        r_json_arg JSONB NULL,
                        r_type TEXT NULL,
                        r_event TEXT NULL, 
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """

        # Execute SQL command to create the table
        try:
            cursor = self.connect_to_db().cursor()
            cursor.execute(create_table_sql)
            self.conn.commit()
            print("Table 'messages' created successfully.")
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error creating table:", error)

    def connect_to_db(self):
        import psycopg2
        # Connect to PostgreSQL
        self.conn = psycopg2.connect(
            dbname = self.db_name,
            user = self.db_user,
            password = self.db_password,
            host = self.host,
            port = self.port
        )
        return self.conn

    def close_db(self):
        self.conn.close()

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
        elif self.engine == "postgresql":
            self._emit_postgresql(event, msg, namespaces)

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

    def _emit_postgresql(self,event, msg, namespaces):
        from psycopg2._json import Json
        print("namespaces:", namespaces)
        for __namespace in namespaces:
            r_type = "event"  # Default type, might be overridden by binary or json
            r_str_args = ''
            r_json_args = {}
            rooms = list(set(self._flatten_list(self.rooms)))
            if isinstance(msg, (bytes, bytearray)):
                # Handle binary data
                r_type = "binary_event"
                encoded_arg = base64.b64encode(msg).decode("utf-8")
                r_json_args = {"_is_binary": True, "data": encoded_arg}
            elif isinstance(msg, dict):
                # Handle JSON data
                r_json_args = msg  # Serialize JSON data
            else:
                # Handle text or other types as is
                r_str_args = msg
            cursor = self.connect_to_db().cursor()
            if rooms:
                for room in rooms:
                    try:
                        table_insert_query = ("INSERT INTO public.messages "
                                              "(r_namespace, r_room, r_str_arg, r_json_arg, r_type, r_event, created_at) "
                                              "VALUES('")+__namespace+"', '"+room+"', '"+str(r_str_args)+"', %s ,'"+str(
                            r_type)+"', '"+event+"',CURRENT_TIMESTAMP);"

                        cursor.execute(table_insert_query, [Json(r_json_args)])
                    except Exception as e:
                        print("Error inserting row:", e)

            else:
                table_insert_query = ("INSERT INTO public.messages "
                                      "(r_namespace, r_room, r_str_arg, r_json_arg, r_type, r_event, created_at) "
                                      "VALUES('")+__namespace+"', null, '"+str(r_str_args)+"', %s ,'"+str(
                    r_type)+"', '"+event+"',CURRENT_TIMESTAMP);"

                try:

                    cursor.execute(table_insert_query, [Json(r_json_args)])
                except Exception as e:
                    print("Error inserting row:", e)
            try:
                from psycopg2 import sql
                cursor.execute(sql.SQL("NOTIFY test_channel, 'pinged'"))
                self.conn.commit()
            except Exception as e:
                print("Error sending notification:", e)
            # Clear the rooms after emitting the message
            self.rooms.clear()


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

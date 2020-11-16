"""Utils file.

This file is to house code common between the Master and the Worker

"""

import json
import socket


def send_message(msg, host, port):
    """Send msg over TCP to host:port."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    message = json.dumps(msg, indent=None)
    sock.sendall(message.encode('utf-8'))
    sock.close()


def check_schema(schema, value):
    """Verify schema format."""
    if isinstance(schema, dict):
        if not isinstance(value, dict):
            return False
        for key in schema:
            if key not in value or not check_schema(schema[key], value[key]):
                return False
    elif isinstance(schema, list):
        if not isinstance(value, list) or len(value) != len(schema):
            return False
        for i, schema_member in enumerate(schema):
            if not check_schema(schema_member, value[i]):
                return False
    else:
        return isinstance(value, schema)
    return True


def create_tcp_socket(host, port):
    """Create a TCP socket listening on port."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen()

    # Socket accept() and recv() will block for a maximum of 1 second
    sock.settimeout(1)

    return sock


def receive_json(clientsocket):
    """Receive a JSON object from clientsocket."""
    message_chunks = []
    while True:
        try:
            data = clientsocket.recv(4096)
        except socket.timeout:
            continue
        if not data:
            break
        message_chunks.append(data)
    clientsocket.close()

    # Parse message chunks into JSON data
    message_bytes = b''.join(message_chunks)
    message_str = message_bytes.decode("utf-8")
    try:
        msg = json.loads(message_str)
    except json.JSONDecodeError:
        return None

    return msg


def partition_input(items, num_partitions):
    """
    Partition the list items into num_partitions partitions.

    Note: Empty partitions may be returned if num_partitions > len(items).
    In this case, it may be desirable to ignore the empty partitions.
    """
    assert num_partitions != 0

    partition = [[]*1 for i in range(num_partitions)]

    i = 0
    for item in items:
        partition[i].append(item)
        i = (i + 1) % num_partitions

    return partition

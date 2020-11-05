"""Utils file.

This file is to house code common between the Master and the Worker

"""

import json
import socket


def send_message(msg, host, port):
    """Sends msg over TCP to host:port."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    message = json.dumps(msg)
    sock.sendall(message.encode('utf-8'))
    sock.close()


def check_schema(schema, value):
    if isinstance(schema, dict):
        if not isinstance(value, dict):
            return False
        for key in schema:
            if not key in value:
                return False
            if not check_schema(schema[key], value[key]):
                return False
        return True
    elif isinstance(schema, list):
        if not isinstance(value, list):
            return False
        if len(value) != len(schema):
            return False
        for i in range(len(schema)):
            if not check_schema(schema[i], value[i]):
                return False
        return True
    else:
        return isinstance(value, schema)


def round_robin(pigeons, holes):
    assert(len(holes) != 0)
    results = []
    for i in range(len(holes)):
        results.append({
            "group": holes[i],
            "assigned": pigeons[i::len(holes)],
        })
    return results

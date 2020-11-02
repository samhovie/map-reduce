import os
import logging
import json
import time
import click
import mapreduce.utils
from pathlib import Path 
import shutil
import threading


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Master:
    def __init__(self, port):
        logging.info("Starting master:%s", port)
        logging.info("Master:%s PWD %s", port, os.getcwd())

        # Create clean folder to store server results 
        p = Path('tmp')
        p.mkdir(exist_ok = True)
        for path in p.glob("job-*"):
            shutil.rmtree(path)
        
        # listen for worker hb

        # Listen for messages on master's port
        listen(port)


def listen(port):
    """Wait on a message from a socket OR a shutdown signal."""

    # Create socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("localhost", port))
    sock.listen()

    # Socket accept() and recv() will block for a maximum of 1 second
    sock.settimeout(1)

    # Accept connections for messages until shutdown message
    shutdown = False 
    while not shutdown:
        # Listen for a connection for 1s.
        try:
            clientsocket, address = sock.accept()
        except socket.timeout:
            continue
        print("Connection from", address[0])

        # Receive data chunks
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
        except JSONDecodeError:
            continue

        # Handle message depending on type
        if msg["message_type"] == "shutdown":
            shutdown = True


@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    Master(port)


if __name__ == '__main__':
    main()



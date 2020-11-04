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
    # Whether or not shutdown message has been received
    shutdown = False
    # Dictionary of registered workers, key is pid and value is dict of worker info
    # TODO: make this an ordered dict
    workers = {
        # [pid]: {
        #   host: 
        #   port:
        #   status:
        #   last_hb_received:
        # },
    }

    def __init__(self, port):
        logging.info("Starting master:%s", port)
        logging.info("Master:%s PWD %s", port, os.getcwd())

        # Initialize port class variable
        self.port = port

        # Create clean folder to store server results 
        p = Path('tmp')
        p.mkdir(exist_ok = True)
        for path in p.glob("job-*"):
            shutil.rmtree(path)
        
        # Listen for worker hb's
        hb_thread = threading.Thread(target=self.heartbeat)
        hb_thread.start()

        # Listen for messages on master's port
        listen_thread = threading.Thread(target=self.listen)
        listen_thread.start()


    def listen():
        """Wait on a message from a socket OR a shutdown signal."""

        # Create socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", self.port))
        sock.listen()

        # Socket accept() and recv() will block for a maximum of 1 second
        sock.settimeout(1)

        # Accept connections for messages until shutdown message
        
        while not self.shutdown:
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
                self.shutdown = True


    def heartbeat():
        """Manage heartbeats for registered workers."""

        # Create socket on master port - 1
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", self.port - 1))
        sock.listen()

        # Socket accept() and recv() will block for a maximum of 1 second
        sock.settimeout(1)

        while not self.shutdown:
            # Check if any workers are dead
            for pid, worker_info in workers:
                if time.time() - worker_info["last_hb_received"] > 10:
                    worker_info["status"] = "dead"

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
            if msg["message_type"] == "heartbeat" and msg["worker_pid"] in workers:
                workers[msg["worker_pid"]]["last_hb_received"] = time.time()

@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    Master(port)


if __name__ == '__main__':
    main()



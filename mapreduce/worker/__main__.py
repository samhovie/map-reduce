import os
import logging
import json
import time
import click
import mapreduce.utils
from pathlib import Path
import threading
import socket


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Worker:
    def __init__(self, master_port, worker_port):
        logging.debug("Starting worker:%s", worker_port)
        logging.debug("Worker:%s PWD %s", worker_port, os.getcwd())

        # Set class variables
        self.port = worker_port
        self.master_port = master_port
        self.shutdown = False 

        # Listen for messages on worker's port
        listen_thread = threading.Thread(target=self.listen)
        listen_thread.start()

        # Initiate hb before registering - storing Timer in case we need to cancel is on shutdown
        self.heartbeat = threading.Timer(2, self.send_heartbeat)

        # Connect to the server
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", self.master_port))

        # Send register message to master
        message = json.dumps({
            "message_type" : "register",
            "worker_host" : "localhost",
            "worker_port" : self.port,
            "worker_pid" : os.getpid(),
        })
        sock.sendall(message.encode('utf-8'))
        sock.close()

        listen_thread.join()
        self.heartbeat.cancel()


    def listen(self):
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
            # Listen for a connection for 1s
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
            except json.JSONDecodeError:
                continue

            if "message_type" not in msg:
                continue

            # Handle message depending on type
            if msg["message_type"] == "shutdown":
                logging.debug("Worker: Shutting down")
                self.shutdown = True

            elif msg["message_type"] == "register_ack":
                self.heartbeat.start()

            elif msg["message_type"] == "new_worker_job":
                self.run_executable(
                    msg["executable"],
                    [Path(file) for file in msg["input_files"]],
                    Path(msg["output_directory"])
                )

    def send_heartbeat(self):
        # Open connection to master port - 1
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("localhost", self.master_port - 1))

        message = json.dumps({
            "message_type" : "heartbeat",
            "worker_pid" : os.getpid(),
        })
        sock.sendall(message.encode('utf-8'))
        sock.close()

    def run_executable(self, exec, input_files, output_dir):
        output_files = []
        for file in input_files:
            output_file = output_dir/file.name
            with input_file_opened as file.open("r"),
                output_file_opened as output_file.open("w"):
                subprocess.run(str(exec), stdin=input_file_opened, stdout=output_file_opened)
            output_files.append(output_file)

        mapreduce.utils.send_message({
            "message_type": "status",
            "output_files": [str(file) for file in output_files],
            "status": "finished",
            "worker_pid": os.getpid(),
        }, "localhost", self.master_port)
        

@click.command()
@click.argument("master_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(master_port, worker_port):
    Worker(master_port, worker_port)


if __name__ == '__main__':
    main()

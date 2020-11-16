"""Map-reduce worker logic."""
import os
import logging
import json
import time
from pathlib import Path
import threading
import subprocess
import socket
import click
import mapreduce.utils


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Worker:
    """Handles performing tasks for Master."""

    def __init__(self, master_port, worker_port):
        """Create a Worker."""
        logging.debug("Starting worker:%s", worker_port)
        logging.debug("Worker:%s PWD %s", worker_port, os.getcwd())

        # Set class variables
        self.port = worker_port
        self.master_port = master_port
        self.shutdown = False

        # Initiate hb before registering
        self.heartbeat = threading.Thread(target=self.heartbeats_timer)

        # Listen for messages on worker's port
        listen_thread = threading.Thread(target=self.listen)
        listen_thread.start()

        listen_thread.join()
        if self.heartbeat.is_alive():
            self.heartbeat.join()

    def listen(self):
        """Wait on a message from a socket OR a shutdown signal."""
        # Create socket
        sock = mapreduce.utils.create_tcp_socket("localhost", self.port)

        # Send register message to master
        msg = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": self.port,
            "worker_pid": os.getpid(),
        }
        mapreduce.utils.send_message(msg, "localhost", self.master_port)

        # Accept connections for messages until shutdown message
        while not self.shutdown:
            # Listen for a connection for 1s
            try:
                clientsocket, address = sock.accept()
            except socket.timeout:
                continue
            logging.debug("Connection from %s", address[0])
            msg = mapreduce.utils.receive_json(clientsocket)
            if msg is None or "message_type" not in msg:
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
            elif msg["message_type"] == "new_sort_job":
                self.sort_files(
                    [Path(file) for file in msg["input_files"]],
                    Path(msg["output_file"])
                )

    def heartbeats_timer(self):
        """Send a heartbeat every 2 seconds until shutdown."""
        while not self.shutdown:
            self.send_heartbeat()
            time.sleep(2)

    def send_heartbeat(self):
        """Send a single heartbeat to the master."""
        # Open connection to master port - 1

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("localhost", self.master_port - 1))

        message = json.dumps({
            "message_type": "heartbeat",
            "worker_pid": os.getpid(),
        }, indent=None)
        sock.sendall(message.encode('utf-8'))
        sock.close()

    def run_executable(self, exec_file, input_files, output_dir):
        """Run exec on input_files, putting output in output_dir."""
        logging.info("Worker: Running executable: %s", exec_file)
        output_files = []
        for file in input_files:
            out_file = output_dir/file.name
            with file.open("r") as in_opened, out_file.open("w") as out_opened:
                subprocess.run(
                    str(exec_file),
                    stdin=in_opened,
                    stdout=out_opened,
                    check=False
                )
            output_files.append(out_file)

        mapreduce.utils.send_message({
            "message_type": "status",
            "output_files": [str(file) for file in output_files],
            "status": "finished",
            "worker_pid": os.getpid(),
        }, "localhost", self.master_port)

    def sort_files(self, input_files, output_file):
        """Sort all the lines in input_files and put them in output_file."""
        logging.debug(
            "Worker: Sorting files %s into %s",
            str(input_files),
            str(output_file)
        )
        lines = []
        for file in input_files:
            with file.open("r") as open_file:
                for line in open_file:
                    lines.append(line)
        lines.sort()
        with output_file.open("w") as open_output_file:
            open_output_file.writelines(lines)

        mapreduce.utils.send_message(
            {
                "message_type": "status",
                "output_file": str(output_file),
                "status": "finished",
                "worker_pid": os.getpid(),
            },
            "localhost",
            self.master_port
        )


@click.command()
@click.argument("master_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(master_port, worker_port):
    """Run the Worker."""
    Worker(master_port, worker_port)


if __name__ == '__main__':
    main()

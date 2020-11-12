import os
import logging
import json
import socket
import time
import click
from mapreduce.master.job import Job
import mapreduce.utils
from pathlib import Path 
from collections import OrderedDict
from queue import Empty, Queue
import shutil
import threading


# Configure logging
logging.basicConfig(level=logging.DEBUG)

class Master:
    def __init__(self, port):
        logging.debug("Starting master:%s", port)
        logging.debug("Master:%s PWD %s", port, os.getcwd())

        # Initialize port class variable
        self.port = port
        
        # Whether or not shutdown message has been received
        self.signals = {"shutdown": False}
        
        # Dictionary of registered workers, key is pid and value is dict of worker info
        self.workers = OrderedDict()

        # Job queue and current job
        self.job_queue = Queue()

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

        # Start job when we receive one
        manage_jobs_thread = threading.Thread(target=self.manage_jobs)
        manage_jobs_thread.start()

        # Wait for everything to finish
        hb_thread.join()
        listen_thread.join()
        manage_jobs_thread.join()


    def listen(self):
        """Wait on a message from a socket OR a shutdown signal."""

        # Create socket
        logging.info(f"Listening for a connection on localhost:{self.port}")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", self.port))
        sock.listen()

        # Socket accept() and recv() will block for a maximum of 1 second
        sock.settimeout(1)

        # Accept connections for messages until shutdown message
        while not self.signals["shutdown"]:
            # Listen for a connection for 1s.
            try:
                clientsocket, address = sock.accept()
            except socket.timeout:
                continue

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
                logging.debug("Master: JSON decoding failed for a message")
                continue

            if "message_type" not in msg:
                logging.debug("Master: Invalid message")
                continue
            
            # Handle message depending on type
            if msg["message_type"] == "shutdown":
                self.signals["shutdown"] = True
                logging.debug("Master: Shutting down")
                for worker in self.workers.values():
                    if worker["status"] != "dead":
                        self.send_shutdown(worker["port"])
            elif msg["message_type"] == "register":
                # Register worker
                logging.debug(f"Registering worker with PID {msg['worker_pid']}")
                self.workers[msg["worker_pid"]] = {
                    "pid": msg["worker_pid"],
                    "host": msg["worker_host"],
                    "port": msg["worker_port"],
                    "status": "ready",
                    "last_hb_received": time.time(),
                    "job_output": None, 
                }
                self.send_register_ack(msg["worker_pid"])
                
            elif msg["message_type"] == "new_master_job":
                self.init_job(msg)

            elif msg["message_type"] == "status":
                worker_pid = msg["worker_pid"]
                logging.info(f"Master: Status update for worker {worker_pid}")
                logging.debug(msg)
                self.workers[worker_pid]["status"] = "ready"
                # TODO: Handle finished messages for Grouping stage
                self.workers[worker_pid]["job_output"] = msg["output_files"]

        sock.close()

    def send_shutdown(self, worker_port):
        message = {
            "message_type" : "shutdown",
        }
        mapreduce.utils.send_message(message, "localhost", worker_port)


    def send_register_ack(self, worker_pid):
        worker = self.workers[worker_pid]
        message = {
            "message_type" : "register_ack",
            "worker_host" : worker["host"],
            "worker_port" : worker["port"],
            "worker_pid" : worker_pid,
        }
        mapreduce.utils.send_message(message, "localhost", worker["port"])

    def heartbeat(self):
        """Manage heartbeats for registered workers."""

        # Create socket on master port - 1
        logging.info(f"Listening for heartbeats on localhost:{self.port-1}")
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", self.port - 1))

        # Socket accept() and recv() will block for a maximum of 1 second
        sock.settimeout(1)

        while not self.signals["shutdown"]:
            # Check if any workers are dead
            for worker_info in self.workers.values():
                if time.time() - worker_info["last_hb_received"] > 10:
                    worker_info["status"] = "dead"

            # Receive data
            try:
                sock.settimeout(1)
                message_bytes = sock.recv(4096)
            except socket.timeout:
                continue
            if not data:
                continue

            # Parse message chunks into JSON data
            message_str = message_bytes.decode("utf-8")
            try:
                msg = json.loads(message_str)
            except json.JSONDecodeError:
                continue

            logging.info("Heartbeat from %s", address[0])

            # Handle message depending on type
            if msg["message_type"] == "heartbeat" and msg["worker_pid"] in self.workers:
                self.workers[msg["worker_pid"]]["last_hb_received"] = time.time()

        sock.close()

    def init_job(self, msg):
        if not mapreduce.utils.check_schema({
            "input_directory": str,
            "output_directory": str,
            "mapper_executable": str,
            "reducer_executable": str,
            "num_mappers": int,
            "num_reducers": int,
        }, msg):
            logging.debug("Master: Invalid job request")
            return

        input_dir = msg["input_directory"]
        output_dir = msg["output_directory"]
        mapper_exec = msg["mapper_executable"]
        reducer_exec = msg["reducer_executable"]
        num_mappers = msg["num_mappers"]
        num_reducers = msg["num_reducers"]

        new_job = Job(
            input_dir,
            output_dir,
            mapper_exec,
            reducer_exec,
            num_mappers,
            num_reducers,
            self.workers,
            self.signals,
        )

        logging.info(f"Master: Adding job to queue")
        self.job_queue.put(new_job)

    def manage_jobs(self):
        while not self.signals["shutdown"]:
            try:
                current_job = self.job_queue.get(timeout=1)
            except Empty:
                continue

            current_job.start()


@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    Master(port)


if __name__ == '__main__':
    main()



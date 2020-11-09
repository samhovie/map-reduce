import os
import logging
import json
import socket
import time
import click
from mapreduce.master.job import Job
import mapreduce.utils
from pathlib import Path 
from queue import Queue
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
        self.shutdown = False
        
        # Dictionary of registered workers, key is pid and value is dict of worker info
        # TODO: make this an ordered dict
        self.workers = {
            # [pid]: {
            #   host: 
            #   port:
            #   status:
            #   last_hb_received:
            # },
        }

        # Job queue and current job
        self.job_queue = Queue()
        self.current_job = None

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

        hb_thread.join()
        listen_thread.join()


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
                continue

            if "message_type" not in msg:
                continue
            
            # Handle message depending on type
            if msg["message_type"] == "shutdown":            
                self.shutdown = True
                logging.debug("Master: Shutting down")
                for worker in self.workers.values():
                    if worker["status"] != "dead":
                        self.send_shutdown(worker["port"])
            elif msg["message_type"] == "register":
                # Register worker
                logging.debug(f"Registering worker with PID {msg['worker_pid']}")
                self.workers[msg["worker_pid"]] = {
                    "host" : msg["worker_host"],
                    "port": msg["worker_port"],
                    "status": "ready",
                    "last_hb_received": time.time(), 
                }
                self.send_register_ack(msg["worker_pid"])
                # TODO: Check the job queue here
                
            elif msg["message_type"] == "new_master_job":
                self.start_job(msg)

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
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", self.port - 1))
        sock.listen()

        # Socket accept() and recv() will block for a maximum of 1 second
        sock.settimeout(1)

        while not self.shutdown:
            # Check if any workers are dead
            for worker_info in self.workers.values():
                if time.time() - worker_info["last_hb_received"] > 10:
                    worker_info["status"] = "dead"
                    pass

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
            except json.JSONDecodeError:
                continue

            # Handle message depending on type
            if msg["message_type"] == "heartbeat" and msg["worker_pid"] in self.workers:
                self.workers[msg["worker_pid"]]["last_hb_received"] = time.time()

    def start_job(self, msg):
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
            num_reducers
        )

        ready_workers = [worker for worker in self.workers if worker["status"] == "ready"]

        if len(ready_workers) == 0 or self.current_job is not None:
            self.job_queue.put(new_job)
        else:
            self.current_job = new_job
            new_job.start()


    def round_robin_workers(self, tasks, executor):
        # TODO: This is not correct
        while len(ready_workers) == 0:
            if not worker_ready.wait(timeout=1.0):
                if shutdown:
                    return False
        return mapreduce.utils.round_robin(tasks, ready_workers)


@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    Master(port)


if __name__ == '__main__':
    main()



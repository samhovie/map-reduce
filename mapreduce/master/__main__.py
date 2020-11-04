import os
import logging
import json
import socket
import time
import click
import mapreduce.utils
from pathlib import Path 
from queue import Queue
import shutil


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


# Global shutdown signal - needed for listen and job threads.
shutdown = False

# Job and job queue
job_queue = Queue()

ready_workers = []


def round_robin_workers(tasks, executor):
    with worker_status_lock:
        while len(ready_workers) == 0:
            if not worker_ready.wait(timeout=1.0):
                if shutdown:
                    return False
        return utils.round_robin(tasks, ready_workers)


class Job:
    _next_id = 0
    _current = None

    def current():
        return Job._current


    def __init__(
            self,
            input_dir,
            output_dir,
            mapper_exec,
            reducer_exec,
            num_mappers,
            num_reducers):
        self._status = "waiting"
        self._input_dir = input_dir
        self._output_dir = output_dir
        self._mapper_exec = mapper_exec
        self._reducer_exec = reducer_exec
        self._num_mappers = num_mappers
        self._num_reducers = num_reducers

        self._id = Job._next_job_id
        Job._next_id += 1

        logging.info(f"Master: Received job {self._id}: {input_dir} "
                     f"{output_dir} {mapper_exec} {reducer_exec} "
                     f"{num_mappers} {num_reducers}"
                    )

        self._folder = Path("tmp")/f"job-{job_id}"

        self._folder.mkdir()
        (self._folder/"mapper-output").mkdir()
        (self._folder/"grouper-output").mkdir()
        (self._folder/"reducer-output").mkdir()

    def start(self):
        logging.info(f"Master: Starting job {self._id}")

        assert(Job._current is None)
        Job._current = self

        self.status = "started"
        self.mapping()

    def mapping(self):
        # TODO: Partition input files for this stage
        logging.info(f"Master: Starting mapping stage for job {self._id}")

    def grouping(self):
        # TODO
        logging.info(f"Master: Starting grouping stage for job {self._id}")

    def reducing(self):
        # TODO
        logging.info(f"Master: Starting reducing stage for job {self._id}")
    
    def cleanup(self):
        # TODO
        logging.info(f"Master: Finishing job {self._id}")
        self.status = "finished"
        Job._current = None

    def status_update(self, worker_status):
        # TODO
        pass


def start_job(msg):
    if not utils.check_schema({
        "input_directory": str,
        "output_directory": str,
        "mapper_executable": str,
        "reducer_executable": str,
        "num_mappers": int,
        "num_reducers": int,
    }, msg):
        return False

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

    if len(ready_workers) == 0 or Job.current() is not None:
        job_queue.put(job)
    else:
        job.start()

    return True


def listen(port):
    """Wait on a message from a socket OR a shutdown signal."""

    global shutdown

    # Create socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("localhost", port))
    sock.listen()

    # Socket accept() and recv() will block for a maximum of 1 second
    sock.settimeout(1)

    # Accept connections for messages until shutdown message
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

        if "message_type" not in msg:
            continue

        # Handle message depending on type
        if msg["message_type"] == "shutdown":
            shutdown = True
        elif msg["message_type"] == "new_master_job":
            start_job(msg)


@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    Master(port)


if __name__ == '__main__':
    main()



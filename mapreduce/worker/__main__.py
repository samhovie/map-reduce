import os
import logging
import json
import time
import click
import mapreduce.utils


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Worker:
    def __init__(self, master_port, worker_port):
        logging.info("Starting worker:%s", worker_port)
        logging.info("Worker:%s PWD %s", worker_port, os.getcwd())

        # Listen for messages on worker's port
        thread = threading.Thread(target=listen, args=(worker_port,))
        thread.start()

        # Connect to the server
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", master_port))

        # Send register message to master
        message = json.dumps({
            "message_type" : "register",
            "worker_host" : "localhost",
            "worker_port" : worker_port,
            "worker_pid" : id
        })
        sock.sendall(message.encode('utf-8'))
        sock.close()

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
        except JSONDecodeError:
            continue

        # Handle message depending on type
        if msg["message_type"] == "shutdown":            
            shutdown = True
        elif message_dict["message_type"] == "register_ack":
            # Do something


@click.command()
@click.argument("master_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(master_port, worker_port):
    Worker(master_port, worker_port)


if __name__ == '__main__':
    main()

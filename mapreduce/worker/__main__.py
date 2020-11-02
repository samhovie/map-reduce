import os
import logging
import json
import time
import click
import mapreduce.utils

def listen(port):
    """Wait on a message from a socket OR a shutdown signal."""
    # print("listen() starting")

    # Create an INET, STREAMing socket, this is TCP
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind the socket to the server
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("localhost", port))
    sock.listen()

    # Socket accept() and recv() will block for a maximum of 1 second.  If you
    # omit this, it blocks indefinitely, waiting for a connection.
    sock.settimeout(1)

    # while not signals["shutdown"]:
    shutdown = False 
    while not shutdown:
        # print("listening")

        # Listen for a connection for 1s.  The socket library avoids consuming
        # CPU while waiting for a connection.
        try:
            clientsocket, address = sock.accept()
        except socket.timeout:
            continue
        print("Connection from", address[0])

        # Receive data, one chunk at a time.  If recv() times out before we can
        # read a chunk, then go back to the top of the loop and try again.
        # When the client closes the connection, recv() returns empty data,
        # which breaks out of the loop.  We make a simplifying assumption that
        # the client will always cleanly close the connection.
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

        # Decode list-of-byte-strings to UTF8 and parse JSON data
        message_bytes = b''.join(message_chunks)
        message_str = message_bytes.decode("utf-8")
        try:
            msg = json.loads(message_str)
        except JSONDecodeError:
            continue

        if msg["message_type"] == "shutdown":
            
            
            shutdown = True


        # message_dict = json.loads(message_str)
        # print(message_dict)





    # print("listen() shutting down")


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Worker:
    def __init__(self, master_port, worker_port):
        logging.info("Starting worker:%s", worker_port)
        logging.info("Worker:%s PWD %s", worker_port, os.getcwd())

        id = os.getpid

        thread = threading.Thread(target=listen, args=(worker_port,))
        thread.start()

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # connect to the server
        sock.connect(("localhost", master_port))

        # send a message
        # message = json.dumps({"hello": "world"})
        message = json.dumps({
            "message_type" : "register",
            "worker_host" : "localhost",
            "worker_port" : worker_port,
            "worker_pid" : id
        })

        sock.sendall(message.encode('utf-8'))
        sock.close()

        if message_dict["message_type"] == "register_ack":



@click.command()
@click.argument("master_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(master_port, worker_port):
    Worker(master_port, worker_port)


if __name__ == '__main__':
    main()

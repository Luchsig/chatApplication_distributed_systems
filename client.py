import concurrent.futures
import json
import logging
import socket
import struct
import threading

# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# constants
BUFFER_SIZE = 1024
MULTICAST_BUFFER_SIZE = 10240
IP_ADDRESS = socket.gethostbyname(socket.gethostname())

BROADCAST_ADDRESS = '255.255.255.255'
BROADCAST_PORT_SERVER = 65431  # dynamic discovery port on server

TCP_SERVER_PORT = 50500
TCP_CLIENT_PORT = 50510
TCP_TIMEOUT = 5

MULTICAST_PORT_CLIENT = 50550  # port for incoming chatroom messages
MULTICAST_GROUP_ADDRESS = '224.1.2.1'


class Client:
    def __init__(self):
        self.shutdown_event = threading.Event()
        self.threads = []

    # start concurrent running threads (CLI and Handling of chat messages)
    def start_client(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            self.threads.append(executor.submit(self.cli))
            self.threads.append(executor.submit(self.handle_chat_messages))
            logger.info('client started!')

            try:
                # Keep the main thread alive while the threads are running
                while not self.shutdown_event.is_set():
                    self.shutdown_event.wait(1)
            except KeyboardInterrupt:
                logger.info("Client shutdown initiated.")
                self.shutdown_event.set()
                for thread in self.threads:
                    thread.cancel()
                executor.shutdown(wait=True)

    def handle_chat_messages(self):
        logger.info('Open socket for incoming chat messages')
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as multicast_socket:
            multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            multicast_socket.bind((IP_ADDRESS, MULTICAST_PORT_CLIENT))
            mreq = struct.pack('4sL', socket.inet_aton(MULTICAST_GROUP_ADDRESS), socket.INADDR_ANY)
            multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

            while not self.shutdown_event.is_set():
                try:
                    data, addr = multicast_socket.recvfrom(BUFFER_SIZE)
                    print(f'{addr}: {data.decode("utf-8")}')
                except socket.timeout as e:
                    continue
                except socket.error as e:
                    logger.error(f'Socket error: {e}')
                except Exception as e:
                    logger.error(f'Unexpected error: {e}')

    # wait for answer with server_address from server and process message
    def handle_tcp_answer(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp_answer_socket:
                tcp_answer_socket.bind((IP_ADDRESS, TCP_CLIENT_PORT))
                tcp_answer_socket.listen()
                tcp_answer_socket.settimeout(TCP_TIMEOUT)
                try:
                    connection, address = tcp_answer_socket.accept()
                    while True:
                        data = connection.recv(BUFFER_SIZE)
                        if not data:
                            break
                        return data.decode()
                except Exception as e:
                    logger.error("Exception in handle_server_answer:", e)
                    return None
        except Exception as e:
            logger.error("Exception in handle_server_answer:", e)
            return None

    #  send out broadcast message to detect currently leading server
    def find_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as broadcast_server_discovery_socket:
            broadcast_server_discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            broadcast_server_discovery_socket.sendto(IP_ADDRESS.encode('ascii'),
                                                     (BROADCAST_ADDRESS, BROADCAST_PORT_SERVER))
            logger.info('Broadcast message for server discovery sent.')
            broadcast_server_discovery_socket.close()
            return self.handle_tcp_answer()

    def send_message_to_server(self, json_message):
        server_address = ''
        while not server_address and not self.shutdown_event.is_set():
            server_address = self.find_server()
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            client_socket.connect((server_address, TCP_SERVER_PORT))
            client_socket.sendall(json_message.encode('utf-8'))

            data = client_socket.recv(1024)
            logger.info(data.decode('utf-8'))

        finally:
            client_socket.close()

    def create_or_join_chat(self):
        chat_id = input("type in the chatId of the chat you want to connect to: ")
        json_message = json.dumps({"function": "create_join", "chatId": chat_id})
        self.send_message_to_server(json_message)

    def send_message(self):
        message = input("type your next message: ")
        json_message = json.dumps({"function": "chat", "msg": message})
        self.send_message_to_server(json_message)

    def leave_chat(self):
        message = input("are you sure you want to leave? y/n: ")
        if message.lower() == 'y':
            json_message = json.dumps({"function": "leave"})
            self.send_message_to_server(json_message)

    # user interface
    def cli(self):
        while not self.shutdown_event.is_set():
            print("please select your next action:")
            print("1 - create or join chat through chatId")
            print("2 - chat")
            print("3 - leave chat")
            user_input = input()
            if user_input == "1":
                self.create_or_join_chat()
            elif user_input == "2":
                self.send_message()
            elif user_input == "3":
                self.leave_chat()

    #  -------------------------------------- EOF --------------------------------------

'''
 TODOS: testing
        nicht senden ermöglichen, wennuser keinem chat beiwohnt
        LOCKING von ressourcen
'''
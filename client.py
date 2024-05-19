import concurrent.futures
import json
import socket
import struct
import threading

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
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            self.threads.append(executor.submit(self.cli))
            self.threads.append(executor.submit(self.handle_chat_messages))
            print('client started!')

            try:
                # Keep the main thread alive while the threads are running
                while not self.shutdown_event.is_set():
                    self.shutdown_event.wait(1)
            except KeyboardInterrupt:
                print("Client shutdown initiated.")
                self.shutdown_event.set()
                for thread in self.threads:
                    thread.cancel()
                executor.shutdown(wait=True)

    def handle_chat_messages(self):
        multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        multicast_socket.bind((IP_ADDRESS, MULTICAST_PORT_CLIENT))

        mreq = struct.pack('4sL', socket.inet_aton(MULTICAST_GROUP_ADDRESS), socket.INADDR_ANY)
        multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        while True:
            data, addr = multicast_socket.recvfrom(BUFFER_SIZE)
            print(f'{addr}: {data.decode("utf-8")}')

    # wait for answer with server_address from server and process message
    def handle_tcp_answer(self):
        tcp_answer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_answer_socket.bind(('', TCP_CLIENT_PORT))
        tcp_answer_socket.listen()
        tcp_answer_socket.settimeout(TCP_TIMEOUT)

        connection, address = tcp_answer_socket.accept()
        try:
            server_address = ''
            while True:
                data = connection.recv(BUFFER_SIZE)
                if not data:
                    break
                server_address = data.decode('ascii')

            tcp_answer_socket.close()
            return server_address
        except Exception as e:
            print("Exception in handle_server_answer:", e)
            return None

    #  send out broadcast message to detect currently leading server
    def find_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as broadcast_server_discovery_socket:
            broadcast_server_discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            broadcast_server_discovery_socket.sendto(IP_ADDRESS.encode('ascii'),
                                                     (BROADCAST_ADDRESS, BROADCAST_PORT_SERVER))
            print('broadcast message sent.')
            broadcast_server_discovery_socket.close()
            return self.handle_tcp_answer()

    def send_message_to_server(self, json_message):
        server_address = self.find_server()
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            client_socket.connect((server_address, TCP_SERVER_PORT))
            client_socket.sendall(json_message.encode('utf-8'))

            data = client_socket.recv(1024)
            print(data.decode('utf-8'))

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
 TODOS: user mitgeben, der die nachricht geschickt hat ( von server seite aus )
        testing
        nicht senden ermöglichen, wennuser keinem chat beiwohnt
        LOCKING von ressourcen
'''
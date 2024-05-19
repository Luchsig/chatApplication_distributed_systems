import socket
import struct
import threading
import concurrent.futures
import time
import json

# constants
BUFFER_SIZE = 1024
MULTICAST_BUFFER_SIZE = 10240
IP_ADDRESS = socket.gethostbyname(socket.gethostname())

BROADCAST_ADDRESS = '255.255.255.255'
BROADCAST_PORT_CLIENT = 65431  # port to open to receive server discovery requests from client
BROADCAST_PORT_SERVER = 65432  # port to open to receive and send server discovery requests

TCP_SERVER_PORT = 50500  # port for incoming messages
TCP_CLIENT_PORT = 50510  # port for outgoing messages like submit messages

MULTICAST_PORT_CLIENT = 50550  # port for outgoing chat messages
MULTICAST_PORT_SERVER = 50560  # port for replication of data (server)
MULTICAST_GROUP_ADDRESS = '224.3.29.71'
MULTICAST_TTL = 2

LCR_PORT = 50600
LEADER_DEATH_TIME = 20

HEARTBEAT_PORT_SERVER = 50570  # port for incoming / outgoing heartbeat (leader)


class Server:
    def __init__(self):
        self.shutdown_event = threading.Event()
        self.threads = []
        self.list_of_known_servers = []
        self.chat_rooms = {}  # dict {"<chatID>", [<client_ip_addresses>]}
        self.lcr_ongoing = False
        self.is_leader = False
        self.last_message_from_leader_ts = 0
        self.direct_neighbour = ''
        self.leader_ip_address = ''

    #  -------------------------------------- START THREADS --------------------------------------
    def start_server(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            self.threads.append(executor.submit(self.handle_broadcast_server_requests))
            self.threads.append(executor.submit(self.lcr))
            self.threads.append(executor.submit(self.handle_tcp_server_answers))
            self.threads.append(executor.submit(self.handle_leader_update))
            self.threads.append(executor.submit(self.detection_of_missing_or_dead_leader))
            self.threads.append(executor.submit(self.handle_leader_update))
            self.threads.append(executor.submit(self.handle_broadcast_client_requests))
            print('server up and running')

            try:
                # Keep the main thread alive while the threads are running
                while not self.shutdown_event.is_set():
                    self.shutdown_event.wait(1)
            except KeyboardInterrupt:
                print("Server shutdown initiated.")
                self.shutdown_event.set()
                for thread in self.threads:
                    thread.cancel()
                executor.shutdown(wait=True)

    #  -------------------------------------- SERVER LOGIC HERE --------------------------------------
    #  ------------ CONNECTION BETWEEN SERVERS ------------

    # send broadcast hello message to other servers SENDER
    def send_broadcast_to_search_for_servers(self):
        print('sending server discovery message via broadcast')
        self.list_of_known_servers = []
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as broadcast_server_discovery_socket:
            broadcast_server_discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            broadcast_server_discovery_socket.sendto(IP_ADDRESS.encode('ascii'),
                                                     (BROADCAST_ADDRESS, BROADCAST_PORT_SERVER))
            broadcast_server_discovery_socket.close()

    # open broadcast socket to receive hello world messages from other servers LISTENER
    def handle_broadcast_server_requests(self):
        listener_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener_socket.bind((IP_ADDRESS, BROADCAST_PORT_SERVER))

        while not self.shutdown_event.is_set():
            msg, addr = listener_socket.recvfrom(BUFFER_SIZE)
            print('received server discovery message via broadcast')
            if addr not in self.list_of_known_servers:
                print(f"Server added with address {addr}")
                self.list_of_known_servers.append(addr)
                self.send_tcp_server_answer(addr)

        listener_socket.close()

    # open tcp port for receiving answers from other servers about their existence
    def handle_tcp_server_answers(self):
        server_answer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_answer_socket.bind((IP_ADDRESS, TCP_SERVER_PORT))
        server_answer_socket.listen()

        while not self.shutdown_event.is_set():
            client_answer_socket, addr = server_answer_socket.accept()
            print('received answer to broadcast call')

            if addr not in self.list_of_known_servers:
                print(f"Server added with address {addr}")
                self.list_of_known_servers.append(addr)

            client_answer_socket.close()
        server_answer_socket.close()

    # answer to broadcast request by tcp
    def send_tcp_server_answer(self, addr):
        print('sending own ip to broadcast requester')
        server_answer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server_answer_socket.connect((addr, TCP_SERVER_PORT))
            server_answer_socket.sendall(IP_ADDRESS.encode('ASCII'))
        finally:
            server_answer_socket.close()

    # ------------ LEADER ELECTION ------------

    def detection_of_missing_or_dead_leader(self):
        while not self.shutdown_event.is_set():
            if not self.is_leader and not self.lcr_ongoing:
                current_time = time.time()
                if (current_time - self.last_message_from_leader_ts) >= LEADER_DEATH_TIME:
                    print('no active leader detected')
                    self.start_lcr()

    def form_ring(self):
        print('forming ring with list of known servers')
        binary_ring_from_server_list = sorted([socket.inet_aton(element) for element in self.list_of_known_servers])
        ip_ring = [socket.inet_ntoa(ip) for ip in binary_ring_from_server_list]
        return ip_ring

    def get_direct_neighbour(self):
        print('preparing to get direct neighbour')
        ring = self.form_ring()
        index = ring.index(IP_ADDRESS) if IP_ADDRESS in ring else -1

        if index != -1:
            if index + 1 == len(ring):
                self.direct_neighbour = ring[0]
            else:
                self.direct_neighbour = ring[index + 1]

    def start_lcr(self):
        print('starting leader election')
        self.send_broadcast_to_search_for_servers()
        time.sleep(5)
        self.get_direct_neighbour()
        lcr_begin_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        election_message = {"mid": IP_ADDRESS, "isLeader": False}
        lcr_begin_socket.sendto((json.dumps(election_message).encode()), (self.direct_neighbour, LCR_PORT))
        self.lcr_ongoing = True
        self.is_leader = False

    def lcr(self):
        lcr_listener_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        lcr_listener_socket.bind((IP_ADDRESS, LCR_PORT))
        while not self.shutdown_event.is_set():
            participant = False
            while not self.is_leader and self.lcr_ongoing:
                if self.lcr_ongoing:
                    # assuming that its the only available server which promotes itself to leader directly
                    if self.direct_neighbour == '':
                        self.is_leader = True
                    else:
                        data, address = lcr_listener_socket.recvfrom(BUFFER_SIZE)
                        election_message = json.loads(data.decode())

                        # isLeader message received, stop LCR locally,
                        if election_message['isLeader']:
                            participant = False
                            lcr_listener_socket.sendto((json.dumps(election_message).encode()),
                                                       (self.direct_neighbour, LCR_PORT))
                            self.leader_ip_address = election_message['mid']
                            self.lcr_ongoing = False

                        # received lcr mid smaller, pass on own IP
                        if election_message['mid'] < IP_ADDRESS and not participant:
                            participant = True
                            election_message = {"mid": IP_ADDRESS, "isLeader": False}
                            lcr_listener_socket.sendto((json.dumps(election_message).encode()),
                                                       (self.direct_neighbour, LCR_PORT))

                        # received lcr mid greater, pass on received message
                        elif election_message['mid'] > IP_ADDRESS:
                            participant = True
                            lcr_listener_socket.sendto((json.dumps(election_message).encode()),
                                                       (self.direct_neighbour, LCR_PORT))

                        # received lcr mid equals own IP address, set internally as leader and send leader msg
                        elif election_message['mid'] == IP_ADDRESS:
                            election_message = {"mid": IP_ADDRESS, "isLeader": True}
                            participant = False
                            lcr_listener_socket.sendto((json.dumps(election_message).encode()),
                                                       (self.direct_neighbour, LCR_PORT))
                            self.leader_ip_address = IP_ADDRESS
                            self.is_leader = True
                            self.lcr_ongoing = False
                        else:
                            pass
            if self.is_leader:
                print('won leader election - starting to send heartbeat')
                leader_thread = threading.Thread(target=self.send_leader_heartbeat(), args=())
                leader_thread.start()

    # ------------ DATA REPLICATION BETWEEN SERVERS ------------
    def handle_leader_update(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_socket.bind((IP_ADDRESS, MULTICAST_PORT_SERVER))
        group = socket.inet_aton(MULTICAST_GROUP_ADDRESS)
        mreq = struct.pack('4sL', group, MULTICAST_TTL, MULTICAST_PORT_SERVER)
        server_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        while not self.shutdown_event.is_set():
            if not self.is_leader:
                data, addr = server_socket.recvfrom(MULTICAST_BUFFER_SIZE)
                if addr == self.leader_ip_address:
                    data = json.loads(data.decode())
                    self.list_of_known_users = data['known_users']
                    self.chat_rooms = data['chat_rooms']

    def send_leader_update(self):
        # TODO: MUSS udp multicast verwenden und die entsprechende Adresse
        if self.is_leader:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            client_socket.bind((IP_ADDRESS, MULTICAST_PORT_SERVER))

            group = socket.inet_aton(MULTICAST_GROUP_ADDRESS)
            mreq = struct.pack('4sL', group, MULTICAST_TTL, MULTICAST_PORT_SERVER)
            client_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

            message = json.dumps({"known_users": self.list_of_known_users, "chat_rooms": self.chat_rooms}).encode()
            for server in self.list_of_known_users:
                client_socket.sendto(message, server)

    # ------------ FAULT TOLERANCE SERVER CRASH ------------

    def handle_leader_heartbeat(self):
        heartbeat_server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        heartbeat_server_socket.bind((IP_ADDRESS, HEARTBEAT_PORT_SERVER))
        group = socket.inet_aton(MULTICAST_GROUP_ADDRESS)
        mreq = struct.pack('4sL', group, MULTICAST_TTL, HEARTBEAT_PORT_SERVER)
        heartbeat_server_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        while not self.lcr_ongoing and not self.shutdown_event.is_set():
            data, addr = heartbeat_server_socket.recvfrom(MULTICAST_BUFFER_SIZE)
            print('received leader heartbeat')
            if addr == self.leader_ip_address and data.decode() == 'HEARTBEAT':
                self.last_message_from_leader_ts = time.time()

    def send_leader_heartbeat(self):
        while self.is_leader and not self.shutdown_event.is_set():
            print('sending heartbeat')
            heartbeat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            message = 'HEARTBEAT'.encode()
            heartbeat_client_socket.sendto(message, (MULTICAST_GROUP_ADDRESS, HEARTBEAT_PORT_SERVER))
            time.sleep(2)

    #  -------------------------------------- CLIENT LOGIC HERE --------------------------------------

    def handle_broadcast_client_requests(self):
        listener_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener_socket.bind((IP_ADDRESS, BROADCAST_PORT_CLIENT))

        while not self.shutdown_event.is_set():
            if self.is_leader:
                msg, addr = listener_socket.recvfrom(BUFFER_SIZE)

                if addr not in self.list_of_known_users:
                    print(f"Client added with address {addr}")
                    self.list_of_known_users.append(addr)
                    self.send_tcp_client_answer(addr)
                    self.send_leader_update()

        listener_socket.close()

    def send_tcp_client_answer(self, addr):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client_socket.connect((addr, TCP_CLIENT_PORT))
            message = "successfully connected"
            client_socket.sendall(message.encode('UTF-8'))
            data = client_socket.recv(BUFFER_SIZE)
        finally:
            client_socket.close()

    def handle_send_message_request(self):
        listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener_socket.bind((IP_ADDRESS, TCP_CLIENT_PORT))
        listener_socket.listen()

        while self.is_leader:
            client, addr = listener_socket.accept()

            while True:
                data = json.loads(client.recv(BUFFER_SIZE).decode('UTF-8'))
                print("received message from client", data)

                if data:
                    if data['function'] == 'create_join':
                        if data['chatId']:
                            if data['chatId'] in list(self.chat_rooms.keys()) and addr not in self.chat_rooms[
                                data['chatId']]:
                                self.chat_rooms[data['chatId']].append(addr)
                                chat_join_message = f'New participiant {addr} joined the chat room'
                                self.forward_message_to_chat_participants(self.find_active_chat_id(addr),
                                                                          chat_join_message)
                            else:
                                self.chat_rooms[data['chatId']] = [addr]
                            self.send_leader_update()
                    elif data['function'] == 'chat':
                        if data['msg']:
                            self.forward_message_to_chat_participants(self.find_active_chat_id(addr), data['msg'])

                    elif data['function'] == 'leave':
                        chat_leave_message = f'Participiant {addr} is leaving'
                        self.forward_message_to_chat_participants(self.find_active_chat_id(addr), chat_leave_message)
                        self.send_leader_update()

                    else:
                        print("data object received from client was invalid")

    def find_active_chat_id(self, addr):
        for key, value_list in self.chat_rooms.items():
            if addr in value_list:
                return key

    def forward_message_to_chat_participants(self, chat_id, msg):
        client_multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        client_multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, MULTICAST_TTL)
        send_message = msg.encode('ascii')

        for addr in self.chat_rooms[chat_id]:
            client_multicast_socket.sendto(send_message, (addr, MULTICAST_PORT_CLIENT))

        client_multicast_socket.close()

    #  -------------------------------------- EOF --------------------------------------

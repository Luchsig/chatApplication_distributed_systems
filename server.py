import socket
import struct
import threading
import concurrent.futures
import time
import json
import logging
from json import JSONDecodeError

# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
            methods = [
                self.handle_broadcast_server_requests,
                self.lcr,
                self.handle_tcp_server_answers,
                self.handle_leader_update,
                self.detection_of_missing_or_dead_leader,
                self.handle_broadcast_client_requests
            ]

            for method in methods:
                self.threads.append(executor.submit(self.run_with_exception_handling, method))

            try:
                # Keep the main thread alive while the threads are running
                while not self.shutdown_event.is_set():
                    self.shutdown_event.wait(1)
            except KeyboardInterrupt:
                logger.info("Server shutdown initiated.")
                self.shutdown_event.set()
                for thread in self.threads:
                    thread.cancel()
            finally:
                executor.shutdown(wait=True)

    def run_with_exception_handling(self, target):
        try:
            target()
        except Exception as e:
            logger.error(f"Error in thread {target.__name__}: {e}")

    #  -------------------------------------- SERVER LOGIC HERE --------------------------------------
    #  ------------ CONNECTION BETWEEN SERVERS ------------

    # send broadcast hello message to other servers SENDER
    def send_broadcast_to_search_for_servers(self):
        logger.info('Sending server discovery message via broadcast')
        self.list_of_known_servers.clear()
        self.list_of_known_servers.append(IP_ADDRESS)

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as broadcast_server_discovery_socket:
                broadcast_server_discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                msg = IP_ADDRESS.encode()
                broadcast_server_discovery_socket.sendto(msg, (BROADCAST_ADDRESS, BROADCAST_PORT_SERVER))
        except Exception as e:
            logger.error(f'Failed to send broadcast message: {e}')

    # open broadcast socket to receive hello world messages from other servers LISTENER
    def handle_broadcast_server_requests(self):
        logger.info('Starting to listen for broadcast Server requests')

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as listener_socket:
                listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener_socket.bind((IP_ADDRESS, BROADCAST_PORT_SERVER))

                while not self.shutdown_event.is_set():
                    try:
                        msg, addr = listener_socket.recvfrom(BUFFER_SIZE)
                        logger.info('received server discovery message via broadcast')
                        if addr not in self.list_of_known_servers:
                            logger.info(f"Server added with address {addr}")
                            self.list_of_known_servers.append(addr)
                            self.send_tcp_server_answer(addr)
                    except socket.error as e:
                        logger.error(f'Socket error: {e}')
                    except Exception as e:
                        logger.error(f'Unexpected error: {e}')

        except socket.error as e:
            logger.error(f'Failed to set up listener socket: {e}')

    # open tcp port for receiving answers from other servers about their existence
    def handle_tcp_server_answers(self):
        logger.info('Starting to listen for TCP Server answers')
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_answer_socket:
                server_answer_socket.bind((IP_ADDRESS, TCP_SERVER_PORT))
                server_answer_socket.listen()

                while not self.shutdown_event.is_set():
                    try:
                        client_answer_socket, addr = server_answer_socket.accept()
                        with client_answer_socket:
                            logger.info('received answer to broadcast call')
                            if addr not in self.list_of_known_servers:
                                logger.info(f"Server added with address {addr}")
                                self.list_of_known_servers.append(addr)
                    except socket.error as e:
                        logger.error(f'Socket error: {e}')
                    except Exception as e:
                        logger.error(f'Unexpected error: {e}')
        except socket.error as e:
            logger.error(f'Failed to set up TCP listener socket: {e}')

    # answer to broadcast request by tcp
    def send_tcp_server_answer(self, addr):
        logger.info('sending own ip to broadcast requester')

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_answer_socket:
                server_answer_socket.connect((addr, TCP_SERVER_PORT))
                server_answer_socket.sendall(IP_ADDRESS.encode('ASCII'))
                logger.info(f'Successfully sent own ip to broadcast requester')
        except socket.error as e:
            logger.error(f'Failed to send TCP server answer to {addr}: {e}')
        except Exception as e:
            logger.error(f"Unexpected error while sending TCP server answer to {addr}: {e}")

    # ------------ LEADER ELECTION ------------

    def detection_of_missing_or_dead_leader(self):
        logger.info('Starting detection of missing or dead leader')
        while not self.shutdown_event.is_set():
            try:
                if not self.is_leader and not self.lcr_ongoing:
                    current_time = time.time()
                    if (current_time - self.last_message_from_leader_ts) >= LEADER_DEATH_TIME:
                        logger.info('No active leader detected')
                        self.start_lcr()
                time.sleep(1)
            except Exception as e:
                logger.error(f'Error during leader detection: {e}')

    def form_ring(self):
        logger.info('Forming ring with list of known servers')
        try:
            binary_ring_from_server_list = sorted([socket.inet_aton(element) for element in self.list_of_known_servers])
            ip_ring = [socket.inet_ntoa(ip) for ip in binary_ring_from_server_list]
            logger.info(f'Ring formed: {ip_ring}')
            return ip_ring
        except socket.error as e:
            logging.error(f'Failed to form ring: {e}')
            return []

    def get_direct_neighbour(self):
        logger.info('Preparing to get direct neighbour')
        try:
            ring = self.form_ring()

            if IP_ADDRESS in ring:
                index = ring.index(IP_ADDRESS)
                direct_neighbour = ring[(index + 1) % len(ring)]
                if direct_neighbour != IP_ADDRESS:
                    self.direct_neighbour = direct_neighbour
                logger.info(f'Direct neighbour: {self.direct_neighbour}')
            else:
                logger.warning(f'IP address {IP_ADDRESS} not in ring')
        except Exception as e:
            logger.error(f'Failed to get direct neighbour: {e}')

    def start_lcr(self):
        logger.info('starting leader election')
        self.send_broadcast_to_search_for_servers()
        time.sleep(5)
        self.get_direct_neighbour()

        if self.direct_neighbour is None:
            logger.error('no direct neighbour found')
            return

        lcr_start_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            election_message = {"mid": IP_ADDRESS, "isLeader": False}
            message = json.dumps(election_message).encode()

            lcr_start_socket.sendto(message, (self.direct_neighbour, LCR_PORT))
            self.lcr_ongoing = True
            self.is_leader = False
            logger.info(f'leader election message sent to {self.direct_neighbour}')
        except socket.error as e:
            logger.error('Socket error occured in start_lcr', e)
        finally:
            lcr_start_socket.close()

    def lcr(self):
        lcr_listener_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        lcr_listener_socket.bind((IP_ADDRESS, LCR_PORT))
        while not self.shutdown_event.is_set():
            participant = False
            while not self.is_leader and self.lcr_ongoing:
                # assuming that its the only available server which promotes itself to leader directly
                if self.direct_neighbour == '':
                    self.is_leader = True
                else:
                    data, address = lcr_listener_socket.recvfrom(BUFFER_SIZE)
                    election_message = json.loads(data.decode())
                    logger.info(f'leader election message received from {address}')

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
                        logger.warning('Unexpected event occured in lcr')
            if self.is_leader:
                logger.info('Current node won leader election!')
                leader_thread = threading.Thread(target=self.send_leader_heartbeat(), args=())
                leader_thread.start()

    # ------------ DATA REPLICATION BETWEEN SERVERS ------------

    def handle_leader_update(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_socket:
                server_socket.bind((IP_ADDRESS, MULTICAST_PORT_SERVER))
                group = socket.inet_aton(MULTICAST_GROUP_ADDRESS)
                mreq = struct.pack('4sL', group, socket.INADDR_ANY)
                server_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
                server_socket.settimeout(1)
                while not self.shutdown_event.is_set():
                    try:
                        if not self.is_leader:
                            data, addr = server_socket.recvfrom(MULTICAST_BUFFER_SIZE)
                            if addr == self.leader_ip_address:
                                data = json.loads(data.decode())
                                self.chat_rooms = data.get('chat_rooms', self.chat_rooms)
                                logger.info('Updated chat rooms according to leader server')
                    except socket.timeout:
                        continue
                    except JSONDecodeError as e:
                        logger.error(f"JSON decode error: {e}")
                    except Exception as e:
                        logger.error(f"Error in handle_leader_update: {e}")
        except Exception as e:
            logger.error(f"Failed to set up the multicast socket in handle_leader_update: {e}")

    def send_leader_update(self):
        if self.is_leader:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client_socket:
                    client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    client_socket.bind((IP_ADDRESS, MULTICAST_PORT_SERVER))

                    group = socket.inet_aton(MULTICAST_GROUP_ADDRESS)
                    mreq = struct.pack('4sL', group, socket.INADDR_ANY)
                    client_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

                    message = json.dumps({"chat_rooms": self.chat_rooms}).encode()

                    client_socket.sendto(message, (MULTICAST_GROUP_ADDRESS, MULTICAST_PORT_SERVER))
                    logger.info('Sent leader update for chat rooms')
            except socket.error as e:
                logger.error('Socket error occurred in send_leader_update: %s', e)
            except Exception as e:
                logger.error('An error occurred: %s', e)

    # ------------ FAULT TOLERANCE SERVER CRASH ------------

    def handle_leader_heartbeat(self):
        heartbeat_server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        heartbeat_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        heartbeat_server_socket.bind((IP_ADDRESS, HEARTBEAT_PORT_SERVER))

        group = socket.inet_aton(MULTICAST_GROUP_ADDRESS)
        mreq = struct.pack('4sL', group, socket.INADDR_ANY)
        heartbeat_server_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        try:
            while not self.lcr_ongoing and not self.shutdown_event.is_set():
                try:
                    data, addr = heartbeat_server_socket.recvfrom(MULTICAST_BUFFER_SIZE)
                    if addr[0] == self.leader_ip_address and data.decode() == 'HEARTBEAT':
                        self.last_message_from_leader_ts = time.time()
                except socket.error as e:
                    logger.error('Socket error: %s', e)
                except Exception as e:
                    logger.error(f"Error: {e}")
        finally:
            heartbeat_server_socket.close()

    def send_leader_heartbeat(self):
        while self.is_leader and not self.shutdown_event.is_set():
            heartbeat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            heartbeat_client_socket.settimeout(1)
            try:
                logger.info('sending heartbeat')
                message = 'HEARTBEAT'.encode()
                heartbeat_client_socket.sendto(message, (MULTICAST_GROUP_ADDRESS, HEARTBEAT_PORT_SERVER))

                time.sleep(2)
            except socket.error as e:
                logger.error(f"Socket error: {e}")
            except Exception as e:
                logger.error(f"Error: {e}")
            finally:
                heartbeat_client_socket.close()

    #  -------------------------------------- CLIENT LOGIC HERE --------------------------------------

    def handle_broadcast_client_requests(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as listener_socket:
                listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener_socket.bind((IP_ADDRESS, BROADCAST_PORT_CLIENT))

                while not self.shutdown_event.is_set():
                    if self.is_leader:
                        try:
                            listener_socket.settimeout(1)  # Timeout to periodically check shutdown_event
                            msg, client_address = listener_socket.recvfrom(BUFFER_SIZE)
                            logger.info(f"Server discovery request by {client_address}")
                            self.send_tcp_client_answer(client_address)
                        except socket.timeout:
                            continue
                        except Exception as e:
                            logger.error(f"Error handling broadcast client request: {e}")
        except Exception as e:
            logger.error(f"Failed to open Socket for handling client Broadcast requests: {e}")


    def send_tcp_client_answer(self, client_address):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client_socket.connect((client_address, TCP_CLIENT_PORT))
            message = "successfully connected"
            client_socket.sendall(message.encode('UTF-8'))
            data = client_socket.recv(BUFFER_SIZE)
        except Exception as e:
            logger.error(f"Error connecting to client {client_address}: {e}")
        finally:
            client_socket.close()

    def handle_send_message_request(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((IP_ADDRESS, TCP_CLIENT_PORT))
        server_socket.listen()

        while self.is_leader:
            client_socket, client_addr = server_socket.accept()

            try:
                while True:
                    data = client_socket.recv(BUFFER_SIZE)
                    logger.info("received message from client", data)
                    client_response_msg = ''
                    if data:
                        json_data = json.loads(data.decode('UTF-8'))
                        if json_data['function'] == 'create_join':
                            if json_data['chatId']:
                                client_response_msg = self.create_or_join_chat_room(client_addr, json_data['chatId'])
                            else:
                                client_response_msg = "No chatId given"
                        elif json_data['function'] == 'chat':
                            if json_data['msg']:
                                client_response_msg = self.send_message(client_addr, json_data['msg'])
                            else:
                                client_response_msg = "No message received to submit"
                        elif json_data['function'] == 'leave':
                            client_response_msg = self.leave_chat_room(client_addr)
                        else:
                            client_response_msg = "Received invalid data object"
                        client_socket.sendall(client_response_msg.encode('UTF-8'))
            finally:
                client_socket.close()

    def create_or_join_chat_room(self, client_addr, chat_room):
        if not self.is_chat_room_assigned_already(client_addr):
            if chat_room in self.chat_rooms:
                self.chat_rooms[chat_room].append(client_addr)
                chat_join_message = f'New participant {client_addr} joined the chat room'
                self.forward_message_to_chat_participants(self.find_active_chat_id(client_addr), chat_join_message,
                                                          "SYSTEM")
                response = f"Successfully joined the chat room (chatId: {chat_room})"
            else:
                self.chat_rooms[chat_room] = [client_addr]
                response = f"Successfully created new chat room (chatId: {chat_room})"

            self.send_leader_update()
            return response

        return "User is already assigned to another chat room"

    def leave_chat_room(self, client_addr):
        active_chat_id = self.find_active_chat_id(client_addr)
        if active_chat_id:
            self.chat_rooms[active_chat_id].remove(client_addr)
            if not self.chat_rooms[active_chat_id]:
                self.chat_rooms.pop(active_chat_id)
                return "Chat room has been closed as the last user left"

            chat_leave_message = f'Participant {client_addr} left the chat room'
            self.forward_message_to_chat_participants(active_chat_id, chat_leave_message, "SYSTEM")
            self.send_leader_update()
            return "Successfully left the chat room"

        return "User is not assigned to any chat room"

    def send_message(self, client_addr, message):
        active_chat_id = self.find_active_chat_id(client_addr)
        if active_chat_id:
            self.forward_message_to_chat_participants(active_chat_id, message, client_addr)
            return 'message sent'

        return "Nobody here to listen - join a chat room first"

    def is_chat_room_assigned_already(self, addr):
        for user_list in self.chat_rooms.values():
            if addr in user_list:
                return True
        return False

    def find_active_chat_id(self, addr):
        for key, value_list in self.chat_rooms.items():
            if addr in value_list:
                return key
        return None

    def forward_message_to_chat_participants(self, chat_id, msg, sender):
        client_multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        client_multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, MULTICAST_TTL)
        send_message = f'{sender}: {msg}'.encode('ascii')

        try:
            for client_addr in self.chat_rooms[chat_id]:
                client_multicast_socket.sendto(send_message, (client_addr, MULTICAST_PORT_CLIENT))
        except Exception as e:
            logger.error(f"Error sending message to chat participants: {e}")
        finally:
            client_multicast_socket.close()

    #  -------------------------------------- EOF --------------------------------------

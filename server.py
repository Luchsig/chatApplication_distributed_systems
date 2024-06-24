import socket
import struct
import threading
import concurrent.futures
import time
import json
import logging
from json import JSONDecodeError
from functions import get_ip_adress

# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# constants
BUFFER_SIZE = 1024
MULTICAST_BUFFER_SIZE = 10240
# IP_ADDRESS = socket.gethostbyname(socket.gethostname())
IP_ADDRESS = get_ip_adress()

BROADCAST_ADDRESS = '255.255.255.255'
BROADCAST_PORT_CLIENT = 65431  # port to open to receive server discovery requests from client
BROADCAST_PORT_SERVER = 65432  # port to open to receive and send server discovery requests

TCP_CLIENT_PORT = 50510  # port for message receiving from clients

MULTICAST_PORT_CLIENT = 50550  # port for outgoing chat messages
MULTICAST_PORT_SERVER = 50560  # port for replication of data (server)
MULTICAST_GROUP_ADDRESS = '239.0.0.1'
MULTICAST_TTL = 2

LCR_PORT = 50600
LEADER_DEATH_TIME = 10

HEARTBEAT_PORT_SERVER = 50570  # port for incoming / outgoing heartbeat (leader)


class Server:
    def __init__(self):
        self.shutdown_event = threading.Event()
        self.threads = []
        self.list_of_known_servers = []
        self.chat_rooms = {}  # dict {"<chatID>", [<client_ip_addresses>]}
        self.lcr_ongoing = False
        self.is_leader = False
        self.last_message_from_leader_ts = time.time()
        self.direct_neighbour = ''
        self.leader_ip_address = ''
        self.lock = threading.Lock()
        self.participant = False

    #  -------------------------------------- START THREADS --------------------------------------
    def start_server(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            methods = [
                self.handle_broadcast_server_requests,
                self.lcr,
                self.handle_leader_update,
                self.handle_leader_heartbeat,
                self.detection_of_missing_or_dead_leader,
                self.handle_broadcast_client_requests,
                self.handle_send_message_request
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
    #  ------------ CONNECTION BETWEEN SERVERS ------------ broadcast

    def send_broadcast_to_search_for_servers(self):
        logger.debug('Sending server discovery message via broadcast')
        with self.lock:
            self.list_of_known_servers.clear()
            self.list_of_known_servers.append(IP_ADDRESS)

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as broadcast_server_discovery_socket:
                broadcast_server_discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                msg = IP_ADDRESS.encode()
                broadcast_server_discovery_socket.sendto(msg, (BROADCAST_ADDRESS, BROADCAST_PORT_SERVER))

                broadcast_server_discovery_socket.settimeout(3)
                while True:
                    try:
                        response, addr = broadcast_server_discovery_socket.recvfrom(BUFFER_SIZE)
                        logger.debug(f'Received server discovery answer from {addr}')
                        if addr[0] not in self.list_of_known_servers:
                            self.list_of_known_servers.append(addr[0])
                    except socket.timeout:
                        logger.debug('No more responses, ending wait')
                        break
                    except Exception as e:
                        logger.error(f'Error receiving response: {e}')
                        break
        except Exception as e:
            logger.error(f'Failed to send broadcast message: {e}')

    def handle_broadcast_server_requests(self):
        logger.debug('Starting to listen for broadcast server requests')

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as listener_socket:
                listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener_socket.bind(('', BROADCAST_PORT_SERVER))
                listener_socket.settimeout(1)

                while not self.shutdown_event.is_set():
                    try:
                        msg, addr = listener_socket.recvfrom(BUFFER_SIZE)
                        logger.debug('Received server discovery message via broadcast')

                        if addr[0] not in self.list_of_known_servers:
                            logger.debug(f"Server added with address {addr}")
                            self.list_of_known_servers.append(addr[0])

                        response_message = 'hello'.encode()
                        listener_socket.sendto(response_message, addr)
                        logger.debug(f'Sent server hello to {addr}')

                    except socket.timeout:
                        continue
                    except socket.error as e:
                        logger.error(f'Socket error: {e}')
                    except Exception as e:
                        logger.error(f'Unexpected error: {e}')

        except socket.error as e:
            logger.error(f'Failed to set up listener socket: {e}')

    # ------------ LEADER ELECTION ------------ udp unicast

    def detection_of_missing_or_dead_leader(self):
        logger.info('Starting detection of missing or dead leader')
        while not self.shutdown_event.is_set():
            time.sleep(3)
            if not self.is_leader and not self.lcr_ongoing:
                if (time.time() - self.last_message_from_leader_ts) >= LEADER_DEATH_TIME:
                    logger.info('No active leader detected')
                    self.start_lcr()

    def form_ring(self):
        logger.debug('Forming ring with list of known servers')
        try:
            binary_ring_from_server_list = sorted([socket.inet_aton(element) for element in self.list_of_known_servers])
            ip_ring = [socket.inet_ntoa(ip) for ip in binary_ring_from_server_list]
            logger.info(f'Ring formed: {ip_ring}')

            return ip_ring
        except socket.error as e:
            logging.error(f'Failed to form ring: {e}')
            return []

    def get_direct_neighbour(self):
        logger.debug('Preparing to get direct neighbour')
        self.direct_neighbour = ''
        try:
            ring = self.form_ring()

            if IP_ADDRESS in ring:
                index = ring.index(IP_ADDRESS)
                direct_neighbour = ring[(index + 1) % len(ring)]
                if direct_neighbour and direct_neighbour != IP_ADDRESS:
                    self.direct_neighbour = direct_neighbour
                    logger.info(f'Direct neighbour: {self.direct_neighbour}')
            else:
                logger.warning(f'Ring is not complete!')
        except Exception as e:
            logger.error(f'Failed to get direct neighbour: {e}')

    def start_lcr(self):
        logger.info('starting leader election')
        retry = 3
        while retry > 0:
            self.send_broadcast_to_search_for_servers()
            time.sleep(8)
            self.get_direct_neighbour()
            if not self.direct_neighbour == '':
                break
            retry -= 1

        if not self.direct_neighbour == '':
            lcr_start_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                election_message = {"mid": IP_ADDRESS, "isLeader": False}
                message = json.dumps(election_message).encode()

                lcr_start_socket.sendto(message, (self.direct_neighbour, LCR_PORT))
                with self.lock:
                    self.lcr_ongoing = True
                    self.is_leader = False
                    self.participant = False
                logger.info(f'lcr start message sent to {self.direct_neighbour}')
            except socket.error as e:
                logger.error('Socket error occurred in start_lcr', e)
            finally:
                lcr_start_socket.close()
        else:
            logger.warning('Assuming to be the only active server - assigning as leader')
            with self.lock:
                self.is_leader = True
                self.participant = False
                self.lcr_ongoing = False

    def lcr(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as lcr_listener_socket:
            lcr_listener_socket.bind((IP_ADDRESS, LCR_PORT))
            while not self.shutdown_event.is_set():
                data, address = lcr_listener_socket.recvfrom(BUFFER_SIZE)
                with self.lock:
                    election_message = json.loads(data.decode())

                    if election_message['isLeader']:
                        leader_ip_address = election_message['mid']
                        if leader_ip_address != IP_ADDRESS:
                            logger.info(f'{IP_ADDRESS}: Leader was elected! {election_message["mid"]}')
                            lcr_listener_socket.sendto(json.dumps(election_message).encode(),
                                                       (self.direct_neighbour, LCR_PORT))
                            self.leader_ip_address = leader_ip_address
                            self.is_leader = False
                        self.participant = False
                        self.lcr_ongoing = False

                    elif election_message['mid'] < IP_ADDRESS and not self.participant:
                        new_election_message = {"mid": IP_ADDRESS, "isLeader": False}
                        self.participant = True
                        lcr_listener_socket.sendto(json.dumps(new_election_message).encode(),
                                                   (self.direct_neighbour, LCR_PORT))

                    elif election_message['mid'] > IP_ADDRESS:
                        self.participant = False
                        lcr_listener_socket.sendto(json.dumps(election_message).encode(),
                                                   (self.direct_neighbour, LCR_PORT))
                    elif election_message['mid'] == IP_ADDRESS:
                        new_election_message = {"mid": IP_ADDRESS, "isLeader": True}
                        lcr_listener_socket.sendto(json.dumps(new_election_message).encode(),
                                                   (self.direct_neighbour, LCR_PORT))
                        self.leader_ip_address = IP_ADDRESS
                        self.is_leader = True
                        self.participant = False
                        self.lcr_ongoing = False
                        logger.info(f'Current node won leader election! {IP_ADDRESS} (sent message to {self.direct_neighbour}) )')
                    else:
                        logger.warning(f'Unexpected event occurred in LCR {election_message}')

    # ------------ DATA REPLICATION BETWEEN SERVERS ------------ multicast

    def handle_leader_update(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_socket:
                server_socket.bind(('', MULTICAST_PORT_SERVER))
                group = socket.inet_aton(MULTICAST_GROUP_ADDRESS)
                mreq = struct.pack('4sL', group, socket.INADDR_ANY)
                server_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
                server_socket.settimeout(1)
                while not self.shutdown_event.is_set():
                    try:
                        if not self.is_leader:
                            data, addr = server_socket.recvfrom(MULTICAST_BUFFER_SIZE)
                            if addr[0] == self.leader_ip_address:
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
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as client_socket:
                    client_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, MULTICAST_TTL)

                    message = json.dumps({"chat_rooms": self.chat_rooms}).encode()
                    client_socket.sendto(message, (MULTICAST_GROUP_ADDRESS, MULTICAST_PORT_SERVER))
                    logger.info(f'Sent leader update for chat rooms ({self.chat_rooms})')
            except socket.error as e:
                logger.error('Socket error occurred in send_leader_update: %s', e)
            except Exception as e:
                logger.error('An error occurred: %s', e)

    # ------------ FAULT TOLERANCE SERVER CRASH ------------ multicast

    def handle_leader_heartbeat(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as heartbeat_server_socket:
            heartbeat_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            heartbeat_server_socket.bind(('', HEARTBEAT_PORT_SERVER))

            group = socket.inet_aton(MULTICAST_GROUP_ADDRESS)
            mreq = struct.pack('4sL', group, socket.INADDR_ANY)
            heartbeat_server_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            heartbeat_server_socket.settimeout(2)

            try:
                while not self.shutdown_event.is_set():
                    if self.is_leader:
                        self.send_leader_heartbeat()
                        continue
                    try:
                        data, addr = heartbeat_server_socket.recvfrom(MULTICAST_BUFFER_SIZE)
                        if data.decode() == 'HEARTBEAT':
                            logger.debug(f'Received heartbeat from leader server at {addr}')
                            if not self.leader_ip_address:
                                self.leader_ip_address = addr[0]
                            with self.lock:
                                self.last_message_from_leader_ts = time.time()
                    except socket.timeout:
                        continue
                    except socket.error as e:
                        logger.error('Socket error occurred while receiving heartbeat: %s', e)
                    except Exception as e:
                        logger.error('Unexpected error occurred: %s', e)
            finally:
                logger.info('Shutting down heartbeat listener')

    def send_leader_heartbeat(self):
        heartbeat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        heartbeat_client_socket.settimeout(1)
        try:
            logger.debug('sending heartbeat')
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

    def handle_broadcast_client_requests(self): #broadcast
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as listener_socket:
                listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener_socket.bind(('', BROADCAST_PORT_CLIENT))
                listener_socket.settimeout(1)

                while not self.shutdown_event.is_set():
                    if self.is_leader:
                        try:
                            msg, client_address = listener_socket.recvfrom(BUFFER_SIZE)
                            logger.debug(f"Server discovery request by {client_address}")

                            response_message = 'hello'.encode()
                            listener_socket.sendto(response_message, client_address)
                            logger.debug(f'Sent server hello to client: {client_address}')
                        except socket.timeout:
                            continue
                        except Exception as e:
                            logger.error(f"Error handling broadcast client request: {e}")
        except Exception as e:
            logger.error(f"Failed to open Socket for handling client Broadcast requests: {e}")

    def handle_send_message_request(self): #tcp unicast
        while not self.shutdown_event.is_set():
            if self.is_leader:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    server_socket.bind((IP_ADDRESS, TCP_CLIENT_PORT))
                    server_socket.listen()

                    client_socket, addr = server_socket.accept()
                    client_addr = addr[0]
                    with client_socket:
                        try:
                            data = client_socket.recv(BUFFER_SIZE)
                            client_response_msg = ''
                            if data:
                                json_data = json.loads(data.decode('UTF-8'))
                                logger.info(f"received message from client {json_data}")
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
                                client_socket.sendall(client_response_msg.encode('UTF-8', errors='replace'))
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
        msg = "User is not assigned to any chat room"
        if active_chat_id:
            self.chat_rooms[active_chat_id].remove(client_addr)
            chat_leave_message = f'Participant {client_addr} left the chat room'
            self.forward_message_to_chat_participants(active_chat_id, chat_leave_message, "SYSTEM")
            self.send_leader_update()
            msg = "Successfully left the chat room"

            if not self.chat_rooms[active_chat_id]:
                self.chat_rooms.pop(active_chat_id)
                msg = "Chat room has been closed as the last user left"

        return msg

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

    def forward_message_to_chat_participants(self, chat_id, msg, sender): #multicast
        client_multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        client_multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, MULTICAST_TTL)
        send_message = f'{sender}: {msg}'.encode('UTF-8')

        try:
            for client_addr in self.chat_rooms[chat_id]:
                client_multicast_socket.sendto(send_message, (client_addr, MULTICAST_PORT_CLIENT))
        except Exception as e:
            logger.error(f"Error sending message to chat participants: {e}")
        finally:
            client_multicast_socket.close()

    #  -------------------------------------- EOF --------------------------------------

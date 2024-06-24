import socket
import struct
import threading

BROADCAST_PORT_SERVER = 65432  # port to open to receive and send server discovery requests
RESPONSE_MESSAGE = b"Server Response"  # message to send back as a response
BROADCAST_ADDRESS = '255.255.255.255'
MULTICAST_GROUP_ADDRESS = '239.0.0.1'
HEARTBEAT_PORT_SERVER = 50570  # port for incoming / outgoing heartbeat (leader)

def listen_for_broadcasts():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as listener_socket:
        listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener_socket.bind(('', BROADCAST_PORT_SERVER))

        print(f"Listening for broadcasts on port {BROADCAST_PORT_SERVER}")

        while True:
            msg, addr = listener_socket.recvfrom(1024)
            print(f"Received message from {addr}: {msg}")

            # Respond to the broadcast message
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as response_socket:
                response_socket.sendto(RESPONSE_MESSAGE, addr)
                print(f"Sent response to {addr}")


def handle_leader_heartbeat():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as heartbeat_server_socket:
        heartbeat_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        heartbeat_server_socket.bind(('', HEARTBEAT_PORT_SERVER))

        group = socket.inet_aton(MULTICAST_GROUP_ADDRESS)
        mreq = struct.pack('4sL', group, socket.INADDR_ANY)
        heartbeat_server_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        heartbeat_server_socket.settimeout(2)

        try:
            while True:
                try:
                    data, addr = heartbeat_server_socket.recvfrom(1024)
                    if data.decode() == 'HEARTBEAT':
                        print(f'Received heartbeat from leader server at {addr}')
                except socket.timeout:
                    continue
        finally:
            print('Shutting down heartbeat listener')

def send_broadcast_to_search_for_servers():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as broadcast_server_discovery_socket:
        broadcast_server_discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        msg = 'hello_world'.encode()
        broadcast_server_discovery_socket.sendto(msg, (BROADCAST_ADDRESS, 65431))

def send_tcp():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.settimeout(4)
            try:
                client_socket.connect((172, TCP_SERVER_PORT))
                client_socket.sendall(json_message.encode('utf-8'))

                data = client_socket.recv(1024)
                print(data.decode('utf-8'))
            except socket.error as e:
                print(f'Socket error: {e}')
    except Exception as e:
        logger.error(f'Error in send_message_to_server: {e}')

if __name__ == '__main__':
    send_broadcast_to_search_for_servers()
    # Start listening for broadcasts in a separate thread
    #listener_thread = threading.Thread(target=listen_for_broadcasts)
    #listener_thread.daemon = True
    #listener_thread.start()

    # Keep the main thread running to keep the listener thread alive
    #try:
    #    while True:
    #        pass
    #except KeyboardInterrupt:
    #    print("Server shutting down.")

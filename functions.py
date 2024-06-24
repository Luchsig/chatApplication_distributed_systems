import socket
import os
import psutil

def get_ip_adress():
    try:
        # Get os
        os_hardware = os.name

        if os_hardware == "nt":
            interface = "Ethernet"
            index = 1
        elif os_hardware == "posix":
            interface = "eth0"
            index = 0

        # Get interfaces
        addresses = psutil.net_if_addrs()

        for intface, addr_list in addresses.items():
            # Search for interface
            if intface == interface:
                ip_adress = addr_list[index][1]
                return ip_adress
    except:
        return None

def get_broadcast_ip():
    try:
        # Resolve the IP address
        ip_address = get_ip_adress()
        
        # Get the network mask
        mask = socket.inet_ntoa(socket.inet_aton('255.255.255.0'))
        
        # Calculate the broadcast IP
        ip_parts = list(map(int, ip_address.split('.')))
        mask_parts = list(map(int, mask.split('.')))
        broadcast_parts = [ip_parts[i] | (~mask_parts[i] & 255) for i in range(4)]
        broadcast_ip = '.'.join(map(str, broadcast_parts))
        
        return broadcast_ip
    except:
        return None
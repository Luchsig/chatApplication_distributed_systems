import socket
import os
import psutil

'''
    File is used for demo day to get the internet connection 
'''
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
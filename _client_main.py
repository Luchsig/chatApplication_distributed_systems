import sys
import threading
from client import Client

if __name__ == '__main__':
    client = Client()
    client.start_client()


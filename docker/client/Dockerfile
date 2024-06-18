FROM python:3
ADD client.py client.py
ADD _client_main.py _client_main.py
EXPOSE 50550/udp 65431/udp 50510/tcp
ENTRYPOINT ["python3", "_client_main.py"]
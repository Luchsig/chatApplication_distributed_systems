FROM python:3
ADD server.py server.py
ADD _server_main.py _server_main.py
EXPOSE 65431/udp 65432/udp 50510/tcp 50560/udp 50600/udp 50570/udp
ENTRYPOINT ["python3", "_server_main.py"]

This repo aims to create a distributed client-server application.

For test purposes Docker can be used. To install docker follow this link:
https://docs.docker.com/engine/install/

You can find Docker files for creating the Docker Images under /docker/<server | client>/.

Execute them by using to the command in CLI:
docker build -f Dockerfile . -t server-application
docker build -f Dockerfile . -t client-application

After building the images server contaiers can be started using the docker compose files in /docker/server 
by using the command:
docker compose -f scaling-server_compose.yml up
docker compose up

To start the Clients in an interactive mode, execute the following command:
docker run -it --name <container_name> \                   
  --network server_application_distributed_systems_test_network \
  -p <PORT>:50550/udp \
  -p <PORT>:65431/udp \
  -p <PORT>:50510 \
  client-application \
  python3 _client_main.py

e.g.
docker run -it --name client1 \                   
  --network server_application_distributed_systems_test_network \
  -p 50420:50550/udp \
  -p 65435:65431/udp \
  -p 50519:50510 \
  client-application \
  python3 _client_main.py

Define the port mappings as you prefer them to be.

MIT License

Copyright (c) 2024 David Weißenberger

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
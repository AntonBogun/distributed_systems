# 1. Features
- [Done] DNS: Master can sign up its IP + port and Data node can connect to get Master's IP and port
- [Done] Heartbeat and Hearbeat_ACK
- [Done] Client uploads/downloads file: Client first asks DNS for Master IP, then asks Master for Data node's IP then connects to that Data node

# 2. System initialization

Currently they are run on the same machine. Do in following orders:

0. Compile
> make

1. Initialize DNS
> ./bin/main -mode dns

2. Initialize Master
> ./bin/main -mode master -p 52340

3. Initialize Data
> ./bin/main -mode data -p 52345


# 3. Steps

## 3.1. Client uploads file
Run command:
> ./bin/main -mode client -p 52350 --upload monument.jpg

## 3.2. Client downloads file
Run command:
> ./bin/main -mode client -p 52350 --download monument.jpg

Note that, client must be upload before downloading that file
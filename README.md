Compile with 

`g++ -std=c++17 -o main main.cpp -Wall -Wextra -Wshadow`

# 1. Features
- [Done] DNS: Master can sign up its IP + port and Data node can connect to get Master's IP and port
- [Done] Heartbeat and Hearbeat_ACK


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

3. Client
> ./bin/main -mode client -p 52350 --upload monument.jpg



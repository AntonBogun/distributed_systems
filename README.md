Compile with 

`g++ -std=c++17 -o main main.cpp -Wall -Wextra -Wshadow`

# 1. Features
- DNS
- Heartbeat from Master to Data

- TODO: Server maintains a map of available Data nodes and only sends heartbeat to these nodes

# 2. System initialization

Currently they are run on the same machine. Do in following orders:

0. Compile
> make

1. Initialize DNS
> ./bin/main -mode dns

2. Initialize Master
> ./bin/main -mode master

3. Initialize Data
> ./bin/main -mode data

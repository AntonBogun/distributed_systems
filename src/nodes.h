#pragma once
#include "transmission.h"
#include "packets.h"

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>
#include <bits/stdc++.h>
#include <queue>

namespace distribsys{


class Node{
    public:
    socket_address dns_address;
    Node(socket_address dns_address_):dns_address(dns_address_){}
};
class Server: public Node
{
public:
    std::queue<Packet> packets;
    std::mutex lockPackets;

    ServerSocket server_socket;

    Server(socket_address dns_address_, in_port_t port): Node(dns_address_), server_socket(port) {}

    void listen() {
        while (true)
        {
            OpenSocket open_socket(SERVER);
            open_socket.accept_connection(server_socket.get_socket_fd());

            TransmissionLayer tl(open_socket);
            tl.initialize_recv();

            // Parse incoming stream to packet
            // TODO: HoangLe [Nov-20]: Continue parsing
            Packet packet = Packet();
            u8 byte;
            tl.read_byte(byte);
            PACKET_ID packeID = static_cast<PACKET_ID>(byte);

            int payloadSize;
            tl.read_i32(payloadSize);
            switch (packeID)
            {
            case HEARTBEAT:
                std::cout << "== HL: " << "payloadSize = " << payloadSize << std::endl;
                break;
            
            default:
                break;
            }

            // Add parsed packet to queue
            std::unique_lock<std::mutex> lock(lockPackets);
            packets.push(packet);
            std::lock_guard<std::mutex> lock(lockPackets);
        }
    }
};
class DataNode: public Server
{
public:
    socket_address master_address; 
    DataNode(socket_address dns_address_, in_port_t port, socket_address master_address_):
    Server(dns_address_, port), master_address(master_address_){
        
    }
};

// ================================================================
struct DataNodeInfo
{
    // TODO: HoangLe [Nov-20]: Implement this: contain info about specific Data node
    int num_nonresponse_heartbeat;
};

class MasterNode : public DataNode
{
public:
    int durationHeartbeat;

    std::map<ipv4_addr, DataNodeInfo> infoDataNodes;
    std::mutex lockInfoDataNode;

    std::condition_variable cv;

    MasterNode(socket_address dns_address_, in_port_t port, int durationHeartbeat):
    DataNode(dns_address_, port, socket_address(0,0,0,0,0)),
    durationHeartbeat(durationHeartbeat)
    {
        std::thread listenThread(listen);
        std::thread miscThread(doMiscTasks);
        std::thread heartbeatThread(sendHeartBeat);

        listenThread.join();
        miscThread.join();
        heartbeatThread.join();
    }

    void doMiscTasks() {
        // 1. Notify DNS about the existence
        // TODO: HoangLe [Nov-19]: Implement this

        // 2. Notify the current MasterNode (if exist)
        // TODO: HoangLe [Nov-19]: Implement this
    }

    void sendHeartBeat() {
        std::this_thread::sleep_for(std::chrono::seconds(durationHeartbeat));

        std::unique_lock<std::mutex> lock(lockInfoDataNode);
        cv.wait(
            lock, 
            [] { 
                // TODO: HoangLe [Nov-20]: loop `infoDataNodes` and send heartbeat
            }
        );
        std::unique_lock<std::mutex> lock(lockInfoDataNode);
    }
};

// ================================================================

class DNS : public Server
{
public:
    DNS():Server(socket_address(0,0,0,0,0), DNS_PORT){}
};

// ================================================================

class Client
{
public:
    Client(/* args */);
    ~Client();
};

}//namespace distribsys
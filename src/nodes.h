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
    std::mutex mutexPackets;
    std::condition_variable cv;
    bool isPacketsReady = false;

    ServerSocket server_socket;

    Server(socket_address dns_address_, in_port_t port): Node(dns_address_), server_socket(port) {}

    void listen() {
        while (true)
        {
            std::cout << "==> HL: " << "Enter inside listen thread." << std::endl;

            OpenSocket open_socket(SERVER);
            open_socket.accept_connection(server_socket.get_socket_fd());

            TransmissionLayer tl(open_socket);
            tl.initialize_recv();

            // Parse incoming stream to packet
            u8 byte;
            tl.read_byte(byte);
            PACKET_ID packetID = static_cast<PACKET_ID>(byte);

            int payloadSize;
            tl.read_i32(payloadSize);
            std::vector<u8> payload(payloadSize, 0);

            for (int i = 0; i < payloadSize; ++i)
                tl.read_byte(payload[i]);

            Packet packet = Packet(packetID, payloadSize, payload);

            // Add parsed packet to queue
            std::lock_guard<std::mutex> lockGuard(mutexPackets);
            packets.push(packet);
            std::cout << "==> HL: " << "Added packet to 'packets' : " << packets.size() << std::endl;

            isPacketsReady = true;
            cv.notify_one();
        }
    }

    void consume() {
        while (true)
        {
            std::unique_lock<std::mutex> lock(mutexPackets);
            cv.wait(lock, [this] { return isPacketsReady; });


            while (packets.size() > 0)
            {
                Packet packet = packets.front();
                std::cout << "==> HL: " << "Consume packet : " << packet.packetID << std::endl;
                packets.pop();
            }
            
            isPacketsReady = false;
        }
    }
};
class DataNode: public Server
{
public:
    socket_address master_address; 
    DataNode(socket_address dns_address_, in_port_t port, socket_address master_address_):
    Server(dns_address_, port), master_address(master_address_){}

    void start(){
        std::thread listenThread(&DataNode::listen, this);
        std::thread consumeThread(&DataNode::consume, this);

        listenThread.join();
        consumeThread.join();
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
    std::mutex mutexInfoDataNode;

    MasterNode(socket_address dns_address_, in_port_t port, int durationHeartbeat):
    DataNode(dns_address_, port, socket_address(0,0,0,0,0)),
    durationHeartbeat(durationHeartbeat)
    {}

    void start(){
        std::thread listenThread(&MasterNode::listen, this);
        // std::thread miscThread(doMiscTasks);
        std::thread heartbeatThread(&MasterNode::sendHeartBeat, this);

        listenThread.join();
        // miscThread.join();
        heartbeatThread.join();
    }

    void doMiscTasks() {
        // 1. Notify DNS about the existence
        // TODO: HoangLe [Nov-19]: Implement this

        // 2. Notify the current MasterNode (if exist)
        // TODO: HoangLe [Nov-19]: Implement this
    }

    void sendHeartBeat() {
        while (true)
        {
            std::this_thread::sleep_for(std::chrono::seconds(durationHeartbeat));

            // std::cout << "==> HL: " << "After waiting in heartbeat' : " << std::endl;

            mutexInfoDataNode.lock();
        
            // TODO: HoangLe [Nov-20]: loop `infoDataNodes` and send heartbeat

            OpenSocket open_socket(CLIENT);
            socket_address addressDataNode(127, 0, 0, 1, 8081);
            open_socket.connect_to_server(addressDataNode);

            TransmissionLayer tl(open_socket);

            // Example to send bytes
            PACKET_ID packID = HEARTBEAT;
            int payloadSize = 0;

            tl.write_byte(packID);
            tl.write_i32(payloadSize);
            tl.finalize_send();

            mutexInfoDataNode.unlock();
        }
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
#pragma once
#include "transmission.h"
#include "packets.h"

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>
#include <bits/stdc++.h>
#include <queue>
#include <map>

namespace distribsys{

constexpr int MAX_NONRESPONSE_HEARTBEART = 2;

class Node{
    public:
    socket_address dns_address;
    Node(socket_address dns_address_):dns_address(dns_address_){}
};
class Server: public Node
{
public:
    ServerSocket server_socket;

    Server(socket_address dns_address_, in_port_t port): Node(dns_address_), server_socket(port) {}

    void start(){}
};
class DataNode: public Server
{
public:
    in_port_t port;             // the port the node is listenning
    std::condition_variable cv;

    std::queue<Packet> packets;
    std::mutex mutexPackets;
    bool isPacketsReady = false;

    socket_address addrMaster; 
    std::mutex mutexAddrMaster;
    bool isAddrMasterReady = false;

    std::vector<std::string> files;

    DataNode(socket_address dns_address_, in_port_t port):
    Server(dns_address_, port), port(port){}

    void start(){
        std::thread listenThread(&DataNode::listen, this);
        std::thread consumeThread(&DataNode::consume, this);
        std::thread miscThread(&DataNode::doMiscTasks, this);

        miscThread.join();
        listenThread.join();
        consumeThread.join();
    }

    void listen() {
        while (true)
        {
            // std::cout << "==> HL: " << "Enter inside listen thread." << std::endl;

            OpenSocket open_socket(SERVER);
            open_socket.accept_connection(server_socket.get_socket_fd());

            // Parse incoming stream to packet
            Packet packet = Packet(open_socket);

            // Add parsed packet to queue
            std::lock_guard<std::mutex> lockGuard(mutexPackets);
            packets.push(packet);
            // std::cout << "==> HL: " << "Added packet to 'packets' : " << packets.size() << std::endl;

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
                // std::cout << "==> HL: " << "Consume packet : " << static_cast<int>(packet.packetID) << std::endl;
                packets.pop();

                switch (packet.packetID)
                {
                    case HEARTBEAT: {
                        Packet packetHearbeatACK = Packet::compose_HEARTBEAT_ACK();
                        packetHearbeatACK.send(addrMaster);

                        break;
                    }

                    case ASK_IP_ACK: {
                        std::lock_guard<std::mutex> lockGuard(mutexAddrMaster);

                        addrMaster = packet.addrParsed;
                        isAddrMasterReady = true;
                        cv.notify_one();

                        std::cout << "==> HL: " << "DNS replies the current Master node address: " << socket_address_to_string(addrMaster) << std::endl;

                        break;
                    }
                }
            }
            
            isPacketsReady = false;
        }
    }

    void doMiscTasks() {
        // 1. Ask DNS about the socket of current Master
        Packet packetDNSAskMaster = Packet::compose_ASK_IP(port);
        packetDNSAskMaster.send(dns_address);

        // 2. Notify the current MasterNode (if exist)
        std::unique_lock<std::mutex> lock(mutexAddrMaster);
        cv.wait(lock, [this] { return isAddrMasterReady; });

        Packet packetNotifyMaster = Packet::compose_NOTIFY(NODE_TYPE::DATA, port);
        packetNotifyMaster.send(addrMaster);
        std::cout << "==> HL: " << "Data sent NOTIFY to Master: " << socket_address_to_string(addrMaster) << std::endl;
    }

};

// ================================================================
class DataNodeInfo
{
public:
    socket_address addr;
    NODE_TYPE typeNode;
    std::vector<std::string> files;

    int numNonRespHeartBeat;
    int numAvaiSlots;

    DataNodeInfo(NODE_TYPE typeNode, socket_address addr): typeNode(typeNode), addr(addr) {}
};

class FileInfo
{
public:
    std::string name;
    std::vector<DataNodeInfo *> nodesData;
};

class MasterNode : public DataNode
{
public:
    int durationHeartbeat;

    std::map<std::string, DataNodeInfo> infoDataNodes;
    bool isDataNodeAvailable = false;
    std::mutex mutexInfoDataNode;


    MasterNode(socket_address dns_address_, in_port_t port, int durationHeartbeat):
    DataNode(dns_address_, port), durationHeartbeat(durationHeartbeat)
    {}

    std::string convAddr2Str(socket_address &addr){
        return std::to_string(addr.ip.a)
            + "-" + std::to_string(addr.ip.b)
            + "-" + std::to_string(addr.ip.c)
            + "-" + std::to_string(addr.ip.d);
    }

    void start(){
        std::thread miscThread(&MasterNode::doMiscTasks, this);
        std::thread listenThread(&MasterNode::listen, this);
        std::thread consumeThread(&MasterNode::consume, this);
        std::thread heartbeatThread(&MasterNode::sendHeartBeat, this);

        miscThread.join();
        listenThread.join();
        heartbeatThread.join();
        consumeThread.join();
    }

    void consume() {
        while (true)
        {
            std::unique_lock<std::mutex> lock(mutexPackets);
            cv.wait(lock, [this] { return isPacketsReady; });

            while (packets.size() > 0)
            {
                Packet packet = packets.front();
                std::cout << "==> HL: " << "Consume packet : " << static_cast<int>(packet.packetID) << std::endl;
                packets.pop();

                switch (packet.packetID)
                {
                    case HEARTBEAT_ACK: {
                        std::cout << "==> HL: " << "Master receives HEARTBEAT_ACK from Data node at: " << socket_address_to_string(packet.addrSrc) << std::endl;

                        auto record = infoDataNodes.find(convAddr2Str(packet.addrSrc));
                        if (record != infoDataNodes.end()) {
                            DataNodeInfo& info = record->second;
                            
                            info.numNonRespHeartBeat--;
                        }

                        break;
                    }
                    case NOTIFY: {
                        std::cout << "==> HL: " << "Master receives NOTIFY from Data node at: " << socket_address_to_string(packet.addrSrc) << std::endl;

                        mutexInfoDataNode.lock();
                        socket_address addrDataNode = packet.addrSrc;
                        addrDataNode = packet.addrSrc;
                        addrDataNode.port = packet.addrParsed.port;

                        infoDataNodes.insert({convAddr2Str(addrDataNode), DataNodeInfo(NODE_TYPE::DATA, addrDataNode)});

                        isDataNodeAvailable = true;
                        mutexInfoDataNode.unlock();
                        break;
                    }
                }
            }
            
            isPacketsReady = false;
        }
    }

    void doMiscTasks() {
        // 1. Notify DNS about the existence
        Packet packetDNSNotify = Packet::compose_NOTIFY(NODE_TYPE::MASTER, port);
        packetDNSNotify.send(dns_address);

        // 2. Notify the current MasterNode (if exist)
        // TODO: HoangLe [Nov-19]: Implement this
    }

    void sendHeartBeat() {
        while (true)
        {
            std::this_thread::sleep_for(std::chrono::seconds(durationHeartbeat));

            std::cout << "==> HL: " << "Start sending heartbeat!" << std::endl;

            mutexInfoDataNode.lock();

            if (isDataNodeAvailable == true) {
                Packet packetHeartbeat = Packet::compose_HEARTBEAT();

                for (auto& [key, info]: infoDataNodes) {
                    info.numNonRespHeartBeat++;

                    if (info.numNonRespHeartBeat >= MAX_NONRESPONSE_HEARTBEART) {
                        std::cout << "==> HL: " << "Trigger Replication with: " << socket_address_to_string(info.addr) << std::endl;
                    }
                    else {
                        std::cout << "==> HL: " << "Sent heartbeat to: " << socket_address_to_string(info.addr) << std::endl;

                        try {
                            packetHeartbeat.send(info.addr);
                        }
                        catch (const std::runtime_error& err) {
                            std::cout << "==> HL: " << "Cannot connect to: " << socket_address_to_string(info.addr) << std::endl;
                        }
                    }
                }

            }
            mutexInfoDataNode.unlock();
        }
    }

    void replicate() {
        // TODO: HoangLe [Nov-26]: Implement this
    }
};

// ================================================================

class DNS : public Server
{
    socket_address addrCurMaster;
public:
    DNS():Server(socket_address(0,0,0,0,0), DNS_PORT){}

    void start() {
        while (true)
        {
            OpenSocket open_socket(SERVER);
            open_socket.accept_connection(server_socket.get_socket_fd());

            Packet packet = Packet(open_socket);
            switch (packet.packetID)
            {
                case NOTIFY: {
                    addrCurMaster = packet.addrSrc;
                    addrCurMaster.port = packet.addrParsed.port;

                    std::cout << "==> HL: " << "Signed up new Master at: " << socket_address_to_string(addrCurMaster) << std::endl;

                    break;
                }

                case ASK_IP: {
                    Packet packetReply =  Packet::compose_ASK_IP_ACK(addrCurMaster);
                    packet.addrSrc.port = packet.addrParsed.port;
                    packetReply.send(packet.addrSrc);

                    break;
                }
            }
            
        }
    }
};

// ================================================================

class Client
{
public:
    Client(/* args */);
    ~Client();
};

}//namespace distribsys
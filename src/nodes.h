#pragma once
#include "transmission.h"
#include "packets.h"

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>
#include <bits/stdc++.h>
#include<fstream>
#include <queue>
#include <map>

namespace distribsys{

constexpr int MAX_NONRESPONSE_HEARTBEART = 2;
const std::string DIR_ROOT_DATA = "data/";

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
            OpenSocket open_socket(SERVER);
            open_socket.accept_connection(server_socket.get_socket_fd());

            // Parse incoming stream to packet
            Packet packet = Packet(open_socket);

            // Add parsed packet to queue
            std::lock_guard<std::mutex> lockGuard(mutexPackets);
            packets.push(packet);

            isPacketsReady = true;
            cv.notify_one();
        }
    }

    void writeFile(std::string &name, BYTES &data) {
        std::ofstream wf(DIR_ROOT_DATA + name, std::ios::out | std::ios::binary);

        throw_if(!wf, "Err: Cannot open file: " + name);

        wf.write((char *)data.data(), data.size());

        wf.close();
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

                    case CLIENT_UPLOAD: {
                        std::cout << "==> HL: " << "Client sent file with name: '" << packet.fileName << "' and size = " << packet.binary.size() << std::endl;

                        // Write file
                        writeFile(packet.fileName, packet.binary);

                        // Send ACK back to Master
                        Packet::compose_CLIENT_REQUEST_ACK(REQUEST::UPLOAD, packet.fileName).send(addrMaster);

                        break;
                    }

                    case REQUEST_FROM_CLIENT: {
                        // Read file
                        std::string path = DIR_ROOT_DATA + packet.fileName;
                        std::ifstream rf(path, std::ios::in | std::ios::binary);
                        throw_if(!rf, "Err: Cannot open file: " + packet.fileName);
                        
                        std::vector<u8> binary((std::istreambuf_iterator<char>(rf)), (std::istreambuf_iterator<char>()));
                        rf.close();

                        // Send packet with binary
                        socket_address addrClient = packet.addrSrc;
                        addrClient = packet.addrSrc;
                        addrClient.port = packet.addrParsed.port;

                        Packet::compose_DATANODE_SEND_DATA(packet.fileName, binary).send(addrClient);

                        // Send ACK back to Master
                        Packet::compose_CLIENT_REQUEST_ACK(REQUEST::DOWNLOAD, packet.fileName).send(addrMaster);

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
    int numAvaiSlots = 100;

    DataNodeInfo(NODE_TYPE typeNode, socket_address addr): typeNode(typeNode), addr(addr) {}
};

class FileInfo
{
public:
    std::string name;
    std::vector<DataNodeInfo *> nodesData;

    FileInfo(DataNodeInfo *node){
        nodesData.push_back(node);
    }

    void addDataNode(DataNodeInfo *node){
        nodesData.push_back(node);
    }
};

class MasterNode : public DataNode
{
public:
    int durationHeartbeat;

    std::map<std::string, DataNodeInfo> infoDataNodes;
    bool isInfoDataNodeAvailable = false;
    std::mutex mutexInfoDataNode;

    std::map<std::string, FileInfo> infoFiles;

    MasterNode(socket_address dns_address_, in_port_t port_, int durationHeartbeat_):
    DataNode(dns_address_, port_), durationHeartbeat(durationHeartbeat_)
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

    DataNodeInfo* findBestDataNode(){
        int bestAvaiSlot = 0;
        DataNodeInfo *bestDataNode = nullptr;

        for (auto& [key, info]: infoDataNodes) {
            std::cout << "==> HL: " << "Loop: " << key << std::endl;

            if (bestAvaiSlot < info.numAvaiSlots){
                bestAvaiSlot = info.numAvaiSlots;
                bestDataNode = &info;
            }
        }

        return bestDataNode;
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

                        mutexInfoDataNode.lock();

                        auto record = infoDataNodes.find(convAddr2Str(packet.addrSrc));
                        if (record != infoDataNodes.end()) {
                            DataNodeInfo& info = record->second;
                            
                            info.numNonRespHeartBeat--;
                        }

                        isInfoDataNodeAvailable = true;
                        mutexInfoDataNode.unlock();

                        break;
                    }
                    case NOTIFY: {
                        std::cout << "==> HL: " << "Master receives NOTIFY from Data node at: " << socket_address_to_string(packet.addrSrc) << std::endl;

                        socket_address addrDataNode = packet.addrSrc;
                        addrDataNode = packet.addrSrc;
                        addrDataNode.port = packet.addrParsed.port;
                        infoDataNodes.insert({convAddr2Str(addrDataNode), DataNodeInfo(NODE_TYPE::DATA, addrDataNode)});

                        isInfoDataNodeAvailable = true;
                        mutexInfoDataNode.unlock();
                        break;
                    }

                    case CLIENT_REQUEST_ACK: {
                        std::cout << "==> HL: " << "Master receives CLIENT_REQUEST_ACK from Data node at: " << socket_address_to_string(packet.addrSrc) << std::endl;
                        std::cout << "==> HL: " << "filename: " << packet.fileName << std::endl;
                        std::cout << "==> HL: " << "request: " << packet.request << std::endl;

                        switch (packet.request)
                        {
                            case REQUEST::UPLOAD: {
                                std::string fileName = packet.fileName;
                                // TODO: HoangLe [Nov-27]: Trigger replication with file packet.fileName 
                                // Ideas: 
                                //  1. use method findBestDataNode() to find best suitable node
                                //  2. create packet and send it to the node holding the file to be replicated
                                //  3. at the data node holding the file, send the packet embedded the file name and binary to the destined data node 
                                //  4. both data nodes send ACK back to master

                                break;
                            }

                            case REQUEST::DOWNLOAD : {
                                // DO nothing
                                break;
                            }
                            
                            default:
                                break;
                        }

                        break;
                    }

                    case REQUEST_FROM_CLIENT: {
                        socket_address addrClient = packet.addrSrc;
                        addrClient = packet.addrSrc;
                        addrClient.port = packet.addrParsed.port;

                        switch (packet.request)
                        {
                            case REQUEST::DOWNLOAD: {
                                auto info = infoFiles.find(packet.fileName);
                                if (info == infoFiles.end()) {
                                    // File not exist, refuse downloading
                                    std::cout << "==> HL: " << "Cannot find Data node containing file '" << packet.fileName << std::endl;

                                    Packet::compose_RESPONSE_NODE_IP().send(addrClient);
                                } else {
                                    DataNodeInfo *nodeInfo = info->second.nodesData[0];
                                    std::cout << "==> HL: " << "Found Data node containing file '" << packet.fileName << "' : " << socket_address_to_string(nodeInfo->addr) << std::endl;
                                    std::cout << "==> HL: " << "Send Data node's IP to '" << socket_address_to_string(addrClient) << std::endl;

                                    Packet::compose_RESPONSE_NODE_IP(nodeInfo->addr).send(addrClient);
                                }

                                break;
                            }

                            case REQUEST::UPLOAD: {
                                auto info = infoFiles.find(packet.fileName);
                                if (info == infoFiles.end()) {
                                    // File not exist, allow write

                                    DataNodeInfo *bestDataNode = findBestDataNode();
                                    if (bestDataNode == nullptr) {
                                        // Cannot find suitable Data Node to store file

                                        std::cout << "==> HL: " << "Cannot find suitable Data node" << std::endl;

                                        Packet::compose_RESPONSE_NODE_IP().send(addrClient);
                                    } else {
                                        std::cout << "==> HL: " << "Found suitable Data node: " << socket_address_to_string(bestDataNode->addr) << std::endl;

                                        // Found suitable Data node, insert new entry
                                        infoFiles.insert({packet.fileName, FileInfo(bestDataNode)});

                                        Packet::compose_RESPONSE_NODE_IP(bestDataNode->addr).send(addrClient);
                                    }

                                } else {
                                    // File exists -> refuse
                                    std::cout << "==> HL: " << "File existed: '" << packet.fileName << "'" << std::endl;
                                    Packet::compose_RESPONSE_NODE_IP().send(addrClient);
                                }

                                break;
                            }

                            default:
                                break;
                        }

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

            if (isInfoDataNodeAvailable == true) {
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

                    std::cout << "==> HL: " << "Reply IP: " << socket_address_to_string(packet.addrSrc) << std::endl;

                    packetReply.send(packet.addrSrc);

                    break;
                }
            }
            
        }
    }
};

// ================================================================

class Client : public DataNode
{
public:
    socket_address addrData;
    std::mutex mutexAddrData;
    bool isAddrDataReady = false;

    Client(socket_address dns_address, in_port_t port):
    DataNode(dns_address, port)
    {}

    void consume() {
        while (true)
        {
            std::unique_lock<std::mutex> lock(mutexPackets);
            cv.wait(lock, [this] { return isPacketsReady; });

            while (packets.size() > 0)
            {
                Packet packet = packets.front();
                packets.pop();

                switch (packet.packetID)
                {
                    case ASK_IP_ACK: {
                        std::lock_guard<std::mutex> lockGuard(mutexAddrMaster);

                        addrMaster = packet.addrParsed;
                        isAddrMasterReady = true;
                        cv.notify_one();

                        std::cout << "==> HL: " << "DNS replies the current Master node address: " << socket_address_to_string(addrMaster) << std::endl;

                        break;
                    }

                    case RESPONSE_NODE_IP: {
                        std::lock_guard<std::mutex> lockGuard(mutexAddrData);

                        addrData = packet.addrParsed;
                        isAddrDataReady = true;
                        cv.notify_one();

                        std::cout << "==> HL: " << "Master replies the Data node address: " << socket_address_to_string(addrData) << std::endl;

                        break;
                    }

                    case DATANODE_SEND_DATA: {
                        std::cout << "==> HL: " << "Data node sends data for file: " << packet.fileName << std::endl;


                        std::string path = "received_" + packet.fileName;
                        writeFile(path, packet.binary);

                        std::exit(0);
                        break;
                    }
                }
            }
            
            isPacketsReady = false;
        }
    }

    void uploadFile(std::string nameFile) {
        std::thread listenThread(&Client::listen, this);
        std::thread consumeThread(&Client::consume, this);

        // 0. Read binary
        std::ifstream rf(nameFile, std::ios::in | std::ios::binary);
        throw_if(!rf, "Err: Cannot open file: " + nameFile);
        
        std::vector<u8> binary((std::istreambuf_iterator<char>(rf)), (std::istreambuf_iterator<char>()));
        rf.close();

        // 1. Connect to DNS to get current Master's ip and port
        Packet packetDNSAskMaster = Packet::compose_ASK_IP(port);
        packetDNSAskMaster.send(dns_address);

        // 2. Connect to current Master
        std::unique_lock<std::mutex> lock(mutexAddrMaster);
        cv.wait(lock, [this] { return isAddrMasterReady; });
        
        Packet packetAskFile = Packet::compose_ASK_READ_WRITE(REQUEST::UPLOAD, nameFile, port);
        packetAskFile.send(addrMaster);
    
        // 3. Connect to assigned Data node
        std::unique_lock<std::mutex> lock2(mutexAddrData);
        cv.wait(lock2, [this] { return isAddrDataReady; });

        if (addrData.port == 0) {
            std::cout << "==> HL: " << "No Data node available to upload file. Terminating..." << std::endl;
            std::exit(0);
        } else {    
            std::cout << "==> HL: " << "Found suitable Data node available to upload file" << std::endl;

            Packet packetBinary = Packet::compose_CLIENT_UPLOAD(nameFile, binary);
            packetBinary.send(addrData);

            std::exit(0);
        }

        listenThread.join();
        consumeThread.join();
    }

    void downloadFile(std::string nameFile){
        std::thread listenThread(&Client::listen, this);
        std::thread consumeThread(&Client::consume, this);

        // 1. Connect to DNS to get current Master's ip and port
        Packet packetDNSAskMaster = Packet::compose_ASK_IP(port);
        packetDNSAskMaster.send(dns_address);

        // 2. Connect to current Master to ask the Data node holding file
        std::unique_lock<std::mutex> lock(mutexAddrMaster);
        cv.wait(lock, [this] { return isAddrMasterReady; });
        
        Packet packetAskFile = Packet::compose_ASK_READ_WRITE(REQUEST::DOWNLOAD, nameFile, port);
        packetAskFile.send(addrMaster);

        // 3. Connect to assigned Data node
        std::unique_lock<std::mutex> lock2(mutexAddrData);
        cv.wait(lock2, [this] { return isAddrDataReady; });

        if (addrData.port == 0) {
            std::cout << "==> HL: " << "No Data node storing the neeed file. Terminating..." << std::endl;
            std::exit(0);
        } else {    
            std::cout << "==> HL: " << "Found Data node having needed file" << std::endl;

            Packet packetBinary = Packet::compose_ASK_READ_WRITE(REQUEST::DOWNLOAD, nameFile, port);
            packetBinary.send(addrData);
        }

        listenThread.join();
        consumeThread.join();
    }

};


}//namespace distribsys
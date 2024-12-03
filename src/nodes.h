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
struct connection_stats{
    bool is_outgoing;
    int fd;
    socket_address other_addr;
    in_port_t local_port;
    std::thread& thread;
};
#define continue_on_err(x) \
    {                           \
        if (x)                  \
        {                       \
            continue;           \
        }                       \
    }

class Server: public Node
{
public:
    ServerSocket server_socket;
    std::mutex connections_mutex;
    std::unordered_map<connection_info, connection_stats> connections;
    Server(socket_address dns_address_, in_port_t port): Node(dns_address_), server_socket(port)
    {}//server_socket constructor already starts listening

    void start() {
        while (true)
        {
            OpenSocket open_socket(SERVER);
            continue_on_err(open_socket.accept_connection(server_socket.get_socket_fd()));

            Packet packet = Packet(open_socket);
            switch (packet.packetID)
            {
                case NOTIFY: {
                    addrCurMaster = packet.addrSrc;
                    addrCurMaster.port = packet.addrParsed.port;

                    norm_log("Signed up new Master at: " + socket_address_to_string(addrCurMaster));

                    break;
                }

                case ASK_IP: {
                    Packet packetReply =  Packet::compose_ASK_IP_ACK(addrCurMaster);
                    packet.addrSrc.port = packet.addrParsed.port;

                    norm_log("Reply IP: " + socket_address_to_string(packet.addrSrc));

                    packetReply.send(packet.addrSrc);

                    break;
                }
            }
            
        }
    }
};
//MASTER/MASTER_BACKUP/DATA
enum node_role{
    MASTER,
    MASTER_BACKUP,
    DATA
};

// ================================================================

class DNS : public Server
{
public:
    DNS():Server(socket_address(0,0,0,0,0), DNS_PORT){}
    socket_address addrCurMaster;
    std::unordered_set<socket_address> nodes;

};
// ================================================================
class DataNode: public Server
{
public:
    node_role role;
    in_port_t port; // the exposed port the node is always listening on

    std::mutex m;
    std::condition_variable cv;

    socket_address addrMaster;
    bool isAddrMasterReady = false;

    std::vector<std::string> files;
    std::unordered_set<int> used_fds;

    DataNode(socket_address dns_address_, in_port_t port, node_role initial_role):
    Server(dns_address_, port), port(port), role(initial_role){}

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
                // norm_log("Consume packet : " + static_cast<int>(packet.packetID));
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

                        norm_log("DNS replies the current Master node address: " + socket_address_to_string(addrMaster));

                        break;
                    }

                    case CLIENT_UPLOAD: {
                        norm_log(prints_new("Client sent file with name: '",packet.fileName,"' and size = ",packet.binary.size()));

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
        norm_log("Data sent NOTIFY to Master: " + socket_address_to_string(addrMaster));
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
    double durationHeartbeat;

    std::map<std::string, DataNodeInfo> infoDataNodes;
    bool isInfoDataNodeAvailable = false;
    std::mutex mutexInfoDataNode;

    std::map<std::string, FileInfo> infoFiles;

    MasterNode(socket_address dns_address_, in_port_t port_, double durationHeartbeat_):
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
            norm_log("Loop: " + key);

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
                norm_log("Consume packet : " + static_cast<int>(packet.packetID));
                packets.pop();

                switch (packet.packetID)
                {
                    case HEARTBEAT_ACK: {
                        norm_log("Master receives HEARTBEAT_ACK from Data node at: " + socket_address_to_string(packet.addrSrc));

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
                        norm_log("Master receives NOTIFY from Data node at: " + socket_address_to_string(packet.addrSrc));

                        socket_address addrDataNode = packet.addrSrc;
                        addrDataNode = packet.addrSrc;
                        addrDataNode.port = packet.addrParsed.port;
                        infoDataNodes.insert({convAddr2Str(addrDataNode), DataNodeInfo(NODE_TYPE::DATA, addrDataNode)});

                        isInfoDataNodeAvailable = true;
                        mutexInfoDataNode.unlock();
                        break;
                    }

                    case CLIENT_REQUEST_ACK: {
                        norm_log("Master receives CLIENT_REQUEST_ACK from Data node at: " + socket_address_to_string(packet.addrSrc));
                        norm_log("filename: " + packet.fileName);
                        norm_log("request: " + packet.request);

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
                                    norm_log("Cannot find Data node containing file '" + packet.fileName);

                                    Packet::compose_RESPONSE_NODE_IP().send(addrClient);
                                } else {
                                    DataNodeInfo *nodeInfo = info->second.nodesData[0];
                                    norm_log("Found Data node containing file '" + packet.fileName + "' : " + socket_address_to_string(nodeInfo->addr));
                                    norm_log("Send Data node's IP to '" + socket_address_to_string(addrClient));

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

                                        norm_log("Cannot find suitable Data node");

                                        Packet::compose_RESPONSE_NODE_IP().send(addrClient);
                                    } else {
                                        norm_log("Found suitable Data node: " + socket_address_to_string(bestDataNode->addr));

                                        // Found suitable Data node, insert new entry
                                        infoFiles.insert({packet.fileName, FileInfo(bestDataNode)});

                                        Packet::compose_RESPONSE_NODE_IP(bestDataNode->addr).send(addrClient);
                                    }

                                } else {
                                    // File exists -> refuse
                                    norm_log("File existed: '" + packet.fileName + "'");
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
            std::this_thread::sleep_for(std::chrono::microseconds(static_cast<int>(durationHeartbeat * 1000000)));

            norm_log("Start sending heartbeat!");

            mutexInfoDataNode.lock();

            if (isInfoDataNodeAvailable == true) {
                Packet packetHeartbeat = Packet::compose_HEARTBEAT();

                for (auto& [key, info]: infoDataNodes) {
                    info.numNonRespHeartBeat++;

                    if (info.numNonRespHeartBeat >= MAX_NONRESPONSE_HEARTBEART) {
                        norm_log("Trigger Replication with: " + socket_address_to_string(info.addr));
                    }
                    else {
                        norm_log("Sent heartbeat to: " + socket_address_to_string(info.addr));

                        try {
                            packetHeartbeat.send(info.addr);
                        }
                        catch (const std::runtime_error& err) {
                            norm_log("Cannot connect to: " + socket_address_to_string(info.addr));
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

                        norm_log("DNS replies the current Master node address: " + socket_address_to_string(addrMaster));

                        break;
                    }

                    case RESPONSE_NODE_IP: {
                        std::lock_guard<std::mutex> lockGuard(mutexAddrData);

                        addrData = packet.addrParsed;
                        isAddrDataReady = true;
                        cv.notify_one();

                        norm_log("Master replies the Data node address: " + socket_address_to_string(addrData));

                        break;
                    }

                    case DATANODE_SEND_DATA: {
                        norm_log("Data node sends data for file: " + packet.fileName);


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
            norm_log("No Data node available to upload file. Terminating...");
            std::exit(0);
        } else {    
            norm_log("Found suitable Data node available to upload file");

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
            norm_log("No Data node storing the neeed file. Terminating...");
            std::exit(0);
        } else {    
            norm_log("Found Data node having needed file");

            Packet packetBinary = Packet::compose_ASK_READ_WRITE(REQUEST::DOWNLOAD, nameFile, port);
            packetBinary.send(addrData);
        }

        listenThread.join();
        consumeThread.join();
    }

};


}//namespace distribsys
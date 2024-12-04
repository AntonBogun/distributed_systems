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
#define continue_on_err(x) \
    {                           \
        if (x)                  \
        {                       \
            continue;           \
        }                       \
    }

struct connection_struct{
    connection_info connection_info;
    uptr<OpenSocket> open_socket;
    uptr<TransmissionLayer> tl;
    std::thread::id owner_thread_id;
    std::atomic<bool> request_close = false;
};
struct thread_struct{
    std::vector<sptr<connection_struct>> owned_connections;
    uptr<std::thread> thread;
    std::atomic<bool> request_close = false;
};
class Server: public Node
{
public:
    ServerSocket server_socket;

    std::mutex connections_mutex;
    //% must hold connections_mutex to access
    std::unordered_map<connection_info, sptr<connection_struct>> connections;
    //% must hold connections_mutex to access
    std::unordered_map<std::thread::id, sptr<thread_struct>> threads;
    //% intended to be used with connections_mutex
    std::condition_variable can_exit_cv;

    Server(socket_address dns_address_, in_port_t port): Node(dns_address_), server_socket(port)
    {}//server_socket constructor already starts listening

    void start() {
        while (true)
        {
            // OpenSocket open_socket(SERVER);
            uptr<OpenSocket> open_socket = std::make_unique<OpenSocket>(SERVER);
            continue_on_err(open_socket->accept_connection(server_socket.get_socket_fd()));
            
            connection_info connection_info(open_socket->local_address, open_socket->other_address);
            sptr<connection_struct> connection = std::make_shared<connection_struct>();
            connection->open_socket = std::move(open_socket);
            connection->connection_info = connection_info;
            {
                std::lock_guard<std::mutex> lock(connections_mutex);
                connections[connection_info] = connection;
                //% below makes a copy of sptr connection
                uptr<std::thread> thread = 
                std::make_unique<std::thread>(&Server::setup_thread, this, connection_info, connection);
                threads[thread->get_id()] = std::make_shared<thread_struct>();
                threads[thread->get_id()]->thread = std::move(thread);
                connection->owner_thread_id = thread->get_id();
                //% makes a copy of sptr connection
                threads[thread->get_id()]->owned_connections.push_back(connection);
                connection.reset();//% release the connection ref from this scope
            }
        }
    }
    //% do not use without lock, but can chain together without releasing the lock every time
    //! does not remove the connection from the thread's owned_connections
    inline void _remove_connection_no_lock(connection_info connection_info){
        connections.erase(connection_info);
    }
    //! does not remove the connection from the thread's owned_connections
    //* use _remove_connection_no_lock if you already have the lock or removing multiple connections
    void remove_connection(connection_info connection_info){
        std::lock_guard<std::mutex> lock(connections_mutex);
        _remove_connection_no_lock(connection_info);
    }
    //! should only be called from the thread being removed
    void remove_thread(std::thread::id thread_id){
        std::lock_guard<std::mutex> lock(connections_mutex);
        throw_if(not_in(thread_id, threads), "Thread not found in threads map");
        
        sptr<thread_struct> this_thread = threads[thread_id];
        for(auto &connection: this_thread->owned_connections){
            _remove_connection_no_lock(connection->connection_info);
            throw_if(connection.use_count() != 1,
            "Connection still has other owners "+std::to_string(connection.use_count()));
        }
        this_thread->owned_connections.clear();
        threads.erase(thread_id);
        throw_if(this_thread.use_count() != 1,
        "Thread still has other owners: "+std::to_string(this_thread.use_count()));
        this_thread.reset();
        can_exit_cv.notify_all();
    }
    //%notice that connection is not a reference to the sptr, it is a new sptr
    //*after handle_first_connection, the only owner of this_thread should be this function so it can safely delete itself
    void setup_thread(connection_info connection_info, sptr<connection_struct> connection){
        std::thread::id this_thread_id = std::this_thread::get_id();
        sptr<thread_struct> this_thread;
        {
            //lock to let the creator finish setting the thread into the map
            std::lock_guard<std::mutex> lock(connections_mutex);
            throw_if(not_in(this_thread_id, threads), "Thread not found in threads map");
            this_thread = threads[this_thread_id];//minor optimization could be to not discard not_in result
            throw_if(not_in(connection_info, connections), "Connection not found in connections map");
            throw_if(connection.get() != connections[connection_info].get(), "Connections mismatch");
            throw_if(connection.get() != this_thread->owned_connections.back().get(), "Connection stack mismatch");
            throw_if(this_thread_id != connection->owner_thread_id, "Thread id mismatch");
            //%from here assume that connections and threads are consistent
        }
        uptr<TransmissionLayer> tl = std::make_unique<TransmissionLayer>(*connection->open_socket);
        connection->tl = std::move(tl);
        connection.reset();//release the connection ref from this scope
        
        //% used to handle if recv failed here, effectively the connection wasn't established
        //* check definition of THROW_IF_RECOVERABLE
        //> also, tl->initialize_recv initializes the receive of the message, meaning that every connection from
        //> client -> server should always send a message from the client first
        THROW_IF_RECOVERABLE(tl->initialize_recv(), "Failed to initialize recv", 
            {
                //release this_thread
                this_thread.reset();
                remove_thread(this_thread_id); 
                return;
            }
        );
        
        handle_first_connection(this_thread);
        //* dont forget to clean up
        this_thread.reset();
        remove_thread(this_thread_id);
    }
    //> returning from this function will cause the thread to exit and close any owned connections
    virtual void handle_first_connection(sptr<thread_struct> this_thread) = 0;
    ~Server(){
        can_exit_cv.notify_all();
    }
};
//MASTER/MASTER_BACKUP/DATA
enum node_role{
    MASTER,
    MASTER_BACKUP,
    DATA
};

// ================================================================
//move from packets.h
enum PACKET_ID : u8
{
    REQUEST_OPEN_HEARTBEAT_CONNECTION=0, // master -> data nodes
    REQUEST_MASTER_ADDRESS=1,            // client -> dns
    SET_MASTER_ADDRESS=2,                // master -> both dns and data nodes
    REQUEST_LIST_OF_ALL_NODES=3,         // master -> dns
    EDIT_LIST_OF_ALL_NODES=4,            // master -> dns
    SET_DATA_NODE_AS_BACKUP=5,           // master -> data node
    CLIENT_CONNECTION_REQUEST=6,         // client -> both to master and data nodes
    DATA_NODE_TO_MASTER_CONNECTION_REQUEST=7, // data node -> master
    MASTER_NODE_TO_DATA_NODE_REPLICATION_REQUEST=8, // master -> data node
    DATA_NODE_TO_DATA_NODE_CONNECTION_REQUEST=9, // data node -> data node
    MASTER_TO_DATA_NODE_INFO_REQUEST=10,  // master -> data node
    MASTER_TO_DATA_NODE_PAUSE_CHANGE=11   // master -> data node
};


enum REQUEST : u8
{
    DOWNLOAD,
    UPLOAD
};
static_assert(sizeof(PACKET_ID) == 1, "Packet ID must be 1 byte");
static_assert(sizeof(REQUEST) == 1, "Request must be 1 byte");

// ================================================================

//% all below true if error
//> for a simple type like PACKET_ID just use read_type and write_type
// [[nodiscard]]
// inline bool read_packet_id(TransmissionLayer &tl, PACKET_ID &packet_id)
// {
//     u8 byte;
//     bool err=tl.read_byte(byte);
//     packet_id = static_cast<PACKET_ID>(byte);
//     return err;
// }
// [[nodiscard]]
// bool write_packet_id(TransmissionLayer &tl, PACKET_ID packet_id)
// {
//     return tl.write_byte(static_cast<u8>(packet_id));
// }
[[nodiscard]]
bool read_address(TransmissionLayer &tl, socket_address &addr)
{
    u32 ip;
    in_port_t port;
    if(tl.read_type(ip)||tl.read_type(port)) return true;
    addr = socket_address(ipv4_addr::from_u32(ip), port);
    return false;
}
[[nodiscard]]
bool write_address(TransmissionLayer &tl, socket_address addr)
{
    return tl.write_type(addr.ip.to_u32())||tl.write_type(addr.port);
}

// ================================================================
class DNS : public Server
{
public:
    DNS():Server(socket_address(0,0,0,0,0), DNS_PORT){}

    std::mutex mutexDNS;//* controls the below two variables
    socket_address addrCurMaster;
    std::unordered_set<socket_address> nodes;

    void handle_first_connection(sptr<thread_struct> this_thread) override {
        //> first, read the packet id
        PACKET_ID packet_id;
        THROW_IF_RECOVERABLE(
            read_packet_id(*this_thread->owned_connections.back()->tl, packet_id), "Failed to read packet id", return;);
        switch (packet_id){

        }
    }

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

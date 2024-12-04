#pragma once
#include "transmission.h"
#include "packets.h"

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>
#include <bits/stdc++.h>
#include <fstream>
#include <queue>
#include <map>
#include <random>

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
//> has addresses of both ends in connection_info, unique_ptr of OpenSocket and TransmissionLayer
//> also has the thread id of owner thread and a flag to request connection close
struct connection_struct{
    connection_info connection_info;
    uptr<OpenSocket> open_socket;
    uptr<TransmissionLayer> tl;
    std::thread::id owner_thread_id;
    std::atomic<bool> request_close = false;
};
//> has a vector of connections owned by the thread and the thread itself
//> also has a flag to request thread close
struct thread_struct{
    std::vector<sptr<connection_struct>> owned_connections;
    uptr<std::thread> thread;
    std::atomic<bool> request_close = false;
};
//> handles basic server things like .start() which starts listening for connections and spawns
//> new threads (via setup_thread) to handle them. Derived classes should implement handle_first_connection()
//> and may use remove_connection() for operations which span multiple connections
class Server: public Node
{
public:
    ServerSocket server_socket;

    //> for accepting on the server socket
    std::mutex accept_mutex;
    //> for accessing connections, threads
    std::mutex connections_data_mutex;
    //> must hold connections_data_mutex to access
    std::unordered_map<connection_info, sptr<connection_struct>> connections;
    //> must hold connections_data_mutex to access
    std::unordered_map<std::thread::id, sptr<thread_struct>> threads;

    //> used with finished_threads
    std::mutex finished_threads_mutex;
    //> not joining with finished threads is UB, so we keep track of them here
    std::vector<uptr<std::thread>> finished_threads;
    
    //> intended to be used with can_exit_cv
    std::mutex can_exit_mutex;
    //> used to wait until all threads have exited
    std::condition_variable can_exit_cv;


    //> used together with setup_cv to let things that must run after
    std::mutex setup_mutex;
    std::condition_variable setup_cv;

    Server(socket_address dns_address_, in_port_t port): Node(dns_address_), server_socket(port){
        //> lock setup_mutex, unlock at start
        setup_mutex.lock();
    }//server_socket constructor already starts listening
    void cleanup_finished_threads(){
        std::lock_guard<std::mutex> lock(finished_threads_mutex);
        for(auto &thread: finished_threads){
            thread->join();
        }
        finished_threads.clear();
    }
    void start() {
        setup_mutex.unlock();//> let e.g. _setup_master run
        while (true)
        {
            //> every loop check and clean up finished threads
            cleanup_finished_threads();

            uptr<OpenSocket> open_socket = std::make_unique<OpenSocket>(SERVER);

            {//> avoid accepting with multiple threads at once
                std::lock_guard<std::mutex> lock(accept_mutex);
                continue_on_err(open_socket->accept_connection(server_socket.get_socket_fd()));
            }
            
            connection_info connection_info(open_socket->local_address, open_socket->other_address);
            sptr<connection_struct> connection = std::make_shared<connection_struct>();
            connection->open_socket = std::move(open_socket);
            connection->connection_info = connection_info;
            {//> use connections_data_mutex because we are modifying connections and threads
                std::lock_guard<std::mutex> lock(connections_data_mutex);
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
        std::lock_guard<std::mutex> lock(connections_data_mutex);
        _remove_connection_no_lock(connection_info);
    }
    //! should only be called from the thread being removed
    //> first removes all owned connections, then removes the thread
    void remove_thread(std::thread::id thread_id){
        sptr<thread_struct> this_thread;
        {
            std::lock_guard<std::mutex> lock(connections_data_mutex);
            throw_if(not_in(thread_id, threads), "Thread not found in threads map");
            
            sptr<thread_struct> this_thread = threads[thread_id];
            for(auto &connection: this_thread->owned_connections){
                _remove_connection_no_lock(connection->connection_info);
                throw_if(connection.use_count() != 1,
                "Connection still has other owners "+std::to_string(connection.use_count()));
            }

            this_thread->owned_connections.clear();
            threads.erase(thread_id);
        }
        throw_if(this_thread.use_count() != 1,
        "Thread still has other owners: "+std::to_string(this_thread.use_count()));
        
        //> move the thread to finished_threads
        {
            std::lock_guard<std::mutex> lock(finished_threads_mutex);
            finished_threads.push_back(std::move(this_thread->thread));
        }
        
        this_thread.reset();//> release the last ref of the struct
        can_exit_cv.notify_all();
    }

    //% notice that connection is not a reference to the sptr, it is a new sptr
    //* after handle_first_connection, the only owner of this_thread should be this function so it can safely delete itself
    //> Note: this already initializes the recv, meaning that the first message should always be sent by the client
    void setup_thread(connection_info connection_info, sptr<connection_struct> connection){
        std::thread::id this_thread_id = std::this_thread::get_id();
        sptr<thread_struct> this_thread;

        //> makes it safe in case of a throw or early return, the thread will still be removed
        OnDelete on_delete_thread([&this_thread,this_thread_id, this](){
            this_thread.reset();
            remove_thread(this_thread_id);
        });

        {
            //lock to let the creator finish setting the thread into the map
            std::lock_guard<std::mutex> lock(connections_data_mutex);
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
        THROW_IF_RECOVERABLE(tl->initialize_recv(), "Failed to initialize recv", return;);

        handle_first_connection(this_thread);
    }

    //> returning from this function will cause the thread to exit and close any owned connections
    virtual void handle_first_connection(sptr<thread_struct> this_thread) = 0;

    ~Server(){
        {//> notify every thread to exit
            std::lock_guard<std::mutex> lock(connections_data_mutex);
            for(auto &thread: threads){
                thread.second->request_close = true;
            }
        }
        //> wait for all threads to exit
        std::unique_lock<std::mutex> lock(can_exit_mutex);
        can_exit_cv.wait(lock, [this](){return threads.empty();});
        //> join with all finished threads
        cleanup_finished_threads();
    }
};
//> MASTER/MASTER_BACKUP/DATA
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
//> append at the end of messages to communicate the dialogue status
//> Note: initially the client owns the dialogue, so the first message should be from the client
//> END_IMMEDIATELY/YOUR_TURN/ANOTHER_MESSAGE
enum DIALOGUE_STATUS : u8
{
    //> END IMMEDIATELY should be used after receiving YOUR_TURN if no more messages should be sent
    //> also can put a END_IMMEDIATELY on the first message if no reply is expected
    END_IMMEDIATELY=0b00000000,
    //> YOUR_TURN gives control over the dialogue to the other side
    YOUR_TURN=0b00001111,
    //> ANOTHER_MESSAGE means that the owning side sends another message
    ANOTHER_MESSAGE=0b11111111
};

static_assert(sizeof(PACKET_ID) == 1, "Packet ID must be 1 byte");
static_assert(sizeof(REQUEST) == 1, "Request must be 1 byte");


// ================================================================

//% all below true if error
//> for a simple type like PACKET_ID just use read_type and write_type

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
//! does not support vectors longer than 2^31-1
//> stores a i32 size and then each element using read_type
template<typename T>
[[nodiscard]]
bool read_vector(TransmissionLayer &tl, std::vector<T> &vec)
{
    i32 size;
    if(tl.read_type(size)) return true;
    vec.resize(size);
    for(auto &elem: vec){
        if(tl.read_type(elem)) return true;
    }
    return false;
}
//! does not support vectors longer than 2^31-1
//> reads a i32 size and then each element using write_type
template<typename T>
[[nodiscard]]
bool write_vector(TransmissionLayer &tl, std::vector<T> &vec)
{
    if(tl.write_type(static_cast<i32>(vec.size()))) return true;
    for(auto &elem: vec){
        if(tl.write_type(elem)) return true;
    }
    return false;
}
//! does not support vectors longer than 2^31-1
//> stores a i32 size and then each element using read_f
//~ read_f(&tl, &vec_elem) should be bool and return true if error
template<typename T, typename F>
[[nodiscard]]
bool read_vector_custom(TransmissionLayer &tl, std::vector<T> &vec, F read_f)
{
    i32 size;
    if(tl.read_type(size)) return true;
    vec.resize(size);
    for(auto &elem: vec){
        if(read_f(tl, elem)) return true;
    }
    return false;
}
//! does not support vectors longer than 2^31-1
//> reads a i32 size and then each element using write_f
//~ write_f(&tl, &vec_elem) should be bool and return true if error
template<typename T, typename F>
[[nodiscard]]
bool write_vector_custom(TransmissionLayer &tl, std::vector<T> &vec, F write_f)
{
    if(tl.write_type(static_cast<i32>(vec.size()))) return true;
    for(auto &elem: vec){
        if(write_f(tl, elem)) return true;
    }
    return false;
}

// ================================================================
//> DNS server, supposed to never fail
//> Client asks for the master node address here, and a restored backup node asks for list of all nodes
class DNS : public Server
{
public:
    DNS():Server(socket_address(0,0,0,0,0), DNS_PORT){}

    std::mutex mutexDNS;//* controls the below two variables
    socket_address addrCurMaster;
    std::unordered_set<socket_address> nodes;

    void handle_first_connection(sptr<thread_struct> this_thread) override {
        // bool this_side_owns_connection = false; //> irrelevant because other side always owns connection with DNS
        while(true){
            //> first, read the packet id
            PACKET_ID packet_id;
            THROW_IF_RECOVERABLE(this_thread->owned_connections.empty(), "No connections", return;);
            {//> make a new scope to not hold onto the &tl after the connection is supposed to be closed
                TransmissionLayer &tl = *this_thread->owned_connections.back()->tl;
                THROW_IF_RECOVERABLE(tl.read_type(packet_id),"Failed to read packet id",return;);
                //> only handle the -> dns ids
                switch (packet_id){
                    case REQUEST_MASTER_ADDRESS:
                        handle_request_master_address(tl);
                        break;
                    case SET_MASTER_ADDRESS:
                        handle_set_master_address(tl);
                        break;
                    case REQUEST_LIST_OF_ALL_NODES:
                        handle_request_list_of_all_nodes(tl);
                        break;
                    case EDIT_LIST_OF_ALL_NODES:
                        handle_edit_list_of_all_nodes(tl);
                        break;
                    default:
                        THROW_IF_RECOVERABLE(true, "Invalid packet id:"+std::to_string(packet_id), return;);
                }
                DIALOGUE_STATUS dialogue_status;
                THROW_IF_RECOVERABLE(tl.read_type(dialogue_status), "Failed to read dialogue status", return;);
                if(dialogue_status==END_IMMEDIATELY){
                    return;
                }else if(dialogue_status==YOUR_TURN){
                    THROW_IF_RECOVERABLE(tl.write_type(END_IMMEDIATELY), "Failed to write dialogue status", return;);
                    THROW_IF_RECOVERABLE(tl.finalize_send(), "Failed to send dialogue status", return;);
                    return;
                }else if(dialogue_status!=ANOTHER_MESSAGE){
                    THROW_IF_RECOVERABLE(true, "Invalid dialogue status:"+std::to_string(dialogue_status), return;);
                }//else continue the dialogue
            }
        }
    }
    void handle_request_master_address(TransmissionLayer &tl){
        socket_address addr;
        {//> grab the master addr safely
            std::lock_guard<std::mutex> lock(mutexDNS);
            THROW_IF_RECOVERABLE(addrCurMaster.is_unset(), "No master address set", return;);
            addr = addrCurMaster;
        }
        THROW_IF_RECOVERABLE(write_address(tl, addr), "Failed to write address", return;);
        THROW_IF_RECOVERABLE(tl.finalize_send(), "Failed to send address", return;);
    }
    void handle_set_master_address(TransmissionLayer &tl){
        socket_address addr;
        THROW_IF_RECOVERABLE(read_address(tl, addr), "Failed to read address", return;);
        {//> set the master addr safely
            std::lock_guard<std::mutex> lock(mutexDNS);
            addrCurMaster = addr;
        }
    }
    void handle_request_list_of_all_nodes(TransmissionLayer &tl){
        std::vector<socket_address> vec;
        {//> grab the nodes safely
            std::lock_guard<std::mutex> lock(mutexDNS);
            THROW_IF_RECOVERABLE(nodes.empty(), "No nodes set on DNS request", return;);
            vec.assign(nodes.begin(), nodes.end());
        }
        THROW_IF_RECOVERABLE(write_vector_custom(tl, vec, write_address), "Failed to write addresses", return;);
        THROW_IF_RECOVERABLE(tl.finalize_send(), "Failed to send addresses", return;);
    }
    void handle_edit_list_of_all_nodes(TransmissionLayer &tl){
        std::vector<socket_address> vec;
        THROW_IF_RECOVERABLE(read_vector_custom(tl, vec, read_address), "Failed to read addresses", return;);
        {//> set the nodes safely
            std::lock_guard<std::mutex> lock(mutexDNS);
            nodes.clear();
            nodes.insert(vec.begin(), vec.end());
        }
    }
};
// ================================================================
//> DEFAULT/RETRY/PAUSED
enum pause_state{
    //> normal state
    DEFAULT,
    //> same as normal state but set by master if before was PAUSED
    //> after successful connection, set back to DEFAULT
    RETRY,
    //> set to this state after a failed connection
    //> also master sets this state if it wants to pause the connection
    PAUSED
};
class DataNode: public Server
{
public:

    //> common m for all variables below, regardless of role
    std::mutex m;
    //> role of the node
    node_role role;
    //> address of master to check every time
    socket_address addrMaster;

    //data node specific
    //> if the outgoing connections to master are paused on the cv
    pause_state paused = DEFAULT;
    //> cv to pause the *outgoing* connections to master
    std::condition_variable paused_cv;
    //> to be used with cv, returns paused!=PAUSED
    bool should_connect() const { return paused != PAUSED; }
    //> whether this node is the backup node, and so should rise to master if master fails
    bool is_backup = false;
    //> map from file name to version (also used as unordered set for filenames)
    std::unordered_map<std::string, i64> own_files;


    //master specific
    //> map from file name to list of data nodes
    std::unordered_map<std::string, std::vector<socket_address>> file_to_nodes;
    //> map from data node to list of files
    std::unordered_map<socket_address, std::vector<std::string>> node_to_files;
    //> which node is the backup node
    socket_address backup_node;

    //> NOTE: even if role is master, nodes should still contain itself, otherwise it will not be used as a data node
    DataNode(socket_address dns_address_, in_port_t port, node_role initial_role):
    Server(dns_address_, port), role(initial_role){}

    //> should be called before start()
    void setup_master_thread(std::vector<socket_address> &nodes_vec){
        //> create a new thread to set up the master node
        std::lock_guard<std::mutex> lock(connections_data_mutex);
        uptr<std::thread> setup_thread=std::make_unique<std::thread>([this, nodes_vec](){
            _setup_master(nodes_vec);
        });
        threads[setup_thread->get_id()] = std::make_shared<thread_struct>();
        threads[setup_thread->get_id()]->thread = std::move(setup_thread);
    }
    void _setup_master(std::vector<socket_address> nodes_vec){
        //% notice that none of the below are THROW_IF_RECOVERABLE,
        //% because it is expected that no failures happen during setup
        {//> make sure this is run after the server has started
            std::lock_guard<std::mutex> setup_lock(setup_mutex);
        }
        std::unique_lock<std::mutex> lock(m);
        throw_if(role != MASTER, "Only master node can run this function");
        auto addresses = getIPAddresses();
        throw_if(addresses.empty(), "No network interfaces found");
        //> addrMaster has to point to self since master node is also a data node
        {
            addrMaster = socket_address(addresses[0], server_socket.get_port());
            //> setup the nodes hashmap
            for(auto &node: nodes_vec){
                node_to_files[node] = {};
            }
        }
        socket_address addrMaster_local = addrMaster;//> avoid accessing the real one without the lock
        lock.unlock();//> can release now because already modified
        
        //> contact the DNS to set the master address
        {
            OpenSocket open_socket(CLIENT);
            //> do not THROW_IF_RECOVERABLE because it is expected that no failures happen during setup
            throw_if(open_socket.connect_to_server(dns_address), "Failed to connect to DNS");
            TransmissionLayer tl(open_socket);
            throw_if(tl.write_type(SET_MASTER_ADDRESS), "Failed to write packet id");
            throw_if(write_address(tl, addrMaster_local), "Failed to write address");
            throw_if(tl.write_type(ANOTHER_MESSAGE), "Failed to write dialogue status");
            throw_if(tl.finalize_send(), "Failed to send address");
            
            //> contact the DNS to set the list of nodes
            throw_if(tl.write_type(EDIT_LIST_OF_ALL_NODES), "Failed to write packet id");
            throw_if(write_vector_custom(tl, nodes_vec, write_address), "Failed to write addresses");
            throw_if(tl.write_type(END_IMMEDIATELY), "Failed to write dialogue status");
            throw_if(tl.finalize_send(), "Failed to send addresses");
        }

        //> contact each data node to set the master address, and pick one as backup

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, nodes_vec.size() - 1);
        int backup_index = dis(gen);
        
        lock.lock();//> relock to modify the backup node
        backup_node = nodes_vec[backup_index];
        lock.unlock();

        for(auto &node: nodes_vec){
            OpenSocket open_socket(CLIENT);
            //> do not THROW_IF_RECOVERABLE because it is expected that no failures happen during setup
            throw_if(open_socket.connect_to_server(node), "Failed to connect to data node");
            TransmissionLayer tl(open_socket);
            throw_if(tl.write_type(SET_MASTER_ADDRESS), "Failed to write packet id");
            throw_if(write_address(tl, addrMaster_local), "Failed to write address");
            throw_if(tl.finalize_send(), "Failed to send address");
            if(node == backup_node){
                throw_if(tl.write_type(SET_DATA_NODE_AS_BACKUP), "Failed to write packet id");
                throw_if(tl.finalize_send(), "Failed to send backup request");
            }
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

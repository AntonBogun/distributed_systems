#pragma once
#include "transmission.h"
#include "packets.h"
#include "logging.h"

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

//> how long until we timeout a client uuid
extern const TimeValue CLIENT_REQUEST_TIMEOUT;//> 5s
extern const int MAX_HEARTBEAT_RETRIES;//> 2
extern const TimeValue HEARTBEAT_INTERVAL;//> =0.5s, 2/s
extern const std::string DIR_ROOT_DATA;

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


// = SERVER structs
// ================================================================

//> DEFAULT/RETRY/PAUSED
enum pause_state: u8{
    //> normal state
    DEFAULT,
    //> same as normal state but set by backup node on restore
    //> after successful connection, set back to DEFAULT
    RETRY,
    //> set to this state after a failed connection
    //> also backup sets this state if it wants to pause the connection
    PAUSED
};
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

    //> only useful with DATA nodes performing CLIENT communication with master
    //> tells this thread if it should pause the retries, or if it should retry
    std::mutex pause_mutex;
    std::condition_variable pause_cv;
    pause_state state = DEFAULT;
    void set_state(pause_state state_){
        state = state_;
        //> no point in notifying if we are paused
        if(state != PAUSED) pause_cv.notify_all();
    }
};


// = SERVER class
// ================================================================

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
    //> used to wait until all threads have exited, use with connections_data_mutex
    std::condition_variable can_exit_cv;


    //> used with finished_threads
    std::mutex finished_threads_mutex;
    //> not joining with finished threads is UB, so we keep track of them here
    std::vector<uptr<std::thread>> finished_threads;
    


    //> used together with setup_cv to let things that must run after
    std::mutex setup_mutex;
    std::condition_variable setup_cv;

    //> locks setup_mutex, later unlocks at start()
    Server(socket_address dns_address_, in_port_t port): Node(dns_address_), server_socket(port){
        //> lock setup_mutex, unlock at start
        setup_mutex.lock();
    }//server_socket constructor already starts listening
    //> joins with all finished threads and removes them
    void cleanup_finished_threads(){
        std::lock_guard<std::mutex> lock(finished_threads_mutex);
        for(auto &thread: finished_threads){
            thread->join();
        }
        finished_threads.clear();
    }
    //> begins accepting connections and spawning threads to handle them (via setup_thread)
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
            
            connection_info connection_info = open_socket->ci;
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
            //> the reason its fine to clear vector after removing from connection is that
            //> we are holding the lock the whole time
            this_thread->owned_connections.clear();

            //> move the thread to finished_threads, must do so before erasing from threads
            {
                std::lock_guard<std::mutex> lock(finished_threads_mutex);
                finished_threads.push_back(std::move(this_thread->thread));
            }
            threads.erase(thread_id);
        }
        throw_if(this_thread.use_count() != 1,
        "Thread still has other owners: "+std::to_string(this_thread.use_count()));
        this_thread.reset();//> release the last ref of the struct
        can_exit_cv.notify_all();
    }

    //! should be called while not holding connections_data_mutex
    //> creates new thread using AutoCloseThreadTemplate(func, &connections_data_mutex)
    //! Func MUST BE void Func(sptr<thread_struct>& this_thread)
    template<typename Func>
    std::thread::id CreateNewThread(const Func& func){
        std::lock_guard<std::mutex> lock(connections_data_mutex);
        uptr<std::thread> thread = std::make_unique<std::thread>(&Server::AutoCloseThreadTemplate<Func, std::mutex>, this, func, &connections_data_mutex);
        std::thread::id thread_id = thread->get_id();
        threads[thread_id] = std::make_shared<thread_struct>();
        threads[thread_id]->thread = std::move(thread);
        return thread_id;
    }
    //! should be called while holding _setup_mutex
    //* mutex_t should be a ptr to a mutex, e.g. either &mutex or sptr<mutex>, waits on it before starting
    //> sets up the thread with auto remove_thread on exit, and calls func(this_thread)
    //! Func MUST BE void Func(sptr<thread_struct>& this_thread)
    template<typename Func, typename mutex_t>
    void AutoCloseThreadTemplate(const Func& func, mutex_t _setup_mutex){
        {//> wait until setup completes
            std::lock_guard<std::mutex> lock(*_setup_mutex);
        }
        std::thread::id this_thread_id = std::this_thread::get_id();
        sptr<thread_struct> this_thread;
        //> makes it safe in case of a throw or early return, the thread will still be removed
        OnDelete on_delete_thread([&this_thread,this_thread_id, this](){
            this_thread.reset();
            remove_thread(this_thread_id);
        });
   
        {//> check consistency and get this_thread
            std::lock_guard<std::mutex> lock(connections_data_mutex);
            throw_if(not_in(this_thread_id, threads), "Thread not found in threads map");
            this_thread = threads[this_thread_id];//minor optimization could be to not discard not_in result
            //%from here assume that threads is consistent
        }
        func(this_thread);
    }
    //! should be called while not holding connections_data_mutex
    //* the alternative to this which creates SERVER socket, and the associated thread is found
    //* in the start() and setup_thread() functions
    //> creates new CLIENT connection to addr and sets owner_thread_id
    sptr<connection_struct> create_new_outgoing_connection(socket_address addr, std::thread::id owner_thread_id, sptr<thread_struct>& this_thread){
        uptr<OpenSocket> open_socket = std::make_unique<OpenSocket>(CLIENT);
        THROW_IF_RECOVERABLE(open_socket->connect_to_server(addr), "Failed to connect to server: "+socket_address_to_string(addr), return nullptr;);
        connection_info connection_info = open_socket->ci;
        sptr<connection_struct> connection = std::make_shared<connection_struct>();
        uptr<TransmissionLayer> tl = std::make_unique<TransmissionLayer>(*open_socket);
        connection->open_socket = std::move(open_socket);
        connection->tl = std::move(tl);
        connection->connection_info = connection_info;
        connection->owner_thread_id = owner_thread_id;
        this_thread->owned_connections.push_back(connection);
        {//> use connections_data_mutex because we are modifying connections and threads
            std::lock_guard<std::mutex> lock(connections_data_mutex);
            connections[connection_info] = connection;
        }
        return connection;
    }

    //= SERVER setup_thread

    //% notice that connection is not a reference to the sptr, it is a new sptr
    //* after handle_first_connection, the only owner of this_thread should be this function so it can safely delete itself
    //~ Note: this already initializes the recv, meaning that the first message should always be sent by the client
    //> sets up a SERVER connection, then calls handle_first_connection
    void setup_thread(connection_info connection_info, sptr<connection_struct> connection){
        {//> wait until setup completes
            std::lock_guard<std::mutex> lock(connections_data_mutex);   
        }
        std::thread::id this_thread_id = std::this_thread::get_id();
        sptr<thread_struct> this_thread;
        //> makes it safe in case of a throw or early return, the thread will still be removed
        OnDelete on_delete_thread([&this_thread,this_thread_id, this](){
            this_thread.reset();
            remove_thread(this_thread_id);
        });

        {//> check consistency and get this_thread
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
    virtual void handle_first_connection(sptr<thread_struct>& this_thread) = 0;

    ~Server(){
        {//> notify every thread to exit
            std::unique_lock<std::mutex> lock(connections_data_mutex);
            for(auto &thread: threads){
                thread.second->request_close = true;
            }
            //> wait for all threads to exit
            can_exit_cv.wait(lock, [this](){return threads.empty();});
        }
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

// = PACKETS
// ================================================================

//move from packets.h
enum PACKET_ID : u8
{
    HEARTBEAT, // master -> data nodes
    REQUEST_MASTER_ADDRESS,            // client -> dns
    SET_MASTER_ADDRESS,                // master -> both dns and data nodes
    REQUEST_LIST_OF_ALL_NODES,         // master -> dns
    EDIT_LIST_OF_ALL_NODES,            // master -> dns
    SET_DATA_NODE_AS_BACKUP,           // master -> data node
    CLIENT_TO_MASTER_CONNECTION_REQUEST, // client -> master node
    CLIENT_TO_DATA_CONNECTION_REQUEST, // client -> data node
    DATA_NODE_TO_MASTER_CONNECTION_REQUEST, // data node -> master
    MASTER_NODE_TO_DATA_NODE_REPLICATION_REQUEST, // master -> data node
    DATA_NODE_TO_DATA_NODE_CONNECTION_REQUEST, // data node -> data node
    MASTER_TO_DATA_NODE_INFO_REQUEST,  // master -> data node
    MASTER_TO_DATA_NODE_PAUSE_CHANGE   // master -> data node
};
enum DATA_TO_MASTER_CASE : u8{
    READ_FINISHED,
    WRITE_FINISHED
};
enum CLIENT_REQUEST : u8{
    READ_REQUEST,
    WRITE_REQUEST
};
#define enum_case_to_string(x) case x: return #x;
std::string packet_id_to_string(PACKET_ID packet_id){
    switch (packet_id){
        enum_case_to_string(HEARTBEAT);
        enum_case_to_string(REQUEST_MASTER_ADDRESS);
        enum_case_to_string(SET_MASTER_ADDRESS);
        enum_case_to_string(REQUEST_LIST_OF_ALL_NODES);
        enum_case_to_string(EDIT_LIST_OF_ALL_NODES);
        enum_case_to_string(SET_DATA_NODE_AS_BACKUP);
        enum_case_to_string(CLIENT_TO_MASTER_CONNECTION_REQUEST);
        enum_case_to_string(CLIENT_TO_DATA_CONNECTION_REQUEST);
        enum_case_to_string(DATA_NODE_TO_MASTER_CONNECTION_REQUEST);
        enum_case_to_string(MASTER_NODE_TO_DATA_NODE_REPLICATION_REQUEST);
        enum_case_to_string(DATA_NODE_TO_DATA_NODE_CONNECTION_REQUEST);
        enum_case_to_string(MASTER_TO_DATA_NODE_INFO_REQUEST);
        enum_case_to_string(MASTER_TO_DATA_NODE_PAUSE_CHANGE);
        default: return "UNKNOWN: "+std::to_string(packet_id);
    }
}



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
static_assert(sizeof(CLIENT_REQUEST) == 1, "Client request must be 1 byte");
static_assert(sizeof(DIALOGUE_STATUS) == 1, "Dialogue status must be 1 byte");
static_assert(sizeof(DATA_TO_MASTER_CASE) == 1, "DATA_TO_MASTER_CASE must be 1 byte");




// = tl FUNCS
// ================================================================

//% all below true if error
//> for a simple type like PACKET_ID just use read_type and write_type


[[nodiscard]]
bool read_string(TransmissionLayer &tl, std::string &str)
{
    i32 size;
    if(tl.read_type(size)) return true;
    str.resize(size);
    return tl.read_bytes(str.data(), size);
}
[[nodiscard]]
bool write_string(TransmissionLayer &tl, const std::string &str)
{
    if(tl.write_type(static_cast<i32>(str.size()))) return true;
    return tl.write_bytes(str.data(), str.size());
}

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
bool read_vector_custom(TransmissionLayer &tl, std::vector<T> &vec, const F& read_f)
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
bool write_vector_custom(TransmissionLayer &tl, std::vector<T> &vec, const F& write_f)
{
    if(tl.write_type(static_cast<i32>(vec.size()))) return true;
    for(auto &elem: vec){
        if(write_f(tl, elem)) return true;
    }
    return false;
}
template<typename T_key, typename T_val, typename F_key, typename F_val>
[[nodiscard]]
bool read_hashmap_custom(TransmissionLayer &tl, std::unordered_map<T_key, T_val> &umap, const F_key& read_key, const F_val& read_val)
{
    i32 size;
    if(tl.read_type(size)) return true;
    for(int i=0;i<size;i++){
        T_key key;
        T_val val;
        if(read_key(tl, key)||read_val(tl, val)) return true;
        umap[key] = val;
    }
    return false;
}
template<typename T_key, typename T_val, typename F_key, typename F_val>
[[nodiscard]]
bool write_hashmap_custom(TransmissionLayer &tl, std::unordered_map<T_key, T_val> &umap, const F_key& write_key, const F_val& write_val)
{
    if(tl.write_type(static_cast<i32>(umap.size()))) return true;
    for(auto &pair: umap){
        if(write_key(tl, pair.first)||write_val(tl, pair.second)) return true;
    }
    return false;
}


// = DNS
// ================================================================

//> DNS server, supposed to never fail
//> Client asks for the master node address here, and a restored backup node asks for list of all nodes
class DNS : public Server
{
public:
    DNS():Server(socket_address(), DNS_PORT){}

    std::mutex mutexDNS;//* controls the below two variables
    socket_address addrCurMaster;
    std::unordered_set<socket_address> nodes;

    void handle_first_connection(sptr<thread_struct>& this_thread) override {
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
        //> do not need YOUR_TURN here because turn doesn't switch despite request
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
        //> do not need YOUR_TURN here because turn doesn't switch despite request
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

// = DATA NODE structs
// ================================================================

struct master_file_info{
    int num_readers = 0;
    int num_writers = 0;
    //> true if write query can be made
    bool not_write_blocked() const { return num_readers == 0 && num_writers == 0; }
    //> true if read query can be made
    bool not_read_blocked() const { return num_writers == 0; }
    std::unordered_set<socket_address> replicas;
    socket_address random_replica(){
        if(replicas.empty()) return {};
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, replicas.size()-1);
        auto it = replicas.begin();
        std::advance(it, dis(gen));
        return *it;
    }
    // i64 version; //> irrelevant to consistently keep track of version from master
};
constexpr int s=sizeof(std::unordered_set<socket_address>);
//> version, num_readers, num_writers
struct data_file_info{
    i64 version=0;
    int num_readers=0;
    int num_writers=0;
};
[[nodiscard]]
bool read_data_file_info(TransmissionLayer &tl, data_file_info &info){
    return tl.read_type(info.version)||tl.read_type(info.num_readers)||tl.read_type(info.num_writers);
}
[[nodiscard]]
bool write_data_file_info(TransmissionLayer &tl, data_file_info &info){
    return tl.write_type(info.version)||tl.write_type(info.num_readers)||tl.write_type(info.num_writers);
}
//> total_readers, total_writers, hashmap<filename, data_file_info>
struct node_info{
    int total_readers = 0;
    int total_writers = 0;
    std::unordered_map<std::string, data_file_info> files;
};
[[nodiscard]]
bool read_node_info(TransmissionLayer &tl, node_info &info){
    return tl.read_type(info.total_readers)||tl.read_type(info.total_writers)||
    read_hashmap_custom(tl, info.files, read_string, read_data_file_info);
}
[[nodiscard]]
bool write_node_info(TransmissionLayer &tl, node_info &info){
    return tl.write_type(info.total_readers)||tl.write_type(info.total_writers)||
    write_hashmap_custom(tl, info.files, write_string, write_data_file_info);
}

struct expected_client_entry{
    std::string filename;
    uuid_t uuid;
    TimeValue timeout;
    CLIENT_REQUEST request;
};

// = DATA NODE
// ================================================================

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
    //> whether this node is the backup node, and so should rise to master if master fails
    bool is_backup = false;
    //> map from file name to version (also used as unordered set for filenames)
    std::unordered_map<std::string, i64> own_files;
    //> master alerts data node about client requests that should arrive soon
    std::queue<expected_client_entry> expected_client_requests;
    node_info this_node_info;


    //master specific
    //> map from file name to list of data nodes
    std::unordered_map<std::string, master_file_info> file_to_info;
    //> map from data node to list of files
    std::unordered_map<socket_address, node_info> node_to_info;
    //> which node is the backup node
    socket_address backup_node;

    //> NOTE: even if role is master, nodes should still contain itself, otherwise it will not be used as a data node
    DataNode(socket_address dns_address_, in_port_t port, node_role initial_role):
    Server(dns_address_, port), role(initial_role){}

    //! call while holding m
    socket_address random_non_master_node(socket_address _addrMaster){
        std::vector<socket_address> nodes_vec;
        for(auto &node: node_to_info){
            if(node.first != _addrMaster){
                nodes_vec.push_back(node.first);
            }
        }
        if(nodes_vec.empty()){
            return {};
        }
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, nodes_vec.size()-1);
        return nodes_vec[dis(gen)];
    }
    //! call while holding m
    socket_address random_node(){
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, node_to_info.size()-1);
        auto it = node_to_info.begin();
        std::advance(it, dis(gen));
        return it->first;
    }
    //! call while holding m
    void clear_timeout_client_requests(){
        //> iterate through both queues and remove any that have timed out
        TimeValue now = TimeValue::now();
        while(!expected_client_requests.empty() && expected_client_requests.front().timeout < now){
            expected_client_entry entry = expected_client_requests.front();
            expected_client_requests.pop();
        }
        while(!cached_client_requests.empty() && cached_client_requests.front().timeout < now){
            cached_client_requests.pop();
        }
    }

    //> should be called before start()
    void initial_setup_master_thread(std::vector<socket_address> &nodes_vec){
        CreateNewThread([this, nodes_vec](sptr<thread_struct>& _this_thread){
            setup_master(nodes_vec, false, {}, _this_thread);
        });
        debug_log("Initial setup thread created");
    }

    //= setup_master

    //> make new thread with nodes_vec, false, {} to do initial setup
    //> call with {}, true, last_master to do setup from backup node after a master failure
    void setup_master(std::vector<socket_address> nodes_vec, bool from_backup, socket_address last_master, sptr<thread_struct>& this_thread){
        //% below doesn't use THROW_IF_RECOVERABLE because we don't handle if master setup or backup recovery fails
        {//> make sure this is run after the server has started
            std::lock_guard<std::mutex> setup_lock(setup_mutex);
        }
        std::unique_lock<std::mutex> lock(m);
        throw_if(role != MASTER, "Only master node can run this function");
        throw_if(from_backup!=nodes_vec.empty() || from_backup==last_master.is_unset(), "Initial setup should have nodes and no last_master, and backup should not");

        auto addresses = getIPAddresses();
        throw_if(addresses.empty(), "No network interfaces found");

        //> addrMaster has to point to self since master node is also a data node
        socket_address addrMaster_local = socket_address(addresses[0], server_socket.get_port());

        //> contact the DNS to set the master address
        {
            sptr<connection_struct> dns_connection = create_new_outgoing_connection(dns_address, std::this_thread::get_id(), this_thread);

            OnDelete on_delete_connection([&dns_connection, this](){
                connection_info ci = dns_connection->connection_info;
                dns_connection.reset();//> release the connection ref from this scope
                remove_connection(ci);
            });

            TransmissionLayer& tl=*dns_connection->tl;

            if(from_backup){
                //> contact the DNS to get the list of all nodes
                throw_if(tl.write_type(REQUEST_LIST_OF_ALL_NODES), "Failed to write packet id");
                throw_if(tl.write_type(ANOTHER_MESSAGE), "Failed to write dialogue status");
                throw_if(tl.finalize_send(), "Failed to send backup request");
                
                throw_if(tl.initialize_recv(), "Failed to initialize recv");
                throw_if(read_vector_custom(tl, nodes_vec, read_address), "Failed to read addresses");

                //> remove the old master from the list
                nodes_vec.erase(std::remove(nodes_vec.begin(), nodes_vec.end(), last_master), nodes_vec.end());
            }

            //> set the master address
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
        if(!from_backup){
            //> setup the nodes hashmap
            for(auto &node: nodes_vec){
                node_to_info[node] = {};
            }
        }

        //> contact each data node to set the master address, and pick one as backup
        backup_node = random_non_master_node(addrMaster_local);

        //> avoid accessing the real ones without the lock
        socket_address backup_node_local = backup_node;

        lock.unlock();//> can release now because already modified
        
        {
            //> local hashmaps to avoid always locking
            std::unordered_map<socket_address, node_info> node_to_info_local;
            std::unordered_map<std::string, master_file_info> file_to_info_local;

            for(socket_address &node: nodes_vec){
                sptr<connection_struct> connection = create_new_outgoing_connection(node, std::this_thread::get_id(), this_thread);
                OnDelete on_delete_connection([&connection, this](){
                    connection_info ci = connection->connection_info;
                    connection.reset();//> release the connection ref from this scope
                    remove_connection(ci);
                });

                TransmissionLayer& tl=*connection->tl;
                
                if(from_backup){
                    //> pause the node
                    throw_if(tl.write_type(MASTER_TO_DATA_NODE_PAUSE_CHANGE), "Failed to write packet id");
                    throw_if(tl.write_type(PAUSED), "Failed to write pause state");
                    throw_if(tl.write_type(ANOTHER_MESSAGE), "Failed to write dialogue status");
                    throw_if(tl.finalize_send(), "Failed to send pause request");
                }
                
                //> set the master address
                throw_if(tl.write_type(SET_MASTER_ADDRESS), "Failed to write packet id");
                throw_if(write_address(tl, addrMaster_local), "Failed to write address");
                throw_if(tl.write_type(ANOTHER_MESSAGE), "Failed to write dialogue status");
                throw_if(tl.finalize_send(), "Failed to send address");

                if(node == backup_node_local){
                    //> tell the node to set itself as backup
                    throw_if(tl.write_type(SET_DATA_NODE_AS_BACKUP), "Failed to write packet id");
                    if(from_backup){
                        throw_if(tl.write_type(ANOTHER_MESSAGE), "Failed to write dialogue status");
                    }else{
                        throw_if(tl.write_type(END_IMMEDIATELY), "Failed to write dialogue status");
                    }
                    throw_if(tl.finalize_send(), "Failed to send backup request");
                }
                if(from_backup){
                    //> get the node info
                    throw_if(tl.write_type(MASTER_TO_DATA_NODE_INFO_REQUEST), "Failed to write packet id");
                    throw_if(tl.write_type(END_IMMEDIATELY), "Failed to write dialogue status");
                    throw_if(tl.finalize_send(), "Failed to send info request");

                    throw_if(tl.initialize_recv(), "Failed to initialize recv");
                    node_info info;
                    throw_if(read_node_info(tl, info), "Failed to read node info");
                    node_to_info_local[node] = info;
                    for(auto &[filename, data_file_info]: info.files){
                        file_to_info_local[filename].replicas.insert(node);
                    }
                }
            }

            if(from_backup){
                //> now set everything
                {
                    lock.lock();
                    node_to_info = std::move(node_to_info_local);
                    file_to_info = std::move(file_to_info_local);
                    addrMaster=addrMaster_local;
                    lock.unlock();
                }

                //> now unpause
                const pause_state new_pause_state = RETRY;
                for(auto &node: nodes_vec){
                    sptr<connection_struct> connection = create_new_outgoing_connection(node, std::this_thread::get_id(), this_thread);
                    OnDelete on_delete_connection([&connection, this](){
                        connection_info ci = connection->connection_info;
                        connection.reset();//> release the connection ref from this scope
                        remove_connection(ci);
                    });

                    TransmissionLayer& tl=*connection->tl;
                    throw_if(tl.write_type(MASTER_TO_DATA_NODE_PAUSE_CHANGE), "Failed to write packet id");
                    throw_if(tl.write_type(new_pause_state), "Failed to write pause state");
                    throw_if(tl.write_type(END_IMMEDIATELY), "Failed to write dialogue status");
                    throw_if(tl.finalize_send(), "Failed to send pause request");
                } 
            }
        }

        //> set heartbeat with each one except for itself
        for(auto &node: nodes_vec){
            if(node == addrMaster_local) continue;
            CreateNewThread([this, node](sptr<thread_struct>& _this_thread){
                _setup_heartbeat(node, _this_thread);
            });
        }
    }

    //= _setup_heartbeat

    void _setup_heartbeat(socket_address addr, sptr<thread_struct>& this_thread){

        std::thread::id this_thread_id = std::this_thread::get_id();
        {
            sptr<connection_struct> connection = create_new_outgoing_connection(addr, this_thread_id, this_thread);
            OnDelete on_delete_connection([&connection, this](){
                connection_info ci = connection->connection_info;
                connection.reset();//> release the connection ref from this scope
                remove_connection(ci);
            });
            TransmissionLayer &tl = *connection->tl;
            bool own_connection=true;//> since we are the client
            while(true){

                if(own_connection){
                    THROW_IF_RECOVERABLE(tl.write_type(HEARTBEAT), "Failed to write packet id", break;);
                    THROW_IF_RECOVERABLE(tl.write_type(YOUR_TURN), "Failed to write dialogue status", break;);
                    THROW_IF_RECOVERABLE(tl.finalize_send(), "Failed to send heartbeat request", break;);
                    own_connection = false;
                }else{
                    THROW_IF_RECOVERABLE(tl.initialize_recv(), "Failed to initialize recv", break;);
                    PACKET_ID packet_id;
                    THROW_IF_RECOVERABLE(tl.read_type(packet_id), "Failed to read packet id", break;);
                    THROW_IF_RECOVERABLE(packet_id != HEARTBEAT, "Invalid packet id: "+packet_id_to_string(packet_id), break;);
                    DIALOGUE_STATUS dialogue_status;
                    THROW_IF_RECOVERABLE(tl.read_type(dialogue_status), "Failed to read dialogue status", break;);
                    THROW_IF_RECOVERABLE(dialogue_status!=YOUR_TURN, "Invalid dialogue status: "+std::to_string(dialogue_status), break;);
                    own_connection = true;
                }
                //> sleep for HEARTBEAT_INTERVAL
                std::this_thread::sleep_for(HEARTBEAT_INTERVAL.to_duration());
            }
        }
        _handle_heartbeat_error(addr, this_thread, this_thread_id);
    }

    //= _handle_heartbeat_error

    //> throw_if here since do not deal with inconsistent state
    void _handle_heartbeat_error(socket_address addr, sptr<thread_struct>& this_thread, std::thread::id this_thread_id){
        std::unique_lock<std::mutex> lock(m);
        auto it = node_to_info.find(addr);
        throw_if(it == node_to_info.end(), "Node not found in node_to_info");
        node_info &info = it->second;
        //> iterate through all files and remove the node from the list
        for (const auto &[filename, in_node_info] : info.files) {
            auto it2 = file_to_info.find(filename);
            throw_if(it2 == file_to_info.end(), "File not found in file_to_info");
            master_file_info &file_info = it2->second;
            auto it3 = file_info.replicas.find(addr);
            throw_if(it3 == file_info.replicas.end(), "Node not found in file_info.replicas");
            file_info.replicas.erase(it3);
            file_info.num_readers -= in_node_info.num_readers;
            file_info.num_writers -= in_node_info.num_writers;
            if(file_info.replicas.empty()){
                file_to_info.erase(it2);
            }
        }
        //> remove the node from the list
        node_to_info.erase(it);
        if(addr == backup_node){
            socket_address backup_node_local=random_non_master_node(addrMaster);
            backup_node = backup_node_local;
            lock.unlock();
            sptr<connection_struct> connection = create_new_outgoing_connection(backup_node_local, this_thread_id, this_thread);
            OnDelete on_delete_connection([&connection, this](){
                connection_info ci = connection->connection_info;
                connection.reset();//> release the connection ref from this scope
                remove_connection(ci);
            });
            TransmissionLayer &tl = *connection->tl;
            throw_if(tl.write_type(SET_DATA_NODE_AS_BACKUP), "Failed to write packet id");
            throw_if(tl.write_type(END_IMMEDIATELY), "Failed to write dialogue status");
            throw_if(tl.finalize_send(), "Failed to send backup request");
        }
    }
    
    //= handle_first_connection

    void handle_first_connection(sptr<thread_struct>& this_thread) override {
        //> first, read the packet id
        THROW_IF_RECOVERABLE(this_thread->owned_connections.empty(), "No connections", return;);
        TransmissionLayer &tl = *this_thread->owned_connections.back()->tl;
        while(true){
            PACKET_ID packet_id;
            THROW_IF_RECOVERABLE(tl.read_type(packet_id),"Failed to read packet id",return;);
            //> only handle the -> data node ids
            switch (packet_id){
                case HEARTBEAT:
                    handle_heartbeat(tl);//on data node
                    break;
                case SET_MASTER_ADDRESS:
                    handle_set_master_address(tl);//on data node
                    break;
                case SET_DATA_NODE_AS_BACKUP:
                    handle_set_data_node_as_backup(tl);//on data node
                    break;
                case CLIENT_TO_MASTER_CONNECTION_REQUEST:
                    handle_client_to_master_connection_request(tl);//on master
                    break;
                case CLIENT_TO_DATA_CONNECTION_REQUEST:
                    handle_client_to_data_connection_request(tl);//on data node
                    break;
                case DATA_NODE_TO_MASTER_CONNECTION_REQUEST:
                    handle_data_node_to_master_connection_request(tl);//on master
                    break;
                case MASTER_NODE_TO_DATA_NODE_REPLICATION_REQUEST:
                    handle_master_node_to_data_node_replication_request(tl);//on data node
                    break;
                case DATA_NODE_TO_DATA_NODE_CONNECTION_REQUEST:
                    handle_data_node_to_data_node_connection_request(tl);//on data node
                    break;
                case MASTER_TO_DATA_NODE_INFO_REQUEST:
                    handle_master_to_data_node_info_request(tl);//on data node
                    break;
                case MASTER_TO_DATA_NODE_PAUSE_CHANGE:
                    handle_master_to_data_node_pause_change(tl);//on data node
                    break;
                default:
                    THROW_IF_RECOVERABLE(true, "Invalid packet id:"+std::to_string(packet_id), return;);
            }
            if(packet_id == HEARTBEAT){
                return;//> only in case heartbeat failed
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
            }
            
        }
    }

    //= handle_heartbeat

    //> on data node
    void handle_heartbeat(TransmissionLayer &tl){
        //> read the first dialogue status
        DIALOGUE_STATUS dialogue_status;
        //> do while to be able to continue
        do{
            THROW_IF_RECOVERABLE(tl.read_type(dialogue_status), "Failed to read dialogue status", continue;);
            THROW_IF_RECOVERABLE(dialogue_status!=YOUR_TURN, "First dialogue status should be YOUR_TURN", continue;);
            bool own_connection = true;
            //> do the first sleep
            std::this_thread::sleep_for(HEARTBEAT_INTERVAL.to_duration());
            while(true){
                if(own_connection){
                    THROW_IF_RECOVERABLE(tl.write_type(HEARTBEAT), "Failed to write packet id", break;);
                    THROW_IF_RECOVERABLE(tl.write_type(YOUR_TURN), "Failed to write dialogue status", break;);
                    THROW_IF_RECOVERABLE(tl.finalize_send(), "Failed to send heartbeat request", break;);
                    own_connection = false;
                }else{
                    THROW_IF_RECOVERABLE(tl.initialize_recv(), "Failed to initialize recv", break;);
                    PACKET_ID packet_id;
                    THROW_IF_RECOVERABLE(tl.read_type(packet_id), "Failed to read packet id", break;);
                    THROW_IF_RECOVERABLE(packet_id != HEARTBEAT, "Invalid packet id: "+packet_id_to_string(packet_id), break;);
                    DIALOGUE_STATUS dialogue_status;
                    THROW_IF_RECOVERABLE(tl.read_type(dialogue_status), "Failed to read dialogue status", break;);
                    THROW_IF_RECOVERABLE(dialogue_status!=YOUR_TURN, "Invalid dialogue status: "+std::to_string(dialogue_status), break;);
                    own_connection = true;
                }
                //> sleep for HEARTBEAT_INTERVAL
                std::this_thread::sleep_for(HEARTBEAT_INTERVAL.to_duration());
            }
        }while(0);

        //> previously paused all threads here but that is unnecessary, rising to master does this anyway

        std::unique_lock<std::mutex> lock(m);
        if(is_backup){
            //> rise to master
            socket_address addrMaster_local = addrMaster;
            CreateNewThread([this, addrMaster_local](sptr<thread_struct>& _this_thread){
                setup_master({}, true, addrMaster_local, _this_thread);
            });
        }        
    }

    //> on data node
    void handle_set_master_address(TransmissionLayer &tl){
        socket_address addr;
        THROW_IF_RECOVERABLE(read_address(tl, addr), "Failed to read address", return;);
        std::lock_guard<std::mutex> lock(m);
        addrMaster = addr;
    }

    //> on data node
    void handle_set_data_node_as_backup(TransmissionLayer &tl){
        std::lock_guard<std::mutex> lock(m);
        is_backup = true;
    }

    //= handle_client_to_master_connection_request
    void handle_client_to_master_connection_request(TransmissionLayer &tl){
        //> read the case
        CLIENT_REQUEST client_case;
        THROW_IF_RECOVERABLE(tl.read_type(client_case), "Failed to read client case", return;);
        if(client_case!=READ_REQUEST && client_case!=WRITE_REQUEST){
            THROW_IF_RECOVERABLE(true, "Invalid client case: "+std::to_string(client_case), return;);
        }
        //> read the filename
        std::string filename;
        THROW_IF_RECOVERABLE(read_string(tl, filename), "Failed to read filename", return;);
        //> check the request is allowed
        bool allowed = false;
        socket_address data_node;
        {
            std::lock_guard<std::mutex> lock(m);
            auto it = file_to_info.find(filename);
            if(client_case==READ_REQUEST){
                if(it!=file_to_info.end()&&it->second.not_read_blocked()){
                    allowed = true;
                    data_node = it->second.random_replica();
                }
            }else if(client_case==WRITE_REQUEST){
                if(it!=file_to_info.end()&&it->second.not_write_blocked()){
                    allowed = true;
                    data_node = it->second.random_replica();
                }else if(it==file_to_info.end()){
                    allowed = true;
                    data_node = random_node();
                    file_to_info[filename].replicas.insert(data_node);
                }
            }
        }
    }














};




}//namespace distribsys

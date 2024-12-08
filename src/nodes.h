#pragma once
#include "transmission.h"
#include "logging.h"

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>
#include <bits/stdc++.h>
#include <fstream>
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
    connection_info ci;
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
        {
            std::lock_guard<std::mutex> lock(pause_mutex);
            state = state_;
        }
        //> no point in notifying if we are paused
        if(state_ != PAUSED) pause_cv.notify_all();
    }
    void wait_until_unpaused(){
        std::unique_lock<std::mutex> lock(pause_mutex);
        pause_cv.wait(lock, [this](){return state != PAUSED;});
    }
    //> sets state to PAUSED, but if it was RETRY then does not wait until unpaused
    void set_failure(){
        {
            DEBUG_PRINT("set_failure on thread");
            std::unique_lock<std::mutex> lock(pause_mutex);
            if(state == DEFAULT){
                state = PAUSED;
                pause_cv.wait(lock, [this](){return state != PAUSED;});
            }else if(state == RETRY){
                state = PAUSED;
                //> do not wait here, just pass
            }else{
                pause_cv.wait(lock, [this](){return state != PAUSED;});
            }
        }
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
            DEBUG_PRINT("joining thread");
            thread->join();
        }
        finished_threads.clear();
    }
    //> data/port/filename
    std::string to_physical_filename(const std::string& filename){
        //> for simplicity do not lock and assume ServerSocket port will never change
        return DIR_ROOT_DATA+std::to_string(server_socket.get_port())+"/"+filename;
    }
    //> data/port/filename_uuid
    std::string to_temp_filename(const std::string& filename, const uuid_t& uuid){ 
        return DIR_ROOT_DATA+std::to_string(server_socket.get_port())+filename+"_"+std::to_string(uuid.val);
    }
    void ensure_physical_directory_exists(){
        std::string dir = DIR_ROOT_DATA+std::to_string(server_socket.get_port());
        std::filesystem::create_directories(dir);
    }
    //> begins accepting connections and spawning threads to handle them (via setup_thread)
    void start() {
        setup_mutex.unlock();//> let e.g. _setup_master run
        while (true)
        {
            DEBUG_PRINT("Server loop");
            //> every loop check and clean up finished threads
            cleanup_finished_threads();
            handler_function();

            uptr<OpenSocket> open_socket = std::make_unique<OpenSocket>(SERVER);

            {//> avoid accepting with multiple threads at once
                std::lock_guard<std::mutex> lock(accept_mutex);
                continue_on_err(open_socket->accept_connection(server_socket.get_socket_fd()));
            }
            // DEBUG_PRINT("Server loop_1");
            connection_info ci = open_socket->ci;
            sptr<connection_struct> connection = std::make_shared<connection_struct>();
            connection->open_socket = std::move(open_socket);
            connection->ci = ci;
            // DEBUG_PRINT("Server loop_2");
            {//> use connections_data_mutex because we are modifying connections and threads
                std::lock_guard<std::mutex> lock(connections_data_mutex);
                connections[ci] = connection;
                // DEBUG_PRINT("Server loop_3");
                //% below makes a copy of sptr connection
                uptr<std::thread> thread = 
                std::make_unique<std::thread>(&Server::setup_thread, this, ci, connection);
                // DEBUG_PRINT("Server loop_4");
                threads[thread->get_id()] = std::make_shared<thread_struct>();
                connection->owner_thread_id = thread->get_id();
                //% makes a copy of sptr connection
                threads[thread->get_id()]->owned_connections.push_back(connection);
                // DEBUG_PRINT("Server loop_5");
                threads[thread->get_id()]->thread = std::move(thread);
                // DEBUG_PRINT("Server loop_6");
                connection.reset();//% release the connection ref from this scope
            }
        }
    }
    //% do not use without lock, but can chain together without releasing the lock every time
    //! does not remove the connection from the thread's owned_connections
    inline void _remove_connection_no_lock(connection_info ci){
        connections.erase(ci);
    }
    //! do not hold connections_data_mutex
    //~ does not remove from the thread's owned_connections
    //* use _remove_connection_no_lock if you already have the lock or removing multiple connections
    void remove_connection(connection_info ci){
        DEBUG_PRINT("remove_connection");
        std::lock_guard<std::mutex> lock(connections_data_mutex);
        _remove_connection_no_lock(ci);
    }
    //! NOTE: expects nothing else to own the thread sptr and connection sptrs
    //  (besides hashmaps and the owned_connections vector)
    //! should only be called from the thread being removed
    //> first removes all owned connections, then removes the thread
    void remove_thread(std::thread::id thread_id){
        DEBUG_PRINT("remove_thread");
        sptr<thread_struct> this_thread;
        {
            std::lock_guard<std::mutex> lock(connections_data_mutex);
            throw_if(not_in(thread_id, threads), "Thread not found in threads map");
            // DEBUG_PRINT("remove_thread_1");
            this_thread = threads[thread_id];
            for(auto &connection: this_thread->owned_connections){
                _remove_connection_no_lock(connection->ci);
                throw_if(connection.use_count() != 1,
                "Connection still has other owners "+std::to_string(connection.use_count()));
            }
            // DEBUG_PRINT("remove_thread_2");
            //> the reason its fine to clear vector after removing from connection is that
            //> we are holding the lock the whole time
            this_thread->owned_connections.clear();

            //> move the thread to finished_threads, must do so before erasing from threads
            {
                std::lock_guard<std::mutex> lock2(finished_threads_mutex);
                finished_threads.push_back(std::move(this_thread->thread));
            }
            // DEBUG_PRINT("remove_thread_3");
            threads.erase(thread_id);
        }
        // DEBUG_PRINT("remove_thread_4");
        throw_if(this_thread.use_count() != 1,
        "Thread still has other owners: "+std::to_string(this_thread.use_count()));
        this_thread.reset();//> release the last ref of the struct
        // DEBUG_PRINT("remove_thread_5");
        can_exit_cv.notify_all();
    }

    //! should be called while not holding connections_data_mutex
    //> creates new thread using AutoCloseThreadTemplate(func, &connections_data_mutex)
    //! Func MUST BE void Func(sptr<thread_struct>& this_thread)
    template<typename Func>
    std::thread::id CreateNewThread(const Func& func){
        DEBUG_PRINT("CreateNewThread");
        std::lock_guard<std::mutex> lock(connections_data_mutex);
        uptr<std::thread> thread = std::make_unique<std::thread>(&Server::AutoCloseThreadTemplate<Func, std::mutex*>, this, func, &connections_data_mutex);
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
        DEBUG_PRINT("AutoCloseThreadTemplate");
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
        DEBUG_PRINT("create_new_outgoing_connection");
        uptr<OpenSocket> open_socket = std::make_unique<OpenSocket>(CLIENT);
        THROW_IF_RECOVERABLE(open_socket->connect_to_server(addr), "Failed to connect to server: "+socket_address_to_string(addr), return nullptr;);
        connection_info ci = open_socket->ci;
        sptr<connection_struct> connection = std::make_shared<connection_struct>();
        uptr<TransmissionLayer> tl = std::make_unique<TransmissionLayer>(*open_socket);
        connection->open_socket = std::move(open_socket);
        connection->tl = std::move(tl);
        connection->ci = ci;
        connection->owner_thread_id = owner_thread_id;
        this_thread->owned_connections.push_back(connection);
        {//> use connections_data_mutex because we are modifying connections and threads
            std::lock_guard<std::mutex> lock(connections_data_mutex);
            connections[ci] = connection;
        }
        return connection;
    }
    //! should be called while not holding connections_data_mutex
    //! NOTE: expects nothing else to own the connection sptr
    //  (besides &connection, connections hashmap and threads owned_connections)
    //> will delete connection from connections after OnDeleteMovable is deleted
    //> and also remove it from the thread's owned_connections
    auto CreateAutodeleteOutgoingConnection(socket_address addr, std::thread::id owner_thread_id, sptr<thread_struct>& this_thread, sptr<connection_struct>& connection){
        connection = create_new_outgoing_connection(addr, owner_thread_id, this_thread);
        DEBUG_PRINT("creating Autodelete connection");
        return OnDeleteMovable([&connection,&this_thread,this](){
            DEBUG_PRINT("Deleting connection");
            connection_info ci = connection->ci;
            {
                std::lock_guard<std::mutex> lock(connections_data_mutex);
                //> find and erase the connection in the thread's owned_connections
                for(auto it = this_thread->owned_connections.begin(); it != this_thread->owned_connections.end(); ++it){
                    if(it->get() == connection.get()){
                        this_thread->owned_connections.erase(it);
                        break;
                    }
                }
                //> erase the connection from connections
                _remove_connection_no_lock(ci);
                //> ensure that the connection is only owned by the thread
                throw_if(connection.use_count() != 1, "Connection still has other owners: "+std::to_string(connection.use_count()));
                connection.reset();
            }
        });
    }


    //= SERVER setup_thread

    //% notice that connection is not a reference to the sptr, it is a new sptr
    //* after handle_first_connection, the only owner of this_thread should be this function so it can safely delete itself
    //~ Note: this already initializes the recv, meaning that the first message should always be sent by the client
    //> sets up a SERVER connection, then calls handle_first_connection
    void setup_thread(connection_info ci, sptr<connection_struct> connection){
        DEBUG_PRINT("setup_thread with "+ci.to_string());
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
            throw_if(not_in(ci, connections), "Connection not found in connections map");
            throw_if(connection.get() != connections[ci].get(), "Connections mismatch");
            throw_if(connection.get() != this_thread->owned_connections.back().get(), "Connection stack mismatch");
            throw_if(this_thread_id != connection->owner_thread_id, "Thread id mismatch");
            //%from here assume that connections and threads are consistent
        }
        uptr<TransmissionLayer> tl = std::make_unique<TransmissionLayer>(*connection->open_socket);

        connection->tl = std::move(tl);
        //% used to handle if recv failed here, effectively the connection wasn't established
        //* check definition of THROW_IF_RECOVERABLE
        //> also, tl->initialize_recv initializes the receive of the message, meaning that every connection from
        //> client -> server should always send a message from the client first
        THROW_IF_RECOVERABLE(connection->tl->initialize_recv(), "Failed to initialize recv", {
            connection.reset();
            return;
        });
        connection.reset();//release the connection ref from this scope

        

        handle_first_connection(this_thread);
    }

    //> returning from this function will cause the thread to exit and close any owned connections
    virtual void handle_first_connection(sptr<thread_struct>& this_thread) = 0;
    //> does nothing by default, called every loop in start to handle things
    virtual void handler_function(){};
    ~Server(){
        {//> notify every thread to exit
            DEBUG_PRINT("Server destructor");
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
enum node_role : u8{
    MASTER,
    MASTER_BACKUP,
    DATA
};

// = PACKETS
// ================================================================

//move from packets.h
enum class PACKET_ID : u8
{
    HEARTBEAT, // master -> data nodes
    REQUEST_MASTER_ADDRESS,            // client -> dns
    SET_MASTER_ADDRESS,                // master -> both dns and data nodes
    REQUEST_LIST_OF_ALL_NODES,         // master -> dns
    EDIT_LIST_OF_ALL_NODES,            // master -> dns
    SET_DATA_NODE_AS_BACKUP,           // master -> data node
    CLIENT_TO_MASTER_CONNECTION_REQUEST, // client -> master node
    CLIENT_TO_DATA_CONNECTION_REQUEST, // client -> data node
    MASTER_NODE_TO_DATA_NODE_ADD_NEW_CLIENT, // master -> data node
    DATA_NODE_TO_MASTER_CONNECTION_REQUEST, // data node -> master
    MASTER_NODE_TO_DATA_NODE_REPLICATION_REQUEST, // master -> data node
    DATA_NODE_TO_DATA_NODE_CONNECTION_REQUEST, // data node -> data node
    MASTER_TO_DATA_NODE_INFO_REQUEST,  // master -> data node
    MASTER_TO_DATA_NODE_PAUSE_CHANGE   // master -> data node
};
enum class DATA_TO_MASTER_CASE : u8{
    READ_FINISHED,
    WRITE_FINISHED
};
enum class CLIENT_REQUEST : u8{
    READ_REQUEST,
    WRITE_REQUEST
};
enum class DENY_REASON: u8{
    ALLOWED,
    INVALID_REQUEST,
    FILE_NOT_FOUND,
    FILE_READ_BLOCKED,
    FILE_WRITE_BLOCKED,
    DATA_NODE_REPONSE_ERROR
};
enum class DATA_DENY_REASON: u8{
    ALLOWED,
    INVALID_REQUEST,
    UUID_NOT_FOUND_OR_EXPIRED,
    REQUEST_MISMATCH,
    FILE_ERROR
};
enum class CLIENT_CONNECTION_END_STATE: u8{
    SUCCESS,
    FAILURE
};
constexpr u8 OK_REPONSE = 1;
#define enum_case_to_string(x) case x: return #x
std::string packet_id_to_string(PACKET_ID packet_id){
    switch (packet_id){
        enum_case_to_string(PACKET_ID::HEARTBEAT);
        enum_case_to_string(PACKET_ID::REQUEST_MASTER_ADDRESS);
        enum_case_to_string(PACKET_ID::SET_MASTER_ADDRESS);
        enum_case_to_string(PACKET_ID::REQUEST_LIST_OF_ALL_NODES);
        enum_case_to_string(PACKET_ID::EDIT_LIST_OF_ALL_NODES);
        enum_case_to_string(PACKET_ID::SET_DATA_NODE_AS_BACKUP);
        enum_case_to_string(PACKET_ID::CLIENT_TO_MASTER_CONNECTION_REQUEST);
        enum_case_to_string(PACKET_ID::CLIENT_TO_DATA_CONNECTION_REQUEST);
        enum_case_to_string(PACKET_ID::MASTER_NODE_TO_DATA_NODE_ADD_NEW_CLIENT);
        enum_case_to_string(PACKET_ID::DATA_NODE_TO_MASTER_CONNECTION_REQUEST);
        enum_case_to_string(PACKET_ID::MASTER_NODE_TO_DATA_NODE_REPLICATION_REQUEST);
        enum_case_to_string(PACKET_ID::DATA_NODE_TO_DATA_NODE_CONNECTION_REQUEST);
        enum_case_to_string(PACKET_ID::MASTER_TO_DATA_NODE_INFO_REQUEST);
        enum_case_to_string(PACKET_ID::MASTER_TO_DATA_NODE_PAUSE_CHANGE);
        default: return "UNKNOWN: "+std::to_string(static_cast<u8>(packet_id));
    }
}
std::string client_case_to_string(CLIENT_REQUEST client_request){
    switch (client_request){
        enum_case_to_string(CLIENT_REQUEST::READ_REQUEST);
        enum_case_to_string(CLIENT_REQUEST::WRITE_REQUEST);
        default: return "UNKNOWN: "+std::to_string(static_cast<u8>(client_request));
    }
}
std::string deny_reason_to_string(DENY_REASON deny_reason){
    switch (deny_reason){
        enum_case_to_string(DENY_REASON::ALLOWED);
        enum_case_to_string(DENY_REASON::INVALID_REQUEST);
        enum_case_to_string(DENY_REASON::FILE_NOT_FOUND);
        enum_case_to_string(DENY_REASON::FILE_READ_BLOCKED);
        enum_case_to_string(DENY_REASON::FILE_WRITE_BLOCKED);
        enum_case_to_string(DENY_REASON::DATA_NODE_REPONSE_ERROR);
        default: return "UNKNOWN: "+std::to_string(static_cast<u8>(deny_reason));
    }
}
std::string data_deny_reason_to_string(DATA_DENY_REASON deny_reason){
    switch (deny_reason){
        enum_case_to_string(DATA_DENY_REASON::ALLOWED);
        enum_case_to_string(DATA_DENY_REASON::INVALID_REQUEST);
        enum_case_to_string(DATA_DENY_REASON::UUID_NOT_FOUND_OR_EXPIRED);
        enum_case_to_string(DATA_DENY_REASON::REQUEST_MISMATCH);
        enum_case_to_string(DATA_DENY_REASON::FILE_ERROR);
        default: return "UNKNOWN: "+std::to_string(static_cast<u8>(deny_reason));
    }
}
std::string client_connection_end_state_to_string(CLIENT_CONNECTION_END_STATE end_state){
    switch (end_state){
        enum_case_to_string(CLIENT_CONNECTION_END_STATE::SUCCESS);
        enum_case_to_string(CLIENT_CONNECTION_END_STATE::FAILURE);
        default: return "UNKNOWN: "+std::to_string(static_cast<u8>(end_state));
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
static_assert(sizeof(DENY_REASON) == 1, "DENY_REASON must be 1 byte");
static_assert(sizeof(DATA_DENY_REASON) == 1, "DATA_DENY_REASON must be 1 byte");
static_assert(sizeof(CLIENT_CONNECTION_END_STATE) == 1, "CLIENT_CONNECTION_END_STATE must be 1 byte");




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
bool write_address(TransmissionLayer &tl, const socket_address addr)
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
bool write_vector(TransmissionLayer &tl, const std::vector<T> &vec)
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
bool write_vector_custom(TransmissionLayer &tl, const std::vector<T> &vec, const F& write_f)
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
bool write_hashmap_custom(TransmissionLayer &tl, const std::unordered_map<T_key, T_val> &umap, const F_key& write_key, const F_val& write_val)
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
    DNS(in_port_t port):Server({}, port){}

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
                DEBUG_PRINT("DNS received packet id: "+packet_id_to_string(packet_id));
                //> only handle the -> dns ids
                switch (packet_id){
                    case PACKET_ID::REQUEST_MASTER_ADDRESS:
                        handle_request_master_address(tl);
                        break;
                    case PACKET_ID::SET_MASTER_ADDRESS:
                        handle_set_master_address(tl);
                        break;
                    case PACKET_ID::REQUEST_LIST_OF_ALL_NODES:
                        handle_request_list_of_all_nodes(tl);
                        break;
                    case PACKET_ID::EDIT_LIST_OF_ALL_NODES:
                        handle_edit_list_of_all_nodes(tl);
                        break;
                    default:
                        THROW_IF_RECOVERABLE(true, "Invalid packet id:"+std::to_string(static_cast<u8>(packet_id)), return;);
                }
                DIALOGUE_STATUS dialogue_status;
                THROW_IF_RECOVERABLE(tl.read_type(dialogue_status), "Failed to read dialogue status", return;);
                if(dialogue_status==END_IMMEDIATELY){
                    return;
                }else if(dialogue_status==YOUR_TURN){
                    THROW_IF_RECOVERABLE(tl.write_type(DIALOGUE_STATUS::END_IMMEDIATELY), "Failed to write dialogue status", return;);
                    THROW_IF_RECOVERABLE(tl.finalize_send(), "Failed to send dialogue status", return;);
                    return;
                }else if(dialogue_status!=ANOTHER_MESSAGE){
                    THROW_IF_RECOVERABLE(true, "Invalid dialogue status:"+std::to_string(dialogue_status), return;);
                }//else continue the dialogue
                THROW_IF_RECOVERABLE(tl.initialize_recv(), "Failed to initialize recv to continue send", return;);

            }
        }
    }
    void handle_request_master_address(TransmissionLayer &tl){
        DEBUG_PRINT("handle_request_master_address");
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
        DEBUG_PRINT("handle_set_master_address");
        socket_address addr;
        THROW_IF_RECOVERABLE(read_address(tl, addr), "Failed to read address", return;);
        {//> set the master addr safely
            std::lock_guard<std::mutex> lock(mutexDNS);
            addrCurMaster = addr;
        }
    }
    void handle_request_list_of_all_nodes(TransmissionLayer &tl){
        DEBUG_PRINT("handle_request_list_of_all_nodes");
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
        DEBUG_PRINT("handle_edit_list_of_all_nodes");
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
bool write_data_file_info(TransmissionLayer &tl, const data_file_info &info){
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
bool write_node_info(TransmissionLayer &tl, const node_info &info){
    return tl.write_type(info.total_readers)||tl.write_type(info.total_writers)||
    write_hashmap_custom(tl, info.files, write_string, write_data_file_info);
}
// enum handling_state: u8{
//     //> normal state, can timeout during the clear_timeout
//     DEFAULT,
//     //> in-use by handler, should not be cleared
//     IN_USE
// };
//> filename, uuid, timeout, CLIENT_REQUEST
struct expected_client_entry{
    std::string filename;
    uuid_t uuid;
    TimeValue timeout;
    CLIENT_REQUEST request;
    socket_address data_node;
    // handling_state state = DEFAULT;
    expected_client_entry(std::string filename_, const uuid_t& uuid_, TimeValue timeout_, CLIENT_REQUEST request_, socket_address data_node_):
    filename(filename_), uuid(uuid_), timeout(timeout_), request(request_), data_node(data_node_){}
};
[[nodiscard]]
bool read_expected_client_entry(TransmissionLayer &tl, sptr<expected_client_entry> &entry){
    std::string filename;
    uuid_t uuid;
    TimeValue timeout(0);
    CLIENT_REQUEST request;
    socket_address data_node;
    if(read_string(tl, filename)||tl.read_type(uuid.val)||tl.read_type(timeout.time)||tl.read_type(request)||read_address(tl, data_node)) return true;
    entry = std::make_shared<expected_client_entry>(filename, uuid, timeout, request, data_node);
    return false;
}
[[nodiscard]]
bool write_expected_client_entry(TransmissionLayer &tl, const expected_client_entry &entry){
    return write_string(tl, entry.filename)||tl.write_type(entry.uuid.val)||tl.write_type(entry.timeout.time)||tl.write_type(entry.request)||write_address(tl, entry.data_node);
}


// = DATA NODE
// ================================================================

class DataNode: public Server
{
public:

    //> common mutex for all member variables in datanode, regardless of role
    std::mutex datanode_mutex;
    //> role of the node
    node_role role;
    //> address of master to check every time
    socket_address addrMaster;

    //data node specific
    //> whether this node is the backup node, and so should rise to master if master fails
    bool is_backup = false;
    //> readers+writers, and map from file name to version (also used as unordered set for filenames)
    node_info this_node_info;
    //> master alerts data node about client requests that should arrive soon
    std::unordered_map<uuid_t,sptr<expected_client_entry>> expected_client_requests;


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

    //! call while holding datanode_mutex
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
    //! call while holding datanode_mutex
    socket_address random_node(){
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, node_to_info.size()-1);
        auto it = node_to_info.begin();
        std::advance(it, dis(gen));
        return it->first;
    }
    //! call while holding datanode_mutex
    void clear_timeout_client_requests(){
        //> iterate through the deque and remove any that have timed out
        TimeValue now = TimeValue::now();
        //> iterate through the hashmap and any that are timed out, remove and spawn handler on it
        std::vector<uuid_t> to_remove;
        for(auto &[uuid, entry_sptr]: expected_client_requests){
            if(entry_sptr->timeout < now){
                to_remove.push_back(uuid);
                //spawn handler
                CreateNewThread([this, entry_sptr](sptr<thread_struct>& this_thread){
                    handle_client_connection_end(entry_sptr, this_thread, CLIENT_CONNECTION_END_STATE::FAILURE);
                });
            }
        }
        for(auto &uuid: to_remove){
            expected_client_requests.erase(uuid);
        }
    }
    void handler_function() override{
        std::lock_guard<std::mutex> lock(datanode_mutex);
        clear_timeout_client_requests();
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
        DEBUG_PRINT("setup_master");
        std::unique_lock<std::mutex> lock(datanode_mutex);
        throw_if(role != MASTER, "Only master node can run this function");
        throw_if(from_backup!=nodes_vec.empty() || from_backup==last_master.is_unset(), "Initial setup should have nodes and no last_master, and backup should not");

        auto addresses = getIPAddresses();
        throw_if(addresses.empty(), "No network interfaces found");

        //> addrMaster has to point to self since master node is also a data node
        socket_address addrMaster_local = socket_address(addresses[0], server_socket.get_port());

        //> contact the DNS to set the master address
        {
            DEBUG_PRINT("Setting master address and editing list of all nodes");
            sptr<connection_struct> dns_connection;
            auto on_delete_dns_connection = CreateAutodeleteOutgoingConnection(dns_address, std::this_thread::get_id(), this_thread, dns_connection);

            TransmissionLayer& tl=*dns_connection->tl;

            if(from_backup){
                //> contact the DNS to get the list of all nodes
                throw_if(tl.write_type(PACKET_ID::REQUEST_LIST_OF_ALL_NODES), "Failed to write packet id");
                throw_if(tl.write_type(DIALOGUE_STATUS::ANOTHER_MESSAGE), "Failed to write dialogue status");
                throw_if(tl.finalize_send(), "Failed to send backup request");
                
                throw_if(tl.initialize_recv(), "Failed to initialize recv");
                throw_if(read_vector_custom(tl, nodes_vec, read_address), "Failed to read addresses");

                //> remove the old master from the list
                nodes_vec.erase(std::remove(nodes_vec.begin(), nodes_vec.end(), last_master), nodes_vec.end());
            }

            //> set the master address
            throw_if(tl.write_type(PACKET_ID::SET_MASTER_ADDRESS), "Failed to write packet id");
            throw_if(write_address(tl, addrMaster_local), "Failed to write address");
            throw_if(tl.write_type(DIALOGUE_STATUS::ANOTHER_MESSAGE), "Failed to write dialogue status");
            throw_if(tl.finalize_send(), "Failed to send address");
            
            //> contact the DNS to set the list of nodes
            throw_if(tl.write_type(PACKET_ID::EDIT_LIST_OF_ALL_NODES), "Failed to write packet id");
            throw_if(write_vector_custom(tl, nodes_vec, write_address), "Failed to write addresses");
            throw_if(tl.write_type(DIALOGUE_STATUS::END_IMMEDIATELY), "Failed to write dialogue status");
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
                DEBUG_PRINT("setting master for "+socket_address_to_string(node));
                sptr<connection_struct> connection;
                auto on_delete_connection = CreateAutodeleteOutgoingConnection(node, std::this_thread::get_id(), this_thread, connection);

                TransmissionLayer& tl=*connection->tl;
                
                if(from_backup){
                    //> pause the node
                    throw_if(tl.write_type(PACKET_ID::MASTER_TO_DATA_NODE_PAUSE_CHANGE), "Failed to write packet id");
                    throw_if(tl.write_type(pause_state::PAUSED), "Failed to write pause state");
                    throw_if(tl.write_type(DIALOGUE_STATUS::ANOTHER_MESSAGE), "Failed to write dialogue status");
                    throw_if(tl.finalize_send(), "Failed to send pause request");
                }
                
                //> set the master address
                throw_if(tl.write_type(PACKET_ID::SET_MASTER_ADDRESS), "Failed to write packet id");
                throw_if(write_address(tl, addrMaster_local), "Failed to write address");
                throw_if(tl.write_type(DIALOGUE_STATUS::ANOTHER_MESSAGE), "Failed to write dialogue status");
                throw_if(tl.finalize_send(), "Failed to send address");

                if(node == backup_node_local){
                    //> tell the node to set itself as backup
                    throw_if(tl.write_type(PACKET_ID::SET_DATA_NODE_AS_BACKUP), "Failed to write packet id");
                    if(from_backup){
                        throw_if(tl.write_type(DIALOGUE_STATUS::ANOTHER_MESSAGE), "Failed to write dialogue status");
                    }else{
                        throw_if(tl.write_type(DIALOGUE_STATUS::END_IMMEDIATELY), "Failed to write dialogue status");
                    }
                    throw_if(tl.finalize_send(), "Failed to send backup request");
                }
                if(from_backup){
                    //> get the node info
                    throw_if(tl.write_type(PACKET_ID::MASTER_TO_DATA_NODE_INFO_REQUEST), "Failed to write packet id");
                    throw_if(tl.write_type(DIALOGUE_STATUS::END_IMMEDIATELY), "Failed to write dialogue status");
                    throw_if(tl.finalize_send(), "Failed to send info request");

                    throw_if(tl.initialize_recv(), "Failed to initialize recv");
                    node_info info;
                    throw_if(read_node_info(tl, info), "Failed to read node info");
                    node_to_info_local[node] = info;
                    for(auto &[filename, _data_file_info]: info.files){
                        master_file_info &file_info = file_to_info_local[filename];
                        file_info.replicas.insert(node);
                        file_info.num_readers += _data_file_info.num_readers;
                        file_info.num_writers += _data_file_info.num_writers;
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
                    DEBUG_PRINT("unpausing "+socket_address_to_string(node));
                    sptr<connection_struct> connection;
                    auto on_delete_connection = CreateAutodeleteOutgoingConnection(node, std::this_thread::get_id(), this_thread, connection);

                    TransmissionLayer& tl=*connection->tl;
                    throw_if(tl.write_type(PACKET_ID::MASTER_TO_DATA_NODE_PAUSE_CHANGE), "Failed to write packet id");
                    throw_if(tl.write_type(new_pause_state), "Failed to write pause state");
                    throw_if(tl.write_type(DIALOGUE_STATUS::END_IMMEDIATELY), "Failed to write dialogue status");
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
        if(!from_backup){
            debug_log("Master setup complete");
        }else{
            debug_log("Backup recovery complete");
        }
    }

    //= _setup_heartbeat

    void _setup_heartbeat(socket_address addr, sptr<thread_struct>& this_thread){
        DEBUG_PRINT("Setting up heartbeat with "+socket_address_to_string(addr));
        std::thread::id this_thread_id = std::this_thread::get_id();
        {
            sptr<connection_struct> connection;
            auto on_delete_connection = CreateAutodeleteOutgoingConnection(addr, this_thread_id, this_thread, connection);

            TransmissionLayer &tl = *connection->tl;
            bool own_connection=true;//> since we are the client
            while(true){
                DEBUG_PRINT("Heartbeat with "+socket_address_to_string(addr)+" turn="+std::to_string(own_connection));
                if(own_connection){
                    THROW_IF_RECOVERABLE(tl.write_type(PACKET_ID::HEARTBEAT), "Failed to write packet id", break;);
                    THROW_IF_RECOVERABLE(tl.write_type(DIALOGUE_STATUS::YOUR_TURN), "Failed to write dialogue status", break;);
                    THROW_IF_RECOVERABLE(tl.finalize_send(), "Failed to send heartbeat request", break;);
                    own_connection = false;
                }else{
                    THROW_IF_RECOVERABLE(tl.initialize_recv(), "Failed to initialize recv", break;);
                    PACKET_ID packet_id;
                    THROW_IF_RECOVERABLE(tl.read_type(packet_id), "Failed to read packet id", break;);
                    THROW_IF_RECOVERABLE(packet_id != PACKET_ID::HEARTBEAT, "Invalid packet id: "+packet_id_to_string(packet_id), break;);
                    DIALOGUE_STATUS dialogue_status;
                    THROW_IF_RECOVERABLE(tl.read_type(dialogue_status), "Failed to read dialogue status", break;);
                    THROW_IF_RECOVERABLE(dialogue_status!=DIALOGUE_STATUS::YOUR_TURN, "Invalid dialogue status: "+std::to_string(dialogue_status), break;);
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
        DEBUG_PRINT("Handling heartbeat error with "+socket_address_to_string(addr));
        std::unique_lock<std::mutex> lock(datanode_mutex);
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

            sptr<connection_struct> connection;
            auto on_delete_connection = CreateAutodeleteOutgoingConnection(backup_node_local, this_thread_id, this_thread, connection);

            TransmissionLayer &tl = *connection->tl;
            throw_if(tl.write_type(PACKET_ID::SET_DATA_NODE_AS_BACKUP), "Failed to write packet id");
            throw_if(tl.write_type(DIALOGUE_STATUS::END_IMMEDIATELY), "Failed to write dialogue status");
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
            DEBUG_PRINT("---Node received packet id: "+packet_id_to_string(packet_id));
            //> only handle the -> data node ids
            //% pass this_thread when handler needs to create new connections
            switch (packet_id){
                case PACKET_ID::HEARTBEAT:
                    handle_heartbeat(tl);//on data node
                    break;
                case PACKET_ID::SET_MASTER_ADDRESS:
                    handle_set_master_address(tl);//on data node
                    break;
                case PACKET_ID::SET_DATA_NODE_AS_BACKUP:
                    handle_set_data_node_as_backup();//on data node
                    break;
                case PACKET_ID::CLIENT_TO_MASTER_CONNECTION_REQUEST:
                    handle_client_to_master_connection_request(tl,this_thread);//on master
                    break;
                case PACKET_ID::CLIENT_TO_DATA_CONNECTION_REQUEST:
                    handle_client_to_data_connection_request(tl);//on data node
                    break;
                case PACKET_ID::MASTER_NODE_TO_DATA_NODE_ADD_NEW_CLIENT:
                    handle_master_node_to_data_node_add_new_client(tl);//on data node
                    break;
                case PACKET_ID::DATA_NODE_TO_MASTER_CONNECTION_REQUEST:
                    handle_data_node_to_master_connection_request(tl);//on master
                    break;
                // case PACKET_ID::MASTER_NODE_TO_DATA_NODE_REPLICATION_REQUEST:
                //     handle_master_node_to_data_node_replication_request(tl);//on data node
                //     break;
                // case PACKET_ID::DATA_NODE_TO_DATA_NODE_CONNECTION_REQUEST:
                //     handle_data_node_to_data_node_connection_request(tl);//on data node
                //     break;
                case PACKET_ID::MASTER_TO_DATA_NODE_INFO_REQUEST:
                    handle_master_to_data_node_info_request(tl);//on data node
                    break;
                case PACKET_ID::MASTER_TO_DATA_NODE_PAUSE_CHANGE:
                    handle_master_to_data_node_pause_change(tl);//on data node
                    break;
                default:
                    THROW_IF_RECOVERABLE(true, "Invalid packet id:"+std::to_string(static_cast<u8>(packet_id)), return;);
            }
            DEBUG_PRINT("---Node handled packet "+packet_id_to_string(packet_id));
            if(packet_id == PACKET_ID::HEARTBEAT){
                return;//> only in case heartbeat failed
            }
            DIALOGUE_STATUS dialogue_status;
            DEBUG_PRINT("---Reading dialogue status");
            THROW_IF_RECOVERABLE(tl.read_type(dialogue_status), "Failed to read dialogue status", return;);
            if(dialogue_status==END_IMMEDIATELY){
                return;
            }else if(dialogue_status==YOUR_TURN){
                THROW_IF_RECOVERABLE(tl.write_type(DIALOGUE_STATUS::END_IMMEDIATELY), "Failed to write dialogue status", return;);
                THROW_IF_RECOVERABLE(tl.finalize_send(), "Failed to send dialogue status", return;);
                return;
            }else if(dialogue_status!=ANOTHER_MESSAGE){
                THROW_IF_RECOVERABLE(true, "Invalid dialogue status:"+std::to_string(dialogue_status), return;);
            }
            DEBUG_PRINT("---Continuing dialogue");
            THROW_IF_RECOVERABLE(tl.initialize_recv(), "Failed to initialize recv to continue send", return;);
            
        }
    }

    //= handle_heartbeat

    //> on data node
    void handle_heartbeat(TransmissionLayer &tl){
        //> read the first dialogue status
        DIALOGUE_STATUS dialogue_status;
        DEBUG_PRINT("Handling heartbeat on data node");
        //> do while to be able to continue
        do{
            THROW_IF_RECOVERABLE(tl.read_type(dialogue_status), "Failed to read dialogue status", continue;);
            THROW_IF_RECOVERABLE(dialogue_status!=YOUR_TURN, "First dialogue status should be YOUR_TURN", continue;);
            bool own_connection = true;
            //> do the first sleep
            std::this_thread::sleep_for(HEARTBEAT_INTERVAL.to_duration());
            while(true){
                DEBUG_PRINT("Heartbeat with master turn="+std::to_string(own_connection));
                if(own_connection){
                    THROW_IF_RECOVERABLE(tl.write_type(PACKET_ID::HEARTBEAT), "Failed to write packet id", break;);
                    THROW_IF_RECOVERABLE(tl.write_type(DIALOGUE_STATUS::YOUR_TURN), "Failed to write dialogue status", break;);
                    THROW_IF_RECOVERABLE(tl.finalize_send(), "Failed to send heartbeat request", break;);
                    own_connection = false;
                }else{
                    THROW_IF_RECOVERABLE(tl.initialize_recv(), "Failed to initialize recv", break;);
                    PACKET_ID packet_id;
                    THROW_IF_RECOVERABLE(tl.read_type(packet_id), "Failed to read packet id", break;);
                    THROW_IF_RECOVERABLE(packet_id != PACKET_ID::HEARTBEAT, "Invalid packet id: "+packet_id_to_string(packet_id), break;);

                    THROW_IF_RECOVERABLE(tl.read_type(dialogue_status), "Failed to read dialogue status", break;);
                    THROW_IF_RECOVERABLE(dialogue_status!=YOUR_TURN, "Invalid dialogue status: "+std::to_string(dialogue_status), break;);
                    own_connection = true;
                }
                //> sleep for HEARTBEAT_INTERVAL
                std::this_thread::sleep_for(HEARTBEAT_INTERVAL.to_duration());
            }
        }while(0);

        //> previously paused all threads here but that is unnecessary, rising to master does this anyway

        std::unique_lock<std::mutex> lock(datanode_mutex);
        if(is_backup){
            //> rise to master
            role = MASTER;
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
        std::lock_guard<std::mutex> lock(datanode_mutex);
        DEBUG_PRINT("Setting master address to "+socket_address_to_string(addr));
        addrMaster = addr;
    }

    //> on data node
    void handle_set_data_node_as_backup(){
        DEBUG_PRINT("Setting data node as backup");
        std::lock_guard<std::mutex> lock(datanode_mutex);
        is_backup = true;
    }



    //= handle_client_to_master_connection_request
    void handle_client_to_master_connection_request(TransmissionLayer &tl, sptr<thread_struct>& this_thread){
        //> read the case
        CLIENT_REQUEST client_case;
        THROW_IF_RECOVERABLE(tl.read_type(client_case), "Failed to read client case", return;);
        if(client_case!=CLIENT_REQUEST::READ_REQUEST && client_case!=CLIENT_REQUEST::WRITE_REQUEST){
            THROW_IF_RECOVERABLE(true, "Invalid client case: "+std::to_string(static_cast<u8>(client_case)), return;);
        }
        //> read the filename
        std::string filename;
        THROW_IF_RECOVERABLE(read_string(tl, filename), "Failed to read filename", return;);
        DEBUG_PRINT("Client request to master for "+client_case_to_string(client_case)+" on "+filename);
        //> check the request is allowed
        bool allowed = false;
        socket_address data_node;
        DENY_REASON deny_reason = DENY_REASON::ALLOWED;
        uuid_t uuid(0);
        bool should_undo = false;
        do{
            {
                std::lock_guard<std::mutex> lock(datanode_mutex);
                auto it = file_to_info.find(filename);//master_file_info
                if(client_case==CLIENT_REQUEST::READ_REQUEST){
                    do{
                        if(it==file_to_info.end()){
                            deny_reason = DENY_REASON::FILE_NOT_FOUND;
                            break;
                        }
                        if(it->second.not_read_blocked()){
                            allowed = true;
                            data_node = it->second.random_replica();
                            //> increment the number of readers on file and node
                            it->second.num_readers++;
                            node_info &info = node_to_info[data_node];
                            info.total_readers++;
                            info.files[filename].num_readers++;
                            should_undo = true;
                        }else{
                            deny_reason = DENY_REASON::FILE_READ_BLOCKED;
                        }
                    }while(0);
                }else if(client_case==CLIENT_REQUEST::WRITE_REQUEST){
                    if(it==file_to_info.end()){
                        allowed = true;
                        //> already holding the lock
                        data_node = random_node();

                        //> creates a new entry in the file_to_info
                        file_to_info[filename].replicas.insert(data_node);
                    }else if(it->second.not_write_blocked()){
                        allowed = true;
                        data_node = it->second.random_replica();
                    }else{
                        deny_reason = DENY_REASON::FILE_WRITE_BLOCKED;
                    }
                    if(allowed){
                        //> increment the number of writers on file and node
                        file_to_info[filename].num_writers++;
                        node_info &info = node_to_info[data_node];
                        info.total_writers++;
                        info.files[filename].num_writers++;
                        should_undo = true;
                    }
                }else{
                    deny_reason = DENY_REASON::INVALID_REQUEST;
                }
            }
            if(!allowed){
                break;
            }else{
                debug_log(client_case_to_string(client_case)+" request for "+filename+" allowed");
                uuid=uuid_t();//> generate a new uuid
                
                //> don't need to add this to the expected_client_requests because it is only on data node
                expected_client_entry entry(filename, uuid, TimeValue::now()+CLIENT_REQUEST_TIMEOUT, client_case, data_node);

                bool success = false;
                DEBUG_PRINT("sending the expected client entry");
                //> contact data node to allow the communication
                do{
                    sptr<connection_struct> connection;
                    auto on_delete_connection = CreateAutodeleteOutgoingConnection(data_node, std::this_thread::get_id(), this_thread, connection);

                    TransmissionLayer &tl2=*connection->tl;
                    THROW_IF_RECOVERABLE(tl2.write_type(PACKET_ID::MASTER_NODE_TO_DATA_NODE_ADD_NEW_CLIENT), "Failed to write packet id", break;);
                    THROW_IF_RECOVERABLE(write_expected_client_entry(tl2, entry), "Failed to write expected client entry", break;);
                    THROW_IF_RECOVERABLE(tl2.write_type(DIALOGUE_STATUS::END_IMMEDIATELY), "Failed to write dialogue status", break;);
                    THROW_IF_RECOVERABLE(tl2.finalize_send(), "Failed to send expected client entry", break;);
                    success = true;
                }while(0);
                if(!success){
                    debug_log("Failed to contact data node "+ socket_address_to_string(data_node) + " for "+client_case_to_string(client_case)+" request for "+filename);
                    allowed = false;
                    deny_reason = DENY_REASON::DATA_NODE_REPONSE_ERROR;
                    break;
                }else{
                    should_undo = false;
                }
            }
        }while(0);
        if(!allowed){
            debug_log(client_case_to_string(client_case)+" request for "+filename+" denied for reason: "+deny_reason_to_string(deny_reason));
            if(should_undo){
                std::lock_guard<std::mutex> lock(datanode_mutex);
                auto it = file_to_info.find(filename);
                if(it!=file_to_info.end()){
                    if(client_case==CLIENT_REQUEST::READ_REQUEST){
                        it->second.num_readers--;
                        node_info &info = node_to_info[data_node];
                        info.total_readers--;
                        info.files[filename].num_readers--;
                    }else if(client_case==CLIENT_REQUEST::WRITE_REQUEST){
                        it->second.num_writers--;
                        node_info &info = node_to_info[data_node];
                        info.total_writers--;
                        info.files[filename].num_writers--;
                    }
                    //% for simplicity, do not remove the file from the file_to_info if it is empty
                }
            }
            THROW_IF_RECOVERABLE(tl.write_type(deny_reason), "Failed to write deny reason", return;);
            THROW_IF_RECOVERABLE(tl.finalize_send(), "Failed to send deny reason", return;);
            return;
        }else{
            //> contact client to allow the communication
            THROW_IF_RECOVERABLE(tl.write_type(DENY_REASON::ALLOWED), "Failed to write allow reason", return;);
            THROW_IF_RECOVERABLE(tl.write_type(uuid.val), "Failed to write uuid", return;);
            THROW_IF_RECOVERABLE(write_address(tl, data_node), "Failed to write data node address", return;);
            THROW_IF_RECOVERABLE(tl.finalize_send(), "Failed to send allow reason", return;);
            //% for simplicity does not restore state in case of failure here
        }
    }

    //= handle_client_to_data_connection_request
    void handle_client_to_data_connection_request(TransmissionLayer &tl){
        //> cleanup the expected_client_requests
        std::string filename;
        uuid_t uuid(0);
        CLIENT_REQUEST request;
        //> read from client
        THROW_IF_RECOVERABLE(read_string(tl, filename), "Failed to read filename", return;);
        THROW_IF_RECOVERABLE(tl.read_type(uuid.val), "Failed to read uuid", return;);
        THROW_IF_RECOVERABLE(tl.read_type(request), "Failed to read request", return;);
        bool failure = false;
        bool connection_failure = false;
        DATA_DENY_REASON deny_reason = DATA_DENY_REASON::ALLOWED;
        sptr<expected_client_entry> entry_sptr;
        std::string physical_filename = to_physical_filename(filename);
        std::string temp_filename = to_temp_filename(filename, uuid);
        DEBUG_PRINT("Client to data connection request for "+client_case_to_string(request)+" on "+filename);
        DEBUG_PRINT("Using physical filename: "+physical_filename);
        do{
            {
                std::lock_guard<std::mutex> lock(datanode_mutex);
                clear_timeout_client_requests();
                //try to find the entry
                auto it = expected_client_requests.find(uuid);
                if(it == expected_client_requests.end()){
                    deny_reason = DATA_DENY_REASON::UUID_NOT_FOUND_OR_EXPIRED;
                    failure = true;
                    break;
                }else{
                    //> get the entry and remove it
                    entry_sptr = it->second;
                    expected_client_requests.erase(it);
                }
            }
            //> check if the entry matches the request
            if(entry_sptr->filename != filename || entry_sptr->request != request){
                deny_reason = DATA_DENY_REASON::REQUEST_MISMATCH;
                failure = true;
                break;
            }
            ensure_physical_directory_exists();
            //> write allowed
            THROW_IF_RECOVERABLE(tl.write_type(DATA_DENY_REASON::ALLOWED), "Failed to write allow reason", break;);
            THROW_IF_RECOVERABLE(tl.finalize_send(), "Failed to send allow reason", break;);
            connection_failure = true;
            DEBUG_PRINT("Client to data connection request allowed, reading/writing file");
            if(request == CLIENT_REQUEST::READ_REQUEST){
                //> do not increment the number of readers since it is incremented on initial master node request
                //> read the vector of bytes representing the file
                std::vector<u8> vec;
                try{
                    ThrowingIfstream file(physical_filename, std::ios::binary);
                    file.read_into_byte_vector(vec);
                }catch(const std::exception &e){
                    err_log("Failed to read file: "+physical_filename+" with error: "+e.what());
                    failure = true;
                    deny_reason = DATA_DENY_REASON::FILE_ERROR;
                    break;
                }
                //> send the vector of bytes
                THROW_IF_RECOVERABLE(write_vector(tl, vec), "Failed to send file", break;);
                THROW_IF_RECOVERABLE(tl.finalize_send(), "Failed to send file", break;);
                //> read to avoid broken pipe
                THROW_IF_RECOVERABLE(tl.initialize_recv(), "Failed to initialize recv", break;);
            }else{//WRITE_REQUEST
                //> write the vector of bytes representing the file
                std::vector<u8> vec;
                THROW_IF_RECOVERABLE(tl.initialize_recv(), "Failed to initialize recv", break;);
                THROW_IF_RECOVERABLE(read_vector(tl, vec), "Failed to receive file", {
                    tl.print_errors();
                    break;
                });
                //> write ok response to avoid broken pipe
                THROW_IF_RECOVERABLE(tl.write_byte(OK_REPONSE), "Failed to write ok response", break;);
                THROW_IF_RECOVERABLE(tl.finalize_send(), "Failed to send ok response", break;);

                try{
                    ThrowingOfstream file(temp_filename, std::ios::binary);
                    file.write_from_byte_vector(vec);
                    file.close();
                    ThrowingOfstream::move_to_file(temp_filename, physical_filename);
                }catch(const std::exception &e){
                    err_log("Failed to write file: "+temp_filename+" and move to " +physical_filename +" with error: "+e.what());
                    failure = true;
                    deny_reason = DATA_DENY_REASON::FILE_ERROR;
                    break;
                }
            }
            connection_failure = false;
        }while(0);
        ThrowingOfstream::remove_file(temp_filename);
        if(failure){
            debug_log("Client to data connection request denied for reason: "+data_deny_reason_to_string(deny_reason));
            THROW_IF_RECOVERABLE(tl.write_type(deny_reason), "Failed to write deny reason", return;);
            THROW_IF_RECOVERABLE(tl.finalize_send(), "Failed to send deny reason", return;);
            if(entry_sptr)
                CreateNewThread([this, entry_sptr](sptr<thread_struct>& this_thread){
                    handle_client_connection_end(entry_sptr, this_thread, CLIENT_CONNECTION_END_STATE::FAILURE);
                });
            return;
        }else if(connection_failure){
            debug_log("Client to data file " + client_case_to_string(request) + " failed");
            if(entry_sptr)
                CreateNewThread([this, entry_sptr](sptr<thread_struct>& this_thread){
                    handle_client_connection_end(entry_sptr, this_thread, CLIENT_CONNECTION_END_STATE::FAILURE);
                });
            return;
        }else{//success
            debug_log("Client to data file " + client_case_to_string(request) + " success");
            CreateNewThread([this, entry_sptr](sptr<thread_struct>& this_thread){
                handle_client_connection_end(entry_sptr, this_thread, CLIENT_CONNECTION_END_STATE::SUCCESS);
            });
        }
    }
    void handle_client_connection_end(sptr<expected_client_entry> entry_sptr, sptr<thread_struct>& this_thread, CLIENT_CONNECTION_END_STATE status){
        //> contact master node
        DEBUG_PRINT("Client connection end for "+entry_sptr->filename+" with status "+client_connection_end_state_to_string(status));
        this_thread->wait_until_unpaused();
        while(true){
            //> outgoing connection meaning we need to retry until succeeds, and pause in cause of failure
            bool failure=true;
            do{
                sptr<connection_struct> connection;
                auto on_delete_connection = CreateAutodeleteOutgoingConnection(addrMaster, std::this_thread::get_id(), this_thread, connection);

                TransmissionLayer &tl = *connection->tl;
                THROW_IF_RECOVERABLE(tl.write_type(PACKET_ID::DATA_NODE_TO_MASTER_CONNECTION_REQUEST), "Failed to write packet id", break;);
                THROW_IF_RECOVERABLE(write_expected_client_entry(tl, *entry_sptr), "Failed to write expected client entry", break;);
                THROW_IF_RECOVERABLE(tl.write_type(status), "Failed to write status", break;);
                THROW_IF_RECOVERABLE(tl.write_type(DIALOGUE_STATUS::END_IMMEDIATELY), "Failed to write dialogue status", break;);
                THROW_IF_RECOVERABLE(tl.finalize_send(), "Failed to notify master ndoe", break;);
                failure=false;
            }while(0);
            if(!failure){
                break;
            }else{
                this_thread->set_failure();
            }
        }
        //> appropriately subtract readers or writers and then send the status to the master
        //% note: this must happen after successfully contacting master so that the state can be correctly recovered if master fails
        {
            std::lock_guard<std::mutex> lock(datanode_mutex);
            //> changes on this_node_info
            if(entry_sptr->request==CLIENT_REQUEST::READ_REQUEST){
                this_node_info.total_readers--;
                data_file_info &fileinfo = this_node_info.files[entry_sptr->filename];
                fileinfo.num_readers--;
            }else if(entry_sptr->request==CLIENT_REQUEST::WRITE_REQUEST){
                this_node_info.total_writers--;
                data_file_info &fileinfo = this_node_info.files[entry_sptr->filename];
                fileinfo.num_writers--;
                if(status==CLIENT_CONNECTION_END_STATE::SUCCESS){
                    //> increment file version
                    fileinfo.version++;
                }
            }else{
                THROW_IF_RECOVERABLE(true, "Invalid request type", return;);
            }
        }
    }
    void handle_master_node_to_data_node_add_new_client(TransmissionLayer &tl){
        sptr<expected_client_entry> entry_sptr;
        THROW_IF_RECOVERABLE(read_expected_client_entry(tl, entry_sptr), "Failed to read expected client entry", return;);
        DEBUG_PRINT("Master node to data node add new client: "+entry_sptr->filename);
        {
            std::lock_guard<std::mutex> lock(datanode_mutex);
            expected_client_requests[entry_sptr->uuid] = entry_sptr;
            //> increment the number of readers or writers
            if(entry_sptr->request==CLIENT_REQUEST::READ_REQUEST){
                this_node_info.total_readers++;
                data_file_info &fileinfo = this_node_info.files[entry_sptr->filename];
                fileinfo.num_readers++;
            }else if(entry_sptr->request==CLIENT_REQUEST::WRITE_REQUEST){
                this_node_info.total_writers++;
                data_file_info &fileinfo = this_node_info.files[entry_sptr->filename];
                fileinfo.num_writers++;
            }else{
                THROW_IF_RECOVERABLE(true, "Invalid request type", return;);
            }
        }
    }
    void handle_data_node_to_master_connection_request(TransmissionLayer &tl){
        sptr<expected_client_entry> entry_sptr;
        THROW_IF_RECOVERABLE(read_expected_client_entry(tl, entry_sptr), "Failed to read expected client entry", return;);
        DEBUG_PRINT("Data node to master connection request: "+entry_sptr->filename);
        CLIENT_CONNECTION_END_STATE status;
        THROW_IF_RECOVERABLE(tl.read_type(status), "Failed to read status", return;);
        {
            //> find in the node_to_info and file_to_info and decrement the number of readers or writers
            std::lock_guard<std::mutex> lock(datanode_mutex);
            auto it = node_to_info.find(entry_sptr->data_node);
            if(it == node_to_info.end()){
                err_log("Data node not found in node_to_info");
                return;
            }
            node_info &info = it->second;
            if(entry_sptr->request==CLIENT_REQUEST::READ_REQUEST){
                info.total_readers--;
                data_file_info &fileinfo = info.files[entry_sptr->filename];
                fileinfo.num_readers--;
            }else if(entry_sptr->request==CLIENT_REQUEST::WRITE_REQUEST){
                info.total_writers--;
                data_file_info &fileinfo = info.files[entry_sptr->filename];
                fileinfo.num_writers--;
            }else{
                THROW_IF_RECOVERABLE(true, "Invalid request type", return;);
            }
            auto it2 = file_to_info.find(entry_sptr->filename);
            if(it2 == file_to_info.end()){
                err_log("File not found in file_to_info");
                return;
            }
            master_file_info &fileinfo = it2->second;
            if(entry_sptr->request==CLIENT_REQUEST::READ_REQUEST){
                fileinfo.num_readers--;
            }else if(entry_sptr->request==CLIENT_REQUEST::WRITE_REQUEST){
                fileinfo.num_writers--;
            }else{
                THROW_IF_RECOVERABLE(true, "Invalid request type", return;);
            }
        }
    }
    void handle_master_to_data_node_info_request(TransmissionLayer &tl){
        DEBUG_PRINT("Master to data node info request");
        node_info info;
        {
            std::lock_guard<std::mutex> lock(datanode_mutex);
            info = this_node_info;
        }
        THROW_IF_RECOVERABLE(write_node_info(tl, info), "Failed to write node info", return;);
        THROW_IF_RECOVERABLE(tl.finalize_send(), "Failed to send node info", return;);
    }
    void handle_master_to_data_node_pause_change(TransmissionLayer &tl){
        DEBUG_PRINT("Master to data node pause change");
        pause_state state;
        THROW_IF_RECOVERABLE(tl.read_type(state), "Failed to read pause state", return;);
        {
            std::lock_guard<std::mutex> lock(datanode_mutex);
            //> go through each thread and set the state
            for(auto &[_, thread]: threads){
                thread->set_state(state);
            }
        }
    }

};

//> note how the previous thread-safe connections and new thread creations are omitted here since ClientNode is a purely single-threaded class

class ClientNode: public Node{
    public:
    ClientNode(const socket_address &_dns_address):Node(_dns_address){}

    void read_or_write_file(const std::string &filename, CLIENT_REQUEST request, std::vector<u8> &vec){
        //> first, ask DNS for master address
        socket_address master_addr;
        {
            OpenSocket open_socket(CLIENT);
            debug_log("Connecting to DNS");
            throw_if(open_socket.connect_to_server(dns_address), "Failed to connect to DNS");

            TransmissionLayer tl(open_socket);

            throw_if(tl.write_type(PACKET_ID::REQUEST_MASTER_ADDRESS), "Failed to write packet id");
            throw_if(tl.write_type(DIALOGUE_STATUS::END_IMMEDIATELY), "Failed to write dialogue status");
            throw_if(tl.finalize_send(), "Failed to send request");

            throw_if(tl.initialize_recv(), "Failed to initialize recv");
            throw_if(read_address(tl, master_addr), "Failed to read address");
        }
        uuid_t uuid(0);
        socket_address data_node;
        //> then, ask the master to read/write the file
        {
            OpenSocket open_socket(CLIENT);
            debug_log("Connecting to master");
            throw_if(open_socket.connect_to_server(master_addr), "Failed to connect to master");

            TransmissionLayer tl(open_socket);

            throw_if(tl.write_type(PACKET_ID::CLIENT_TO_MASTER_CONNECTION_REQUEST), "Failed to write packet id");
            throw_if(tl.write_type(request), "Failed to write request");
            throw_if(write_string(tl, filename), "Failed to write filename");
            throw_if(tl.write_type(DIALOGUE_STATUS::END_IMMEDIATELY), "Failed to write dialogue status");
            throw_if(tl.finalize_send(), "Failed to send request");

            throw_if(tl.initialize_recv(), "Failed to initialize recv");
            DENY_REASON deny_reason;
            throw_if(tl.read_type(deny_reason), "Failed to read deny reason");
            if(deny_reason!=DENY_REASON::ALLOWED){
                throw std::runtime_error("Read request denied for reason: "+deny_reason_to_string(deny_reason));
            }
            throw_if(tl.read_type(uuid.val), "Failed to read uuid");
            throw_if(read_address(tl, data_node), "Failed to read data node address");
        }
        //> finally contact the data node to read/write the file
        {
            OpenSocket open_socket(CLIENT);
            debug_log("Connecting to data node");
            throw_if(open_socket.connect_to_server(data_node), "Failed to connect to data node");

            TransmissionLayer tl(open_socket);

            throw_if(tl.write_type(PACKET_ID::CLIENT_TO_DATA_CONNECTION_REQUEST), "Failed to write packet id");
            throw_if(write_string(tl, filename), "Failed to write filename");
            throw_if(tl.write_type(uuid.val), "Failed to write uuid");
            throw_if(tl.write_type(request), "Failed to write request");
            throw_if(tl.finalize_send(), "Failed to send request");

            //receive allow
            DATA_DENY_REASON deny_reason;
            throw_if(tl.initialize_recv(), "Failed to initialize recv");
            throw_if(tl.read_type(deny_reason), "Failed to read deny reason");
            if(deny_reason!=DATA_DENY_REASON::ALLOWED){
                throw std::runtime_error("Read request denied for reason: "+data_deny_reason_to_string(deny_reason));
            }

            if(request==CLIENT_REQUEST::READ_REQUEST){
                throw_if(tl.write_type(DIALOGUE_STATUS::END_IMMEDIATELY), "Failed to write dialogue status");
                throw_if(tl.finalize_send(), "Failed to send request");

                throw_if(tl.initialize_recv(), "Failed to initialize recv");
                throw_if(read_vector(tl, vec), "Failed to read file");

                //> avoid broken pipe
                throw_if(tl.write_byte(OK_REPONSE), "Failed to write ok response");
                throw_if(tl.finalize_send(), "Failed to send ok response");

            }else if(request==CLIENT_REQUEST::WRITE_REQUEST){
                throw_if(write_vector(tl, vec), "Failed to send file");
                throw_if(tl.write_type(DIALOGUE_STATUS::END_IMMEDIATELY), "Failed to write dialogue status");
                throw_if(tl.finalize_send(), "Failed to send file");

                //> avoid broken pipe
                throw_if(tl.initialize_recv(), "Failed to initialize recv");

            }else{
                throw std::runtime_error("Invalid request type");
            }
        }
        debug_log(client_case_to_string(request)+" request for "+filename+" success");
    }


};



}//namespace distribsys

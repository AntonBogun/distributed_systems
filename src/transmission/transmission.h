#pragma once
#include "sockets.h"
#include "utils.h"


#include <algorithm>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>
#include <queue>
#include <chrono>



enum SOCKET_TYPE
{
    CLIENT,
    SERVER
};

enum class error_code
{
    NO_ERROR = 0,
    MAX_BATCH_SEND_TIME = 1,
    MAX_BATCH_RECEIVE_TIME = 2,
    TOO_LOW_THROUGHPUT = 3,
    SOCKET_ERROR = 4 //+ errno set
};


constexpr in_port_t DEFAULT_PORT = 8080;
constexpr int MAX_MESSAGE_SIZE = 4096;
constexpr int BACKLOG = 5;

constexpr int TRANSMISSION_LAYER_HEADER = sizeof(int) + sizeof(bool); // datalen + batches_continue
constexpr int MAX_DATA_SIZE = MAX_MESSAGE_SIZE - TRANSMISSION_LAYER_HEADER;

const TimeValue NO_DELAY{0.0};

#define DEBUG_PRINTS 0


static std::mutex io_mutex;
// safely print to cout on multiple threads
#define MUTEX_PRINT(x)                              \
    do                                              \
    {                                               \
        std::lock_guard<std::mutex> lock(io_mutex); \
        std::cout << x << std::endl;                \
    } while (0)
#define MUTEX_PRINT_INLINE(x)                       \
    do                                              \
    {                                               \
        std::lock_guard<std::mutex> lock(io_mutex); \
        std::cout << x << std::endl;                \
    } while (0)

#if DEBUG_PRINTS
#define DEBUG_PRINT(x) MUTEX_PRINT(x)
#define DEBUG_PRINT_INLINE(x) MUTEX_PRINT_INLINE(x)
#else
#define DEBUG_PRINT(x)
#define DEBUG_PRINT_INLINE(x)
#endif


static_assert(sizeof(int) == sizeof(i32), "int and i32 must be the same size");




class OpenSocket
{
public:
    bool valid = false;

    FCVector<char> in_buffer;
    FCVector<char> out_buffer;

    int allocated_fd; // file descriptor associated with socket
    socket_address other_address;
    SOCKET_TYPE sock_type;

    i64 num_send = 0;
    i64 num_recv = 0;
    void reset_counts(){
        num_send = 0;
        num_recv = 0;
    }

    OpenSocket(SOCKET_TYPE sock_type_) : in_buffer(MAX_MESSAGE_SIZE, 0),
                                         out_buffer(MAX_MESSAGE_SIZE, 0),
                                         sock_type(sock_type_)
    {
    }

    void accept_connection(in_port_t server_fd)
    { // server file descriptor
        throw_if(sock_type != SERVER, "Cannot accept connection on client socket");
        throw_if(valid, "Cannot accept, socket already valid");

        struct sockaddr_in client_address;    // overwritten by accept
        int addrlen = sizeof(client_address); // overwritten by accept
        allocated_fd = accept(server_fd, (struct sockaddr *)&client_address, (socklen_t *)&addrlen);
        throw_if(allocated_fd < 0,
                 prints_new("Failed to accept connection, errno:", errno));

        other_address = socket_address::from_sockaddr_in(client_address);

        socket_address client_fd_address = socket_address::from_fd_remote(allocated_fd);

        throw_if(client_fd_address != other_address,
                 prints_new("Accept address mismatch: ", socket_address_to_string(client_fd_address), " != ", socket_address_to_string(other_address)));

        MUTEX_PRINT(prints_new("Accepted connection from client: ", socket_address_to_string(other_address),
                               " to server: ", socket_address_to_string(socket_address::from_fd_local(allocated_fd))));
        valid = true;
    }
    void connect_to_server(socket_address address)
    {
        throw_if(sock_type != CLIENT, "Cannot connect to server on server socket");
        throw_if(valid, "Cannot connect, socket already valid");

        // create new file descriptor and then connect binds it to free port
        allocated_fd = create_socket_throw();

        set_socket_options_throw(allocated_fd);

        struct sockaddr_in server_address = address.to_sockaddr_in();

        throw_if(connect(allocated_fd, (struct sockaddr *)&server_address, sizeof(server_address)) < 0,
                 prints_new("Failed to connect to server, errno:", errno));

        other_address = address;

        socket_address server_fd_address = socket_address::from_fd_remote(allocated_fd);

        throw_if(server_fd_address != other_address,
                 prints_new("Connect address mismatch: ", socket_address_to_string(server_fd_address), " != ", socket_address_to_string(other_address)));

        MUTEX_PRINT(prints_new("Connected to server: ", socket_address_to_string(other_address),
                               " from client: ", socket_address_to_string(socket_address::from_fd_local(allocated_fd))));
        valid = true;
    }
    [[nodiscard]]
    ssize_t Send(int fd, const void *buf, size_t len, int flags)
    {
        num_send++;
        return ::send(fd, buf, len, flags);
    }
    [[nodiscard]]
    ssize_t Recv(int fd, void *buf, size_t len, int flags)
    {
        num_recv++;
        return ::recv(fd, buf, len, flags);
    }
    ~OpenSocket()
    {
        if (valid)
        {
            close(allocated_fd);
        }
    }
};





class TransmissionLayer{
    //=layer description:
    // i32 batch size
    // bool batches continue
    public:
    OpenSocket& sock;
    //actual buf size, represent total amount of data written so far 
    //% note: starts at TRANSMISSION_LAYER_HEADER since that is always reserved
    int total_written = TRANSMISSION_LAYER_HEADER;
    //represents received thus far (during recv_n) or total (after recv_n) in a batch
    int total_received = 0;
    //actual sent size, used during transmission
    //represents total amount of data sent so far
    int total_sent = 0;
    //actual read size, starts at TRANSMISSION_LAYER_HEADER
    int total_read = TRANSMISSION_LAYER_HEADER;
    //MAX_MESSAGE_SIZE - total_written
    inline int remaining_out_buf_to_write() const { return MAX_MESSAGE_SIZE - total_written; }
    //total_received - total_read
    //% NOTICE: different from remaining_out_buf_to_write
    //% this is because read.. cares about remaining *received* space in the buffer
    //% while write.. cares about remaining *free* space in the buffer
    inline int remaining_in_buf_to_read() const { return total_received - total_read; }

    i64 batches_sent = 0;
    i64 batches_received = 0;

    //% assumes all of these non-zero
    TimeValue since_last_non0_timeout = 5;//timeout after this many seconds of no data
    TimeValue max_batch_send_time = 20;//timeout after this many seconds of sending one batch
    TimeValue max_batch_receive_time = 25;//timeout after this many seconds of receiving one batch
    double min_data_throughput_limit = 4000;//bytes per second
    i32 min_data_throughput_batch_size = 2000;//bytes, how large a batch should be to be considered for throughput limit
    double last_batches = 30;//how many batches until we start checking for throughput limit

    std::queue<std::pair<TimeValue,i32>> batch_send_info;
    std::pair<TimeValue,i64> batch_send_info_cumulative = {0,0};

    //cached
    int recv_batch_len = -1;
    bool recv_batch_continue = false;


    double get_throughput(){
        if(batch_send_info_cumulative.first.is_near(0)){
            return 0;
        }
        return batch_send_info_cumulative.second/batch_send_info_cumulative.first.to_double();
    }

    //% returns true if we should timeout
    [[nodiscard]]
    bool update_throughput_queue(TimeValue time, i32 size){
        if(size>=min_data_throughput_batch_size){
            batch_send_info.push({time,size});
            batch_send_info_cumulative.first+=time;
            batch_send_info_cumulative.second+=size;
            while(batch_send_info.size()>last_batches){
                auto front = batch_send_info.front();
                batch_send_info.pop();
                batch_send_info_cumulative.first-=front.first;
                batch_send_info_cumulative.second-=front.second;
            }
            if(batch_send_info.size()>=last_batches){
                double throughput = get_throughput();
                if(throughput<min_data_throughput_limit){
                    return true;
                }
            }
        }
        return false;
    }
    void reset_throughput(){
        while(!batch_send_info.empty()){
            batch_send_info.pop();
        }
        batch_send_info_cumulative = {0,0};
    }

    error_code last_error = error_code::NO_ERROR;
    void reset_error(){
        last_error = error_code::NO_ERROR;
    }
    void reset(){
        total_written = TRANSMISSION_LAYER_HEADER;
        total_received = 0;
        total_sent = 0;
        total_read = TRANSMISSION_LAYER_HEADER;
        reset_throughput();
        reset_error();
        batches_sent = 0;
        batches_received = 0;

        recv_batch_len = -1;
        recv_batch_continue = false;
    }

    TransmissionLayer(OpenSocket& sock_):
        sock(sock_){
            throw_if(!sock.valid, "Socket not valid on TransmissionLayer creation");
        }

    inline char* out_buf_at(int offset){
        return sock.out_buffer.data()+offset;
    }
    inline char* in_buf_at(int offset){
        return sock.in_buffer.data()+offset;
    }

    //% true if error
    [[nodiscard]]
    bool send_if_dont_fit(const char* stream, int size){
        while(size > remaining_out_buf_to_write()){//send loop
            int remaining = remaining_out_buf_to_write();
            DEBUG_PRINT(prints_new("send:", size, ">", remaining,"written:", total_written));
            // std::memcpy(sock.out_buffer.data()+total_written, stream, remaining_out_buf());
            std::memcpy(out_buf_at(total_written), stream, remaining);
            size-=remaining;//subtract written
            stream+=remaining;//move pointer forward
            total_written+=remaining;//finish up the buffer
            if (construct_and_send(1)) return true;
        }
        if(size>0){
            std::memcpy(out_buf_at(total_written), stream, size);
            total_written+=size;
        }
        return false;
    }

    //% true if error
    [[nodiscard]]
    bool construct_and_send(bool batches_continue){
        throw_if(!sock.valid, "Socket not valid on construct_and_send");
        const int data_len = total_written-TRANSMISSION_LAYER_HEADER;

        throw_if(batches_continue&&data_len!=MAX_DATA_SIZE, 
            prints_new("Data length mismatch on construct_and_send:", data_len, " != ", MAX_DATA_SIZE));

        std::memcpy(out_buf_at(0), &data_len, sizeof(int));//set data length
        std::memset(out_buf_at(sizeof(int)), batches_continue, sizeof(bool));//set batches continue

        if(send_n(total_written)){
            return true;
        }
        DEBUG_PRINT(prints_new("Sent batch: ", batches_sent, " with data length: ", data_len, " and continue: ", batches_continue));
        total_sent = 0;//reset total sent
        total_written = TRANSMISSION_LAYER_HEADER;//the header is reserved

        batches_sent++;
        return false;
    }
    //! Note: Does not do any checks:
    // - does not check validity of socket
    // - does not reset buffer so it should be large enough to hold the message
    // - does not check if the message is too large
    //% true if error
    [[nodiscard]]
    bool send_n(int to_send)
    {
        TimeValue start=TimeValue::now();
        TimeValue max_time = start+max_batch_send_time;
        while (to_send > 0)
        {
            TimeValue now = TimeValue::now();
            if(now>max_time){//timeout
                last_error = error_code::MAX_BATCH_SEND_TIME;
                return true;
            }
            if(set_socket_send_timeout(sock.allocated_fd, min(max_time-now, since_last_non0_timeout))){
                last_error = error_code::SOCKET_ERROR;
                return true;
            }
            i64 valsend = sock.Send(sock.allocated_fd, out_buf_at(total_sent), to_send, 0);
            if(valsend<0){
                if(errno==EINTR) continue; //interrupted, retry
                if(errno==EAGAIN||errno==EWOULDBLOCK) continue; //timeout, retry
                last_error = error_code::SOCKET_ERROR;
                return true;
            }
            throw_if(valsend == 0, "Unexpected valsend == 0 on send_n");//~should never happen
            total_sent += valsend;
            to_send -= valsend;
        }
        if(TimeValue::now()>max_time){
            last_error = error_code::MAX_BATCH_SEND_TIME;
            return true;
        }
        throw_if(to_send<0, "Unexpected to_send < 0 on send_n");
        if(update_throughput_queue(TimeValue::now()-start, total_sent)){
            last_error = error_code::TOO_LOW_THROUGHPUT;
            return true;
        }
        return false;
    }
    [[nodiscard]]
    bool write_byte(u8 byte){
        return send_if_dont_fit((char*)&byte, 1);
    }
    [[nodiscard]]
    bool write_i32(i32 val){
        return send_if_dont_fit((char*)&val, sizeof(i32));
    }
    [[nodiscard]]
    bool write_u64(u64 val){
        return send_if_dont_fit((char*)&val, sizeof(u64));
    }
    [[nodiscard]]
    bool write_i64(i64 val){
        return send_if_dont_fit((char*)&val, sizeof(i64));
    }
    [[nodiscard]]
    bool write_double(double val){
        return send_if_dont_fit((char*)&val, sizeof(double));
    }
    [[nodiscard]]
    bool write_string(const std::string& str){
        if(write_i32(str.size())) return true;
        return send_if_dont_fit(str.data(), str.size());
    }
    //% does not add size of array to the message
    //% warning: handles at most INT_MAX bytes
    [[nodiscard]]
    bool write_bytes(const char* arr, int size){
        return send_if_dont_fit(arr, size);
    }
    [[nodiscard]]
    bool finalize_send(){
        return construct_and_send(0);
    }

    
    //% true if error
    [[nodiscard]]
    bool recv_if_not_enough(char* stream,int size){
        while(size>remaining_in_buf_to_read()){
            //trying to read more than the partially full last batch
            throw_if(total_read+size>total_received && total_received<MAX_MESSAGE_SIZE, 
            prints_new("Attemt to read beyond total message size: size=",size,",", total_read+size, " > ", total_received));
            //trying to read more than the full last batch
            throw_if(!recv_batch_continue && total_read+size>total_received, 
            prints_new("Attemt to request a new batch on the last batch: size=",size,",", total_read+size, " > ", total_received));

            int remaining = remaining_in_buf_to_read();
            std::memcpy(stream, in_buf_at(total_read), remaining);
            size-=remaining;//subtract read
            stream+=remaining;//move pointer forward
            total_read+=remaining;//finish up the buffer
            if(recv_new_batch()) return true;
        }
        if(size>0){
            std::memcpy(stream, in_buf_at(total_read), size);
            total_read+=size;
        }
        return false;
    }
    [[nodiscard]]
    bool initialize_recv(){
        if(recv_new_batch()){
            return true;
        }
        return false;
    }
    //% true if error
    //% necessary because need to first get TRANSMISSION_LAYER_HEADER to know how much to read
    [[nodiscard]]
    bool recv_n(int to_recv){
        TimeValue start=TimeValue::now();
        TimeValue max_time = start+max_batch_receive_time;
        while (to_recv > 0)
        {
            TimeValue now = TimeValue::now();
            if(now>max_time){//timeout
                last_error = error_code::MAX_BATCH_RECEIVE_TIME;
                return true;
            }
            if(set_socket_recv_timeout(sock.allocated_fd, min(max_time-now, since_last_non0_timeout))){
                last_error = error_code::SOCKET_ERROR;
                return true;
            }
            i64 valrecv = sock.Recv(sock.allocated_fd, in_buf_at(total_received), to_recv, 0);
            if(valrecv<0){
                if(errno==EINTR) continue; //interrupted, retry
                if(errno==EAGAIN||errno==EWOULDBLOCK) continue; //timeout, retry
                last_error = error_code::SOCKET_ERROR;
                return true;
            }
            throw_if(valrecv == 0, "Unexpected valrecv == 0 on recv_n");//~should never happen
            total_received += valrecv;
            to_recv -= valrecv;
        }
        if(TimeValue::now()>max_time){
            last_error = error_code::MAX_BATCH_RECEIVE_TIME;
            return true;
        }
        throw_if(to_recv<0, "Unexpected to_recv < 0 on recv_n");
        return false;
    }
    //% true if error
    [[nodiscard]]
    bool recv_new_batch(){
        throw_if(!sock.valid, "Socket not valid on recv_new_batch");
        TimeValue start=TimeValue::now();
        total_received = 0;//reset total_received
        if(recv_n(TRANSMISSION_LAYER_HEADER)){
            return true;
        }
        std::memcpy(&recv_batch_len, in_buf_at(0), sizeof(int));
        std::memcpy(&recv_batch_continue, in_buf_at(sizeof(int)), sizeof(bool));
        
        //% check for protocol errors
        throw_if(recv_batch_len>MAX_DATA_SIZE,
            prints_new("Data length mismatch on recv_new_batch:", recv_batch_len, " > ", MAX_DATA_SIZE));
        throw_if(recv_batch_len!=MAX_DATA_SIZE && recv_batch_continue,
            prints_new("Invalid batch len while batches continue:", recv_batch_len));

        if(recv_n(recv_batch_len)){
            return true;
        }
        total_read = TRANSMISSION_LAYER_HEADER;//since we've just read the header

        batches_received++;

        if(update_throughput_queue(TimeValue::now()-start, total_received)){
            last_error = error_code::TOO_LOW_THROUGHPUT;
            return true;
        }
        DEBUG_PRINT(prints_new("Received batch: ", batches_received, " with data length: ", recv_batch_len, " and continue: ", recv_batch_continue));
        return false;
    }
    [[nodiscard]]
    bool read_byte(u8& byte){
        return recv_if_not_enough((char*)&byte, 1);
    }
    [[nodiscard]]
    bool read_i32(i32& val){
        return recv_if_not_enough((char*)&val, sizeof(i32));
    }
    [[nodiscard]]
    bool read_u64(u64& val){
        return recv_if_not_enough((char*)&val, sizeof(u64));
    }
    [[nodiscard]]
    bool read_i64(i64& val){
        return recv_if_not_enough((char*)&val, sizeof(i64));
    }
    [[nodiscard]]
    bool read_double(double& val){
        return recv_if_not_enough((char*)&val, sizeof(double));
    }
    [[nodiscard]]
    bool read_string(std::string& str){
        i32 size;
        if(read_i32(size)) return true;
        str.resize(size);
        return recv_if_not_enough(str.data(), size);
    }
    [[nodiscard]]
    bool read_bytes(char* arr, int size){
        return recv_if_not_enough(arr, size);
    }
    void print_errors(){
        double throughput;
        switch(last_error){
            case error_code::NO_ERROR:
                MUTEX_PRINT("No error");
                break;
            case error_code::MAX_BATCH_SEND_TIME:
                MUTEX_PRINT("Max batch send time exceeded");
                break;
            case error_code::MAX_BATCH_RECEIVE_TIME:
                MUTEX_PRINT("Max batch receive time exceeded");
                break;
            case error_code::TOO_LOW_THROUGHPUT:
                throughput = get_throughput();
                MUTEX_PRINT("Too low throughput: "+std::to_string(throughput)+" bytes per second");
                break;
            case error_code::SOCKET_ERROR:
                MUTEX_PRINT("Socket error: errno="+std::to_string(static_cast<int>(errno)));
                break;
            default:
                MUTEX_PRINT("Unknown error: errno="+std::to_string(static_cast<int>(errno)));
                break;
        }
    }
};


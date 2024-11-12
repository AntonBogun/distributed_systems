#include "utils.h"
#include "sockets.h"
#include <algorithm>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>
#include <queue>
#include <chrono>

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


constexpr in_port_t DEFAULT_PORT = 8080;
constexpr int MAX_MESSAGE_SIZE = 4096;
constexpr int BACKLOG = 5;
#define DO_SOCKETS 1
#if DO_SOCKETS
enum SOCKET_TYPE
{
    CLIENT,
    SERVER
};
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
constexpr int TRANSMISSION_LAYER_INFO = sizeof(int)+sizeof(bool);//datalen + batches_continue
constexpr int CONTINUITY_LEN = sizeof(u64);//continuity between batches
constexpr int TRANSMISSION_LAYER_HEADER = TRANSMISSION_LAYER_INFO+CONTINUITY_LEN;
constexpr int MAX_DATA_SIZE = MAX_MESSAGE_SIZE - TRANSMISSION_LAYER_HEADER;
enum class error_code{
    NO_ERROR=0,
    MAX_BATCH_SEND_TIME=1,
    MAX_BATCH_RECEIVE_TIME=2,
    TOO_LOW_THROUGHPUT=3,
    SOCKET_ERROR=4//+ errno set
};
std::string rate_to_string(double rate){//bytes per s
    std::stringstream ss;
    if(rate<1e3){
        ss<<rate<<"B/s";
    }else if(rate<1e6){
        ss<<rate/1e3<<"KB/s";
    }else if(rate<1e9){
        ss<<rate/1e6<<"MB/s";
    }else{
        ss<<rate/1e9<<"GB/s";
    }
    return ss.str();
}
class TransmissionLayer{
    //=layer description:
    // i32 batch size
    // bool batches continue
    public:
    OpenSocket& sock;
    FCVector<u8> continuity_buffer;
    //actual buf size, represent total amount of data written so far 
    //% note: starts at TRANSMISSION_LAYER_HEADER since that is always reserved
    int total_written = TRANSMISSION_LAYER_HEADER;
    //represents received thus far (during recv_n) or total (after recv_n) in a batch
    int total_received = 0;
    //actual sent size, used during transmission
    //represents total amount of data sent so far
    int total_sent = 0;
    //actual read size, starts at TRANSMISSION_LAYER_HEADER 
    //% NOTE: this can be less than TRANSMISSION_LAYER_HEADER (by at most CONTINUITY_LEN (-1?))
    //% when performing continuity merge
    int total_read = TRANSMISSION_LAYER_HEADER;
    //MAX_MESSAGE_SIZE - total_written
    inline int remaining_out_buf_to_write() const { return MAX_MESSAGE_SIZE - total_written; }
    //MAX_MESSAGE_SIZE - total_read
    //% NOTICE: this uses total_written instead of total_written as with out_buf_to_write
    //% this is because read.. cares about remaining *received* space in the buffer
    //% while write.. cares about remaining *free* space in the buffer
    inline int remaining_in_buf_to_read() const { return MAX_MESSAGE_SIZE - total_read; }

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
        sock(sock_),
        continuity_buffer(CONTINUITY_LEN, 0){
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

        //~ neither of these are required since the continuity segment is never used in sending,
        //~ it may be anything on send
        // std::memcpy(out_buf_at(TRANSMISSION_LAYER_INFO), continuity_buffer.data(), CONTINUITY_LEN);
        // std::memset(out_buf_at(TRANSMISSION_LAYER_INFO), 0, CONTINUITY_LEN);

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
    [[nodiscard]]
    bool finalize_send(){
        return construct_and_send(0);
    }

    
    //% true if error
    //~ note that this is undefined if size is above CONTINUITY_LEN
    [[nodiscard]]
    bool recv_if_not_enough(int size){
        //trying to read more than the protocol allows
        throw_if(!recv_batch_continue && total_read+size>total_received, 
            prints_new("Size mismatch on recv_if_not_enough: size=",size,",", total_read+size, " > ", total_received));
        if(size > remaining_in_buf_to_read()){//have to read more
            //protocol error
            throw_if(!recv_batch_continue, "Unexpected batches continue == 0 on recv_if_not_enough");
            auto continuity = remaining_in_buf_to_read(); 
            DEBUG_PRINT(prints_new("Continuity read: ", continuity));
            if(recv_new_batch(1)){
                return true;
            }
            //places read pointer backward into the continuity segment copied from the previous batch
            total_read -= continuity;
        }
        return false;
    }
    [[nodiscard]]
    bool recv_next_if_need(){
        //% check for total_received==MAX_MESSAGE_SIZE expected to have happened earlier
        if(recv_batch_continue && total_read==total_received){
            if(recv_new_batch(1)){
                return true;
            }
        }
        return false;
    }
    [[nodiscard]]
    bool initialize_recv(){
        if(recv_new_batch(0)){
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
    bool recv_new_batch(bool copy_continuity){
        //copy the last batch's continuity segment to continuity_buffer
        //%note: doesn't check for the total_received to be MAX_MESSAGE_SIZE for simplicity
        throw_if(!sock.valid, "Socket not valid on recv_new_batch");
        TimeValue start=TimeValue::now();
        if(copy_continuity){
            std::memcpy(continuity_buffer.data(), in_buf_at(total_received-CONTINUITY_LEN), CONTINUITY_LEN);
        }

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
        total_read = TRANSMISSION_LAYER_HEADER;//since we just read the header

        batches_received++;

        if(copy_continuity){
            std::memcpy(in_buf_at(TRANSMISSION_LAYER_INFO), continuity_buffer.data(), CONTINUITY_LEN);
            //% do not adjust total_read here based on continuity because 
            //% it doesn't get passed into the function
        }
        if(update_throughput_queue(TimeValue::now()-start, total_received)){
            last_error = error_code::TOO_LOW_THROUGHPUT;
            return true;
        }
        DEBUG_PRINT(prints_new("Received batch: ", batches_received, " with data length: ", recv_batch_len, " and continue: ", recv_batch_continue));
        return false;
    }
    [[nodiscard]]
    bool read_byte(u8& byte){
        if(recv_if_not_enough(1)){
            return true;
        }
        std::memcpy(&byte, in_buf_at(total_read), 1);
        total_read+=1;
        return false;
    }
    [[nodiscard]]
    bool read_i32(i32& val){
        if(recv_if_not_enough(sizeof(i32))){
            return true;
        }
        std::memcpy(&val, in_buf_at(total_read), sizeof(i32));
        total_read+=sizeof(i32);
        return false;
    }
    [[nodiscard]]
    bool read_u64(u64& val){
        if(recv_if_not_enough(sizeof(u64))){
            return true;
        }
        std::memcpy(&val, in_buf_at(total_read), sizeof(u64));
        total_read+=sizeof(u64);
        return false;
    }
    [[nodiscard]]
    bool read_i64(i64& val){
        if(recv_if_not_enough(sizeof(i64))){
            return true;
        }
        std::memcpy(&val, in_buf_at(total_read), sizeof(i64));
        total_read+=sizeof(i64);
        return false;
    }
    [[nodiscard]]
    bool read_double(double& val){
        if(recv_if_not_enough(sizeof(double))){
            return true;
        }
        std::memcpy(&val, in_buf_at(total_read), sizeof(double));
        total_read+=sizeof(double);
        return false;
    }
    [[nodiscard]]
    bool read_string(std::string& str){
        i32 size;
        if(read_i32(size)) return true;
        if(recv_if_not_enough(size)){
            return true;
        }
        str.resize(size);
        //need to make a loop since string may be larger than remaining_in_buf
        int read = 0;
        while(read<size){
            int to_read = min(size-read, remaining_in_buf_to_read());
            std::memcpy(&str[read], in_buf_at(total_read), to_read);
            total_read+=to_read;
            read+=to_read;
            if(recv_next_if_need()){
                return true;
            }
        }
        return false;
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


class ServerSocket
{
    int socket_fd;

public:
    bool connection_open = false;
    ServerSocket(in_port_t port)
    {

        socket_fd = create_socket_throw();

        set_socket_options_throw(socket_fd);

        // struct sockaddr_in server_address;
        // server_address.sin_family = AF_INET;
        // server_address.sin_addr.s_addr = htonl(INADDR_ANY);
        // server_address.sin_port = htons(PORT);
        struct sockaddr_in bind_address = socket_address(0, 0, 0, 0, port).to_sockaddr_in(); // 0,0,0,0 to bind to all interfaces
        throw_if(bind(socket_fd, (struct sockaddr *)&bind_address, sizeof(bind_address)) < 0,
                 prints_new("Failed to bind socket, errno:", errno));

        socket_address bound_address = socket_address::from_fd_local(socket_fd);

        throw_if(bound_address.port != port,
                 prints_new("Bind port mismatch: bound ", bound_address.port, " != target ", port));

        printcout(prints_new("Server socket bound to: ", socket_address_to_string(bound_address)));

        throw_if(listen(socket_fd, BACKLOG) < 0,
                 prints_new("Failed to listen, errno:", errno));

        printcout("Listening for connections...");
    }

    int get_socket_fd()
    {
        return socket_fd;
    }
    void close_socket()
    {
        close(socket_fd);
    }
    ~ServerSocket()
    {
        close_socket();
    }
};
#endif

std::vector<ipv4_addr> getIPAddresses()
{
    std::vector<ipv4_addr> addresses;
    struct ifaddrs *ifAddrStruct = nullptr;
    struct ifaddrs *ifa = nullptr;

    if (getifaddrs(&ifAddrStruct) == -1)
    {
        throw std::runtime_error("Failed to get network interfaces");
    }

    for (ifa = ifAddrStruct; ifa != nullptr; ifa = ifa->ifa_next)
    {
        if (!ifa->ifa_addr)
        {
            continue;
        }

        // Check for IPv4 addresses
        if (ifa->ifa_addr->sa_family == AF_INET)
        {
            auto *sockaddr_in_ptr = reinterpret_cast<struct sockaddr_in *>(ifa->ifa_addr);
            void *tmpAddrPtr = &sockaddr_in_ptr->sin_addr;

            printcout(prints_new(
                "family: ", ifa->ifa_addr->sa_family,
                "sin_port: ", sockaddr_in_ptr->sin_port,
                "sin_addr: ", ip_to_string(ipv4_addr::from_in_addr(sockaddr_in_ptr->sin_addr))));

            char addressBuffer[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);

            // Skip localhost addresses
            if (strcmp(addressBuffer, "127.0.0.1") != 0)
            {
                addresses.push_back(ipv4_addr::from_in_addr(sockaddr_in_ptr->sin_addr));
            }
        }
        // // Optionally check for IPv6 addresses
        // else if (ifa->ifa_addr->sa_family == AF_INET6) {
        //     void* tmpAddrPtr = &((struct sockaddr_in6*)ifa->ifa_addr)->sin6_addr;
        //     char addressBuffer[INET6_ADDRSTRLEN];
        //     inet_ntop(AF_INET6, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN);

        //     // Skip localhost addresses
        //     if (strcmp(addressBuffer, "::1") != 0) {
        //         addresses.push_back(addressBuffer);
        //     }
        // }
    }

    if (ifAddrStruct != nullptr)
    {
        freeifaddrs(ifAddrStruct);
    }

    return addresses;
}
#define TL_ERR_and_return(x, y) \
    {                           \
        if (x)                  \
        {                       \
            y;                  \
            return;             \
        }                       \
    }

int main()
{

    //==== testing
    printcout(prints_new("Hello", "World"));
    printcout(prints_new("INET_ADDRSTRLEN: ", INET_ADDRSTRLEN));
    printcout(prints_new("INET6_ADDRSTRLEN: ", INET6_ADDRSTRLEN));
    printcout(prints_new("sizeof(sockaddr): ", sizeof(sockaddr)));
    printcout(prints_new("sizeof(sockaddr_in): ", sizeof(sockaddr_in)));
    printcout(prints_new("AF_INET: ", AF_INET));
    printcout(prints_new("AF_INET6: ", AF_INET6));

    in_port_t test_port = 8080;
    printcout(prints_new("Test Port: ", test_port));
    // struct in_addr test_addr;
    // inet_aton("192.168.1.1", &test_addr);
    struct in_addr test_addr = ipv4_addr{192, 168, 1, 1}.to_in_addr();
    // inet_aton("192.168.1.1", &test_addr);

    ipv4_addr mytest_addr = ipv4_addr::from_in_addr(test_addr);
    std::string s1, s2;
    s1 = ip_to_string(test_addr);
    printcout(prints_new("test_addr: ", s1));
    printcout(prints_new("mytest_addr: ", ip_to_string(mytest_addr)));
    struct in_addr test_addr2 = mytest_addr.to_in_addr();
    s2 = ip_to_string(test_addr2);
    printcout(prints_new("test_addr2: ", s2));
    if (s1 != s2)
    {
        throw std::runtime_error("Conversion failed");
    }

    // struct sockaddr_in test_sockaddr;
    // test_sockaddr.sin_family = AF_INET;
    // test_sockaddr.sin_addr = test_addr;
    // test_sockaddr.sin_port = htons(port);
    // TODO: HoangLe [Nov-10]: Parameterize the IP
    struct sockaddr_in test_sockaddr = socket_address{192, 168, 1, 1, test_port}.to_sockaddr_in();
    socket_address mytest_sockaddr = socket_address::from_sockaddr_in(test_sockaddr);
    std::string s3, s4;
    s3 = socket_address_to_string(test_sockaddr);
    printcout(prints_new("test_sockaddr: ", s3));
    printcout(prints_new("mytest_sockaddr: ", socket_address_to_string(mytest_sockaddr)));
    struct sockaddr_in test_sockaddr2 = mytest_sockaddr.to_sockaddr_in();
    s4 = socket_address_to_string(test_sockaddr2);
    printcout(prints_new("test_sockaddr2: ", s4));
    if (s3 != s4)
    {
        throw std::runtime_error("Conversion failed");
    }
    printcout("\n");
    //==== testing end

    std::vector<ipv4_addr> addresses = getIPAddresses();
    if (addresses.empty())
    {
        throw std::runtime_error("No network interfaces found");
    }
    ipv4_addr ip = addresses[0];
    printcout(prints_new("using ip: ", ip_to_string(ip)));

    in_port_t port = 8080;
    printcout(prints_new("Using Server Port: ", port));
    TimeValue tv=TimeValue::now();
    printcout(prints_new("Time: ", tv.to_duration_string()));
    printcout(prints_new("Date: ", tv.to_date_string()));
    TimeValue tv2=TimeValue(1.5);
    printcout(prints_new("Duration: ", tv2.to_duration_string()));
    TimeValue tv3=TimeValue(-0.4);
    printcout(prints_new("Duration: ", tv3.to_duration_string()));
    #if DO_SOCKETS
    ServerSocket server_socket(port);
    int server_fd = server_socket.get_socket_fd();

    // Create thread for server and client service
    int vector_n=100000;
    auto server_thread = std::thread(
        [&server_socket, &server_fd, vector_n]()
        {
            OpenSocket open_socket(SERVER);
            open_socket.accept_connection(server_fd);
            // open_socket.read_message();
            TransmissionLayer tl(open_socket);
            TL_ERR_and_return(tl.write_string("(Server -> Client) Hello World"), tl.print_errors());
            TL_ERR_and_return(tl.finalize_send(), tl.print_errors());
            std::string str;
            TL_ERR_and_return(tl.initialize_recv(), tl.print_errors());
            TL_ERR_and_return(tl.read_string(str), tl.print_errors());
            printcout(prints_new("Received: ", str));
            std::vector<int> v(vector_n, 0);
            i64 sum=0;
            for(int i=0;i<vector_n;i++){
                v[i]=i;
                sum+=i;
            }
            printcout(prints_new("Sum: ", sum));
            printcout("sending vector");
            for(int i=0;i<vector_n;i++){
                TL_ERR_and_return(tl.write_i32(v[i]),
                 {printcout(prints_new("break at i",i)); tl.print_errors();});
            }
            TL_ERR_and_return(tl.finalize_send(), tl.print_errors());
            printcout(prints_new("final server throughput: ", rate_to_string(tl.get_throughput())));
            // NOTE: HoangLe [Nov-10]: Temporarily comment out this for testing purpose
            // open_socket.send_message("\nServer:Client Hello World\n");
        });
    auto client_thread = std::thread(
        [&ip, &port, vector_n]()
        {
            OpenSocket open_socket(CLIENT);
            socket_address server_address(ip, port);
            open_socket.connect_to_server(server_address);

            TransmissionLayer tl(open_socket);
            std::string str;
            TL_ERR_and_return(tl.initialize_recv(), tl.print_errors());
            TL_ERR_and_return(tl.read_string(str), tl.print_errors());
            printcout(prints_new("Received: ", str));
            TL_ERR_and_return(tl.write_string("(Client -> Server) Hello World"), tl.print_errors());
            TL_ERR_and_return(tl.finalize_send(), tl.print_errors());
            std::vector<int> v(vector_n, 0);
            printcout("receiving vector");
            TL_ERR_and_return(tl.initialize_recv(), tl.print_errors());
            i64 sum=0;
            i32 expected=0;
            for(int i=0;i<vector_n;i++){
                TL_ERR_and_return(tl.read_i32(v[i]), tl.print_errors());
                sum+=v[i];
                if(v[i]!=expected){
                    printcout(prints_new("Mismatch at i: ", i, " expected: ", expected, " got: ", v[i]));
                    break;
                }
                expected++;
            }
            printcout(prints_new("Sum: ", sum));
            printcout(prints_new("final client throughput: ", rate_to_string(tl.get_throughput())));


            // NOTE: HoangLe [Nov-10]: Read binary file
            // std::string path = "amazon-beauty_test_non_metrics.xlsx";
            // std::ifstream input(path, std::ios::binary);
            // std::vector<char> v_data(std::istreambuf_iterator<char>(input), {});
            // std::string data(v_data.begin(), v_data.end());
            // open_socket.send_message(data);

            // open_socket.send_message("\nClient:Server Hello World\n");
            // open_socket.read_message();
        });

    server_thread.join();
    client_thread.join();
    #endif

    return 0;
}
// g++ -std=c++17 -o main main.cpp -Wall -Wextra -Wshadow
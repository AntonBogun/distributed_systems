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



static_assert(sizeof(int) == sizeof(i32), "int and i32 must be the same size");


constexpr in_port_t DEFAULT_PORT = 8080;
constexpr int MAX_MESSAGE_SIZE = 4096;
constexpr int BACKLOG = 5;
#define DO_SOCKETS 0
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
    SINCE_LAST_NON0_TIMEOUT=2,
    SOCKET_ERROR=3
};
class TransmitionLayer{
    //=layer description:
    // i32 batch size
    // bool batches continue
    public:
    OpenSocket& sock;
    FCVector<u8> continuity_buffer;
    int total_written = 0;//actual buf size
    int total_received = 0;//actual buf size
    int total_sent = 0;//logical sent size
    int total_read = 0;//logical read size
    inline int remaining_in_buf() const { return MAX_MESSAGE_SIZE - total_received; }
    inline int remaining_out_buf() const { return MAX_MESSAGE_SIZE - total_written; }

    double since_last_non0_timeout = 10;//timeout after this many seconds of no data
    double min_data_throughput_limit = 4000;//bytes per second
    i32 min_data_throughput_batch_size = 2000;//bytes, how large a batch should be to be considered for throughput limit
    double last_batches = 30;//how many batches until we start checking for throughput limit
    std::queue<std::pair<double,i32>> batch_send_info;
    double max_batch_send_time = 30;//timeout after this many seconds of sending one batch
    std::pair<double,i64> batch_send_info_cumulative = {0,0};

    [[nodiscard]]
    bool update_throughput_queue(double time, i32 size){
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
                double throughput = batch_send_info_cumulative.second;
                if(throughput/batch_send_info_cumulative.first<min_data_throughput_limit){
                    return true;
                }
            }
        }
        return false;
    }
    void clear_throughput_queue(){
        while(!batch_send_info.empty()){
            batch_send_info.pop();
        }
        batch_send_info_cumulative = {0,0};
    }

    error_code last_error = error_code::NO_ERROR;

    TransmitionLayer(OpenSocket& sock_):
        sock(sock_),
        continuity_buffer(CONTINUITY_LEN, 0){
            throw_if(!sock.valid, "Socket not valid on TransmitionLayer creation");
        }
    
    [[nodiscard]]
    bool send_if_dont_fit(char* stream, int size){
        if(size > remaining_out_buf()){
            std::memcpy(sock.out_buffer.data()+total_written, stream, remaining_out_buf());
            size-=remaining_out_buf();
            stream+=remaining_out_buf();
            total_written+=remaining_out_buf();
            if (construct_and_send(1)) return true;
        }
        std::memcpy(sock.out_buffer.data()+total_written, stream, size);

    }
    inline char* out_buf_at(int offset){
        return sock.out_buffer.data()+offset;
    }
    inline char* in_buf_at(int offset){
        return sock.in_buffer.data()+offset;
    }
    [[nodiscard]]
    bool construct_and_send(bool batches_continue){
        throw_if(!sock.valid, "Socket not valid on construct_and_send");
        const int data_len = total_written-TRANSMISSION_LAYER_HEADER;
        throw_if(batches_continue&&data_len!=MAX_DATA_SIZE, 
            prints_new("Data length mismatch on construct_and_send:", data_len, " != ", MAX_DATA_SIZE));
        std::memcpy(out_buf_at(0), &data_len, sizeof(int));
        std::memset(out_buf_at(sizeof(int)), batches_continue, sizeof(bool));
        std::memcpy(out_buf_at(TRANSMISSION_LAYER_INFO), continuity_buffer.data(), CONTINUITY_LEN);
        send_n(total_written);
            
    }
    void write_byte(u8 byte){
        out_buffer[total_written++] = byte;
    }

    //! Note: Does not do any checks:
    // - does not check validity of socket
    // - does not reset buffer so it should be large enough to hold the message
    // - does not check if the message is too large
    [[nodiscard]]
    bool send_n(int to_send)
    {
        auto start = timenow();
        while (to_send > 0)
        {
            int valsend = send(sock.allocated_fd, out_buf_at(total_sent), to_send, 0);
            // TODO: timeout + block if valsend == 0
            throw_if(valsend < 0, prints_new("Failed to send message, errno:", errno));
            total_sent += valsend;
            to_send -= valsend;
        }
        total_sent += size;
    }

    //! Note: does not check validity of socket
    //! Note: does not reset buffer so it should be large enough to hold the message
    void read_until(int size)
    {
        throw_if(size > MAX_MESSAGE_SIZE - total_read,
                 prints_new("Message too large: ", size, " > ", MAX_MESSAGE_SIZE - total_read, "total_read: ", total_read));

        int to_receive = size - remaining_read();
        while (to_receive > 0)
        {
            //! danger: to_receive = remaining_in_buf() could read the next message
            int valread = recv(sock.allocated_fd, sock.in_buffer.data() + total_received, to_receive, 0);
            // printcout(prints_new("valread: ",valread));//!debug
            throw_if(valread < 0, prints_new("Failed to read message, errno:", errno));
            // TODO: timeout + block if valread == 0
            total_received += valread;
            to_receive -= valread;
        }
        total_read += size;
    }



    void read_message()
    {
        throw_if(!sock.valid, "Socket not valid");
        total_read = 0;
        total_received = 0;

        read_until(MESSAGE_DATA_OFFSET);
        int message_size;
        std::memcpy(&message_size, sock.in_buffer.data(), MESSAGE_DATA_OFFSET);

        throw_if(message_size > MAX_MESSAGE_DATA_SIZE, "Message too large");
        throw_if(message_size < 0, "Invalid message size");

        read_until(message_size);
        MUTEX_PRINT(prints_new("Received message of length", message_size, "from: ", socket_address_to_string(sock.other_address)));

        std::stringstream ss;
        for (int i = 0; i < message_size; ++i)
        {
            ss << sock.in_buffer[i + MESSAGE_DATA_OFFSET];
        }
        MUTEX_PRINT(ss.str());
    }
    void send_message(const std::string &message)
    {
        throw_if(!sock.valid, "Socket not valid");
        throw_if(message.size() > MAX_MESSAGE_DATA_SIZE, "Message too large"); // TODO: support longer messages

        total_sent = 0;
        total_written = 0;

        int message_size = message.size();
        std::memcpy(sock.out_buffer.data(), &message_size, MESSAGE_DATA_OFFSET);
        std::memcpy(sock.out_buffer.data() + MESSAGE_DATA_OFFSET, message.c_str(), message.size());
        send_until(message_size + MESSAGE_DATA_OFFSET);
        MUTEX_PRINT(prints_new("Sent message of length", message_size, "to: ", socket_address_to_string(sock.other_address)));
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
    auto server_thread = std::thread(
        [&server_socket, &server_fd]()
        {
            OpenSocket open_socket(SERVER);
            open_socket.accept_connection(server_fd);
            open_socket.read_message();

            // NOTE: HoangLe [Nov-10]: Temporarily comment out this for testing purpose
            // open_socket.send_message("\nServer:Client Hello World\n");
        });
    auto client_thread = std::thread(
        [&ip, &port]()
        {
            OpenSocket open_socket(CLIENT);
            socket_address server_address(ip, port);
            open_socket.connect_to_server(server_address);

            // NOTE: HoangLe [Nov-10]: Read binary file
            // std::string path = "amazon-beauty_test_non_metrics.xlsx";
            // std::ifstream input(path, std::ios::binary);
            // std::vector<char> v_data(std::istreambuf_iterator<char>(input), {});
            // std::string data(v_data.begin(), v_data.end());
            // open_socket.send_message(data);

            open_socket.send_message("\nClient:Server Hello World\n");
            open_socket.read_message();
        });

    server_thread.join();
    client_thread.join();
    #endif

    return 0;
}
// g++ -std=c++17 -o main main.cpp -Wall -Wextra -Wshadow
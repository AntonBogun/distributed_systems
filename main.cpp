#include "utils.h"
#include <sys/types.h>
#include <ifaddrs.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <thread>
#include <mutex>

static std::mutex io_mutex;
//safely print to cout on multiple threads
#define MUTEX_PRINT(x) do { \
    std::lock_guard<std::mutex> lock(io_mutex); \
    std::cout << x << std::endl; \
} while (0)
#define MUTEX_PRINT_INLINE(x) do { \
    std::lock_guard<std::mutex> lock(io_mutex); \
    std::cout << x << std::endl; \
} while (0)

struct ipv4_addr{
    u8 a;
    u8 b;
    u8 c;
    u8 d;
    ipv4_addr() : a(0), b(0), c(0), d(0) {}
    ipv4_addr(u8 a_, u8 b_, u8 c_, u8 d_) : a(a_), b(b_), c(c_), d(d_) {}
    static ipv4_addr from_in_addr(struct in_addr addr){
        ipv4_addr ip;
        ip.a = addr.s_addr & 0xFF;//reverse order, host is little endian but network is big endian
        ip.b = (addr.s_addr >> 8) & 0xFF;
        ip.c = (addr.s_addr >> 16) & 0xFF;
        ip.d = (addr.s_addr >> 24) & 0xFF;
        return ip;
    }
    struct in_addr to_in_addr(){
        struct in_addr addr;
        addr.s_addr = a | (b << 8) | (c << 16) | (d << 24);
        return addr;
    }
    bool operator==(ipv4_addr other) const {
        return a == other.a && b == other.b && c == other.c && d == other.d;
    }
    bool operator!=(ipv4_addr other) const {
        return !(*this == other);
    }
};

std::string ip_to_string(ipv4_addr ip){
    std::stringstream ss;
    ss<<(int)ip.a<<"."<<(int)ip.b<<"."<<(int)ip.c<<"."<<(int)ip.d;
    return ss.str();
}
std::string ip_to_string(struct in_addr addr){
    char buffer[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &addr, buffer, INET_ADDRSTRLEN);
    return std::string(buffer);
}
struct socket_address{
    ipv4_addr ip;
    in_port_t port;
    socket_address() : ip(), port(0) {}
    socket_address(ipv4_addr ip_, in_port_t port_) : ip(ip_), port(port_) {}
    socket_address(u8 a, u8 b, u8 c, u8 d, in_port_t port_) : ip(a, b, c, d), port(port_) {}
    static socket_address from_sockaddr_in(struct sockaddr_in addr){
        socket_address saddr;
        saddr.ip = ipv4_addr::from_in_addr(addr.sin_addr);
        saddr.port = ntohs(addr.sin_port);
        return saddr;
    }
    static socket_address from_fd_local(int fd){
        struct sockaddr_in addr;
        socklen_t len = sizeof(addr);
        getsockname(fd, (struct sockaddr *)&addr, &len);
        return from_sockaddr_in(addr);
    }
    static socket_address from_fd_remote(int fd){
        struct sockaddr_in addr;
        socklen_t len = sizeof(addr);
        getpeername(fd, (struct sockaddr *)&addr, &len);
        return from_sockaddr_in(addr);
    }
    struct sockaddr_in to_sockaddr_in(){
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_addr = ip.to_in_addr();
        addr.sin_port = htons(port);
        return addr;
    }
    bool operator==(socket_address other) const {
        return ip == other.ip && port == other.port;
    }
    bool operator!=(socket_address other) const {
        return !(*this == other);
    }
};
std::string socket_address_to_string(socket_address addr){
    std::stringstream ss;
    ss<<ip_to_string(addr.ip)<<":"<<addr.port;
    return ss.str();
}
std::string socket_address_to_string(struct sockaddr_in addr){
    std::stringstream ss;
    ss<<ip_to_string(addr.sin_addr)<<":"<<ntohs(addr.sin_port);
    return ss.str();
}
// void throw_if(bool condition, const std::string &message){
//     if (condition) {
//         throw std::runtime_error(message);
//     }
// }
#define throw_if(condition, message) if (condition) { throw std::runtime_error(message); }

int create_socket(){
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    throw_if(socket_fd < 0,prints_new("Failed to create socket, errno:",errno));
    return socket_fd;
}
void set_socket_options(int socket_fd){
    int opt = 1;
    throw_if(setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))<0,
        prints_new("Failed to set SO_REUSEADDR, errno:",errno));
    throw_if(setsockopt(socket_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt))<0,
        prints_new("Failed to set SO_REUSEPORT, errno:",errno));
    throw_if(setsockopt(socket_fd, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt))<0,
        prints_new("Failed to set SO_KEEPALIVE, errno:",errno));
}

static_assert(sizeof(int) ==  sizeof(i32), "int and i32 must be the same size");

constexpr in_port_t DEFAULT_PORT = 8080; 
constexpr int MAX_MESSAGE_SIZE = 1024;
constexpr int MESSAGE_DATA_OFFSET = sizeof(int);
constexpr int MAX_MESSAGE_DATA_SIZE = MAX_MESSAGE_SIZE - MESSAGE_DATA_OFFSET;
constexpr int BACKLOG = 5;
#if 1
enum SOCKET_TYPE{
    CLIENT,
    SERVER
};
class OpenSocket{
    public:
    bool valid = false;

    FCVector<char> in_buffer;
    FCVector<char> out_buffer;
    int total_read = 0;
    int total_received = 0;
    int total_sent = 0;
    int total_written = 0;
    inline int remaining_read() const { return total_received-total_read; }
    inline int remaining_write() const { return total_written-total_sent; }
    inline int remaining_in_buf() const { return MAX_MESSAGE_SIZE-total_read; }
    inline int remaining_out_buf() const { return MAX_MESSAGE_SIZE-total_sent; }


    int allocated_fd;//file descriptor associated with socket
    socket_address other_address;
    SOCKET_TYPE sock_type;

    OpenSocket(SOCKET_TYPE sock_type_):in_buffer(MAX_MESSAGE_SIZE,0),
    out_buffer(MAX_MESSAGE_SIZE,0),
    sock_type(sock_type_){
    }

    void accept_connection(in_port_t server_fd){//server file descriptor
        throw_if(sock_type!=SERVER,"Cannot accept connection on client socket");
        throw_if(valid,"Cannot accept, socket already valid");

        struct sockaddr_in client_address;//overwritten by accept
        int addrlen = sizeof(client_address);//overwritten by accept
        allocated_fd = accept(server_fd, (struct sockaddr *)&client_address, (socklen_t*)&addrlen);
        throw_if(allocated_fd < 0,
            prints_new("Failed to accept connection, errno:",errno));

        other_address = socket_address::from_sockaddr_in(client_address);

        socket_address client_fd_address=socket_address::from_fd_remote(allocated_fd);

        throw_if(client_fd_address!=other_address,
            prints_new("Accept address mismatch: ",socket_address_to_string(client_fd_address)," != ",socket_address_to_string(other_address)));

        MUTEX_PRINT(prints_new("Accepted connection from client: ", socket_address_to_string(other_address),
        " to server: ", socket_address_to_string(socket_address::from_fd_local(allocated_fd))));
        valid = true;
    }
    void connect_to_server(socket_address address){
        throw_if(sock_type!=CLIENT,"Cannot connect to server on server socket");
        throw_if(valid,"Cannot connect, socket already valid");

        //create new file descriptor and then connect binds it to free port
        allocated_fd = create_socket();

        set_socket_options(allocated_fd);

        struct sockaddr_in server_address=address.to_sockaddr_in();
        
        throw_if(connect(allocated_fd, (struct sockaddr *)&server_address, sizeof(server_address)) < 0,
            prints_new("Failed to connect to server, errno:",errno));

        other_address = address;

        socket_address server_fd_address=socket_address::from_fd_remote(allocated_fd);

        throw_if(server_fd_address!=other_address,
            prints_new("Connect address mismatch: ",socket_address_to_string(server_fd_address)," != ",socket_address_to_string(other_address)));


        MUTEX_PRINT(prints_new("Connected to server: ", socket_address_to_string(other_address),
        " from client: ", socket_address_to_string(socket_address::from_fd_local(allocated_fd))));
        valid = true;
    }

    //!Note: does not check validity of socket
    //!Note: does not reset buffer so it should be large enough to hold the message
    void read_until(int size){
        throw_if(!valid,"Socket not valid");
        throw_if(size > MAX_MESSAGE_SIZE-total_read,
            prints_new("Message too large: ",size," > ",MAX_MESSAGE_SIZE-total_read,"total_read: ",total_read));

        int to_receive = size-remaining_read();
        while(to_receive>0){
            //!danger: to_receive = remaining_in_buf() could read the next message
            int valread = recv(allocated_fd, in_buffer.data()+total_received, to_receive, 0);
            // printcout(prints_new("valread: ",valread));//!debug
            throw_if(valread < 0,prints_new("Failed to read message, errno:",errno));
            //TODO: timeout + block if valread == 0
            total_received += valread;
            to_receive -= valread;
        }
        total_read += size;
    }
    void read_message(){
        throw_if(!valid,"Socket not valid");
        total_read = 0;
        total_received = 0;

        read_until(MESSAGE_DATA_OFFSET);
        int message_size;
        std::memcpy(&message_size, in_buffer.data(), MESSAGE_DATA_OFFSET);

        throw_if(message_size > MAX_MESSAGE_DATA_SIZE,"Message too large");
        throw_if(message_size < 0,"Invalid message size");

        read_until(message_size);
        MUTEX_PRINT(prints_new("Received message of length",message_size,"from: ", socket_address_to_string(other_address)));

        std::stringstream ss;
        for (int i = 0; i < message_size; ++i) {
            ss<<in_buffer[i+MESSAGE_DATA_OFFSET];
        }
        MUTEX_PRINT(ss.str());
    }
    //!Note: does not check validity of socket
    //!Note: does not reset buffer so it should be large enough to hold the message
    void send_until(int size){
        throw_if(size > MAX_MESSAGE_SIZE-total_sent,
            prints_new("Message too large: ",size," > ",MAX_MESSAGE_SIZE-total_sent,"total_sent: ",total_sent));

        int to_send = size-remaining_write();
        while(to_send>0){
            int valsend = send(allocated_fd, out_buffer.data()+total_written, to_send, 0);
            //TODO: timeout + block if valsend == 0
            throw_if(valsend < 0,prints_new("Failed to send message, errno:",errno));
            total_written += valsend;
            to_send -= valsend;
        }
        total_sent += size;
    }
    void send_message(const std::string &message){
        throw_if(!valid,"Socket not valid");
        throw_if(message.size() > MAX_MESSAGE_DATA_SIZE,"Message too large");//TODO: support longer messages
        
        total_sent = 0;
        total_written = 0;
        
        int message_size = message.size();
        std::memcpy(out_buffer.data(), &message_size, MESSAGE_DATA_OFFSET);
        std::memcpy(out_buffer.data()+MESSAGE_DATA_OFFSET, message.c_str(), message.size());
        send_until(message_size+MESSAGE_DATA_OFFSET);
        MUTEX_PRINT(prints_new("Sent message of length",message_size,"to: ", socket_address_to_string(other_address)));
    }
    ~OpenSocket(){
        if(valid){
            close(allocated_fd);
        }
    }
};
class ServerSocket{
    int socket_fd;
    public:
    bool connection_open = false;
    ServerSocket(in_port_t port){
        
        socket_fd = create_socket();

        set_socket_options(socket_fd);

        // struct sockaddr_in server_address;
        // server_address.sin_family = AF_INET;
        // server_address.sin_addr.s_addr = htonl(INADDR_ANY);
        // server_address.sin_port = htons(PORT);
        struct sockaddr_in bind_address=socket_address(0,0,0,0,port).to_sockaddr_in();//0,0,0,0 to bind to all interfaces
        throw_if(bind(socket_fd, (struct sockaddr *)&bind_address, sizeof(bind_address)) < 0,
            prints_new("Failed to bind socket, errno:",errno));
        
        socket_address bound_address=socket_address::from_fd_local(socket_fd);

        throw_if(bound_address.port!=port,
            prints_new("Bind port mismatch: bound ",bound_address.port," != target ",port));


        printcout(prints_new("Server socket bound to: ", socket_address_to_string(bound_address)));

        throw_if(listen(socket_fd, BACKLOG) < 0,
            prints_new("Failed to listen, errno:",errno));

        printcout("Listening for connections...");
    }

    int get_socket_fd(){
        return socket_fd;
    }
    void close_socket(){
        close(socket_fd);
    }
    ~ServerSocket(){
        close_socket();
    }
};
#endif




std::vector<ipv4_addr> getIPAddresses() {
    std::vector<ipv4_addr> addresses;
    struct ifaddrs* ifAddrStruct = nullptr;
    struct ifaddrs* ifa = nullptr;

    if (getifaddrs(&ifAddrStruct) == -1) {
        throw std::runtime_error("Failed to get network interfaces");
    }

    for (ifa = ifAddrStruct; ifa != nullptr; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr) {
            continue;
        }

        // Check for IPv4 addresses
        if (ifa->ifa_addr->sa_family == AF_INET) {
            auto* sockaddr_in_ptr = reinterpret_cast<struct sockaddr_in*>(ifa->ifa_addr);
            void* tmpAddrPtr = &sockaddr_in_ptr->sin_addr;

            printcout(prints_new(
                "family: ", ifa->ifa_addr->sa_family, 
                "sin_port: ", sockaddr_in_ptr->sin_port, 
                "sin_addr: ", ip_to_string(ipv4_addr::from_in_addr(sockaddr_in_ptr->sin_addr))
            ));

            char addressBuffer[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);

            // Skip localhost addresses
            if (strcmp(addressBuffer, "127.0.0.1") != 0) {
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

    if (ifAddrStruct != nullptr) {
        freeifaddrs(ifAddrStruct);
    }

    return addresses;
}

int main() {

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
    struct in_addr test_addr=ipv4_addr{192,168,1,1}.to_in_addr();
    // inet_aton("192.168.1.1", &test_addr);

    ipv4_addr mytest_addr=ipv4_addr::from_in_addr(test_addr);
    std::string s1,s2;
    s1=ip_to_string(test_addr);
    printcout(prints_new("test_addr: ", s1));
    printcout(prints_new("mytest_addr: ", ip_to_string(mytest_addr)));
    struct in_addr test_addr2=mytest_addr.to_in_addr();
    s2=ip_to_string(test_addr2);
    printcout(prints_new("test_addr2: ", s2));
    if(s1!=s2){
        throw std::runtime_error("Conversion failed");
    }

    // struct sockaddr_in test_sockaddr;
    // test_sockaddr.sin_family = AF_INET;
    // test_sockaddr.sin_addr = test_addr;
    // test_sockaddr.sin_port = htons(port);
    struct sockaddr_in test_sockaddr=socket_address{192,168,1,1,test_port}.to_sockaddr_in();
    socket_address mytest_sockaddr=socket_address::from_sockaddr_in(test_sockaddr);
    std::string s3,s4;
    s3=socket_address_to_string(test_sockaddr);
    printcout(prints_new("test_sockaddr: ", s3));
    printcout(prints_new("mytest_sockaddr: ", socket_address_to_string(mytest_sockaddr)));
    struct sockaddr_in test_sockaddr2=mytest_sockaddr.to_sockaddr_in();
    s4=socket_address_to_string(test_sockaddr2);
    printcout(prints_new("test_sockaddr2: ", s4));
    if(s3!=s4){
        throw std::runtime_error("Conversion failed");
    }
    printcout("\n");
    //==== testing end

    std::vector<ipv4_addr> addresses = getIPAddresses();
    if (addresses.empty()) {
        throw std::runtime_error("No network interfaces found");
    }
    ipv4_addr ip = addresses[0];
    printcout(prints_new("using ip: ", ip_to_string(ip)));

    in_port_t port = 8080;
    printcout(prints_new("Using Server Port: ", port));
    ServerSocket server_socket(port);
    int server_fd = server_socket.get_socket_fd();

    auto server_thread = std::thread([&server_socket, &server_fd](){
        OpenSocket open_socket(SERVER);
        open_socket.accept_connection(server_fd);
        open_socket.read_message();
        open_socket.send_message("\nServer:Client Hello World\n");
    });
    auto client_thread = std::thread([&ip, &port](){
        OpenSocket open_socket(CLIENT);
        socket_address server_address(ip, port);
        open_socket.connect_to_server(server_address);
        open_socket.send_message("\nClient:Server Hello World\n");
        open_socket.read_message();
    });

    server_thread.join();
    client_thread.join();

    return 0;
}
//g++ -std=c++17 -o main main.cpp -Wall -Wextra -Wshadow
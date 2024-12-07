#pragma once

#include "utils.h"

#include <arpa/inet.h>
#include <ctime>
#include <ifaddrs.h>
#include <iomanip>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <thread>
#include <mutex>
namespace distribsys{

#define throw_if(condition, message)                                      \
    if (condition)                                                        \
    {                                                                     \
        throw std::runtime_error(std::string(message) +                   \
                                 "; in file " + __FILE__ +                \
                                 " at line " + std::to_string(__LINE__)); \
    }

struct ipv4_addr
{
    u8 a;
    u8 b;
    u8 c;
    u8 d;
    ipv4_addr() : a(0), b(0), c(0), d(0) {}
    ipv4_addr(u8 a_, u8 b_, u8 c_, u8 d_) : a(a_), b(b_), c(c_), d(d_) {}
    static ipv4_addr from_in_addr(struct in_addr addr)
    {
        ipv4_addr ip;
        ip.a = addr.s_addr & 0xFF; // reverse order, host is little endian but network is big endian
        ip.b = (addr.s_addr >> 8) & 0xFF;
        ip.c = (addr.s_addr >> 16) & 0xFF;
        ip.d = (addr.s_addr >> 24) & 0xFF;
        return ip;
    }
    struct in_addr to_in_addr() const{
        struct in_addr addr;
        addr.s_addr = a | (b << 8) | (c << 16) | (d << 24);
        return addr;
    }
    u32 to_u32() const {
        return a | (b << 8) | (c << 16) | (d << 24);
    }
    static ipv4_addr from_u32(u32 val){
        ipv4_addr ip;
        ip.a = val & 0xFF;
        ip.b = (val >> 8) & 0xFF;
        ip.c = (val >> 16) & 0xFF;
        ip.d = (val >> 24) & 0xFF;
        return ip;
    }
    bool operator==(ipv4_addr other) const
    {
        return a == other.a && b == other.b && c == other.c && d == other.d;
    }
    bool operator!=(ipv4_addr other) const
    {
        return !(*this == other);
    }
    bool is_unset() const {
        return a==0 && b==0 && c==0 && d==0;
    }
};


std::string ip_to_string(ipv4_addr ip)
{
    std::stringstream ss;
    ss << (int)ip.a << "." << (int)ip.b << "." << (int)ip.c << "." << (int)ip.d;
    return ss.str();
}
std::string ip_to_string(struct in_addr addr)
{
    char buffer[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &addr, buffer, INET_ADDRSTRLEN);
    return std::string(buffer);
}
struct socket_address
{
    ipv4_addr ip;
    in_port_t port;
    socket_address() : ip(), port(0) {}
    socket_address(ipv4_addr ip_, in_port_t port_) : ip(ip_), port(port_) {}
    socket_address(u8 a, u8 b, u8 c, u8 d, in_port_t port_) : ip(a, b, c, d), port(port_) {}
    static socket_address from_sockaddr_in(struct sockaddr_in addr)
    {
        socket_address saddr;
        saddr.ip = ipv4_addr::from_in_addr(addr.sin_addr);
        saddr.port = ntohs(addr.sin_port);
        return saddr;
    }

    struct sockaddr_in to_sockaddr_in() const {
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_addr = ip.to_in_addr();
        addr.sin_port = htons(port);
        return addr;
    }
    bool operator==(socket_address other) const
    {
        return ip == other.ip && port == other.port;
    }
    bool operator!=(socket_address other) const
    {
        return !(*this == other);
    }
    //% uses 0.0.0.0:0 as unset
    bool is_unset() const {
        return ip.is_unset() && port==0;
    }
};

extern std::mutex ifaddrs_mutex;
std::vector<ipv4_addr> getIPAddresses()
{
    //> unsafe to call this function concurrently
    std::lock_guard<std::mutex> lock(ifaddrs_mutex);

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
            if (addressBuffer!="127.0.0.1")
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
        //     if (
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

struct file_descriptor{
    int fd=-1;
public:
    bool is_valid() const {
        return fd>=0;
    }
    void close_fd(){
        if(is_valid()){
            ::close(fd);
            fd=-1;
        }
    }
    int get_fd() const {
        return fd;
    }
    file_descriptor(){}
    explicit file_descriptor(int fd_):fd(fd_){}
    file_descriptor(file_descriptor&& other){
        fd=other.fd;
        other.fd=-1;
    }

    file_descriptor& operator=(file_descriptor&& other){
        if(this==&other) return *this;
        if(fd!=other.fd){
            close_fd();
            fd=other.fd;
        }
        other.fd=-1;
        return *this;
    }
    file_descriptor& operator=(int fd_){
        if(fd==fd_) return *this;
        close_fd();
        fd=fd_;
        return *this;
    }

    //~ does not throw, simply is_valid() will return false
    //~ does not check for valid fd
    file_descriptor accept_connection(socket_address& addr) const {
        struct sockaddr_in client_address;
        socklen_t len = sizeof(client_address);
        int new_fd = accept(fd, (struct sockaddr *)&client_address, &len);
        addr = socket_address::from_sockaddr_in(client_address);
        return file_descriptor(new_fd);
    }
    //! false on failure
    bool connect_to_server(socket_address addr) const {
        struct sockaddr_in server_address = addr.to_sockaddr_in();
        int res = connect(fd, (struct sockaddr *)&server_address, sizeof(server_address));
        return res>=0;
    }
    ssize_t Send(const void *buf, size_t len, int flags) const {
        return send(fd, buf, len, flags);
    }
    ssize_t Recv(void *buf, size_t len, int flags) const {
        return recv(fd, buf, len, flags);
    }
    socket_address local_address() const {
        struct sockaddr_in addr;
        socklen_t len = sizeof(addr);
        getsockname(get_fd(), (struct sockaddr *)&addr, &len);
        return socket_address::from_sockaddr_in(addr);
    }
    socket_address remote_address() const {
        struct sockaddr_in addr;
        socklen_t len = sizeof(addr);
        getpeername(get_fd(), (struct sockaddr *)&addr, &len);
        return socket_address::from_sockaddr_in(addr);
    }

    ~file_descriptor(){
        close_fd();
    }
};
std::string socket_address_to_string(socket_address addr)
{
    std::stringstream ss;
    ss << ip_to_string(addr.ip) << ":" << addr.port;
    return ss.str();
}
std::string socket_address_to_string(struct sockaddr_in addr)
{
    std::stringstream ss;
    ss << ip_to_string(addr.sin_addr) << ":" << ntohs(addr.sin_port);
    return ss.str();
}

auto timenow()
{
    return std::chrono::high_resolution_clock::now();
}
auto timeinsec(std::chrono::high_resolution_clock::duration t)
{
    return std::chrono::duration<double>(t);
}
auto timeinmsec(std::chrono::high_resolution_clock::duration t)
{
    return std::chrono::duration<double, std::milli>(t);
}
std::string timeval_to_string(struct timeval tv)
{
    std::stringstream ss;
    ss << tv.tv_sec << "." << std::setfill('0') << std::setw(6) << tv.tv_usec;
    return ss.str();
}
// in seconds
// both for duration and timepoints

struct TimeValue
{
    double time;
    TimeValue(double t) : time(t) {}
    explicit TimeValue(struct timeval tv) : time(static_cast<double>(tv.tv_sec) + static_cast<double>(tv.tv_usec) / 1e6) {}
    static TimeValue now()
    {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        return TimeValue{tv};
    }
    double to_double() const
    {
        return time;
    }
    //** undefined when negative
    struct timeval to_timeval() const
    {
        throw_if(time < 0, "TimeValue to_timeval: negative time");
        struct timeval tv;
        tv.tv_sec = static_cast<time_t>(time);
        tv.tv_usec = static_cast<suseconds_t>((time - static_cast<double>(tv.tv_sec)) * 1e6);
        return tv;
    }
    std::string to_duration_string() const
    {
        std::stringstream ss;
        // at most 6 decimal places
        ss << std::setprecision(6) << time << "s";
        return ss.str();
    }
    //** undefined when negative
    std::string to_date_string() const
    {
        struct tm *tm_info;
        char buffer[30];
        struct timeval tv = to_timeval();
        tm_info = localtime(&tv.tv_sec);
        strftime(buffer, 30, "%Y-%m-%d %H:%M:%S", tm_info);
        std::stringstream ss;
        ss << buffer << "." << std::setfill('0') << std::setw(6) << tv.tv_usec;
        return ss.str();
    }
    TimeValue operator+(TimeValue other) const
    {
        return TimeValue(time + other.time);
    }
    TimeValue operator+=(TimeValue other)
    {
        time += other.time;
        return *this;
    }
    TimeValue operator-(TimeValue other) const
    {
        return TimeValue(time - other.time);
    }
    TimeValue operator-=(TimeValue other)
    {
        time -= other.time;
        return *this;
    }
    TimeValue operator*(double other) const
    {
        return TimeValue(time * other);
    }
    TimeValue operator/(double other) const
    {
        return TimeValue(time / other);
    }
    bool operator<(const TimeValue other) const
    {
        return time < other.time;
    }
    bool operator>(const TimeValue other) const
    {
        return time > other.time;
    }
    bool operator<=(const TimeValue other) const
    {
        return time <= other.time;
    }
    bool operator>=(const TimeValue other) const
    {
        return time >= other.time;
    }
    // do not compare floating point numbers for equality
    //this instead checks if the difference is less than epsilon (by default 1e-6)
    bool is_near(TimeValue other, double epsilon = 1e-6) const
    {
        return std::abs(time - other.time) < epsilon;
    }
    std::chrono::duration<double> to_duration() const
    {
        return std::chrono::duration<double>(time);
    }
    // bool operator==(TimeValue other){
    //     return time==other.time;
    // }
    
};
extern const TimeValue NO_DELAY;

file_descriptor create_socket_throw()
{
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    throw_if(socket_fd < 0, prints_new("Failed to create socket, errno:", errno));
    return file_descriptor(socket_fd);
}
void set_socket_options_throw(const file_descriptor& socket_fd)
{
    int opt = 1;
    throw_if(setsockopt(socket_fd.get_fd(), SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0,
             prints_new("Failed to set SO_REUSEADDR, errno:", errno));
    throw_if(setsockopt(socket_fd.get_fd(), SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0,
             prints_new("Failed to set SO_REUSEPORT, errno:", errno));
    throw_if(setsockopt(socket_fd.get_fd(), SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt)) < 0,
             prints_new("Failed to set SO_KEEPALIVE, errno:", errno));
}
void set_timeout_throw(const file_descriptor& socket_fd, TimeValue timeout)
{
    struct timeval tv = timeout.to_timeval();
    throw_if(setsockopt(socket_fd.get_fd(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0,
             prints_new("Failed to set SO_RCVTIMEO, errno:", errno));
    throw_if(setsockopt(socket_fd.get_fd(), SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0,
             prints_new("Failed to set SO_SNDTIMEO, errno:", errno));
}
//% true means error + errno set
[[nodiscard]]
bool set_socket_send_timeout(const file_descriptor& socket_fd, TimeValue timeout)
{
    struct timeval tv = timeout.to_timeval();
    return setsockopt(socket_fd.get_fd(), SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0;
}
[[nodiscard]]
bool set_socket_recv_timeout(const file_descriptor& socket_fd, TimeValue timeout)
{
    struct timeval tv = timeout.to_timeval();
    return setsockopt(socket_fd.get_fd(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0;
}


constexpr int BACKLOG = 5;
//!NOTE: ServerSocket MUST BE ALIVE for the entire duration of usage of the file_descriptor
class ServerSocket
{
    file_descriptor socket_fd;
    const in_port_t port;

public:
    ServerSocket(in_port_t port_) : port(port_)
    {

        socket_fd = create_socket_throw();

        set_socket_options_throw(socket_fd);

        // struct sockaddr_in server_address;
        // server_address.sin_family = AF_INET;
        // server_address.sin_addr.s_addr = htonl(INADDR_ANY);
        // server_address.sin_port = htons(PORT);
        struct sockaddr_in bind_address = socket_address(0, 0, 0, 0, port).to_sockaddr_in(); // 0,0,0,0 to bind to all interfaces
        throw_if(bind(socket_fd.get_fd(), (struct sockaddr *)&bind_address, sizeof(bind_address)) < 0,
                 prints_new("Failed to bind socket, errno:", errno));

        socket_address bound_address = socket_fd.local_address();

        throw_if(bound_address.port != port,
                 prints_new("Bind port mismatch: bound ", bound_address.port, " != target ", port));

        printcout(prints_new("Server socket bound to: ", socket_address_to_string(bound_address)));

        throw_if(listen(socket_fd.get_fd(), BACKLOG) < 0,
                 prints_new("Failed to listen, errno:", errno));

        printcout("Listening for connections...");
    }
    //!NOTE: ServerSocket MUST BE ALIVE for the entire duration of usage of the file_descriptor
    const file_descriptor& get_socket_fd() const
    {
        return socket_fd;
    }
    const in_port_t get_port() const
    {
        return port;
    }
};

struct connection_info
{
    socket_address local_address;
    socket_address external_address;
    connection_info(socket_address local_address_, socket_address external_address_) : local_address(local_address_), external_address(external_address_) {}
    connection_info() : local_address(), external_address() {}

    bool operator==(connection_info other) const
    {
        return local_address == other.local_address && external_address == other.external_address;
    }
    bool operator!=(connection_info other) const
    {
        return !(*this == other);
    }
    std::string to_string() const
    {
        std::stringstream ss;
        ss << "(local: " << socket_address_to_string(local_address) << ", external: " << socket_address_to_string(external_address) << ")";
        return ss.str();
    }
};
extern std::mutex uuid_gen_mutex;
extern std::mt19937_64 uuid_gen;
extern std::uniform_int_distribution<u64> uuid_dist;
struct uuid_t
{
    u64 val;
    uuid_t() : val(0){
        std::lock_guard<std::mutex> lock(uuid_gen_mutex);
        val = uuid_dist(uuid_gen);
    }
    uuid_t(u64 val_) : val(val_){}
    bool operator==(uuid_t other) const {
        return val == other.val;
    }
    bool operator!=(uuid_t other) const {
        return !(*this == other);
    }
};

}//namespace distribsys


namespace std{
    template<>
    struct hash<distribsys::ipv4_addr>{
        std::size_t operator()(const distribsys::ipv4_addr& ip) const noexcept{
            return std::hash<distribsys::u32>{}(ip.to_u32());
        }
    };
    template<>
    struct hash<distribsys::socket_address>{
        std::size_t operator()(const distribsys::socket_address& addr) const noexcept{
            return std::hash<distribsys::u32>{}(addr.ip.to_u32()) ^ std::hash<in_port_t>{}(addr.port);
        }
    };
    template<>
    struct hash<distribsys::TimeValue>{
        std::size_t operator()(const distribsys::TimeValue& tv) const noexcept{
            return std::hash<double>{}(tv.to_double());
        }
    };
    template<>
    struct hash<distribsys::connection_info>{
        std::size_t operator()(const distribsys::connection_info& info) const noexcept{
            return std::hash<distribsys::socket_address>{}(info.local_address) ^ std::hash<distribsys::socket_address>{}(info.external_address);
        }
    };
    template<>
    struct hash<distribsys::uuid_t>{
        std::size_t operator()(const distribsys::uuid_t& id) const noexcept{
            return std::hash<distribsys::u64>{}(id.val);
        }
    };
}
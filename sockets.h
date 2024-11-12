#include "utils.h"
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <sys/time.h>
#include <ctime>
#include <iomanip>

#define throw_if(condition, message)       \
    if (condition)                         \
    {                                      \
        throw std::runtime_error(message); \
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
    struct in_addr to_in_addr()
    {
        struct in_addr addr;
        addr.s_addr = a | (b << 8) | (c << 16) | (d << 24);
        return addr;
    }
    bool operator==(ipv4_addr other) const
    {
        return a == other.a && b == other.b && c == other.c && d == other.d;
    }
    bool operator!=(ipv4_addr other) const
    {
        return !(*this == other);
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
    static socket_address from_fd_local(int fd)
    {
        struct sockaddr_in addr;
        socklen_t len = sizeof(addr);
        getsockname(fd, (struct sockaddr *)&addr, &len);
        return from_sockaddr_in(addr);
    }
    static socket_address from_fd_remote(int fd)
    {
        struct sockaddr_in addr;
        socklen_t len = sizeof(addr);
        getpeername(fd, (struct sockaddr *)&addr, &len);
        return from_sockaddr_in(addr);
    }
    struct sockaddr_in to_sockaddr_in()
    {
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

auto timenow(){
  return std::chrono::high_resolution_clock::now();
}
auto timeinsec(std::chrono::high_resolution_clock::duration t){
  return std::chrono::duration<double>(t);
}
auto timeinmsec(std::chrono::high_resolution_clock::duration t){
  return std::chrono::duration<double, std::milli>(t);
}
std::string timeval_to_string(struct timeval tv){
    std::stringstream ss;
    ss<<tv.tv_sec<<"."<<std::setfill('0')<<std::setw(6)<<tv.tv_usec;
    return ss.str();
}
//in seconds
//both for duration and timepoints
extern const TimeValue NO_DELAY;
struct TimeValue{
    double time;
    TimeValue(double t): time(t){}
    explicit TimeValue(struct timeval tv): 
    time(static_cast<double>(tv.tv_sec)+static_cast<double>(tv.tv_usec)/1e6){}
    static TimeValue now() {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        return TimeValue{tv};
    }
    double to_double(){
        return time;
    }
    //** undefined when negative
    struct timeval to_timeval(){
        throw_if(time<0, "TimeValue to_timeval: negative time");
        struct timeval tv;
        tv.tv_sec=static_cast<time_t>(time);
        tv.tv_usec=static_cast<suseconds_t>((time-static_cast<double>(tv.tv_sec))*1e6);
        return tv;
    }
    std::string to_duration_string(){
        std::stringstream ss;
        //at most 6 decimal places
        ss<<std::setprecision(6)<<time<<"s";
        return ss.str();
    }
    //** undefined when negative
    std::string to_date_string(){
        struct tm* tm_info;
        char buffer[30];
        struct timeval tv = to_timeval();
        tm_info = localtime(&tv.tv_sec);
        strftime(buffer, 30, "%Y-%m-%d %H:%M:%S", tm_info);
        std::stringstream ss;
        ss<<buffer<<"."<<std::setfill('0')<<std::setw(6)<<tv.tv_usec;
        return ss.str();
    }
    TimeValue operator+(TimeValue other){
        return TimeValue(time+other.time);
    }
    TimeValue operator+=(TimeValue other){
        time+=other.time;
        return *this;
    }
    TimeValue operator-(TimeValue other){
        return TimeValue(time-other.time);
    }
    TimeValue operator-=(TimeValue other){
        time-=other.time;
        return *this;
    }
    TimeValue operator*(double other){
        return TimeValue(time*other);
    }
    TimeValue operator/(double other){
        return TimeValue(time/other);
    }
    bool operator<(TimeValue other){
        return time<other.time;
    }
    bool operator>(TimeValue other){
        return time>other.time;
    }
    bool operator<=(TimeValue other){
        return time<=other.time;
    }
    bool operator>=(TimeValue other){
        return time>=other.time;
    }
    // do not compare floating point numbers for equality
    // bool operator==(TimeValue other){
    //     return time==other.time;
    // }
    bool is_near(TimeValue other, double epsilon=1e-6){
        return std::abs(time-other.time)<epsilon;
    }   
};


int create_socket_throw()
{
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    throw_if(socket_fd < 0, prints_new("Failed to create socket, errno:", errno));
    return socket_fd;
}
void set_socket_options_throw(int socket_fd)
{
    int opt = 1;
    throw_if(setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0,
             prints_new("Failed to set SO_REUSEADDR, errno:", errno));
    throw_if(setsockopt(socket_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0,
             prints_new("Failed to set SO_REUSEPORT, errno:", errno));
    throw_if(setsockopt(socket_fd, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt)) < 0,
             prints_new("Failed to set SO_KEEPALIVE, errno:", errno));
}

//% true means error + errno set
[[nodiscard]]
bool set_socket_send_timeout(int socket_fd, TimeValue timeout){
    struct timeval tv = timeout.to_timeval();
    return setsockopt(socket_fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0;
}
[[nodiscard]]
bool set_socket_recv_timeout(int socket_fd, TimeValue timeout){
    struct timeval tv = timeout.to_timeval();
    return setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0;
}
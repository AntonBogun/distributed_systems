#include "sockets.h"
#include "transmission/transmission.h"
#include "utils.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#define DO_SOCKETS 1
#if DO_SOCKETS

constexpr int TRANSMISSION_LAYER_INFO = sizeof(int) + sizeof(bool); // datalen + batches_continue
constexpr int CONTINUITY_LEN = sizeof(u64);                         // continuity between batches
constexpr int TRANSMISSION_LAYER_HEADER = TRANSMISSION_LAYER_INFO + CONTINUITY_LEN;
constexpr int MAX_DATA_SIZE = MAX_MESSAGE_SIZE - TRANSMISSION_LAYER_HEADER;
enum class error_code
{
    NO_ERROR = 0,
    MAX_BATCH_SEND_TIME = 1,
    MAX_BATCH_RECEIVE_TIME = 2,
    TOO_LOW_THROUGHPUT = 3,
    SOCKET_ERROR = 4 //+ errno set
};
std::string rate_to_string(double rate)
{ // bytes per s
    std::stringstream ss;
    if (rate < 1e3)
    {
        ss << rate << "B/s";
    }
    else if (rate < 1e6)
    {
        ss << rate / 1e3 << "KB/s";
    }
    else if (rate < 1e9)
    {
        ss << rate / 1e6 << "MB/s";
    }
    else
    {
        ss << rate / 1e9 << "GB/s";
    }
    return ss.str();
}

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
    TimeValue tv = TimeValue::now();
    printcout(prints_new("Time: ", tv.to_duration_string()));
    printcout(prints_new("Date: ", tv.to_date_string()));
    TimeValue tv2 = TimeValue(1.5);
    printcout(prints_new("Duration: ", tv2.to_duration_string()));
    TimeValue tv3 = TimeValue(-0.4);
    printcout(prints_new("Duration: ", tv3.to_duration_string()));
#if DO_SOCKETS
    ServerSocket server_socket(port);
    int server_fd = server_socket.get_socket_fd();

    // Create thread for server and client service
    int vector_n = 100000;
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
            i64 sum = 0;
            for (int i = 0; i < vector_n; i++)
            {
                v[i] = i;
                sum += i;
            }
            printcout(prints_new("Sum: ", sum));
            printcout("sending vector");
            for (int i = 0; i < vector_n; i++)
            {
                TL_ERR_and_return(tl.write_i32(v[i]),
                                  {printcout(prints_new("break at i",i)); tl.print_errors(); });
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
            i64 sum = 0;
            i32 expected = 0;
            for (int i = 0; i < vector_n; i++)
            {
                TL_ERR_and_return(tl.read_i32(v[i]), tl.print_errors());
                sum += v[i];
                if (v[i] != expected)
                {
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
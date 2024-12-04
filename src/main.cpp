#include "sockets.h"
#include "transmission.h"
#include "utils.h"
#include "nodes.h"
#include "packets.h"
#include "logging.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>
#include <endian.h>
static_assert(BYTE_ORDER == LITTLE_ENDIAN, "This code only works on little-endian systems");

namespace distribsys{
    std::atomic<bool> _do_logging = true;
    std::atomic<bool> _do_verbose = false;
    std::mutex _mutex_logging;
    std::mutex ifaddrs_mutex;
}

using namespace distribsys;

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


#define TL_ERR_and_return(x, y) \
    {                           \
        if (x)                  \
        {                       \
            y;                  \
            return;             \
        }                       \
    }

int test()
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
    ServerSocket server_socket(port);
    const file_descriptor& server_fd = server_socket.get_socket_fd();

    // Create thread for server and client service
    int vector_n = 100000;
    auto server_thread = std::thread(
        [&server_socket, &server_fd, vector_n]()
        {
            OpenSocket open_socket(SERVER);
            TL_ERR_and_return(open_socket.accept_connection(server_fd), printcout("timeout on accept"));
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
            TL_ERR_and_return(open_socket.connect_to_server(server_address), printcout("timeout on connect"));

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


    return 0;
}

constexpr int error_exit_code = -1;
int cerr_and_return(const std::string msg, int exit_code=error_exit_code) {
    std::cerr << msg << std::endl;
    return exit_code;
}
int main(int argc, char *argv[])
{
    return test();//./bin/main
    // return node_main(argc, argv);
}

int node_main(int argc, char *argv[])
{
    // Parse arguments
    int i = 1;
    std::string mode="";
    in_port_t port=0;
    std::string nameFile = "";
    std::string action = "";

    std::unordered_set<std::string> valid_modes = {"client", "data", "master", "dns"};

    auto display_help = [&](){
        printcout("Usage: ./main -mode <data|master|dns> -p <port_num>");
        printcout("Usage: ./main -mode client -p <port_num> [--download monument.jpg | --upload monument.jpg]");
    };
    if(argc==1) {
        display_help();
        return 0;
    }
    while (i < argc) {
        std::string s(argv[i++]);
        if(s=="-h" || s=="--help") {
            display_help();
            return 0;
        } else if (s == "-mode") {
            if(i>=argc) return cerr_and_return("Err: missing argument for -mode");
            if(mode!="") return cerr_and_return("Err: duplicate mode input: " + mode);
            mode = argv[i++];
            if(valid_modes.find(mode)==valid_modes.end()) return cerr_and_return("Err: invalid mode: " + mode);
        }else if (s == "-p") {
            if(i>=argc) return cerr_and_return("Err: missing argument for -p");
            try {
                if(port!=0) return cerr_and_return("Err: duplicate port input: " + std::string(argv[i-1]));
                port = static_cast<in_port_t>(std::stoi(argv[i++]));
                if(port==0) return cerr_and_return("Err: port 0 unsupported");
            } catch (const std::exception& e) {
                return cerr_and_return("Err: invalid port number: " + std::string(argv[i-1]));
            }
        }else if (s == "--download" || s == "--upload") {
            if(action != "") return cerr_and_return("Err: duplicate action: " + s);
            action = s.substr(2);
            if(i>=argc) return cerr_and_return("Err: missing argument for " + s);
            nameFile = argv[i++];
        }else {
            return cerr_and_return("Err: invalid argument: " + s);
        }
    }
    if(port==0) return cerr_and_return("Err: missing port number");
    if(mode=="client"){
        if(action=="") return cerr_and_return("Err: client mode requires --download or --upload");
        if(nameFile=="") return cerr_and_return("Err: client mode requires file path");
        if(action=="upload" && !ThrowingIfstream::check_file_exists(nameFile)){
            return cerr_and_return("Err: upload file not found: " + nameFile);
        }
        if(action=="download" && !ThrowingOfstream::check_path_valid(nameFile)){
            return cerr_and_return("Err: invalid download path: " + nameFile);
        }
    }

    // Determine DNS's IP and port
    std::vector<ipv4_addr> addresses = getIPAddresses();
    if (addresses.empty())
    {
        throw std::runtime_error("No network interfaces found");
    }
    ipv4_addr ip = addresses[0];
    socket_address dnsAddress(ip, DNS_PORT);


    constexpr double DURATION_HEARBEAT = 2;     // in seconds;


    if (mode == "client") {
        norm_log("Enter client mode.");

        Client client(dnsAddress, port);

        if (action == "upload") {
            client.uploadFile(nameFile);
        } else if (action == "download") {
            client.downloadFile(nameFile);
        }
    } else if (mode == "master")
    {
        norm_log("Enter master mode.");

        DataNode master(dnsAddress, port, node_role::MASTER);
        master.start();
    } else if (mode == "data")
    {
        norm_log("Enter Data mode.");

        DataNode data(dnsAddress, port,node_role::DATA);
        data.start();

    } else if (mode == "dns")
    {
        norm_log("Enter DNS mode.");

        DNS dns;
        dns.start();
    }
    
    return 0;
}

// g++ -std=c++17 -o main main.cpp -Wall -Wextra -Wshadow
// master: ./bin/main -mode master
// data: ./bin/main -mode data
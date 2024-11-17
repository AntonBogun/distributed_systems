#pragma once
#include "transmission.h"
#include "packets.h"
namespace distribsys{


class Node{
    public:
    socket_address dns_address;
    Node(socket_address dns_address_):dns_address(dns_address_){}
};
class Server: public Node
{
public:
    ServerSocket server_socket;
    Server(socket_address dns_address_, in_port_t port):
    Node(dns_address_), server_socket(port){}
};
class DataNode: public Server
{
public:
    socket_address master_address; 
    DataNode(socket_address dns_address_, in_port_t port, socket_address master_address_):
    Server(dns_address_, port), master_address(master_address_){}
};

// ================================================================

class MasterNode : public DataNode
{
public:
    MasterNode(socket_address dns_address_, in_port_t port):
    DataNode(dns_address_, port, socket_address(0,0,0,0,0)){}
};

// ================================================================

class DNS : public Server
{
public:
    DNS():Server(socket_address(0,0,0,0,0), DNS_PORT){}
};

// ================================================================

class Client
{
public:
    Client(/* args */);
    ~Client();
};

}//namespace distribsys
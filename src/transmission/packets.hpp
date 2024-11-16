#pragma once
#include <cstddef>
#include <string>
#include <vector>

using namespace std;

typedef vector<byte> BYTES;

enum PACKET_ID
{
    HEARTBEAT,
    HEARTBEAT_ACK,
    REQUEST_SEND_REPLICA,
    SEND_REPLICA,
    SEND_REPLICA_ACK,
    ASK_IP,
    ASK_IP_ACK,
    REQUEST_FROM_CLIENT,
    RESPONSE_NODE_IP,
    CLIENT_UPLOAD,
    DATANODE_SEND_DATA,
    CLIENT_REQUEST_ACK,
    STATE_SYNC,
    STATE_SYNC_ACK
};

enum REQUEST
{
    READ,
    WRITE
};

class Packet
{
public:
    PACKET_ID packetID;
    int payloadSize;
    BYTES payload;

    string fileName;

    Packet(/* args */);

    static Packet fromBytes(BYTES data);
    BYTES toBytes();

    ~Packet();
};

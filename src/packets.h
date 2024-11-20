#pragma once
#include <cstddef>
#include <string>
#include <vector>
#include <initializer_list>

namespace distribsys{

typedef std::vector<u8> BYTES;

enum PACKET_ID : u8
{
    HEARTBEAT=0,
    HEARTBEAT_ACK=1,
    REQUEST_SEND_REPLICA=2,
    SEND_REPLICA=3,
    SEND_REPLICA_ACK=4,
    ASK_IP=5,
    ASK_IP_ACK=6,
    REQUEST_FROM_CLIENT=7,
    RESPONSE_NODE_IP=8,
    CLIENT_UPLOAD=9,
    DATANODE_SEND_DATA=10,
    CLIENT_REQUEST_ACK=11,
    STATE_SYNC=12,
    STATE_SYNC_ACK=13
};


enum REQUEST : u8
{
    READ,
    WRITE
};
static_assert(sizeof(PACKET_ID) == 1, "Packet ID must be 1 byte");
static_assert(sizeof(REQUEST) == 1, "Request must be 1 byte");

class Packet
{
public:
    PACKET_ID packetID;
    int payloadSize;
    BYTES payload;

    std::string fileName;

    Packet(PACKET_ID packetID, int payloadSize, BYTES payload):
    packetID(packetID), payloadSize(payloadSize), payload(payload) {}

    static Packet fromSocket(BYTES data){
        // TODO: HoangLe [Nov-15]: Implement this
        // Packet packet;

        // packet.packetID = static_cast<PACKET_ID>(data[0]);
        
        // switch (packet.packetID)
        // {
        //     // For Heartbeat
        //     case HEARTBEAT:
        //     case HEARTBEAT_ACK:
        //         break;

        //     // For Replication
        //     case REQUEST_SEND_REPLICA:
        //         // TODO: HoangLe [Nov-15]: Implement thiss
        //         break;
        //     case SEND_REPLICA:
        //         // TODO: HoangLe [Nov-15]: Implement thiss
        //         break;
        //     case SEND_REPLICA_ACK:
        //         // TODO: HoangLe [Nov-15]: Implement thiss
        //         break;

        //     default:
        //         break;
        // }
    }
    void toBytes(){
        BYTES data;

        data.push_back(static_cast<u8>(this->packetID));

        // TODO: HoangLe [Nov-15]: Implement this
    }

    ~Packet(){}
};

}//namespace distribsys
#include "packets.hpp"

Packet::Packet(/* args */)
{
}

Packet::~Packet()
{
}

Packet Packet::fromBytes(BYTES data)
{
    // TODO: HoangLe [Nov-15]: Implement this
    Packet packet;

    packet.packetID = static_cast<PACKET_ID>(data[0]);
    switch (packet.packetID)
    {
    // For Heartbeat
    case HEARTBEAT:
    case HEARTBEAT_ACK:
        break;

    // For Replication
    case REQUEST_SEND_REPLICA:
        // TODO: HoangLe [Nov-15]: Implement thiss
        break;
    case SEND_REPLICA:
        // TODO: HoangLe [Nov-15]: Implement thiss
        break;
    case SEND_REPLICA_ACK:
        // TODO: HoangLe [Nov-15]: Implement thiss
        break;

    default:
        break;
    }
}

BYTES Packet::toBytes()
{
    BYTES data;

    data.push_back(static_cast<byte>(this->packetID));

    // TODO: HoangLe [Nov-15]: Implement this

    return data;
}
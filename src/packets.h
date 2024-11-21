#pragma once
#include <cstddef>
#include <string>
#include <vector>
#include <initializer_list>

namespace distribsys{

typedef std::vector<u8> BYTES;

inline void convUInt16ToBytes(uint16_t *src, uint8_t *dst) {memcpy(dst, src, 2);}
inline void convBytesToUInt16(uint8_t *src, uint16_t *dst) {memcpy(dst, src, 2);}

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
    STATE_SYNC_ACK=13,
    NOTIFY=14,
};


enum REQUEST : u8
{
    READ,
    WRITE
};

enum NODE_TYPE : u8
{
    MASTER,
    DATA
};

static_assert(sizeof(PACKET_ID) == 1, "Packet ID must be 1 byte");
static_assert(sizeof(REQUEST) == 1, "Request must be 1 byte");

class Packet
{
public:
    PACKET_ID packetID;
    int payloadSize;
    BYTES payload;
    socket_address addrSrc;

    std::string fileName;
    BYTES binary;

    socket_address addrParsed;

    NODE_TYPE typeNotifyingNode;

    Packet() {}

    Packet(OpenSocket &open_socket) {
        addrSrc = open_socket.other_address;

        TransmissionLayer tl(open_socket);
        tl.initialize_recv();

        u8 byte;
        tl.read_byte(byte);
        packetID = static_cast<PACKET_ID>(byte);

        tl.read_i32(payloadSize);
        payload.resize(payloadSize);

        for (int i = 0; i < payloadSize; ++i)
            tl.read_byte(payload[i]);

        // Parse payload based on packetID
        switch (packetID)
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

            case ASK_IP: {
                convBytesToUInt16(&payload[0], &addrParsed.port);

                break;
            }
            case ASK_IP_ACK: {
                addrParsed.ip.a = payload[0];
                addrParsed.ip.b = payload[1];
                addrParsed.ip.c = payload[2];
                addrParsed.ip.d = payload[3];

                convBytesToUInt16(&payload[4], &addrParsed.port);

                break;
            }
        

            case NOTIFY: {
                typeNotifyingNode = static_cast<NODE_TYPE>(payload[0]);
                convBytesToUInt16(&payload[1], &addrParsed.port);

                break;
            }


            default:
                break;
        }
    }

    static Packet compose_HEARTBEAT() {
        Packet packet;

        packet.packetID = HEARTBEAT;
        packet.payloadSize = 0;

        return packet;
    }

    static Packet compose_HEARTBEAT_ACK() {
        Packet packet;

        packet.packetID = HEARTBEAT_ACK;
        packet.payloadSize = 0;

        return packet;
    }

    static Packet compose_ASK_IP(in_port_t port){
        Packet packet;

        packet.packetID = ASK_IP;
        packet.payloadSize = 2;

        packet.payload.resize(packet.payloadSize);
        convUInt16ToBytes(&port, &packet.payload[0]);

        return packet;
    }

    static Packet compose_ASK_IP_ACK(socket_address addr){
        Packet packet;

        packet.packetID = ASK_IP_ACK;
        packet.payloadSize = 6;

        packet.payload.resize(packet.payloadSize);
        packet.payload[0] = addr.ip.a;
        packet.payload[1] = addr.ip.b;
        packet.payload[2] = addr.ip.c;
        packet.payload[3] = addr.ip.d;

        convUInt16ToBytes(&addr.port, &packet.payload[4]);

        return packet;
    }

    static Packet compose_NOTIFY(NODE_TYPE typeNotifyingNode, in_port_t port){
        Packet packet;

        packet.packetID = NOTIFY;
        packet.payloadSize = 3;

        packet.payload.resize(packet.payloadSize);
        packet.payload[0] = typeNotifyingNode;

        convUInt16ToBytes(&port, &packet.payload[1]);

        return packet;
    }

    void send(socket_address &addrDst) {
        OpenSocket open_socket(CLIENT);
        socket_address addrAskNode(addrDst);
        open_socket.connect_to_server(addrAskNode);

        TransmissionLayer tl(open_socket);

        // Example to send bytes
        tl.write_byte(packetID);
        tl.write_i32(payloadSize);
        for (int i = 0; i < payloadSize; ++i)
            tl.write_byte(payload[i]);

        tl.finalize_send();
    }

    ~Packet(){}
};

}//namespace distribsys
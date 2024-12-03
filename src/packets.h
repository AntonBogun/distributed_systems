#pragma once

#include <cstddef>
#include <string>
#include <vector>
#include <initializer_list>
#include <sstream>

namespace distribsys{

typedef std::vector<u8> BYTES;

inline void convUInt16ToBytes(uint16_t *src, uint8_t *dst) {memcpy(dst, src, 2);}
inline void convBytesToUInt16(uint8_t *src, uint16_t *dst) {memcpy(dst, src, 2);}

enum PACKET_ID : u8
{
    REQUEST_OPEN_HEARTBEAT_CONNECTION=0, // master -> data nodes
    REQUEST_MASTER_ADDRESS=1,            // client -> dns
    SET_MASTER_ADDRESS=2,                // master -> both dns and data nodes
    REQUEST_LIST_OF_ALL_NODES=3,         // master -> dns
    EDIT_LIST_OF_ALL_NODES=4,            // master -> dns
    SET_DATA_NODE_AS_BACKUP=5,           // master -> data node
    CLIENT_CONNECTION_REQUEST=6,         // client -> both to master and data nodes
    DATA_NODE_TO_MASTER_CONNECTION_REQUEST=7, // data node -> master
    MASTER_NODE_TO_DATA_NODE_REPLICATION_REQUEST=8, // master -> data node
    DATA_NODE_TO_DATA_NODE_CONNECTION_REQUEST=9, // data node -> data node
    MASTER_TO_DATA_NODE_INFO_REQUEST=10,  // master -> data node
    MASTER_TO_DATA_NODE_PAUSE_CHANGE=11   // master -> data node
};


enum REQUEST : u8
{
    DOWNLOAD,
    UPLOAD
};


constexpr u8 BYTE_SEP_CHARACTER = 124;      // byte value of character '|'

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
    REQUEST request;

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

            case REQUEST_FROM_CLIENT: {
                // Parse client' port from payload
                convBytesToUInt16(&payload[0], &addrParsed.port);

                // Parse file name
                int lenFileName = payloadSize - 3;
                std::stringstream ss;
                for (int i = 2; i < 2 + lenFileName; ++i)
                    ss << payload[i];
                fileName = ss.str();

                // Parse request: UPLOAD or DOWNLOAD
                request = static_cast<REQUEST>(payload[payloadSize - 1]);

                break;
            }

            case RESPONSE_NODE_IP: {
                if (payloadSize > 0) {
                    addrParsed.ip.a = payload[0];
                    addrParsed.ip.b = payload[1];
                    addrParsed.ip.c = payload[2];
                    addrParsed.ip.d = payload[3];

                    convBytesToUInt16(&payload[4], &addrParsed.port);
                }

                break;
            }

            case CLIENT_UPLOAD: {
                // Parse file name
                int lenFileName = payloadSize - 3;
                std::stringstream ss;
                for (int i = 0; i < payloadSize; ++i) {
                    if (payload[i] == BYTE_SEP_CHARACTER) {
                        if ((i < payloadSize - 2) && (payload[i + 1] == BYTE_SEP_CHARACTER)) 
                            // This is the end of file name
                            break;
                    } else {
                        ss << payload[i];
                    }
                }
                fileName = ss.str();

                // Parse binary
                int posBinary = fileName.length() + 2;
                int lenBinary = payloadSize - 2 - fileName.length();
                binary.resize(lenBinary);

                memcpy(&binary[0], &payload[posBinary], lenBinary);

                break;
            }

            case DATANODE_SEND_DATA: {
                // Parse file name
                int lenFileName = payloadSize - 3;
                std::stringstream ss;
                for (int i = 0; i < payloadSize; ++i) {
                    if (payload[i] == BYTE_SEP_CHARACTER) {
                        if ((i < payloadSize - 2) && (payload[i + 1] == BYTE_SEP_CHARACTER)) 
                            // This is the end of file name
                            break;
                    } else {
                        ss << payload[i];
                    }
                }
                fileName = ss.str();

                // Parse binary
                int posBinary = fileName.length() + 2;
                int lenBinary = payloadSize - 2 - fileName.length();
                binary.resize(lenBinary);

                memcpy(&binary[0], &payload[posBinary], lenBinary);

                break;
            }

            case CLIENT_REQUEST_ACK: {
                // Parse file name
                int lenFileName = payloadSize - 1;
                std::stringstream ss;
                for (int i = 0; i < lenFileName; ++i)
                    ss << payload[i];
                fileName = ss.str();

                // Parse request: UPLOAD or DOWNLOAD
                request = static_cast<REQUEST>(payload[payloadSize - 1]);

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

    static Packet compose_ASK_READ_WRITE(REQUEST req, std::string &nameFile, in_port_t port){
        Packet packet;

        packet.packetID = REQUEST_FROM_CLIENT;
        packet.payloadSize = 2 + nameFile.length() + 1;

        packet.payload.resize(packet.payloadSize);

        // Dump port to payload
        convUInt16ToBytes(&port, &packet.payload[0]);

        // Dump filename to payload
        memcpy(&packet.payload[2], nameFile.data(), nameFile.length());

        // Dump byte READ_WRITE
        packet.payload[packet.payloadSize - 1] = req;

        return packet;
    }

    static Packet compose_CLIENT_UPLOAD(std::string name, std::vector<u8> &binary) {
        Packet packet;

        packet.packetID = CLIENT_UPLOAD;
        packet.payloadSize = name.length() + 2 + binary.size();

        packet.payload.resize(packet.payloadSize);

        // Dump file name to payload
        memcpy(&packet.payload[0], name.data(), name.length());

        // Dump 2 characters '||' to payload
        packet.payload[name.length()] = BYTE_SEP_CHARACTER;
        packet.payload[name.length() + 1] = BYTE_SEP_CHARACTER;

        // Dump binary to payload
        memcpy(&packet.payload[name.length() + 2], &binary[0], binary.size());

        return packet;
    }

    static Packet compose_RESPONSE_NODE_IP() {
        Packet packet;

        packet.packetID = RESPONSE_NODE_IP;
        packet.payloadSize = 0;

        return packet;
    }

    static Packet compose_RESPONSE_NODE_IP(socket_address addr){
        Packet packet;

        packet.packetID = RESPONSE_NODE_IP;
        packet.payloadSize = 6;

        packet.payload.resize(packet.payloadSize);
        packet.payload[0] = addr.ip.a;
        packet.payload[1] = addr.ip.b;
        packet.payload[2] = addr.ip.c;
        packet.payload[3] = addr.ip.d;

        convUInt16ToBytes(&addr.port, &packet.payload[4]);

        return packet;
    }

    static Packet compose_CLIENT_REQUEST_ACK(REQUEST req, std::string &nameFile){
        Packet packet;

        packet.packetID = CLIENT_REQUEST_ACK;
    
        packet.payloadSize = nameFile.length() + 1;
        packet.payload.resize(packet.payloadSize);

        // Dump filename to payload
        memcpy(&packet.payload[0], nameFile.data(), nameFile.length());

        // Dump byte READ_WRITE
        packet.payload[packet.payloadSize - 1] = req;

        return packet;
    }

    static Packet compose_DATANODE_SEND_DATA(std::string name, std::vector<u8> &binary) {
        Packet packet;

        packet.packetID = DATANODE_SEND_DATA;
        packet.payloadSize = name.length() + 2 + binary.size();

        packet.payload.resize(packet.payloadSize);

        // Dump file name to payload
        memcpy(&packet.payload[0], name.data(), name.length());

        // Dump 2 characters '||' to payload
        packet.payload[name.length()] = BYTE_SEP_CHARACTER;
        packet.payload[name.length() + 1] = BYTE_SEP_CHARACTER;

        // Dump binary to payload
        memcpy(&packet.payload[name.length() + 2], &binary[0], binary.size());

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

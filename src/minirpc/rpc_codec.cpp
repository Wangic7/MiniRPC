#include <minirpc/rpc_codec.h>

#include <arpa/inet.h>
#include <cstring>


bool RpcCodec::EncodeRequest(
    const minirpc::RpcHeader& header,
    const std::string& payload,
    std::string& packet)
{
    std::string header_data;


    if(!header.SerializeToString(
            &header_data))
    {
        return false;
    }


    uint32_t header_size =
        static_cast<uint32_t>(
            header_data.size()
        );


    uint32_t network_header_size =
        htonl(header_size);



    packet.clear();


    packet.append(
        reinterpret_cast<const char*>(
            &network_header_size),
        sizeof(network_header_size)
    );


    packet.append(header_data);

    packet.append(payload);


    return true;
}



bool RpcCodec::DecodeRequest(
    const std::string& packet,
    minirpc::RpcHeader& header,
    std::string& payload)
{
    if(packet.size() < sizeof(uint32_t))
    {
        return false;
    }


    uint32_t network_header_size = 0;


    memcpy(
        &network_header_size,
        packet.data(),
        sizeof(uint32_t)
    );


    uint32_t header_size =
        ntohl(network_header_size);



    if(packet.size()
        < sizeof(uint32_t) + header_size)
    {
        return false;
    }



    std::string header_data(
        header_size,
        '\0'
    );


    memcpy(
        header_data.data(),
        packet.data() + sizeof(uint32_t),
        header_size
    );



    if(!header.ParseFromString(
            header_data))
    {
        return false;
    }



    size_t payload_offset =
        sizeof(uint32_t)
        + header_size;



    payload.assign(
        packet.data() + payload_offset,
        packet.size() - payload_offset
    );


    return true;
}



bool RpcCodec::EncodeResponse(
    const minirpc::RpcResponseHeader& header,
    const std::string& payload,
    std::string& packet)
{
    std::string header_data;


    if(!header.SerializeToString(
            &header_data))
    {
        return false;
    }


    uint32_t header_size =
        static_cast<uint32_t>(
            header_data.size()
        );


    uint32_t network_header_size =
        htonl(header_size);



    packet.clear();


    packet.append(
        reinterpret_cast<const char*>(
            &network_header_size),
        sizeof(network_header_size)
    );


    packet.append(header_data);

    packet.append(payload);


    return true;
}


bool RpcCodec::DecodeResponse(
    const std::string& packet,
    minirpc::RpcResponseHeader& header,
    std::string& payload)
{
    if(packet.size() < sizeof(uint32_t))
    {
        return false;
    }


    uint32_t network_header_size = 0;


    std::memcpy(
        &network_header_size,
        packet.data(),
        sizeof(uint32_t)
    );


    uint32_t header_size =
        ntohl(network_header_size);



    if(packet.size()
        < sizeof(uint32_t) + header_size)
    {
        return false;
    }



    if(!header.ParseFromArray(
            packet.data() + sizeof(uint32_t),
            static_cast<int>(header_size)))
    {
        return false;
    }



    size_t payload_offset =
        sizeof(uint32_t) + header_size;



    if(packet.size() < payload_offset)
    {
        return false;
    }



    payload.assign(
        packet.data() + payload_offset,
        packet.size() - payload_offset
    );


    return true;
}

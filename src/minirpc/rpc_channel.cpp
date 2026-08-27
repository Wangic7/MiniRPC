#include <minirpc/rpc_channel.h>
#include <minirpc/rpc_config.h>

#include <atomic>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <string>
#include <utility>

#include "rpc_header.pb.h"

#include <sys/time.h>

#include <cerrno>
#include <fcntl.h>
#include <poll.h>

#include <minirpc/rpc_codec.h>

RpcChannel::RpcChannel(
    std::string host,
    uint16_t port,
    int timeout_ms)
    : host_(std::move(host)),
      port_(port),
      timeout_ms_(timeout_ms),
      sockfd_(-1)
{
}


RpcChannel::~RpcChannel()
{
    if(sockfd_ >= 0)
    {
        close(sockfd_);
        sockfd_ = -1;
    }
}



bool RpcChannel::Connect()
{
    sockfd_ =
        socket(AF_INET, SOCK_STREAM, 0);

    if(sockfd_ < 0)
    {
        std::cerr
            << "socket() failed"
            << std::endl;

        return false;
    }


    timeval timeout{};

    timeout.tv_sec =
        timeout_ms_ / 1000;

    timeout.tv_usec =
        (timeout_ms_ % 1000) * 1000;


    if(setsockopt(
            sockfd_,
            SOL_SOCKET,
            SO_RCVTIMEO,
            &timeout,
            sizeof(timeout)) < 0)
    {
        close(sockfd_);
        sockfd_ = -1;
        return false;
    }


    if(setsockopt(
            sockfd_,
            SOL_SOCKET,
            SO_SNDTIMEO,
            &timeout,
            sizeof(timeout)) < 0)
    {
        close(sockfd_);
        sockfd_ = -1;
        return false;
    }


    sockaddr_in server_addr{};

    server_addr.sin_family = AF_INET;
    server_addr.sin_port =
        htons(port_);


    if(inet_pton(
            AF_INET,
            host_.c_str(),
            &server_addr.sin_addr) <= 0)
    {
        std::cerr
            << "Invalid server address"
            << std::endl;

        close(sockfd_);
        sockfd_ = -1;

        return false;
    }


    if(connect(
            sockfd_,
            reinterpret_cast<sockaddr*>(
                &server_addr),
            sizeof(server_addr)) < 0)
    {
        std::cerr
            << "connect() failed"
            << std::endl;

        close(sockfd_);
        sockfd_ = -1;

        return false;
    }


    return true;
}



bool RpcChannel::SendAll(
    int sockfd,
    const void* buffer,
    size_t length)
{
    const char* data =
        static_cast<const char*>(buffer);


    size_t sent = 0;


    while(sent < length)
    {
        ssize_t n =
            send(
                sockfd,
                data + sent,
                length - sent,
                0
            );


        if(n <= 0)
        {
            return false;
        }


        sent +=
            static_cast<size_t>(n);
    }


    return true;
}



bool RpcChannel::RecvAll(
    int sockfd,
    void* buffer,
    size_t length)
{
    char* data =
        static_cast<char*>(buffer);


    size_t received = 0;


    while(received < length)
    {
        ssize_t n =
            recv(
                sockfd,
                data + received,
                length - received,
                0
            );


        if(n <= 0)
        {
            return false;
        }


        received +=
            static_cast<size_t>(n);
    }


    return true;
}

bool RpcChannel::Call(
    const std::string& service_name,
    const std::string& method_name,
    const google::protobuf::Message& request,
    google::protobuf::Message& response)
{
    // 1. 确保连接存在
    if(sockfd_ < 0)
    {
        if(!Connect())
        {
            return false;
        }
    }


    // 2. 序列化请求参数
    std::string args_data;

    if(!request.SerializeToString(&args_data))
    {
        std::cerr
            << "Failed to serialize RPC request"
            << std::endl;

        return false;
    }


    // 3. 构造 RPC Header

    static std::atomic<uint64_t> next_request_id{1};

    uint64_t request_id =
        next_request_id.fetch_add(
            1,
            std::memory_order_relaxed
        );


    minirpc::RpcHeader header;

    header.set_service_name(
        service_name
    );

    header.set_method_name(
        method_name
    );

    header.set_request_id(
        request_id
    );


    header.set_magic(
        minirpc::RPC_MAGIC
    );


    header.set_version(
        minirpc::RPC_VERSION
    );


    header.set_args_size(
        static_cast<uint32_t>(
            args_data.size()
        )
    );


std::string packet;

if(!RpcCodec::EncodeRequest(
        header,
        args_data,
        packet))
{
    std::cerr
        << "Failed to encode RPC request"
        << std::endl;

    return false;
}



    // 5. 发送请求

    if(!SendAll(
            sockfd_,
            packet.data(),
            packet.size()))
    {
        std::cerr
            << "RPC send failed"
            << std::endl;


        close(sockfd_);
        sockfd_ = -1;

        return false;
    }



// =================================
// 6. 接收 Response Header 长度
// =================================

uint32_t network_response_header_size = 0;

if(!RecvAll(
        sockfd_,
        &network_response_header_size,
        sizeof(network_response_header_size)))
{
    std::cerr
        << "RPC receive failed or timed out after "
        << timeout_ms_
        << " ms"
        << std::endl;

    close(sockfd_);
    sockfd_ = -1;

    return false;
}


uint32_t response_header_size =
    ntohl(network_response_header_size);


// Header长度保护
if(response_header_size == 0 ||
   response_header_size > minirpc::MAX_HEADER_SIZE)
{
    std::cerr
        << "Invalid RPC response header size"
        << std::endl;

    close(sockfd_);
    sockfd_ = -1;

    return false;
}



// =================================
// 7. 接收 Response Header
// =================================

std::string response_header_data(
    response_header_size,
    '\0'
);


if(!RecvAll(
        sockfd_,
        response_header_data.data(),
        response_header_size))
{
    std::cerr
        << "Failed to receive response header"
        << std::endl;

    close(sockfd_);
    sockfd_ = -1;

    return false;
}



// 先解析一次Header，目的是获得payload_size
minirpc::RpcResponseHeader framing_header;


if(!framing_header.ParseFromString(
        response_header_data))
{
    std::cerr
        << "Failed to parse response header"
        << std::endl;

    close(sockfd_);
    sockfd_ = -1;

    return false;
}



// Payload长度保护
if(framing_header.payload_size()
    > minirpc::MAX_PAYLOAD_SIZE)
{
    std::cerr
        << "RPC response payload too large"
        << std::endl;

    close(sockfd_);
    sockfd_ = -1;

    return false;
}



// =================================
// 8. 接收 Response Payload
// =================================

uint32_t payload_size =
    framing_header.payload_size();


std::string response_payload(
    payload_size,
    '\0'
);


if(payload_size > 0)
{
    if(!RecvAll(
            sockfd_,
            response_payload.data(),
            payload_size))
    {
        std::cerr
            << "Failed to receive response payload"
            << std::endl;

        close(sockfd_);
        sockfd_ = -1;

        return false;
    }
}



// =================================
// 9. 重新组成完整Response Packet
// =================================

std::string response_packet;


response_packet.append(
    reinterpret_cast<const char*>(
        &network_response_header_size),
    sizeof(network_response_header_size)
);


response_packet.append(
    response_header_data
);


response_packet.append(
    response_payload
);



// =================================
// 10. 使用 RpcCodec 解码
// =================================

minirpc::RpcResponseHeader response_header;

std::string response_data;


if(!RpcCodec::DecodeResponse(
        response_packet,
        response_header,
        response_data))
{
    std::cerr
        << "Failed to decode RPC response"
        << std::endl;

    close(sockfd_);
    sockfd_ = -1;

    return false;
}


if(response_header.payload_size()
    != response_data.size())
{
    std::cerr
        << "RPC response payload size mismatch"
        << std::endl;

    close(sockfd_);
    sockfd_ = -1;

    return false;
}



    // 8. 校验 request_id

    if(response_header.request_id()
        != request_id)
    {
        std::cerr
            << "RPC request_id mismatch"
            << std::endl;


        return false;
    }



    // 9. RPC错误处理

    if(response_header.error_code()
        != minirpc::RPC_OK)
    {
        std::cerr
            << "RPC error: "
            << response_header.error_code()
            << " - "
            << response_header.error_message()
            << std::endl;


        return false;
    }



    



    // 11. 解析响应

    if(!response.ParseFromString(
            response_data))
    {
        std::cerr
            << "Failed to parse RPC response"
            << std::endl;

        return false;
    }


    return true;
}
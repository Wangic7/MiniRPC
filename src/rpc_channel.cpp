#include "rpc_channel.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <string>
#include <utility>

#include "proto/rpc_header.pb.h"

RpcChannel::RpcChannel(
    std::string host,
    uint16_t port)
    : host_(std::move(host)),
      port_(port)
{
}

bool RpcChannel::SendAll(
    int sockfd,
    const void* buffer,
    size_t length)
{
    const char* data =
        static_cast<const char*>(buffer);

    size_t sent = 0;

    while (sent < length)
    {
        ssize_t n = send(
            sockfd,
            data + sent,
            length - sent,
            0
        );

        if (n <= 0)
        {
            return false;
        }

        sent += static_cast<size_t>(n);
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

    while (received < length)
    {
        ssize_t n = recv(
            sockfd,
            data + received,
            length - received,
            0
        );

        if (n <= 0)
        {
            return false;
        }

        received += static_cast<size_t>(n);
    }

    return true;
}

bool RpcChannel::Call(
    const std::string& service_name,
    const std::string& method_name,
    const google::protobuf::Message& request,
    google::protobuf::Message& response)
{
    // 1. 序列化 RPC 参数
    std::string args_data;

    if (!request.SerializeToString(&args_data))
    {
        std::cerr
            << "Failed to serialize RPC request"
            << std::endl;

        return false;
    }

    // 2. 构造 RPC Header
    minirpc::RpcHeader header;

    header.set_service_name(service_name);
    header.set_method_name(method_name);

    header.set_args_size(
        static_cast<uint32_t>(
            args_data.size()
        )
    );

    std::string header_data;

    if (!header.SerializeToString(
            &header_data))
    {
        std::cerr
            << "Failed to serialize RPC header"
            << std::endl;

        return false;
    }

    // 3. 构造完整 RPC Packet
    uint32_t header_size =
        static_cast<uint32_t>(
            header_data.size()
        );

    uint32_t network_header_size =
        htonl(header_size);

    std::string packet;

    packet.append(
        reinterpret_cast<const char*>(
            &network_header_size),
        sizeof(network_header_size)
    );

    packet.append(header_data);
    packet.append(args_data);

    // 4. 创建 Socket
    int sockfd =
        socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0)
    {
        std::cerr << "socket() failed"
                  << std::endl;

        return false;
    }

    sockaddr_in server_addr{};

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port_);

    if (inet_pton(
            AF_INET,
            host_.c_str(),
            &server_addr.sin_addr) <= 0)
    {
        std::cerr
            << "Invalid server address"
            << std::endl;

        close(sockfd);
        return false;
    }

    // 5. 连接 Server
    if (connect(
            sockfd,
            reinterpret_cast<sockaddr*>(
                &server_addr),
            sizeof(server_addr)) < 0)
    {
        std::cerr << "connect() failed"
                  << std::endl;

        close(sockfd);
        return false;
    }

    // 6. 发送 RPC 请求
    if (!SendAll(
            sockfd,
            packet.data(),
            packet.size()))
    {
        std::cerr << "send() failed"
                  << std::endl;

        close(sockfd);
        return false;
    }

    // 7. 接收 response_size
    uint32_t network_response_size = 0;

    if (!RecvAll(
            sockfd,
            &network_response_size,
            sizeof(network_response_size)))
    {
        std::cerr
            << "Failed to receive response size"
            << std::endl;

        close(sockfd);
        return false;
    }

    uint32_t response_size =
        ntohl(network_response_size);

    // 8. 接收 RPC Response
    std::string response_data(
        response_size,
        '\0'
    );

    if (!RecvAll(
            sockfd,
            response_data.data(),
            response_size))
    {
        std::cerr
            << "Failed to receive RPC response"
            << std::endl;

        close(sockfd);
        return false;
    }

    close(sockfd);

    // 9. 反序列化
    if (!response.ParseFromString(
            response_data))
    {
        std::cerr
            << "Failed to parse RPC response"
            << std::endl;

        return false;
    }

    return true;
}
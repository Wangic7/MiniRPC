#include <arpa/inet.h>
#include <cstdint>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

#include "proto/calculator.pb.h"
#include "proto/rpc_header.pb.h"

// 保证把指定数量的数据全部发送出去
bool send_all(int sockfd, const void* buffer, size_t length)
{
    const char* data = static_cast<const char*>(buffer);
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

bool recv_all(int sockfd, void* buffer, size_t length)
{
    char* data = static_cast<char*>(buffer);
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

int main()
{
    // 1. 创建业务请求
    minirpc::AddRequest request;
    request.set_a(10);
    request.set_b(20);

    std::string args_data;

    if (!request.SerializeToString(&args_data))
    {
        std::cerr << "Serialize AddRequest failed"
                  << std::endl;
        return 1;
    }

    // 2. 创建 RPC Header
    minirpc::RpcHeader header;

    header.set_service_name("Calculator");
    header.set_method_name("Add");
    header.set_args_size(
        static_cast<uint32_t>(args_data.size())
    );

    std::string header_data;

    if (!header.SerializeToString(&header_data))
    {
        std::cerr << "Serialize RpcHeader failed"
                  << std::endl;
        return 1;
    }

    // 3. Header 长度
    uint32_t header_size =
        static_cast<uint32_t>(header_data.size());

    uint32_t network_header_size =
        htonl(header_size);

    // 4. 拼出完整 RPC Packet
    std::string packet;

    packet.append(
        reinterpret_cast<const char*>(&network_header_size),
        sizeof(network_header_size)
    );

    packet.append(header_data);
    packet.append(args_data);

    // 5. 创建 TCP Socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0)
    {
        std::cerr << "socket() failed" << std::endl;
        return 1;
    }

    // 6. 配置服务器地址
    sockaddr_in server_addr{};

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(9000);

    if (inet_pton(
            AF_INET,
            "127.0.0.1",
            &server_addr.sin_addr) <= 0)
    {
        std::cerr << "Invalid server address"
                  << std::endl;

        close(sockfd);
        return 1;
    }

    // 7. 连接服务器
    if (connect(
            sockfd,
            reinterpret_cast<sockaddr*>(&server_addr),
            sizeof(server_addr)) < 0)
    {
        std::cerr << "connect() failed"
                  << std::endl;

        close(sockfd);
        return 1;
    }

    std::cout << "Connected to MiniRPC server."
              << std::endl;

    // 8. 发送完整 RPC Packet
    if (!send_all(
            sockfd,
            packet.data(),
            packet.size()))
    {
        std::cerr << "send() failed"
                  << std::endl;

        close(sockfd);
        return 1;
    }

    std::cout << "RPC request sent successfully."
              << std::endl;

    std::cout << "Service: Calculator"
              << std::endl;

    std::cout << "Method : Add"
              << std::endl;

    std::cout << "Args   : 10, 20"
              << std::endl;

    std::cout << "Packet size: "
          << packet.size()
          << " bytes"
          << std::endl;
    // 9. 接收响应长度
uint32_t network_response_size = 0;

if (!recv_all(
        sockfd,
        &network_response_size,
        sizeof(network_response_size)))
{
    std::cerr << "Failed to receive response size"
              << std::endl;

    close(sockfd);
    return 1;
}

uint32_t response_size =
    ntohl(network_response_size);

// 10. 接收响应内容
std::string response_data(
    response_size,
    '\0'
);

if (!recv_all(
        sockfd,
        response_data.data(),
        response_size))
{
    std::cerr << "Failed to receive response"
              << std::endl;

    close(sockfd);
    return 1;
}

// 11. 解析 AddResponse
minirpc::AddResponse response;

if (!response.ParseFromString(response_data))
{
    std::cerr << "Failed to parse AddResponse"
              << std::endl;

    close(sockfd);
    return 1;
}

std::cout << std::endl;
std::cout << "===== RPC Response ====="
          << std::endl;

std::cout << "Result = "
          << response.result()
          << std::endl;

    close(sockfd);


close(sockfd);

    close(sockfd);

    return 0;
}
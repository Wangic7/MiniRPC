#include <arpa/inet.h>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

#include "proto/calculator.pb.h"
#include "proto/rpc_header.pb.h"

// 确保从 TCP 中接收到指定数量的字节
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


int main()
{
    // 1. 创建 TCP socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd < 0)
    {
        std::cerr << "socket() failed" << std::endl;
        return 1;
    }

    // 2. 配置服务器地址
    sockaddr_in server_addr{};

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(9000);
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    // 3. 绑定 9000 端口
    if (bind(
            server_fd,
            reinterpret_cast<sockaddr*>(&server_addr),
            sizeof(server_addr)) < 0)
    {
        std::cerr << "bind() failed" << std::endl;
        close(server_fd);
        return 1;
    }

    // 4. 开始监听
    if (listen(server_fd, 5) < 0)
    {
        std::cerr << "listen() failed" << std::endl;
        close(server_fd);
        return 1;
    }

    std::cout
        << "MiniRPC server listening on port 9000..."
        << std::endl;

    // 5. 等待客户端连接
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);

    int client_fd = accept(
        server_fd,
        reinterpret_cast<sockaddr*>(&client_addr),
        &client_len
    );

    if (client_fd < 0)
    {
        std::cerr << "accept() failed" << std::endl;
        close(server_fd);
        return 1;
    }

    std::cout << "Client connected." << std::endl;

    // 6. 读取前 4 字节 header_size
    uint32_t network_header_size = 0;

    if (!recv_all(
            client_fd,
            &network_header_size,
            sizeof(network_header_size)))
    {
        std::cerr << "Failed to receive header size"
                  << std::endl;
        return 1;
    }

    uint32_t header_size =
        ntohl(network_header_size);

    // 7. 接收 RPC Header
    std::string header_data(header_size, '\0');

    if (!recv_all(
            client_fd,
            header_data.data(),
            header_size))
    {
        std::cerr << "Failed to receive RPC header"
                  << std::endl;
        return 1;
    }

    minirpc::RpcHeader header;

    if (!header.ParseFromString(header_data))
    {
        std::cerr << "Failed to parse RPC header"
                  << std::endl;
        return 1;
    }

    // 8. 接收 RPC 参数
    std::string args_data(
        header.args_size(),
        '\0'
    );

    if (!recv_all(
            client_fd,
            args_data.data(),
            header.args_size()))
    {
        std::cerr << "Failed to receive arguments"
                  << std::endl;
        return 1;
    }

    // 9. 解析 AddRequest
    minirpc::AddRequest request;

    if (!request.ParseFromString(args_data))
    {
        std::cerr << "Failed to parse AddRequest"
                  << std::endl;
        return 1;
    }

    std::cout << "\n===== RPC Request ====="
              << std::endl;

    std::cout << "Service: "
              << header.service_name()
              << std::endl;

    std::cout << "Method : "
              << header.method_name()
              << std::endl;

    std::cout << "a = "
              << request.a()
              << std::endl;

    std::cout << "b = "
              << request.b()
              << std::endl;

    std::cout << "Result = "
              << request.a() + request.b()
              << std::endl;
    // 10. 构造 RPC 响应
minirpc::AddResponse response;

response.set_result(
    request.a() + request.b()
);

std::string response_data;

if (!response.SerializeToString(&response_data))
{
    std::cerr << "Failed to serialize response"
              << std::endl;

    close(client_fd);
    close(server_fd);
    return 1;
}

// 11. 先发送响应长度
uint32_t response_size =
    static_cast<uint32_t>(response_data.size());

uint32_t network_response_size =
    htonl(response_size);

if (!send_all(
        client_fd,
        &network_response_size,
        sizeof(network_response_size)))
{
    std::cerr << "Failed to send response size"
              << std::endl;

    close(client_fd);
    close(server_fd);
    return 1;
}

// 12. 再发送响应数据
if (!send_all(
        client_fd,
        response_data.data(),
        response_data.size()))
{
    std::cerr << "Failed to send response"
              << std::endl;

    close(client_fd);
    close(server_fd);
    return 1;
}

std::cout << "RPC response sent." << std::endl;

    close(client_fd);
    close(server_fd);

    return 0;
}
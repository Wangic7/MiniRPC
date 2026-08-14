#include <arpa/inet.h>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

#include "proto/calculator.pb.h"
#include "proto/rpc_header.pb.h"

#include "rpc_dispatcher.h"

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


bool handle_client(
    int client_fd,
    const RpcDispatcher& dispatcher)
{
    // 1. 接收 header_size
    uint32_t network_header_size = 0;

    if (!recv_all(
            client_fd,
            &network_header_size,
            sizeof(network_header_size)))
    {
        std::cerr << "Failed to receive header size"
                  << std::endl;
        return false;
    }

    uint32_t header_size =
        ntohl(network_header_size);

    // 2. 接收 RpcHeader
    std::string header_data(header_size, '\0');

    if (!recv_all(
            client_fd,
            header_data.data(),
            header_size))
    {
        std::cerr << "Failed to receive RPC header"
                  << std::endl;
        return false;
    }

    minirpc::RpcHeader header;

    if (!header.ParseFromString(header_data))
    {
        std::cerr << "Failed to parse RPC header"
                  << std::endl;
        return false;
    }

    // 3. 接收参数
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
        return false;
    }

    // 4. Dispatcher 分发
    std::string response_data;

    if (!dispatcher.Dispatch(
            header.service_name(),
            header.method_name(),
            args_data,
            response_data))
    {
        std::cerr
            << "RPC method not found or execution failed: "
            << header.service_name()
            << "."
            << header.method_name()
            << std::endl;

        return false;
    }

    std::cout
        << "RPC dispatched successfully: "
        << header.service_name()
        << "."
        << header.method_name()
        << std::endl;

    // 5. 发送 response_size
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

        return false;
    }

    // 6. 发送 response
    if (!send_all(
            client_fd,
            response_data.data(),
            response_data.size()))
    {
        std::cerr << "Failed to send response"
                  << std::endl;

        return false;
    }

    std::cout << "RPC response sent."
              << std::endl;

    return true;
}


int main()
{
    RpcDispatcher dispatcher;

dispatcher.Register(
    "Calculator",
    "Add",
    [](const std::string& request_data,
       std::string& response_data)
    {
        minirpc::AddRequest request;

        if (!request.ParseFromString(request_data))
        {
            return false;
        }

        minirpc::AddResponse response;

        response.set_result(
            request.a() + request.b()
        );

        std::cout
            << "Executing Calculator.Add("
            << request.a()
            << ", "
            << request.b()
            << ")"
            << std::endl;

        return response.SerializeToString(
            &response_data
        );
    }
);


dispatcher.Register(
    "Calculator",
    "Subtract",
    [](const std::string& request_data,
       std::string& response_data)
    {
        minirpc::SubtractRequest request;

        if (!request.ParseFromString(request_data))
        {
            return false;
        }

        minirpc::SubtractResponse response;

        response.set_result(
            request.a() - request.b()
        );

        std::cout
            << "Executing Calculator.Subtract("
            << request.a()
            << ", "
            << request.b()
            << ")"
            << std::endl;

        return response.SerializeToString(
            &response_data
        );
    }
);
    
    // 1. 创建 TCP socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd < 0)
    {
        std::cerr << "socket() failed" << std::endl;
        return 1;
    }


    // 允许服务器快速重新绑定同一个端口
    int opt = 1;

    if (setsockopt(
        server_fd,
        SOL_SOCKET,
        SO_REUSEADDR,
        &opt,
        sizeof(opt)) < 0)
{
    std::cerr << "setsockopt() failed" << std::endl;
    close(server_fd);
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

    // 5. 持续接受客户端
while (true)
{
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);

    std::cout << "\nWaiting for client..."
              << std::endl;

    int client_fd = accept(
        server_fd,
        reinterpret_cast<sockaddr*>(&client_addr),
        &client_len
    );

    if (client_fd < 0)
    {
        std::cerr << "accept() failed"
                  << std::endl;

        continue;
    }

    std::cout << "Client connected."
              << std::endl;

    if (!handle_client(
            client_fd,
            dispatcher))
    {
        std::cerr << "RPC request failed."
                  << std::endl;
    }

    close(client_fd);

    std::cout
        << "Client disconnected."
        << std::endl;
}

close(server_fd);

return 0;

}
#include <minirpc/rpc_server.h>

#include <arpa/inet.h>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

#include "rpc_header.pb.h"

RpcServer::RpcServer(
    std::size_t worker_count)
    : thread_pool_(worker_count)
{
}

void RpcServer::RegisterMethod(
    const std::string& service_name,
    const std::string& method_name,
    Handler handler)
{
    dispatcher_.Register(
        service_name,
        method_name,
        std::move(handler)
    );
}

bool RpcServer::RecvAll(
    int sockfd,
    void* buffer,
    size_t length)
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

bool RpcServer::SendAll(
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
            MSG_NOSIGNAL//客户端提前断开时，Server 不会因为 SIGPIPE 被直接杀掉
        );

        if (n <= 0)
        {
            return false;
        }

        sent += static_cast<size_t>(n);
    }

    return true;
}

bool RpcServer::HandleClient(int client_fd)
{
    uint32_t network_header_size = 0;

    if (!RecvAll(
            client_fd,
            &network_header_size,
            sizeof(network_header_size)))
    {
        return false;
    }

    uint32_t header_size =
        ntohl(network_header_size);

    std::string header_data(
        header_size,
        '\0'
    );

    if (!RecvAll(
            client_fd,
            header_data.data(),
            header_size))
    {
        return false;
    }

    minirpc::RpcHeader header;

    if (!header.ParseFromString(header_data))
    {
        return false;
    }

    std::string args_data(
        header.args_size(),
        '\0'
    );

    if (!RecvAll(
            client_fd,
            args_data.data(),
            header.args_size()))
    {
        return false;
    }

    
    std::string response_data;

minirpc::RpcErrorCode error_code =
    minirpc::RPC_OK;

std::string error_message;

// 方法不存在
if (!dispatcher_.HasMethod(
        header.service_name(),
        header.method_name()))
{
    error_code =
        minirpc::RPC_METHOD_NOT_FOUND;

    error_message =
        "RPC method not found";

    response_data.clear();
}

// 方法存在，执行失败
else if (!dispatcher_.Dispatch(
             header.service_name(),
             header.method_name(),
             args_data,
             response_data))
{
    error_code =
        minirpc::RPC_BAD_REQUEST;

    error_message =
        "RPC request execution failed";

    response_data.clear();
}


// =================================
// 构造 RPC Response Header
// =================================

minirpc::RpcResponseHeader response_header;

response_header.set_request_id(
    header.request_id()
);

response_header.set_error_code(
    error_code
);

response_header.set_error_message(
    error_message
);

response_header.set_payload_size(
    static_cast<uint32_t>(
        response_data.size()
    )
);

std::string response_header_data;

if (!response_header.SerializeToString(
        &response_header_data))
{
    return false;
}


// =================================
// 发送 Response Header 长度
// =================================

uint32_t response_header_size =
    static_cast<uint32_t>(
        response_header_data.size()
    );

uint32_t network_response_header_size =
    htonl(response_header_size);

if (!SendAll(
        client_fd,
        &network_response_header_size,
        sizeof(network_response_header_size)))
{
    return false;
}


// =================================
// 发送 Response Header
// =================================

if (!SendAll(
        client_fd,
        response_header_data.data(),
        response_header_data.size()))
{
    return false;
}


// =================================
// 发送业务 Response
// =================================

if (!response_data.empty())
{
    if (!SendAll(
            client_fd,
            response_data.data(),
            response_data.size()))
    {
        return false;
    }
}

std::cout
    << "RPC completed: "
    << header.service_name()
    << "."
    << header.method_name()
    << " [request_id="
    << header.request_id()
    << "]"
    << std::endl;

return true;


}

bool RpcServer::Start(uint16_t port)
{
    int server_fd =
        socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd < 0)
    {
        std::cerr << "socket() failed"
                  << std::endl;

        return false;
    }

    int opt = 1;

    if (setsockopt(
            server_fd,
            SOL_SOCKET,
            SO_REUSEADDR,
            &opt,
            sizeof(opt)) < 0)
    {
        std::cerr << "setsockopt() failed"
                  << std::endl;

        close(server_fd);
        return false;
    }

    sockaddr_in server_addr{};

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr =
        htonl(INADDR_ANY);

    if (bind(
            server_fd,
            reinterpret_cast<sockaddr*>(
                &server_addr),
            sizeof(server_addr)) < 0)
    {
        std::cerr << "bind() failed"
                  << std::endl;

        close(server_fd);
        return false;
    }

    if (listen(server_fd, 5) < 0)
    {
        std::cerr << "listen() failed"
                  << std::endl;

        close(server_fd);
        return false;
    }

    std::cout
        << "MiniRPC server listening on port "
        << port
        << "..."
        << std::endl;

    while (true)
    {
        sockaddr_in client_addr{};
        socklen_t client_len =
            sizeof(client_addr);

        std::cout
            << "\nWaiting for client..."
            << std::endl;

        int client_fd = accept(
            server_fd,
            reinterpret_cast<sockaddr*>(
                &client_addr),
            &client_len
        );

        if (client_fd < 0)
        {
            std::cerr << "accept() failed"
                      << std::endl;

            continue;
        }

std::cout
    << "Client connected."
    << std::endl;

thread_pool_.Submit(
    [this, client_fd]()
    {
        if (!HandleClient(client_fd))
        {
            std::cerr
                << "RPC request failed."
                << std::endl;
        }

        close(client_fd);

        std::cout
            << "Client disconnected."
            << std::endl;
    }
);//accept 线程不再亲自干业务
    }

    close(server_fd);

    return true;
}
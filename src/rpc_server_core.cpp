#include "rpc_server.h"

#include <arpa/inet.h>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

#include "proto/rpc_header.pb.h"

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

    if (!dispatcher_.Dispatch(
            header.service_name(),
            header.method_name(),
            args_data,
            response_data))
    {
        std::cerr
            << "RPC dispatch failed: "
            << header.service_name()
            << "."
            << header.method_name()
            << std::endl;

        return false;
    }

    uint32_t response_size =
        static_cast<uint32_t>(
            response_data.size()
        );

    uint32_t network_response_size =
        htonl(response_size);

    if (!SendAll(
            client_fd,
            &network_response_size,
            sizeof(network_response_size)))
    {
        return false;
    }

    if (!SendAll(
            client_fd,
            response_data.data(),
            response_data.size()))
    {
        return false;
    }

    std::cout
        << "RPC completed: "
        << header.service_name()
        << "."
        << header.method_name()
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

        std::cout << "Client connected."
                  << std::endl;

        if (!HandleClient(client_fd))
        {
            std::cerr << "RPC request failed."
                      << std::endl;
        }

        close(client_fd);

        std::cout << "Client disconnected."
                  << std::endl;
    }

    close(server_fd);

    return true;
}
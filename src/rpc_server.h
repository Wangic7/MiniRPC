#pragma once

#include <cstdint>
#include <string>

#include "rpc_dispatcher.h"

class RpcServer
{
public:
    using Handler = RpcDispatcher::Handler;

    RpcServer() = default;

    void RegisterMethod(
        const std::string& service_name,
        const std::string& method_name,
        Handler handler
    );

    bool Start(uint16_t port);

private:
    bool HandleClient(int client_fd);

    static bool RecvAll(
        int sockfd,
        void* buffer,
        size_t length
    );

    static bool SendAll(
        int sockfd,
        const void* buffer,
        size_t length
    );

private:
    RpcDispatcher dispatcher_;
};
#pragma once

#include <cstdint>
#include <string>

#include <google/protobuf/message.h>

class RpcChannel
{
public:
    RpcChannel(
        std::string host,
        uint16_t port
    );

    bool Call(
        const std::string& service_name,
        const std::string& method_name,
        const google::protobuf::Message& request,
        google::protobuf::Message& response
    );

private:
    static bool SendAll(
        int sockfd,
        const void* buffer,
        size_t length
    );

    static bool RecvAll(
        int sockfd,
        void* buffer,
        size_t length
    );

private:
    std::string host_;
    uint16_t port_;
};
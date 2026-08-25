#pragma once

#include <string>

#include "rpc_header.pb.h"


class RpcCodec
{
public:

    // 编码 RPC 请求
    static bool EncodeRequest(
        const minirpc::RpcHeader& header,
        const std::string& payload,
        std::string& packet
    );


    // 解码 RPC 请求
    static bool DecodeRequest(
        const std::string& packet,
        minirpc::RpcHeader& header,
        std::string& payload
    );


    // 编码 RPC 响应
    static bool EncodeResponse(
        const minirpc::RpcResponseHeader& header,
        const std::string& payload,
        std::string& packet
    );


    // 解码 RPC 响应
    static bool DecodeResponse(
        const std::string& packet,
        minirpc::RpcResponseHeader& header,
        std::string& payload
    );
};

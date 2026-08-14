#pragma once

#include <string>

#include "rpc_server.h"
//RpcServer = RPC 框架
class CalculatorService//业务代码
{
public:
    void Register(RpcServer& server);

private:
    bool Add(
        const std::string& request_data,
        std::string& response_data
    );

    bool Subtract(
        const std::string& request_data,
        std::string& response_data
    );
};
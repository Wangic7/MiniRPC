#include <iostream>
#include <string>

#include "proto/calculator.pb.h"
#include "rpc_server.h"

int main()
{
    RpcServer server;

    // =========================
    // 注册 Calculator.Add
    // =========================
    server.RegisterMethod(
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

    // =========================
    // 注册 Calculator.Subtract
    // =========================
    server.RegisterMethod(
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

    // 启动 MiniRPC Server
    if (!server.Start(9000))
    {
        std::cerr
            << "Failed to start MiniRPC server."
            << std::endl;

        return 1;
    }

    return 0;
}
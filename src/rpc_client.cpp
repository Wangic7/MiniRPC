#include <iostream>
#include <string>

#include "proto/calculator.pb.h"
#include <minirpc/rpc_channel.h>

int main(int argc, char* argv[])
{
    if (argc != 4)
    {
        std::cerr
            << "Usage: ./rpc_client <add|subtract> <a> <b>"
            << std::endl;

        return 1;
    }

    std::string operation = argv[1];

    int a = std::stoi(argv[2]);
    int b = std::stoi(argv[3]);

    // 创建 RPC 通道
    RpcChannel channel(
        "127.0.0.1",
        9000
    );

    // =========================
    // Add
    // =========================
    if (operation == "add")
    {
        minirpc::AddRequest request;
        minirpc::AddResponse response;

        request.set_a(a);
        request.set_b(b);

        if (!channel.Call(
                "Calculator",
                "Add",
                request,
                response))
        {
            std::cerr
                << "RPC call failed: Calculator.Add"
                << std::endl;

            return 1;
        }

        std::cout
            << "===== RPC Response ====="
            << std::endl;

        std::cout
            << "Calculator.Add("
            << a
            << ", "
            << b
            << ") = "
            << response.result()
            << std::endl;
    }

    // =========================
    // Subtract
    // =========================
    else if (operation == "subtract")
    {
        minirpc::SubtractRequest request;
        minirpc::SubtractResponse response;

        request.set_a(a);
        request.set_b(b);

        if (!channel.Call(
                "Calculator",
                "Subtract",
                request,
                response))
        {
            std::cerr
                << "RPC call failed: Calculator.Subtract"
                << std::endl;

            return 1;
        }

        std::cout
            << "===== RPC Response ====="
            << std::endl;

        std::cout
            << "Calculator.Subtract("
            << a
            << ", "
            << b
            << ") = "
            << response.result()
            << std::endl;
    }

    else
    {
        std::cerr
            << "Unknown operation: "
            << operation
            << std::endl;

        return 1;
    }

    return 0;
}
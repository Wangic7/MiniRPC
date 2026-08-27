#include <iostream>
#include <string>

#include "calculator_service.h"
#include <minirpc/rpc_server.h>

int main(int argc, char* argv[])
{
    std::size_t worker_count = 4;

    if (argc >= 2)
    {
        worker_count =
            static_cast<std::size_t>(
                std::stoul(argv[1])
            );
    }

    std::cout
        << "Worker count: "
        << worker_count
        << std::endl;

    RpcServer server(
        worker_count,
        100
    );

    CalculatorService calculator_service;

    calculator_service.Register(server);

    if (!server.Start(9000))
    {
        std::cerr
            << "Failed to start MiniRPC server."
            << std::endl;

        return 1;
    }

    return 0;
}
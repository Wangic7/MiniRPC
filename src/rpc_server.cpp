#include <iostream>

#include "calculator_service.h"
#include "rpc_server.h"

int main()
{
    RpcServer server;

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
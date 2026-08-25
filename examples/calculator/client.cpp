#include <iostream>
#include <string>

#include "calculator.pb.h"
#include <minirpc/rpc_channel.h>

int main()
{
    RpcChannel channel(
        "127.0.0.1",
        9000
    );


    // 第一次RPC
    {
        minirpc::AddRequest request;
        minirpc::AddResponse response;

        request.set_a(10);
        request.set_b(20);

        if(!channel.Call(
                "Calculator",
                "Add",
                request,
                response))
        {
            std::cerr
                << "Add failed"
                << std::endl;

            return 1;
        }


        std::cout
            << "Add result = "
            << response.result()
            << std::endl;
    }



    // 第二次RPC
    {
        minirpc::SubtractRequest request;
        minirpc::SubtractResponse response;

        request.set_a(20);
        request.set_b(5);


        if(!channel.Call(
                "Calculator",
                "Subtract",
                request,
                response))
        {
            std::cerr
                << "Subtract failed"
                << std::endl;

            return 1;
        }


        std::cout
            << "Subtract result = "
            << response.result()
            << std::endl;
    }



    // 第三次RPC
    {
        minirpc::AddRequest request;
        minirpc::AddResponse response;

        request.set_a(100);
        request.set_b(200);


        if(!channel.Call(
                "Calculator",
                "Add",
                request,
                response))
        {
            std::cerr
                << "Add2 failed"
                << std::endl;

            return 1;
        }


        std::cout
            << "Add2 result = "
            << response.result()
            << std::endl;
    }


    return 0;
}
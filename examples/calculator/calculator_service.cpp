#include "calculator_service.h"

#include <iostream>

#include "calculator.pb.h"

void CalculatorService::Register(RpcServer& server)
{
    server.RegisterMethod(
        "Calculator",
        "Add",
        [this](
            const std::string& request_data,
            std::string& response_data)//[this](...)表示Lambda 捕获当前 CalculatorService 对象，然后调用Add(...)和Subtract(...)
        {
            return Add(
                request_data,
                response_data
            );
        }
    );

    server.RegisterMethod(
        "Calculator",
        "Subtract",
        [this](
            const std::string& request_data,
            std::string& response_data)
        {
            return Subtract(
                request_data,
                response_data
            );
        }
    );
}

bool CalculatorService::Add(
    const std::string& request_data,
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

bool CalculatorService::Subtract(
    const std::string& request_data,
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
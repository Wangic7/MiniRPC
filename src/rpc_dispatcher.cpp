#include "rpc_dispatcher.h"

std::string RpcDispatcher::MakeKey(
    const std::string& service_name,
    const std::string& method_name)
{
    return service_name + "." + method_name;
}

void RpcDispatcher::Register(
    const std::string& service_name,
    const std::string& method_name,
    Handler handler)
{
    handlers_[MakeKey(service_name, method_name)] =
        std::move(handler);
}

bool RpcDispatcher::Dispatch(
    const std::string& service_name,
    const std::string& method_name,
    const std::string& request_data,
    std::string& response_data) const
{
    auto it = handlers_.find(
        MakeKey(service_name, method_name)
    );

    if (it == handlers_.end())
    {
        return false;
    }

    return it->second(
        request_data,
        response_data
    );
}
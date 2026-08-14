#pragma once

#include <functional>
#include <string>
#include <unordered_map>

class RpcDispatcher
{
public:
    using Handler =
        std::function<bool(
            const std::string& request_data,
            std::string& response_data)>;

    void Register(
        const std::string& service_name,
        const std::string& method_name,
        Handler handler
    );

    bool HasMethod(
    const std::string& service_name,
    const std::string& method_name
    ) const;

    bool Dispatch(
        const std::string& service_name,
        const std::string& method_name,
        const std::string& request_data,
        std::string& response_data
    ) const;

private:
    static std::string MakeKey(
        const std::string& service_name,
        const std::string& method_name
    );

    std::unordered_map<std::string, Handler> handlers_;
};
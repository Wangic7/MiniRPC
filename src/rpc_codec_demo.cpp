#include <arpa/inet.h>
#include <cstring>
#include <iostream>
#include <string>

#include "proto/calculator.pb.h"
#include "proto/rpc_header.pb.h"

int main()
{
    // ============================
    // 客户端：构造 RPC 请求
    // ============================

    // 1. 构造业务参数
    minirpc::AddRequest request;
    request.set_a(10);
    request.set_b(20);

    std::string args_data;

    if (!request.SerializeToString(&args_data))
    {
        std::cerr << "Serialize AddRequest failed!" << std::endl;
        return 1;
    }

    // 2. 构造 RPC Header
    minirpc::RpcHeader header;

    header.set_service_name("Calculator");
    header.set_method_name("Add");
    header.set_args_size(
        static_cast<uint32_t>(args_data.size())
    );

    std::string header_data;

    if (!header.SerializeToString(&header_data))
    {
        std::cerr << "Serialize RpcHeader failed!" << std::endl;
        return 1;
    }

    // 3. Header 长度
    uint32_t header_size =
        static_cast<uint32_t>(header_data.size());

    // 转换为网络字节序
    uint32_t network_header_size = htonl(header_size);

    // 4. 拼接完整 RPC 数据包
    std::string packet;

    packet.append(
        reinterpret_cast<const char*>(&network_header_size),
        sizeof(network_header_size)
    );

    packet.append(header_data);
    packet.append(args_data);

    std::cout << "===== Client =====" << std::endl;
    std::cout << "Service: Calculator" << std::endl;
    std::cout << "Method : Add" << std::endl;
    std::cout << "Args   : 10, 20" << std::endl;
    std::cout << "Packet size: "
              << packet.size()
              << " bytes"
              << std::endl;


    // ============================
    // 服务器：解析 RPC 请求
    // ============================

    if (packet.size() < sizeof(uint32_t))
    {
        std::cerr << "Invalid packet!" << std::endl;
        return 1;
    }

    // 5. 读取前 4 字节：Header 长度
    uint32_t received_network_header_size = 0;

    std::memcpy(
        &received_network_header_size,
        packet.data(),
        sizeof(uint32_t)
    );

    uint32_t received_header_size =
        ntohl(received_network_header_size);

    // 防止数据不完整
    if (packet.size()
        < sizeof(uint32_t) + received_header_size)
    {
        std::cerr << "Incomplete RPC header!" << std::endl;
        return 1;
    }

    // 6. 解析 RpcHeader
    minirpc::RpcHeader received_header;

    if (!received_header.ParseFromArray(
            packet.data() + sizeof(uint32_t),
            static_cast<int>(received_header_size)))
    {
        std::cerr << "Parse RpcHeader failed!" << std::endl;
        return 1;
    }

    // 7. 找到参数开始位置
    size_t args_offset =
        sizeof(uint32_t) + received_header_size;

    if (packet.size()
        < args_offset + received_header.args_size())
    {
        std::cerr << "Incomplete RPC arguments!" << std::endl;
        return 1;
    }

    // 8. 解析 AddRequest
    minirpc::AddRequest received_request;

    if (!received_request.ParseFromArray(
            packet.data() + args_offset,
            static_cast<int>(received_header.args_size())))
    {
        std::cerr << "Parse AddRequest failed!" << std::endl;
        return 1;
    }

    std::cout << std::endl;
    std::cout << "===== Server =====" << std::endl;

    std::cout << "Service: "
              << received_header.service_name()
              << std::endl;

    std::cout << "Method : "
              << received_header.method_name()
              << std::endl;

    std::cout << "a = "
              << received_request.a()
              << std::endl;

    std::cout << "b = "
              << received_request.b()
              << std::endl;

    std::cout << "Result = "
              << received_request.a()
                   + received_request.b()
              << std::endl;

    return 0;
}
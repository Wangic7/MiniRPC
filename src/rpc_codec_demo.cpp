#include <iostream>
#include <string>

#include <minirpc/rpc_codec.h>
#include <minirpc/rpc_config.h>

#include "calculator.pb.h"


int main()
{
    // ============================
    // Client:
    // 构造 RPC 请求
    // ============================

    minirpc::AddRequest request;

    request.set_a(10);
    request.set_b(20);


    std::string args_data;

    if(!request.SerializeToString(
            &args_data))
    {
        std::cerr
            << "Serialize request failed"
            << std::endl;

        return 1;
    }



    minirpc::RpcHeader header;

    header.set_service_name(
        "Calculator"
    );

    header.set_method_name(
        "Add"
    );

    header.set_request_id(
        1
    );

    header.set_magic(
        minirpc::RPC_MAGIC
    );

    header.set_version(
        minirpc::RPC_VERSION
    );

    header.set_args_size(
        static_cast<uint32_t>(
            args_data.size()
        )
    );



    std::string packet;


    if(!RpcCodec::EncodeRequest(
            header,
            args_data,
            packet))
    {
        std::cerr
            << "Encode RPC request failed"
            << std::endl;

        return 1;
    }



    std::cout
        << "===== Encode Request ====="
        << std::endl;

    std::cout
        << "Packet size: "
        << packet.size()
        << " bytes"
        << std::endl;



    // ============================
    // Server:
    // 解码 RPC 请求
    // ============================


    minirpc::RpcHeader decoded_header;

    std::string decoded_payload;


    if(!RpcCodec::DecodeRequest(
            packet,
            decoded_header,
            decoded_payload))
    {
        std::cerr
            << "Decode RPC request failed"
            << std::endl;

        return 1;
    }



    minirpc::AddRequest decoded_request;


    if(!decoded_request.ParseFromString(
            decoded_payload))
    {
        std::cerr
            << "Parse AddRequest failed"
            << std::endl;

        return 1;
    }



    std::cout
        << std::endl;

    std::cout
        << "===== Decode Request ====="
        << std::endl;


    std::cout
        << "Service: "
        << decoded_header.service_name()
        << std::endl;


    std::cout
        << "Method : "
        << decoded_header.method_name()
        << std::endl;


    std::cout
        << "Request ID: "
        << decoded_header.request_id()
        << std::endl;


    std::cout
        << "Magic: "
        << decoded_header.magic()
        << std::endl;


    std::cout
        << "Version: "
        << decoded_header.version()
        << std::endl;


    std::cout
        << "a = "
        << decoded_request.a()
        << std::endl;


    std::cout
        << "b = "
        << decoded_request.b()
        << std::endl;


    std::cout
        << "Result = "
        << decoded_request.a()
           +
           decoded_request.b()
        << std::endl;



    // ============================
    // Test Response Codec
    // ============================


    minirpc::RpcResponseHeader response_header;


    response_header.set_request_id(
        1
    );


    response_header.set_error_code(
        minirpc::RPC_OK
    );

    response_header.set_magic(
    minirpc::RPC_MAGIC
);

response_header.set_version(
    minirpc::RPC_VERSION
);


    response_header.set_error_message(
        ""
    );


    std::string response_payload =
        "success";



    std::string response_packet;



    if(!RpcCodec::EncodeResponse(
            response_header,
            response_payload,
            response_packet))
    {
        std::cerr
            << "Encode response failed"
            << std::endl;

        return 1;
    }



    minirpc::RpcResponseHeader decoded_response_header;

    std::string decoded_response_payload;



    if(!RpcCodec::DecodeResponse(
            response_packet,
            decoded_response_header,
            decoded_response_payload))
    {
        std::cerr
            << "Decode response failed"
            << std::endl;

        return 1;
    }



    std::cout
        << std::endl;


    std::cout
        << "===== Decode Response ====="
        << std::endl;


    std::cout
        << "Request ID: "
        << decoded_response_header.request_id()
        << std::endl;


    std::cout
        << "Error Code: "
        << decoded_response_header.error_code()
        << std::endl;


    std::cout
        << "Payload: "
        << decoded_response_payload
        << std::endl;



    return 0;
}
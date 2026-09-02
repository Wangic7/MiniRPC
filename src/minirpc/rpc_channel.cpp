#include <minirpc/rpc_channel.h>
#include <minirpc/rpc_config.h>

#include <atomic>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <string>
#include <utility>

#include "rpc_header.pb.h"

#include <sys/time.h>

#include <cerrno>
#include <fcntl.h>
#include <poll.h>

#include <minirpc/rpc_codec.h>
#include <minirpc/logger.h>

#include <cstring>

RpcChannel::RpcChannel(
    std::string host,
    uint16_t port,
    int timeout_ms)
    : host_(std::move(host)),
      port_(port),
      timeout_ms_(timeout_ms),
      sockfd_(-1),
      receive_buffer_(8192)
{
}


RpcChannel::~RpcChannel()
{
    if(sockfd_ >= 0)
    {
        close(sockfd_);
        sockfd_ = -1;
    }
}



bool RpcChannel::Connect()
{

    receive_buffer_.Clear();

    sockfd_ =
        socket(AF_INET, SOCK_STREAM, 0);

    if(sockfd_ < 0)
    {
        LOG_ERROR() << "socket() failed";

        return false;
    }


    timeval timeout{};

    timeout.tv_sec =
        timeout_ms_ / 1000;

    timeout.tv_usec =
        (timeout_ms_ % 1000) * 1000;


    if(setsockopt(
            sockfd_,
            SOL_SOCKET,
            SO_RCVTIMEO,
            &timeout,
            sizeof(timeout)) < 0)
    {
        close(sockfd_);
        sockfd_ = -1;
        return false;
    }


    if(setsockopt(
            sockfd_,
            SOL_SOCKET,
            SO_SNDTIMEO,
            &timeout,
            sizeof(timeout)) < 0)
    {
        close(sockfd_);
        sockfd_ = -1;
        return false;
    }


    sockaddr_in server_addr{};

    server_addr.sin_family = AF_INET;
    server_addr.sin_port =
        htons(port_);


    if(inet_pton(
            AF_INET,
            host_.c_str(),
            &server_addr.sin_addr) <= 0)
    {
        LOG_ERROR()
            << "Invalid server address: "
            << host_;

        close(sockfd_);
        sockfd_ = -1;

        return false;
    }


    if(connect(
            sockfd_,
            reinterpret_cast<sockaddr*>(
                &server_addr),
            sizeof(server_addr)) < 0)
    {
        LOG_ERROR()
            << "connect() failed for "
            << host_
            << ':'
            << port_;

        close(sockfd_);
        sockfd_ = -1;

        return false;
    }


    return true;
}



bool RpcChannel::SendAll(
    int sockfd,
    const void* buffer,
    size_t length)
{
    const char* data =
        static_cast<const char*>(buffer);


    size_t sent = 0;


    while(sent < length)
    {
        ssize_t n =
            send(
                sockfd,
                data + sent,
                length - sent,
                0
            );


        if(n <= 0)
        {
            return false;
        }


        sent +=
            static_cast<size_t>(n);
    }


    return true;
}



bool RpcChannel::RecvAll(
    int sockfd,
    void* buffer,
    size_t length)
{
    char* data =
        static_cast<char*>(buffer);


    size_t received = 0;


    while(received < length)
    {
        ssize_t n =
            recv(
                sockfd,
                data + received,
                length - received,
                0
            );


        if(n <= 0)
        {
            return false;
        }


        received +=
            static_cast<size_t>(n);
    }


    return true;
}

bool RpcChannel::Call(
    const std::string& service_name,
    const std::string& method_name,
    const google::protobuf::Message& request,
    google::protobuf::Message& response)
{
    // 1. 确保连接存在
    if(sockfd_ < 0)
    {
        if(!Connect())
        {
            return false;
        }
    }


    // 2. 序列化请求参数
    std::string args_data;

    if(!request.SerializeToString(&args_data))
    {
        LOG_ERROR()
            << "Failed to serialize RPC request";

        return false;
    }


    // 3. 构造 RPC Header

    static std::atomic<uint64_t> next_request_id{1};

    uint64_t request_id =
        next_request_id.fetch_add(
            1,
            std::memory_order_relaxed
        );


    minirpc::RpcHeader header;

    header.set_service_name(
        service_name
    );

    header.set_method_name(
        method_name
    );

    header.set_request_id(
        request_id
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
    LOG_ERROR()
        << "Failed to encode RPC request";

    return false;
}



    // 5. 发送请求

    if(!SendAll(
            sockfd_,
            packet.data(),
            packet.size()))
    {
        LOG_ERROR() << "RPC send failed";


        close(sockfd_);
        sockfd_ = -1;

        return false;
    }



// =================================
// 6. 使用 RpcBuffer 接收完整 Response
// =================================

char temp_buffer[8192];


auto ReadMoreData = [&]() -> bool
{
    ssize_t n = recv(
        sockfd_,
        temp_buffer,
        sizeof(temp_buffer),
        0
    );

    if (n <= 0)
    {
        LOG_ERROR()
            << "RPC receive failed or timed out after "
            << timeout_ms_
            << " ms";

        close(sockfd_);
        sockfd_ = -1;

        receive_buffer_.Clear();

        return false;
    }


    receive_buffer_.Append(
        temp_buffer,
        static_cast<std::size_t>(n)
    );

    return true;
};


// =================================
// 7. 等待 response header size
// =================================

while (receive_buffer_.ReadableBytes()
       < sizeof(uint32_t))
{
    if (!ReadMoreData())
    {
        return false;
    }
}


uint32_t network_response_header_size = 0;


std::memcpy(
    &network_response_header_size,
    receive_buffer_.Peek(),
    sizeof(network_response_header_size)
);


uint32_t response_header_size =
    ntohl(network_response_header_size);


if (response_header_size == 0 ||
    response_header_size
        > minirpc::MAX_HEADER_SIZE)
{
    LOG_ERROR()
        << "Invalid RPC response header size";

    close(sockfd_);
    sockfd_ = -1;

    receive_buffer_.Clear();

    return false;
}


// =================================
// 8. 等待完整 Response Header
// =================================

std::size_t header_end =
    sizeof(uint32_t)
    +
    response_header_size;


while (receive_buffer_.ReadableBytes()
       < header_end)
{
    if (!ReadMoreData())
    {
        return false;
    }
}


minirpc::RpcResponseHeader framing_header;


if (!framing_header.ParseFromArray(
        receive_buffer_.Peek()
            + sizeof(uint32_t),
        static_cast<int>(
            response_header_size)))
{
    LOG_ERROR()
        << "Failed to parse response header";

    close(sockfd_);
    sockfd_ = -1;

    receive_buffer_.Clear();

    return false;
}


if (framing_header.payload_size()
    > minirpc::MAX_PAYLOAD_SIZE)
{
    LOG_ERROR()
        << "RPC response payload too large";

    close(sockfd_);
    sockfd_ = -1;

    receive_buffer_.Clear();

    return false;
}


// =================================
// 9. 等待完整 Response Frame
// =================================

std::size_t response_packet_size =
    sizeof(uint32_t)
    +
    response_header_size
    +
    framing_header.payload_size();


while (receive_buffer_.ReadableBytes()
       < response_packet_size)
{
    if (!ReadMoreData())
    {
        return false;
    }
}


// 只消费当前 Response。
// 剩余数据继续留在 Buffer 中。
std::string response_packet =
    receive_buffer_.RetrieveAsString(
        response_packet_size
    );


// =================================
// 10. RpcCodec 解码
// =================================

minirpc::RpcResponseHeader response_header;

std::string response_data;


if (!RpcCodec::DecodeResponse(
        response_packet,
        response_header,
        response_data))
{
    LOG_ERROR()
        << "Failed to decode RPC response";

    close(sockfd_);
    sockfd_ = -1;

    receive_buffer_.Clear();

    return false;
}


if (response_header.payload_size()
    != response_data.size())
{
    LOG_ERROR()
        << "RPC response payload size mismatch";

    close(sockfd_);
    sockfd_ = -1;

    receive_buffer_.Clear();

    return false;
}


if(response_header.payload_size()
    != response_data.size())
{
    LOG_ERROR()
        << "RPC response payload size mismatch";

    close(sockfd_);
    sockfd_ = -1;

    return false;
}



    // 8. 校验 request_id

    if(response_header.request_id()
        != request_id)
    {
        LOG_ERROR()
            << "RPC request_id mismatch";


        return false;
    }



    // 9. RPC错误处理

    if(response_header.error_code()
        != minirpc::RPC_OK)
    {
        LOG_ERROR()
            << "RPC error: "
            << response_header.error_code()
            << " - "
            << response_header.error_message();


        return false;
    }



    



    // 11. 解析响应

    if(!response.ParseFromString(
            response_data))
    {
        LOG_ERROR()
            << "Failed to parse RPC response";

        return false;
    }


    return true;
}

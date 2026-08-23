#include <minirpc/rpc_channel.h>
#include <minirpc/rpc_config.h>

#include <atomic>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <string>
#include <utility>

#include "rpc_header.pb.h"

#include <sys/time.h>


#include <cerrno>
#include <fcntl.h>
#include <poll.h>


RpcChannel::RpcChannel(
    std::string host,
    uint16_t port,
    int timeout_ms)
    : host_(std::move(host)),
      port_(port),
      timeout_ms_(timeout_ms)
{
}

bool RpcChannel::SendAll(
    int sockfd,
    const void* buffer,
    size_t length)
{
    const char* data =
        static_cast<const char*>(buffer);

    size_t sent = 0;

    while (sent < length)
    {
        ssize_t n = send(
            sockfd,
            data + sent,
            length - sent,
            0
        );

        if (n <= 0)
        {
            return false;
        }

        sent += static_cast<size_t>(n);
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

    while (received < length)
    {
        ssize_t n = recv(
            sockfd,
            data + received,
            length - received,
            0
        );

        if (n <= 0)
        {
            return false;
        }

        received += static_cast<size_t>(n);
    }

    return true;
}

bool RpcChannel::Call(
    const std::string& service_name,
    const std::string& method_name,
    const google::protobuf::Message& request,
    google::protobuf::Message& response)
{
    // 1. 序列化 RPC 参数
    std::string args_data;

    if (!request.SerializeToString(&args_data))
    {
        std::cerr
            << "Failed to serialize RPC request"
            << std::endl;

        return false;
    }

    // 2. 构造 RPC Header
    minirpc::RpcHeader header;

static std::atomic<uint64_t> next_request_id{1};//用atomic是为了后面做多线程客户端时仍然安全

uint64_t request_id =
    next_request_id.fetch_add(
        1,
        std::memory_order_relaxed
    );

header.set_service_name(service_name);
header.set_method_name(method_name);
header.set_request_id(request_id);
// RPC协议校验信息
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
    std::string header_data;

    if (!header.SerializeToString(
            &header_data))
    {
        std::cerr
            << "Failed to serialize RPC header"
            << std::endl;

        return false;
    }

    // 3. 构造完整 RPC Packet
    uint32_t header_size =
        static_cast<uint32_t>(
            header_data.size()
        );

    uint32_t network_header_size =
        htonl(header_size);

    std::string packet;

    packet.append(
        reinterpret_cast<const char*>(
            &network_header_size),
        sizeof(network_header_size)
    );

    packet.append(header_data);
    packet.append(args_data);

    // 4. 创建 Socket
    int sockfd =
        socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0)
    {
        std::cerr << "socket() failed"
                  << std::endl;

        return false;
    }

    timeval timeout{};

timeout.tv_sec =
    timeout_ms_ / 1000;

timeout.tv_usec =
    (timeout_ms_ % 1000) * 1000;

if (setsockopt(
        sockfd,
        SOL_SOCKET,
        SO_RCVTIMEO,
        &timeout,
        sizeof(timeout)) < 0)
{
    std::cerr
        << "Failed to set receive timeout"
        << std::endl;

    close(sockfd);
    return false;
}

if (setsockopt(
        sockfd,
        SOL_SOCKET,
        SO_SNDTIMEO,
        &timeout,
        sizeof(timeout)) < 0)
{
    std::cerr
        << "Failed to set send timeout"
        << std::endl;

    close(sockfd);
    return false;
}

    sockaddr_in server_addr{};

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port_);

    if (inet_pton(
            AF_INET,
            host_.c_str(),
            &server_addr.sin_addr) <= 0)
    {
        std::cerr
            << "Invalid server address"
            << std::endl;

        close(sockfd);
        return false;
    }

    // 5. 连接 Server
// =================================
// Connect timeout
// =================================

// 保存原来的 socket flags
int old_flags = fcntl(
    sockfd,
    F_GETFL,
    0
);

if (old_flags < 0)
{
    std::cerr
        << "fcntl(F_GETFL) failed"
        << std::endl;

    close(sockfd);
    return false;
}

// 临时设置为 non-blocking
if (fcntl(
        sockfd,
        F_SETFL,
        old_flags | O_NONBLOCK) < 0)
{
    std::cerr
        << "fcntl(F_SETFL) failed"
        << std::endl;

    close(sockfd);
    return false;
}

int connect_result = connect(
    sockfd,
    reinterpret_cast<sockaddr*>(
        &server_addr),
    sizeof(server_addr)
);

if (connect_result < 0)
{
    // 对 non-blocking connect 来说，
    // EINPROGRESS 表示连接正在建立，并不是失败。
    if (errno != EINPROGRESS)
    {
        std::cerr
            << "connect() failed"
            << std::endl;

        close(sockfd);
        return false;
    }

    pollfd pfd{};

    pfd.fd = sockfd;
    pfd.events = POLLOUT;

    int poll_result = poll(
        &pfd,
        1,
        timeout_ms_
    );

    if (poll_result == 0)
    {
        std::cerr
            << "RPC connect timed out after "
            << timeout_ms_
            << " ms"
            << std::endl;

        close(sockfd);
        return false;
    }

    if (poll_result < 0)
    {
        std::cerr
            << "poll() failed while connecting"
            << std::endl;

        close(sockfd);
        return false;
    }

    // poll 可写不一定代表连接成功，
    // 必须读取 SO_ERROR 再确认。

//  poll()
//   ↓
// socket 可写  
//   ↓
// getsockopt(SO_ERROR)
//   ├─ 0       → connect 成功
//   └─ 非0     → connect 失败
    int socket_error = 0;
    socklen_t error_length =
        sizeof(socket_error);

    if (getsockopt(
            sockfd,
            SOL_SOCKET,
            SO_ERROR,
            &socket_error,
            &error_length) < 0)
    {
        std::cerr
            << "getsockopt(SO_ERROR) failed"
            << std::endl;

        close(sockfd);
        return false;
    }

    if (socket_error != 0)
    {
        std::cerr
            << "connect() failed with error "
            << socket_error
            << std::endl;

        close(sockfd);
        return false;
    }
}

// 连接成功后恢复 blocking 模式
// 现有的
// SendAll()
// RecvAll()
// 都是按照 blocking socket 写的。所以我们的设计是：
// 连接阶段
//     ↓
// non-blocking + poll
//     ↓
// 连接完成
//     ↓
// 恢复 blocking
//     ↓
// SO_SNDTIMEO / SO_RCVTIMEO
//     ↓
// SendAll / RecvAll
//这样不用一下把整个网络层都重写成非阻塞状态机。
if (fcntl(
        sockfd,
        F_SETFL,
        old_flags) < 0)
{
    std::cerr
        << "Failed to restore blocking mode"
        << std::endl;

    close(sockfd);
    return false;
}

    // 6. 发送 RPC 请求
    if (!SendAll(
            sockfd,
            packet.data(),
            packet.size()))
    {
        std::cerr << "send() failed"
                  << std::endl;

        close(sockfd);
        return false;
    }

    // =================================
// 7. 接收 Response Header 长度
// =================================

uint32_t network_response_header_size = 0;

if (!RecvAll(
        sockfd,
        &network_response_header_size,
        sizeof(network_response_header_size)))
{
    std::cerr
        << "RPC receive failed or timed out after "
        << timeout_ms_
        << " ms"
        << std::endl;

    close(sockfd);
    return false;
}

uint32_t response_header_size =
    ntohl(network_response_header_size);
    std::cerr
    << "[DEBUG] response_header_size = "
    << response_header_size
    << std::endl;


// =================================
// 8. 接收 Response Header
// =================================

std::string response_header_data(
    response_header_size,
    '\0'
);

if (!RecvAll(
        sockfd,
        response_header_data.data(),
        response_header_size))
{
    std::cerr
        << "Failed to receive response header"
        << std::endl;

    close(sockfd);
    return false;
}

minirpc::RpcResponseHeader response_header;

if (!response_header.ParseFromString(
        response_header_data))
{
    std::cerr
        << "Failed to parse response header"
        << std::endl;

    std::cerr
    << "[DEBUG] request_id = "
    << response_header.request_id()
    << ", error_code = "
    << response_header.error_code()
    << ", error_message = "
    << response_header.error_message()
    << ", payload_size = "
    << response_header.payload_size()
    << std::endl;

    close(sockfd);
    return false;
}


// =================================
// 9. 校验 request_id
// =================================

if (response_header.request_id()
    != request_id)
{
    std::cerr
        << "RPC request_id mismatch"
        << std::endl;

    close(sockfd);
    return false;
}


// =================================
// 10. 检查 RPC 错误
// =================================

if (response_header.error_code()
    != minirpc::RPC_OK)
{
    std::cerr
        << "RPC error: "
        << response_header.error_code()
        << " - "
        << response_header.error_message()
        << std::endl;

    close(sockfd);
    return false;
}


// =================================
// 11. 接收业务 Response
// =================================

uint32_t response_size =
    response_header.payload_size();

std::string response_data(
    response_size,
    '\0'
);

if (response_size > 0)
{
    if (!RecvAll(
            sockfd,
            response_data.data(),
            response_size))
    {
        std::cerr
            << "Failed to receive RPC response"
            << std::endl;

        close(sockfd);
        return false;
    }
}

close(sockfd);


// =================================
// 12. 反序列化业务 Response
// =================================

if (!response.ParseFromString(
        response_data))
{
    std::cerr
        << "Failed to parse RPC response"
        << std::endl;

    return false;
}

return true;

    return true;
}

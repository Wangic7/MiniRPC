#include <minirpc/rpc_config.h>
#include <minirpc/rpc_server.h>

#include <arpa/inet.h>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>
#include <sys/time.h>

#include "rpc_header.pb.h"

#include <minirpc/rpc_codec.h>

#include <minirpc/rpc_buffer.h>
#include <cstring>

#include <minirpc/rpc_connection.h>

#include <minirpc/epoller.h>

RpcServer::RpcServer(
    std::size_t worker_count,
    std::size_t max_queue_size)
    : thread_pool_(
          worker_count,
          max_queue_size)
{
}

void RpcServer::RegisterMethod(
    const std::string& service_name,
    const std::string& method_name,
    Handler handler)
{
    dispatcher_.Register(
        service_name,
        method_name,
        std::move(handler)
    );
}

bool RpcServer::RecvAll(
    int sockfd,
    void* buffer,
    size_t length)
{
    char* data = static_cast<char*>(buffer);
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

bool RpcServer::SendAll(
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
            MSG_NOSIGNAL//客户端提前断开时，Server 不会因为 SIGPIPE 被直接杀掉
        );

        if (n <= 0)
        {
            return false;
        }

        sent += static_cast<size_t>(n);
    }

    return true;
}

bool RpcServer::SendErrorResponse(
    int client_fd,
    uint64_t request_id,
    minirpc::RpcErrorCode error_code,
    const std::string& message)
{
    minirpc::RpcResponseHeader response_header;

    response_header.set_request_id(
        request_id
    );

    response_header.set_error_code(
        error_code
    );

    response_header.set_error_message(
        message
    );

    response_header.set_payload_size(
        0
    );

    response_header.set_magic(
        minirpc::RPC_MAGIC
    );

    response_header.set_version(
        minirpc::RPC_VERSION
    );


    std::string response_packet;

    if (!RpcCodec::EncodeResponse(
            response_header,
            "",
            response_packet))
    {
        return false;
    }


    return SendAll(
        client_fd,
        response_packet.data(),
        response_packet.size()
    );
}

bool RpcServer::HandleClient(
    RpcConnection& connection)
{
    int client_fd =
        connection.Fd();

    RpcBuffer& receive_buffer =
        connection.InputBuffer();

    char temp_buffer[8192];

    auto ReadMoreData = [&]() -> bool
    {
        ssize_t n = recv(
            client_fd,
            temp_buffer,
            sizeof(temp_buffer),
            0
        );

        if (n <= 0)
        {
            return false;
        }

        receive_buffer.Append(
            temp_buffer,
            static_cast<std::size_t>(n)
        );

        return true;
    };


    while (true)
    {
        // =================================
        // 1. 至少获得 4 字节 header_size
        // =================================

        while (receive_buffer.ReadableBytes()
               < sizeof(uint32_t))
        {
            if (!ReadMoreData())
            {
                return false;
            }
        }


        uint32_t network_header_size = 0;

        std::memcpy(
            &network_header_size,
            receive_buffer.Peek(),
            sizeof(network_header_size)
        );


        uint32_t header_size =
            ntohl(network_header_size);


        if (header_size == 0 ||
            header_size > minirpc::MAX_HEADER_SIZE)
        {
            SendErrorResponse(
                client_fd,
                0,
                minirpc::RPC_BAD_REQUEST,
                "Invalid header size"
            );

            return false;
        }


        // =================================
        // 2. 等待完整 RpcHeader
        // =================================

        std::size_t header_end =
            sizeof(uint32_t)
            +
            header_size;


        while (receive_buffer.ReadableBytes()
               < header_end)
        {
            if (!ReadMoreData())
            {
                return false;
            }
        }


        minirpc::RpcHeader framing_header;


        if (!framing_header.ParseFromArray(
                receive_buffer.Peek()
                    + sizeof(uint32_t),
                static_cast<int>(header_size)))
        {
            SendErrorResponse(
                client_fd,
                0,
                minirpc::RPC_BAD_REQUEST,
                "Failed to parse RPC header"
            );

            return false;
        }


        if (framing_header.args_size()
            > minirpc::MAX_PAYLOAD_SIZE)
        {
            SendErrorResponse(
                client_fd,
                framing_header.request_id(),
                minirpc::RPC_BAD_REQUEST,
                "Payload too large"
            );

            return false;
        }


        // =================================
        // 3. 等待完整 RPC Frame
        // =================================

        std::size_t packet_size =
            sizeof(uint32_t)
            +
            header_size
            +
            framing_header.args_size();


        while (receive_buffer.ReadableBytes()
               < packet_size)
        {
            if (!ReadMoreData())
            {
                return false;
            }
        }


        // 只消费当前 RPC。
        // 如果 buffer 中还有下一个 RPC，
        // 剩余数据继续保留。
        std::string request_packet =
            receive_buffer.RetrieveAsString(
                packet_size
            );


        // =================================
        // 4. RpcCodec 解码
        // =================================

        minirpc::RpcHeader header;

        std::string args_data;


        if (!RpcCodec::DecodeRequest(
                request_packet,
                header,
                args_data))
        {
            SendErrorResponse(
                client_fd,
                framing_header.request_id(),
                minirpc::RPC_BAD_REQUEST,
                "Failed to decode RPC request"
            );

            return false;
        }


        if (header.args_size()
            != args_data.size())
        {
            SendErrorResponse(
                client_fd,
                header.request_id(),
                minirpc::RPC_BAD_REQUEST,
                "RPC payload size mismatch"
            );

            return false;
        }


        // =================================
        // 5. RPC 协议检查
        // =================================

        if (header.magic()
            != minirpc::RPC_MAGIC)
        {
            SendErrorResponse(
                client_fd,
                header.request_id(),
                minirpc::RPC_BAD_REQUEST,
                "Invalid RPC magic"
            );

            return false;
        }


        if (header.version()
            != minirpc::RPC_VERSION)
        {
            SendErrorResponse(
                client_fd,
                header.request_id(),
                minirpc::RPC_BAD_REQUEST,
                "Unsupported RPC version"
            );

            return false;
        }


        // =================================
        // 6. Dispatcher
        // =================================

        std::string response_data;

        minirpc::RpcErrorCode error_code =
            minirpc::RPC_OK;

        std::string error_message;


        if (!dispatcher_.HasMethod(
                header.service_name(),
                header.method_name()))
        {
            error_code =
                minirpc::RPC_METHOD_NOT_FOUND;

            error_message =
                "RPC method not found";

            response_data.clear();
        }
        else if (!dispatcher_.Dispatch(
                     header.service_name(),
                     header.method_name(),
                     args_data,
                     response_data))
        {
            error_code =
                minirpc::RPC_BAD_REQUEST;

            error_message =
                "RPC request execution failed";

            response_data.clear();
        }


        // =================================
        // 7. 构造 Response
        // =================================

        minirpc::RpcResponseHeader response_header;


        response_header.set_request_id(
            header.request_id()
        );

        response_header.set_error_code(
            error_code
        );

        response_header.set_error_message(
            error_message
        );

        response_header.set_payload_size(
            static_cast<uint32_t>(
                response_data.size()
            )
        );

        response_header.set_magic(
            minirpc::RPC_MAGIC
        );

        response_header.set_version(
            minirpc::RPC_VERSION
        );


        std::string response_packet;


        if (!RpcCodec::EncodeResponse(
                response_header,
                response_data,
                response_packet))
        {
            return false;
        }


        if (!SendAll(
                client_fd,
                response_packet.data(),
                response_packet.size()))
        {
            return false;
        }
    }
}

bool RpcServer::RejectOverloadedClient(
    int client_fd)
{
    // 拒绝路径不能长时间阻塞 accept 线程
    timeval timeout{};

    timeout.tv_sec = 0;
    timeout.tv_usec = 200000;  // 200 ms

    setsockopt(
        client_fd,
        SOL_SOCKET,
        SO_RCVTIMEO,
        &timeout,
        sizeof(timeout)
    );

    // =================================
    // 读取 Request Header 长度
    // =================================

    uint32_t network_header_size = 0;

    if (!RecvAll(
            client_fd,
            &network_header_size,
            sizeof(network_header_size)))
    {
        return false;
    }

    uint32_t header_size =
        ntohl(network_header_size);

    // 防止异常客户端要求分配巨大内存
    constexpr uint32_t kMaxHeaderSize =
        64 * 1024;

    if (header_size == 0 ||
        header_size > kMaxHeaderSize)
    {
        return false;
    }

    // =================================
    // 读取并解析 RpcHeader
    // =================================

    std::string header_data(
        header_size,
        '\0'
    );

    if (!RecvAll(
            client_fd,
            header_data.data(),
            header_size))
    {
        return false;
    }

    

    minirpc::RpcHeader header;

if (!header.ParseFromString(header_data))
{
    std::cerr
        << "RpcHeader Parse failed, size="
        << header_data.size()
        << std::endl;

    return false;
}


    // =================================
    // 把业务参数读掉，但不执行
    // =================================

    constexpr uint32_t kMaxArgsSize =
        1024 * 1024;

    if (header.args_size() >
        kMaxArgsSize)
    {
        return false;
    }

    std::string discarded_args(
        header.args_size(),
        '\0'
    );

    if (!discarded_args.empty())
    {
        if (!RecvAll(
                client_fd,
                discarded_args.data(),
                discarded_args.size()))
        {
            return false;
        }
    }

    // =================================
    // 构造 SERVER_BUSY Response
    // =================================

    minirpc::RpcResponseHeader response_header;

    response_header.set_request_id(
        header.request_id()
    );

    response_header.set_error_code(
        minirpc::RPC_SERVER_BUSY
    );

    response_header.set_error_message(
        "Server is busy"
    );

    response_header.set_payload_size(0);

    std::string response_header_data;

    if (!response_header.SerializeToString(
            &response_header_data))
    {
        return false;
    }

    // =================================
    // 发送 Response Header 长度
    // =================================

    uint32_t response_header_size =
        static_cast<uint32_t>(
            response_header_data.size()
        );

    uint32_t network_response_header_size =
        htonl(response_header_size);

    if (!SendAll(
            client_fd,
            &network_response_header_size,
            sizeof(network_response_header_size)))
    {
        return false;
    }

    // =================================
    // 发送 Response Header
    // =================================

    if (!SendAll(
            client_fd,
            response_header_data.data(),
            response_header_data.size()))
    {
        return false;
    }

    return true;
}

bool RpcServer::Start(uint16_t port)
{
    int server_fd =
        socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd < 0)
    {
        std::cerr << "socket() failed"
                  << std::endl;

        return false;
    }

    int opt = 1;

    if (setsockopt(
            server_fd,
            SOL_SOCKET,
            SO_REUSEADDR,
            &opt,
            sizeof(opt)) < 0)
    {
        std::cerr << "setsockopt() failed"
                  << std::endl;

        close(server_fd);
        return false;
    }

    sockaddr_in server_addr{};

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr =
        htonl(INADDR_ANY);

    if (bind(
            server_fd,
            reinterpret_cast<sockaddr*>(
                &server_addr),
            sizeof(server_addr)) < 0)
    {
        std::cerr << "bind() failed"
                  << std::endl;

        close(server_fd);
        return false;
    }

    if (listen(server_fd, 5) < 0)
    {
        std::cerr << "listen() failed"
                  << std::endl;

        close(server_fd);
        return false;
    }

    std::cout
        << "MiniRPC server listening on port "
        << port
        << "..."
        << std::endl;

     Epoller epoller;

    if (!epoller.Add(
            server_fd,
            EPOLLIN))
    {
        std::cerr
            << "Failed to add server socket to epoll"
            << std::endl;

        close(server_fd);

        return false;
    }


    while (true)
    {
        int ready =
            epoller.Wait(-1);

        if (ready < 0)
        {
            std::cerr
                << "epoll_wait() failed"
                << std::endl;

            break;
        }


        for (int i = 0;
             i < ready;
             ++i)
        {
            const epoll_event& event =
                epoller.Event(
                    static_cast<std::size_t>(i)
                );


            if (event.data.fd != server_fd)
            {
                continue;
            }


            if (!(event.events & EPOLLIN))
            {
                continue;
            }


            sockaddr_in client_addr{};

            socklen_t client_len =
                sizeof(client_addr);


            int client_fd =
                accept(
                    server_fd,
                    reinterpret_cast<sockaddr*>(
                        &client_addr),
                    &client_len
                );


            if (client_fd < 0)
            {
                std::cerr
                    << "accept() failed"
                    << std::endl;

                continue;
            }


            bool submitted =
                thread_pool_.Submit(
                    [this, client_fd]()
                    {
                        RpcConnection connection(
                            client_fd
                        );


                        if (!HandleClient(
                                connection))
                        {
                            std::cout
                                << "Client closed connection."
                                << std::endl;
                        }


                        std::cout
                            << "Client disconnected."
                            << std::endl;
                    }
                );


            if (!submitted)
            {
                std::cerr
                    << "Server overloaded: "
                    << "task queue is full. "
                    << "Rejecting client."
                    << std::endl;


                if (!RejectOverloadedClient(
                        client_fd))
                {
                    std::cerr
                        << "Failed to send SERVER_BUSY response."
                        << std::endl;
                }


                close(client_fd);
            }
        }
    }


    close(server_fd);

    return false;
}

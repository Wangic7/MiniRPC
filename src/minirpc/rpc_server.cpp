#include <minirpc/rpc_config.h>
#include <minirpc/rpc_server.h>

#include <arpa/inet.h>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

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

bool RpcServer::SendErrorResponse(
    RpcConnection& connection,
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


    connection.OutputBuffer().Append(
        response_packet.data(),
        response_packet.size()
    );

    return true;
}

RpcServer::FrameStatus
RpcServer::TryExtractRequestPacket(
    RpcConnection& connection,
    std::string& request_packet)
{
    RpcBuffer& receive_buffer =
        connection.InputBuffer();




    // 1. 还没有收到完整的 header_size
    if (receive_buffer.ReadableBytes()
        < sizeof(uint32_t))
    {
        return FrameStatus::NeedMoreData;
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
        header_size >
            minirpc::MAX_HEADER_SIZE)
    {
        connection.MarkCloseAfterWrite();

        SendErrorResponse(
            connection,
            0,
            minirpc::RPC_BAD_REQUEST,
            "Invalid header size"
        );

        return FrameStatus::Error;
    }


    // 2. RpcHeader 还没有完整到达
    std::size_t header_end =
        sizeof(uint32_t)
        +
        header_size;

    if (receive_buffer.ReadableBytes()
        < header_end)
    {
        return FrameStatus::NeedMoreData;
    }


    minirpc::RpcHeader framing_header;

    if (!framing_header.ParseFromArray(
            receive_buffer.Peek()
                + sizeof(uint32_t),
            static_cast<int>(
                header_size)))
    {
        connection.MarkCloseAfterWrite();

        SendErrorResponse(
            connection,
            0,
            minirpc::RPC_BAD_REQUEST,
            "Failed to parse RPC header"
        );

        return FrameStatus::Error;
    }


    if (framing_header.args_size()
        > minirpc::MAX_PAYLOAD_SIZE)
    {
        connection.MarkCloseAfterWrite();

        SendErrorResponse(
            connection,
            framing_header.request_id(),
            minirpc::RPC_BAD_REQUEST,
            "Payload too large"
        );

        return FrameStatus::Error;
    }


    // 3. 整个 RPC Frame 还没有完整到达
    std::size_t packet_size =
        sizeof(uint32_t)
        +
        header_size
        +
        framing_header.args_size();

    if (receive_buffer.ReadableBytes()
        < packet_size)
    {
        return FrameStatus::NeedMoreData;
    }


    // 4. 一条完整 RPC 已经到达
    request_packet =
        receive_buffer.RetrieveAsString(
            packet_size
        );

    return FrameStatus::Ready;
}

bool RpcServer::HandleWriteEvent(
    int client_fd
)
{
    auto it =
        connections_.find(
            client_fd
        );

    if (it == connections_.end())
    {
        return false;
    }


    RpcConnection& connection =
        *(it->second);


    if (!connection.FlushOutput())
    {
        return false;
    }

    return true;
}


bool RpcServer::UpdateConnectionEvents(
    RpcConnection& connection
)
{
    uint32_t events = 0;

    if (!connection.IsPeerReadClosed() &&
        !connection.ShouldCloseAfterWrite())
    {
        events |= EPOLLIN | EPOLLRDHUP;
    }

    if (connection.HasOutput())
    {
        events |= EPOLLOUT;
    }

    return epoller_.Modify(
        connection.Fd(),
        events
    );
}


void RpcServer::CloseConnection(
    int client_fd)
{
    auto it = connections_.find(client_fd);

    if (it == connections_.end())
    {
        return;
    }

    epoller_.Delete(client_fd);

    // RpcConnection owns the fd. Erasing it closes the descriptor exactly once.
    connections_.erase(it);
}


bool RpcServer::HandleClientEvent(
    int client_fd,
    uint32_t events
)
{
    auto it =
        connections_.find(
            client_fd
        );

    if (it == connections_.end())
    {
        return false;
    }


    RpcConnection& connection =
        *(it->second);

    if (events & EPOLLERR)
    {
        return false;
    }

    if (events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP))
    {
        if (!HandleClient(connection))
        {
            return false;
        }
    }

    if (events & (EPOLLRDHUP | EPOLLHUP))
    {
        connection.MarkPeerReadClosed();
    }

    if ((events & EPOLLOUT) &&
        connection.HasOutput())
    {
        if (!HandleWriteEvent(client_fd))
        {
            return false;
        }
    }

    if ((connection.IsPeerReadClosed() ||
         connection.ShouldCloseAfterWrite()) &&
        !connection.HasOutput())
    {
        return false;
    }

    return UpdateConnectionEvents(connection);
}

bool RpcServer::ProcessRequest(
    RpcConnection& connection,
    const std::string& request_packet
)
{
    minirpc::RpcHeader header;

    std::string args_data;


    if (!RpcCodec::DecodeRequest(
            request_packet,
            header,
            args_data))
    {
        connection.MarkCloseAfterWrite();

        return SendErrorResponse(
            connection,
            0,
            minirpc::RPC_BAD_REQUEST,
            "Failed to decode RPC request"
        );
    }


    if (header.args_size()
        != args_data.size())
    {
        connection.MarkCloseAfterWrite();

        return SendErrorResponse(
            connection,
            header.request_id(),
            minirpc::RPC_BAD_REQUEST,
            "RPC payload size mismatch"
        );
    }


    if (header.magic()
        != minirpc::RPC_MAGIC)
    {
        connection.MarkCloseAfterWrite();

        return SendErrorResponse(
            connection,
            header.request_id(),
            minirpc::RPC_BAD_REQUEST,
            "Invalid RPC magic"
        );
    }


    if (header.version()
        != minirpc::RPC_VERSION)
    {
        connection.MarkCloseAfterWrite();

        return SendErrorResponse(
            connection,
            header.request_id(),
            minirpc::RPC_BAD_REQUEST,
            "Unsupported RPC version"
        );
    }


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
    }


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


    connection.OutputBuffer().Append(
        response_packet.data(),
        response_packet.size()
    );

    return true;
}

bool RpcServer::HandleClient(
    RpcConnection& connection)
{
    RpcBuffer& receive_buffer =
        connection.InputBuffer();

    // Drain a level-triggered socket until it would block. If EOF follows
    // readable bytes, parse those bytes before deciding whether to close.
    while (true)
    {
        RpcReadStatus read_status =
            connection.ReadOnce();

        if (read_status == RpcReadStatus::Data)
        {
            continue;
        }

        if (read_status == RpcReadStatus::WouldBlock)
        {
            break;
        }

        if (read_status == RpcReadStatus::PeerClosed)
        {
            connection.MarkPeerReadClosed();
            break;
        }

        return false;
    }

    while (true)
    {
        std::string request_packet;

        FrameStatus frame_status =
            TryExtractRequestPacket(
                connection,
                request_packet
            );

        if (frame_status == FrameStatus::Error)
        {
            // TryExtractRequestPacket queued the protocol error and marked
            // the connection to close once the output has been flushed.
            return true;
        }

        if (frame_status == FrameStatus::NeedMoreData)
        {
            if (connection.IsPeerReadClosed() &&
                receive_buffer.ReadableBytes() > 0)
            {
                connection.MarkCloseAfterWrite();

                return SendErrorResponse(
                    connection,
                    0,
                    minirpc::RPC_BAD_REQUEST,
                    "Truncated RPC request"
                );
            }

            return true;
        }

        if (!ProcessRequest(
                connection,
                request_packet))
        {
            return false;
        }

        if (connection.ShouldCloseAfterWrite())
        {
            return true;
        }
    }
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

    if (!epoller_.Add(
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
            epoller_.Wait(-1);

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
                epoller_.Event(
                    static_cast<std::size_t>(i)
                );

            if (event.data.fd != server_fd)
            {
                if (!HandleClientEvent(
                        event.data.fd,
                        event.events))
                {
                    CloseConnection(event.data.fd);
                }

                continue;
            }

            if (event.events & (EPOLLERR | EPOLLHUP))
            {
                std::cerr
                    << "Server socket epoll error"
                    << std::endl;

                epoller_.Delete(server_fd);
                close(server_fd);

                while (!connections_.empty())
                {
                    CloseConnection(
                        connections_.begin()->first
                    );
                }

                return false;
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


           RpcConnection connection(
    client_fd
);


            if (!connection.SetNonBlocking())
            {
                std::cerr
                    << "Failed to set non-blocking"
                    << std::endl;

                // The local RpcConnection owns and closes client_fd.
                continue;
            }


            if (!epoller_.Add(
                    client_fd,
                    EPOLLIN | EPOLLRDHUP))
            {
                std::cerr
                    << "Failed to add client fd to epoll"
                    << std::endl;

                // The local RpcConnection owns and closes client_fd.
                continue;
            }


connections_.emplace(
    client_fd,
    std::make_unique<RpcConnection>(
        std::move(connection)
    )
);


std::cout
    << "Client connected fd="
    << client_fd
    << std::endl;
    }
    }


    epoller_.Delete(server_fd);
    close(server_fd);

    while (!connections_.empty())
    {
        CloseConnection(
            connections_.begin()->first
        );
    }

    return false;
}

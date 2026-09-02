#include <minirpc/rpc_config.h>
#include <minirpc/rpc_server.h>

#include <arpa/inet.h>
#include <cerrno>
#include <stdexcept>
#include <string>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

#include "rpc_header.pb.h"

#include <minirpc/rpc_codec.h>

#include <minirpc/rpc_buffer.h>
#include <cstring>

#include <minirpc/rpc_connection.h>

#include <minirpc/epoller.h>
#include <minirpc/logger.h>


class RpcEventFd
{
public:
    RpcEventFd()
        : fd_(eventfd(
              0,
              EFD_NONBLOCK | EFD_CLOEXEC))
    {
        if (fd_ < 0)
        {
            throw std::runtime_error(
                "eventfd() failed"
            );
        }
    }

    ~RpcEventFd()
    {
        if (fd_ >= 0)
        {
            close(fd_);
            fd_ = -1;
        }
    }

    RpcEventFd(const RpcEventFd&) = delete;
    RpcEventFd& operator=(const RpcEventFd&) = delete;

    int Fd() const
    {
        return fd_;
    }

    bool Notify() const
    {
        uint64_t value = 1;

        while (true)
        {
            ssize_t written = write(
                fd_,
                &value,
                sizeof(value)
            );

            if (written == static_cast<ssize_t>(sizeof(value)))
            {
                return true;
            }

            if (written < 0 && errno == EINTR)
            {
                continue;
            }

            // EAGAIN means the counter is already saturated and readable,
            // so the EventLoop is guaranteed to receive a wakeup.
            return written < 0 && errno == EAGAIN;
        }
    }

    bool Drain() const
    {
        uint64_t value = 0;

        while (true)
        {
            ssize_t received = read(
                fd_,
                &value,
                sizeof(value)
            );

            if (received == static_cast<ssize_t>(sizeof(value)))
            {
                return true;
            }

            if (received < 0 && errno == EINTR)
            {
                continue;
            }

            // A coalesced/stale readiness notification may already have
            // been drained by an earlier event in the same batch.
            return received < 0 &&
                (errno == EAGAIN || errno == EWOULDBLOCK);
        }
    }

private:
    int fd_;
};


RpcServer::RpcServer(
    std::size_t worker_count,
    std::size_t max_queue_size)
    : running_(false),
      started_(false),
      event_fd_(
          std::make_unique<RpcEventFd>()),
      thread_pool_(
          worker_count,
          max_queue_size),
      next_connection_id_(1)
{
}


RpcServer::~RpcServer()
{
    Stop();
}

void RpcServer::Stop()
{
    if (running_.exchange(
            false,
            std::memory_order_acq_rel))
    {
        if (!event_fd_->Notify())
        {
            LOG_ERROR()
                << "Failed to notify EventLoop during stop";
        }
    }
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
        events |= EPOLLRDHUP;

        if (!connection.IsProcessing())
        {
            events |= EPOLLIN;
        }
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

    connection_index_.erase(it->second->Id());

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

    if ((events & EPOLLHUP) &&
        connection.IsProcessing())
    {
        // A full hangup cannot receive the worker response. Close now and
        // let the later completion be discarded by connection_id.
        return false;
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
        !connection.HasOutput() &&
        !connection.IsProcessing())
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


    uint64_t request_id = header.request_id();

    RpcTask task{
        connection.Id(),
        std::move(header),
        std::move(args_data)
    };

    bool submitted = thread_pool_.Submit(
        [this, task = std::move(task)]() mutable
        {
            ExecuteRpcTask(std::move(task));
        }
    );

    if (!submitted)
    {
        return SendErrorResponse(
            connection,
            request_id,
            minirpc::RPC_SERVER_BUSY,
            "Server is busy"
        );
    }

    connection.SetProcessing(true);

    return true;
}


void RpcServer::ExecuteRpcTask(
    RpcTask task)
{
    std::string response_data;

    minirpc::RpcErrorCode error_code =
        minirpc::RPC_OK;

    std::string error_message;

    try
    {
        if (!dispatcher_.HasMethod(
                task.header.service_name(),
                task.header.method_name()))
        {
            error_code =
                minirpc::RPC_METHOD_NOT_FOUND;

            error_message =
                "RPC method not found";
        }
        else if (!dispatcher_.Dispatch(
                    task.header.service_name(),
                    task.header.method_name(),
                    task.args_data,
                    response_data))
        {
            error_code =
                minirpc::RPC_BAD_REQUEST;

            error_message =
                "RPC request execution failed";
        }
    }
    catch (...)
    {
        response_data.clear();
        error_code = minirpc::RPC_INTERNAL_ERROR;
        error_message = "RPC handler threw an exception";
    }

    minirpc::RpcResponseHeader response_header;

    response_header.set_request_id(
        task.header.request_id()
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
        response_packet.clear();
    }

    CompletedResponse completed{
        task.connection_id,
        std::move(response_packet)
    };

    {
        std::lock_guard<std::mutex> lock(
            completion_mutex_
        );

        completion_queue_.push(
            std::move(completed)
        );
    }

    event_fd_->Notify();
}


void RpcServer::ProcessCompletedResponses()
{
    std::queue<CompletedResponse> completed;

    {
        std::lock_guard<std::mutex> lock(
            completion_mutex_
        );

        completed.swap(completion_queue_);
    }

    while (!completed.empty())
    {
        CompletedResponse response =
            std::move(completed.front());

        completed.pop();

        auto index_it = connection_index_.find(
            response.connection_id
        );

        if (index_it == connection_index_.end())
        {
            continue;
        }

        int client_fd = index_it->second;

        auto connection_it = connections_.find(
            client_fd
        );

        if (connection_it == connections_.end() ||
            connection_it->second->Id() !=
                response.connection_id)
        {
            // Keep the secondary index self-healing if invariants are broken.
            connection_index_.erase(index_it);
            continue;
        }

        RpcConnection& connection =
            *(connection_it->second);

        connection.SetProcessing(false);

        if (response.response_packet.empty())
        {
            CloseConnection(client_fd);
            continue;
        }

        connection.OutputBuffer().Append(
            response.response_packet.data(),
            response.response_packet.size()
        );

        if (!UpdateConnectionEvents(connection))
        {
            CloseConnection(client_fd);
        }
    }
}

bool RpcServer::HandleClient(
    RpcConnection& connection)
{
    if (connection.IsProcessing())
    {
        return true;
    }

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

        if (connection.IsProcessing())
        {
            return true;
        }
    }
}

bool RpcServer::Start(uint16_t port)
{
    bool expected = false;

    if (!running_.compare_exchange_strong(
            expected,
            true,
            std::memory_order_acq_rel))
    {
        LOG_ERROR()
            << "RpcServer is already running";

        return false;
    }

    bool first_start = false;

    if (!started_.compare_exchange_strong(
            first_start,
            true,
            std::memory_order_acq_rel))
    {
        running_.store(
            false,
            std::memory_order_release
        );

        LOG_ERROR()
            << "RpcServer cannot be restarted";

        return false;
    }

    auto fail_start = [this]()
    {
        running_.store(
            false,
            std::memory_order_release
        );

        started_.store(
            false,
            std::memory_order_release
        );

        return false;
    };

    int server_fd =
        socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd < 0)
    {
        LOG_ERROR() << "socket() failed";

        return fail_start();
    }

    int opt = 1;

    if (setsockopt(
            server_fd,
            SOL_SOCKET,
            SO_REUSEADDR,
            &opt,
            sizeof(opt)) < 0)
    {
        LOG_ERROR() << "setsockopt() failed";

        close(server_fd);
        return fail_start();
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
        LOG_ERROR() << "bind() failed";

        close(server_fd);
        return fail_start();
    }

    if (listen(server_fd, 5) < 0)
    {
        LOG_ERROR() << "listen() failed";

        close(server_fd);
        return fail_start();
    }

    LOG_INFO()
        << "MiniRPC server listening on port "
        << port
        << "...";

    if (!epoller_.Add(
            server_fd,
            EPOLLIN))
    {
        LOG_ERROR()
            << "Failed to add server socket to epoll";

        close(server_fd);

        return fail_start();
    }

    if (!epoller_.Add(
            event_fd_->Fd(),
            EPOLLIN))
    {
        LOG_ERROR()
            << "Failed to add eventfd to epoll";

        epoller_.Delete(server_fd);
        close(server_fd);

        return fail_start();
    }


    bool event_loop_ok = true;

    while (running_.load(
        std::memory_order_acquire))
    {
        int ready =
            epoller_.Wait(-1);

        if (ready < 0)
        {
            LOG_ERROR() << "epoll_wait() failed";

            event_loop_ok = false;
            running_.store(
                false,
                std::memory_order_release
            );

            break;
        }

        if (!running_.load(
                std::memory_order_acquire))
        {
            if (!event_fd_->Drain())
            {
                LOG_ERROR()
                    << "eventfd drain failed during stop";

                event_loop_ok = false;
            }

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

            if (event.data.fd == event_fd_->Fd())
            {
                if ((event.events & (EPOLLERR | EPOLLHUP)) ||
                    !event_fd_->Drain())
                {
                    LOG_ERROR() << "eventfd epoll error";

                    event_loop_ok = false;
                    running_.store(
                        false,
                        std::memory_order_release
                    );

                    break;
                }

                if (!running_.load(
                        std::memory_order_acquire))
                {
                    break;
                }

                ProcessCompletedResponses();
                continue;
            }

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
                LOG_ERROR()
                    << "Server socket epoll error";

                event_loop_ok = false;
                running_.store(
                    false,
                    std::memory_order_release
                );

                break;
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
                LOG_WARN() << "accept() failed";

                continue;
            }


            uint64_t connection_id =
                next_connection_id_++;

            RpcConnection connection(
                client_fd,
                connection_id,
                8192
            );


            if (!connection.SetNonBlocking())
            {
                LOG_ERROR()
                    << "Failed to set non-blocking for fd="
                    << client_fd;

                // The local RpcConnection owns and closes client_fd.
                continue;
            }


            if (!epoller_.Add(
                    client_fd,
                    EPOLLIN | EPOLLRDHUP))
            {
                LOG_ERROR()
                    << "Failed to add client fd="
                    << client_fd
                    << " to epoll";

                // The local RpcConnection owns and closes client_fd.
                continue;
            }


            auto [connection_it, inserted] =
                connections_.emplace(
                    client_fd,
                    std::make_unique<RpcConnection>(
                        std::move(connection)
                    )
                );

            if (!inserted)
            {
                epoller_.Delete(client_fd);
                continue;
            }

            bool indexed = connection_index_.emplace(
                connection_id,
                client_fd
            ).second;

            if (!indexed)
            {
                epoller_.Delete(client_fd);
                connections_.erase(connection_it);
                continue;
            }


            LOG_INFO()
                << "Client connected fd="
                << client_fd;
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

    connections_.clear();
    connection_index_.clear();

    thread_pool_.Stop();

    epoller_.Delete(event_fd_->Fd());

    {
        std::lock_guard<std::mutex> lock(
            completion_mutex_
        );

        std::queue<CompletedResponse> empty;
        completion_queue_.swap(empty);
    }

    running_.store(
        false,
        std::memory_order_release
    );

    return event_loop_ok;
}

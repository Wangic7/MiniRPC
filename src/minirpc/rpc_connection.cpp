#include <minirpc/rpc_connection.h>
#include <minirpc/logger.h>

#include <unistd.h>

#include <fcntl.h>

#include <cerrno>
#include <sys/socket.h>


RpcConnection::RpcConnection(
    int fd,
    std::size_t buffer_size)
    : RpcConnection(
          fd,
          0,
          buffer_size)
{
}


RpcConnection::RpcConnection(
    int fd,
    uint64_t connection_id,
    std::size_t buffer_size)
    : fd_(fd),
      connection_id_(connection_id),
      input_buffer_(buffer_size),
      output_buffer_(buffer_size),
      peer_read_closed_(false),
      close_after_write_(false),
      processing_(false)
{
}


RpcConnection::~RpcConnection()
{
    Close();
}


int RpcConnection::Fd() const
{
    return fd_;
}


uint64_t RpcConnection::Id() const
{
    return connection_id_;
}


bool RpcConnection::IsOpen() const
{
    return fd_ >= 0;
}

bool RpcConnection::SetNonBlocking()
{
    if (fd_ < 0)
    {
        LOG_ERROR()
            << "Cannot set non-blocking mode on invalid fd";
        return false;
    }

    int flags =
        fcntl(
            fd_,
            F_GETFL,
            0
        );

    if (flags < 0)
    {
        LOG_ERROR()
            << "fcntl(F_GETFL) failed for fd="
            << fd_
            << ", errno="
            << errno;
        return false;
    }

    if (flags & O_NONBLOCK)
    {
        return true;
    }

    bool updated = fcntl(
        fd_,
        F_SETFL,
        flags | O_NONBLOCK
    ) == 0;

    if (!updated)
    {
        LOG_ERROR()
            << "fcntl(F_SETFL) failed for fd="
            << fd_
            << ", errno="
            << errno;
    }

    return updated;
}

RpcReadStatus RpcConnection::ReadOnce()
{
    char buffer[8192];

    while (true)
    {
        ssize_t n =
            recv(
                fd_,
                buffer,
                sizeof(buffer),
                0
            );

        if (n > 0)
        {
            input_buffer_.Append(
                buffer,
                static_cast<std::size_t>(n)
            );

            return RpcReadStatus::Data;
        }

        if (n == 0)
        {
            return RpcReadStatus::PeerClosed;
        }

        if (errno == EINTR)
        {
            continue;
        }

        if (errno == EAGAIN ||
            errno == EWOULDBLOCK)
        {
            return RpcReadStatus::WouldBlock;
        }

        LOG_WARN()
            << "recv() failed for fd="
            << fd_
            << ", errno="
            << errno;

        return RpcReadStatus::Error;
    }
}


void RpcConnection::Close()
{
    if (fd_ >= 0)
    {
        LOG_DEBUG()
            << "Closing connection id="
            << connection_id_
            << ", fd="
            << fd_;

        close(fd_);

        fd_ = -1;
    }
}


RpcBuffer& RpcConnection::InputBuffer()
{
    return input_buffer_;
}


RpcBuffer& RpcConnection::OutputBuffer()
{
    return output_buffer_;
}

bool RpcConnection::HasOutput() const
{
    return output_buffer_.ReadableBytes() > 0;
}

bool RpcConnection::FlushOutput()
{
    while (output_buffer_.ReadableBytes() > 0)
    {
        ssize_t n =
            send(
                fd_,
                output_buffer_.Peek(),
                output_buffer_.ReadableBytes(),
                MSG_NOSIGNAL
            );

        if (n < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }

            if (errno == EAGAIN ||
                errno == EWOULDBLOCK)
            {
                return true;
            }

            LOG_WARN()
                << "send() failed for fd="
                << fd_
                << ", errno="
                << errno;

            return false;
        }

        if (n == 0)
        {
            LOG_WARN()
                << "send() returned zero for fd="
                << fd_;
            return false;
        }

        output_buffer_.Retrieve(
            static_cast<std::size_t>(n)
        );
    }

    return true;
}


void RpcConnection::MarkPeerReadClosed()
{
    peer_read_closed_ = true;
}


bool RpcConnection::IsPeerReadClosed() const
{
    return peer_read_closed_;
}


void RpcConnection::MarkCloseAfterWrite()
{
    close_after_write_ = true;
}


bool RpcConnection::ShouldCloseAfterWrite() const
{
    return close_after_write_;
}


bool RpcConnection::IsProcessing() const
{
    return processing_;
}


void RpcConnection::SetProcessing(
    bool processing)
{
    processing_ = processing;
}



RpcConnection::RpcConnection(
    RpcConnection&& other
) noexcept
    :
    fd_(other.fd_),
    connection_id_(other.connection_id_),
    input_buffer_(std::move(other.input_buffer_)),
    output_buffer_(std::move(other.output_buffer_)),
    peer_read_closed_(other.peer_read_closed_),
    close_after_write_(other.close_after_write_),
    processing_(other.processing_)
{
    other.fd_ = -1;
    other.connection_id_ = 0;
    other.peer_read_closed_ = true;
    other.close_after_write_ = true;
    other.processing_ = false;
}


RpcConnection& RpcConnection::operator=(
    RpcConnection&& other
) noexcept
{
    if (this != &other)
    {
        if (fd_ >= 0)
        {
            close(fd_);
        }

        fd_ = other.fd_;

        connection_id_ = other.connection_id_;

        input_buffer_ =
            std::move(other.input_buffer_);

        output_buffer_ =
            std::move(other.output_buffer_);

        peer_read_closed_ =
            other.peer_read_closed_;

        close_after_write_ =
            other.close_after_write_;

        processing_ =
            other.processing_;

        other.fd_ = -1;
        other.connection_id_ = 0;
        other.peer_read_closed_ = true;
        other.close_after_write_ = true;
        other.processing_ = false;
    }

    return *this;
}

#include <minirpc/rpc_connection.h>

#include <unistd.h>

#include <fcntl.h>

#include <cerrno>
#include <sys/socket.h>


RpcConnection::RpcConnection(
    int fd,
    std::size_t buffer_size)
    : fd_(fd),
      input_buffer_(buffer_size),
      output_buffer_(buffer_size)
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


bool RpcConnection::IsOpen() const
{
    return fd_ >= 0;
}

bool RpcConnection::SetNonBlocking()
{
    if (fd_ < 0)
    {
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
        return false;
    }

    if (flags & O_NONBLOCK)
    {
        return true;
    }

    return fcntl(
        fd_,
        F_SETFL,
        flags | O_NONBLOCK
    ) == 0;
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

        return RpcReadStatus::Error;
    }
}


void RpcConnection::Close()
{
    if (fd_ >= 0)
    {
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
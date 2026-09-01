#include <minirpc/rpc_connection.h>

#include <unistd.h>


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
#pragma once

#include <cstddef>

#include <minirpc/rpc_buffer.h>


enum class RpcReadStatus
{
    Data,
    WouldBlock,
    PeerClosed,
    Error
};

class RpcConnection
{
public:
    explicit RpcConnection(
        int fd,
        std::size_t buffer_size = 8192
    );

    RpcConnection(
    RpcConnection&& other
) noexcept;


RpcConnection& operator=(
    RpcConnection&& other
) noexcept;

    ~RpcConnection();

    RpcConnection(
        const RpcConnection&
    ) = delete;

    RpcConnection& operator=(
        const RpcConnection&
    ) = delete;


    int Fd() const;

    bool IsOpen() const;
    bool SetNonBlocking();
    RpcReadStatus ReadOnce();

    void Close();


    RpcBuffer& InputBuffer();

    RpcBuffer& OutputBuffer();


private:
    int fd_;

    RpcBuffer input_buffer_;

    RpcBuffer output_buffer_;
};
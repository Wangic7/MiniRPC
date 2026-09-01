#include <iostream>
#include <string>

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <minirpc/rpc_connection.h>


int main()
{
    int sockets[2];

    if (socketpair(
            AF_UNIX,
            SOCK_STREAM,
            0,
            sockets) != 0)
    {
        std::cerr
            << "[FAIL] socketpair"
            << std::endl;

        return 1;
    }


    RpcConnection connection(
        sockets[0]
    );


    // 1. 设置非阻塞
    if (!connection.SetNonBlocking())
    {
        std::cerr
            << "[FAIL] SetNonBlocking"
            << std::endl;

        close(sockets[1]);

        return 1;
    }


    int flags =
        fcntl(
            connection.Fd(),
            F_GETFL,
            0
        );

    if (flags < 0 ||
        !(flags & O_NONBLOCK))
    {
        std::cerr
            << "[FAIL] O_NONBLOCK not set"
            << std::endl;

        close(sockets[1]);

        return 1;
    }

    std::cout
        << "[PASS] non-blocking mode"
        << std::endl;


    // 2. 当前没有数据
    RpcReadStatus status =
        connection.ReadOnce();

    if (status !=
        RpcReadStatus::WouldBlock)
    {
        std::cerr
            << "[FAIL] expected WouldBlock"
            << std::endl;

        close(sockets[1]);

        return 1;
    }

    std::cout
        << "[PASS] WouldBlock"
        << std::endl;


    // 3. 对端发送数据
    const std::string message =
        "MiniRPC";

    ssize_t sent =
        write(
            sockets[1],
            message.data(),
            message.size()
        );

    if (sent !=
        static_cast<ssize_t>(
            message.size()))
    {
        std::cerr
            << "[FAIL] write"
            << std::endl;

        close(sockets[1]);

        return 1;
    }


    status =
        connection.ReadOnce();

    if (status !=
        RpcReadStatus::Data)
    {
        std::cerr
            << "[FAIL] expected Data"
            << std::endl;

        close(sockets[1]);

        return 1;
    }


    std::string received =
        connection.InputBuffer()
            .RetrieveAllAsString();

    if (received != message)
    {
        std::cerr
            << "[FAIL] received data mismatch"
            << std::endl;

        close(sockets[1]);

        return 1;
    }

    std::cout
        << "[PASS] Data"
        << std::endl;


    // 4. 对端关闭连接
    close(sockets[1]);

    status =
        connection.ReadOnce();

    if (status !=
        RpcReadStatus::PeerClosed)
    {
        std::cerr
            << "[FAIL] expected PeerClosed"
            << std::endl;

        return 1;
    }

    std::cout
        << "[PASS] PeerClosed"
        << std::endl;


    std::cout
        << "RpcConnection test passed."
        << std::endl;

    return 0;
}
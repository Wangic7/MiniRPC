#pragma once
#include "rpc_header.pb.h"
#include <cstdint>
#include <string>

#include <minirpc/rpc_dispatcher.h>

#include <cstddef>
#include <minirpc/thread_pool.h>

#include <minirpc/rpc_connection.h>

#include <unordered_map>
#include <memory>
#include "minirpc/epoller.h"

class RpcServer
{
public:

    using Handler = RpcDispatcher::Handler;

    explicit RpcServer(
    std::size_t worker_count = 4,     //4 个 Worker 100 个最大等待任务
    std::size_t max_queue_size = 100
    );


    void RegisterMethod(
        const std::string& service_name,
        const std::string& method_name,
        Handler handler
    );

    bool Start(uint16_t port);

private:

    enum class FrameStatus
{
    NeedMoreData,
    Ready,
    Error
};

FrameStatus TryExtractRequestPacket(
    RpcConnection& connection,
    std::string& request_packet
);

    bool HandleClient(RpcConnection& connection);
    bool HandleClientEvent(
        int client_fd,
        uint32_t events
    );

    bool HandleWriteEvent(
        int client_fd
    );

    bool ProcessRequest(
        RpcConnection& connection,
        const std::string& request_packet
    );

    bool UpdateConnectionEvents(
        RpcConnection& connection
    );

    void CloseConnection(int client_fd);

    bool SendErrorResponse(
        RpcConnection& connection,
        uint64_t request_id,
        minirpc::RpcErrorCode error_code,
        const std::string& message
    );

private:
    RpcDispatcher dispatcher_;
    ThreadPool thread_pool_;
    Epoller epoller_;

        std::unordered_map<
        int,
        std::unique_ptr<RpcConnection>
    > connections_;
};

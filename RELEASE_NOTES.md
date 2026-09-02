# Release Notes

## v1.0.0

MiniRPC v1.0.0 完成了 C++17/Linux RPC 框架的核心功能和基础工程化支持。

### 完成内容

- **Reactor 完成**：基于 Linux epoll 实现 EventLoop，统一处理连接建立、非阻塞读写和连接关闭。
- **Async RPC 完成**：完成请求解析、异步任务执行、响应队列和 EventLoop 回写链路。
- **ThreadPool 完成**：业务 Handler 在线程池执行，并通过有界任务队列提供基础过载保护。
- **Graceful shutdown 完成**：支持 `RpcServer::Stop()`，通过 eventfd 唤醒 EventLoop，并安全关闭监听 socket、客户端连接及 worker。
- **Logger 完成**：提供 DEBUG、INFO、WARN、ERROR 日志级别，以及时间戳、文件名和行号信息。
- **CTest 完成**：将 RpcBuffer、RpcConnection 和 Epoller 测试接入 CTest。

### 技术范围

- C++17
- Linux epoll
- Protocol Buffers
- CMake / CTest

### 当前边界

- 客户端 `RpcChannel` 为同步调用接口。
- 单连接最多保留一个 in-flight RPC 任务。
- 网络后端当前仅支持 Linux epoll。

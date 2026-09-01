# MiniRPC

A lightweight RPC framework implemented in C++.

MiniRPC is a simple remote procedure call framework designed to understand the core architecture of modern RPC systems.

The project implements TCP communication, Protobuf serialization, service registration, request dispatching, thread pool concurrency and protocol validation.

---

## Features

### 1. TCP Communication

- TCP socket based communication
- Reliable data transmission
- Custom RPC message protocol
- SendAll / RecvAll handling for complete message transmission


### 2. Protobuf Serialization

MiniRPC uses Google Protocol Buffers for RPC message serialization.

Supported:

- Request serialization
- Response serialization
- Binary protocol transmission


### 3. RPC Protocol

Request:

```
+----------------+
| Header Size    |
+----------------+
| RpcHeader      |
+----------------+
| Payload        |
+----------------+
```


Response:

```
+----------------+
| Header Size    |
+----------------+
| ResponseHeader |
+----------------+
| Payload        |
+----------------+
```


RpcHeader contains:

- service name
- method name
- request id
- payload size
- protocol magic
- protocol version


---

## Architecture

```
                TCP

+-------------+              +-------------+
|   Client    | ------------> |   Server    |
+-------------+              +-------------+
        |                           |
        |                           |
        |                    RpcDispatcher
        |                           |
        |                     ThreadPool
        |                           |
        |                    Service Handler
        |                           |
        +<--------------------------+

```

---

## Current Supported Example


Calculator Service:

```
Calculator
 |
 +-- Add
 |
 +-- Subtract
 |
 +-- SlowAdd
```


Example:

```bash
./rpc_client add 10 20
```

Output:

```
Calculator.Add(10, 20) = 30
```


---

## Error Handling

MiniRPC supports RPC error responses.

Example:

```bash
./rpc_client invalid 1 2
```


Output:

```
RPC error: 1 - RPC method not found
```


Supported error codes:

| Code | Description |
|---|---|
| RPC_OK | Success |
| RPC_METHOD_NOT_FOUND | Method does not exist |
| RPC_BAD_REQUEST | Invalid request |
| RPC_INTERNAL_ERROR | Server error |
| RPC_SERVER_BUSY | Server overloaded |


---

## Protocol Safety

MiniRPC provides basic protocol protection:

### Magic Validation

Each RPC request contains a magic number:

```
RPC_MAGIC
```


Used to detect invalid RPC packets.


### Version Validation

RPC packets contain protocol version information:

```
RPC_VERSION
```


Allows future protocol upgrades.


### Message Size Protection

To avoid abnormal memory allocation:

- Maximum header size limitation
- Maximum payload size limitation


---

## Concurrency

Server uses a thread pool model:


```
Client Requests

        |
        v

     RPC Server

        |
        v

   Thread Pool

        |
        +---- Worker Thread
        +---- Worker Thread
        +---- Worker Thread

```


Features:

- Multi-thread request processing
- Task queue
- Queue size limitation
- Server busy protection


---

## Build

Requirements:

- C++17
- CMake
- Protobuf
- Linux / WSL


Build:

```bash
mkdir build
cd build

cmake ..

make
```


---

## Run


Start server:

```bash
./rpc_server
```


Run client:

```bash
./rpc_client add 10 20
```


---

## Project Structure

```
MiniRPC
|
├── include
|   └── minirpc
|
├── src
|   └── minirpc
|
├── examples
|   └── calculator
|
├── proto
|
└── build

```


---

## Future Improvements

Planned:

- TCP long connection
- Connection pool
- Async RPC call
- RPC benchmark
- Performance optimization
- More flexible service registration


---

## License

MIT License# MiniRPC


## Benchmark

Environment:

- OS: WSL Ubuntu
- Compiler: g++
- Threads: 16
- Requests: 10000


Result:

| Metric | Value |
|---|---:|
| Success | 10000 |
| QPS | 23048 |
| Average latency | 0.679 ms |
| P50 latency | 0.655 ms |
| P95 latency | 1.002 ms |
| P99 latency | 1.209 ms |
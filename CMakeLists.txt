cmake_minimum_required(VERSION 3.16)

project(MiniRPC LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Protobuf REQUIRED)

# 让代码可以通过
# #include "proto/xxx.pb.h"
# 找到项目根目录下的头文件
include_directories("${CMAKE_SOURCE_DIR}")


# =========================
# Protobuf demo
# =========================
add_executable(
    protobuf_demo
    src/protobuf_demo.cpp
    proto/calculator.pb.cc
)

target_link_libraries(
    protobuf_demo
    PRIVATE
    protobuf::libprotobuf
)


# =========================
# RPC codec demo
# =========================
add_executable(
    rpc_codec_demo
    src/rpc_codec_demo.cpp
    proto/calculator.pb.cc
    proto/rpc_header.pb.cc
)

target_link_libraries(
    rpc_codec_demo
    PRIVATE
    protobuf::libprotobuf
)


# =========================
# RPC Server
# =========================
add_executable(
    rpc_server
    src/rpc_server.cpp
    src/rpc_server_core.cpp
    src/rpc_dispatcher.cpp
    proto/calculator.pb.cc
    proto/rpc_header.pb.cc
)

target_link_libraries(
    rpc_server
    PRIVATE
    protobuf::libprotobuf
)


# =========================
# RPC Client
# =========================
add_executable(
    rpc_client
    src/rpc_client.cpp
    proto/calculator.pb.cc
    proto/rpc_header.pb.cc
)

target_link_libraries(
    rpc_client
    PRIVATE
    protobuf::libprotobuf
)
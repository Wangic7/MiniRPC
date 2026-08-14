cmake_minimum_required(VERSION 3.16)

project(MiniRPC LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Protobuf REQUIRED)


# ==================================================
# MiniRPC Framework Library
# ==================================================

add_library(
    minirpc
    STATIC

    src/minirpc/rpc_server.cpp
    src/minirpc/rpc_dispatcher.cpp
    src/minirpc/rpc_channel.cpp

    proto/rpc_header.pb.cc
)

target_include_directories(
    minirpc

    PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}
)

target_link_libraries(
    minirpc

    PUBLIC
    protobuf::libprotobuf
)


# ==================================================
# Calculator Example Server
# ==================================================

add_executable(
    rpc_server

    examples/calculator/server.cpp
    examples/calculator/calculator_service.cpp
    examples/calculator/calculator.pb.cc
)

target_include_directories(
    rpc_server

    PRIVATE
    ${CMAKE_SOURCE_DIR}/examples/calculator
)

target_link_libraries(
    rpc_server

    PRIVATE
    minirpc
)


# ==================================================
# Calculator Example Client
# ==================================================

add_executable(
    rpc_client

    examples/calculator/client.cpp
    examples/calculator/calculator.pb.cc
)

target_include_directories(
    rpc_client

    PRIVATE
    ${CMAKE_SOURCE_DIR}/examples/calculator
)

target_link_libraries(
    rpc_client

    PRIVATE
    minirpc
)
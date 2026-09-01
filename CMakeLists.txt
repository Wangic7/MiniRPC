cmake_minimum_required(VERSION 3.16)

project(MiniRPC LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Protobuf REQUIRED)


# ==================================================
# Generated Protobuf directories
# ==================================================

set(MINIRPC_PROTO_GEN_DIR
    ${CMAKE_BINARY_DIR}/generated/minirpc
)

set(CALCULATOR_PROTO_GEN_DIR
    ${CMAKE_BINARY_DIR}/generated/calculator
)

file(MAKE_DIRECTORY
    ${MINIRPC_PROTO_GEN_DIR}
)

file(MAKE_DIRECTORY
    ${CALCULATOR_PROTO_GEN_DIR}
)


# ==================================================
# MiniRPC protocol
# ==================================================

protobuf_generate(
    LANGUAGE cpp
    OUT_VAR MINIRPC_PROTO_GENERATED

    PROTOC_OUT_DIR
    ${MINIRPC_PROTO_GEN_DIR}

    APPEND_PATH

    PROTOS
    ${CMAKE_SOURCE_DIR}/proto/rpc_header.proto
)


# ==================================================
# MiniRPC Framework
# ==================================================

add_library(
    minirpc
    STATIC

    src/minirpc/rpc_server.cpp
    src/minirpc/rpc_dispatcher.cpp
    src/minirpc/rpc_channel.cpp
    src/minirpc/thread_pool.cpp
    src/minirpc/rpc_codec.cpp
    src/minirpc/rpc_buffer.cpp
    src/minirpc/rpc_connection.cpp
    src/minirpc/epoller.cpp
    ${MINIRPC_PROTO_GENERATED}
)

target_include_directories(
    minirpc

    PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${MINIRPC_PROTO_GEN_DIR}
)

target_link_libraries(
    minirpc

    PUBLIC
    protobuf::libprotobuf
)


# ==================================================
# Calculator protocol
# ==================================================

protobuf_generate(
    LANGUAGE cpp
    OUT_VAR CALCULATOR_PROTO_GENERATED

    PROTOC_OUT_DIR
    ${CALCULATOR_PROTO_GEN_DIR}

    APPEND_PATH

    PROTOS
    ${CMAKE_SOURCE_DIR}/examples/calculator/calculator.proto
)

add_library(
    calculator_proto
    STATIC

    ${CALCULATOR_PROTO_GENERATED}
)

target_include_directories(
    calculator_proto

    PUBLIC
    ${CALCULATOR_PROTO_GEN_DIR}
)

target_link_libraries(
    calculator_proto

    PUBLIC
    protobuf::libprotobuf
)


# ==================================================
# Calculator Server
# ==================================================

add_executable(
    rpc_server

    examples/calculator/server.cpp
    examples/calculator/calculator_service.cpp
)

add_executable(rpc_codec_demo
    src/rpc_codec_demo.cpp
)

target_link_libraries(rpc_codec_demo
    minirpc
    calculator_proto
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
    calculator_proto
)


# ==================================================
# Calculator Client
# ==================================================

add_executable(
    rpc_client

    examples/calculator/client.cpp
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
    calculator_proto
)


add_executable(rpc_benchmark
    benchmarks/rpc_benchmark.cpp
)

target_link_libraries(rpc_benchmark
    minirpc
    calculator_proto
)


add_executable(
    rpc_buffer_test
    tests/rpc_buffer_test.cpp
)

target_link_libraries(
    rpc_buffer_test
    minirpc
)

add_executable(
    epoller_test
    tests/epoller_test.cpp
)

target_link_libraries(
    epoller_test
    minirpc
)
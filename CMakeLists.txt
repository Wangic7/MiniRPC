cmake_minimum_required(VERSION 3.16)

project(MiniRPC LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Protobuf REQUIRED)

# 告诉编译器从 MiniRPC 项目根目录寻找头文件
include_directories("${CMAKE_SOURCE_DIR}")

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
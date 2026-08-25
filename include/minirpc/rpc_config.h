#pragma once

#include <cstdint>

namespace minirpc
{

// RPC协议魔数
constexpr uint32_t RPC_MAGIC = 0x20260823;


// RPC协议版本
constexpr uint32_t RPC_VERSION = 1;


// 最大RPC Header大小
constexpr uint32_t MAX_HEADER_SIZE = 64 * 1024;


// 最大RPC Payload大小
constexpr uint32_t MAX_PAYLOAD_SIZE = 4 * 1024 * 1024;


}

#pragma once

#include <cstdint>

namespace minirpc
{

// RPC协议魔数
// 用于快速判断是否为MiniRPC数据包
constexpr uint32_t RPC_MAGIC = 0x20260823;


// RPC协议版本
constexpr uint32_t RPC_VERSION = 1;


}

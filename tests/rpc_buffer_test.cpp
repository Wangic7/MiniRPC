#include <cassert>
#include <iostream>
#include <string>

#include <minirpc/rpc_buffer.h>


int main()
{
    // =================================
    // Test 1: 基本 Append / Retrieve
    // =================================

    {
        RpcBuffer buffer(16);

        buffer.Append("hello");

        assert(buffer.ReadableBytes() == 5);

        std::string result =
            buffer.RetrieveAsString(2);

        assert(result == "he");
        assert(buffer.ReadableBytes() == 3);

        result =
            buffer.RetrieveAllAsString();

        assert(result == "llo");
        assert(buffer.ReadableBytes() == 0);

        std::cout
            << "[PASS] Basic append/retrieve"
            << std::endl;
    }


    // =================================
    // Test 2: 自动扩容
    // =================================

    {
        RpcBuffer buffer(4);

        std::string data =
            "abcdefghij";

        buffer.Append(data);

        assert(buffer.ReadableBytes()
               == data.size());

        std::string result =
            buffer.RetrieveAllAsString();

        assert(result == data);

        std::cout
            << "[PASS] Buffer expansion"
            << std::endl;
    }


    // =================================
    // Test 3: 已消费空间重新利用
    // =================================

    {
        RpcBuffer buffer(8);

        buffer.Append("abcdef");

        std::string first =
            buffer.RetrieveAsString(4);

        assert(first == "abcd");
        assert(buffer.ReadableBytes() == 2);

        // 此时前面已经有4字节空间
        // 再写入4字节，应能够搬移剩余数据
        buffer.Append("WXYZ");

        assert(buffer.ReadableBytes() == 6);

        std::string result =
            buffer.RetrieveAllAsString();

        assert(result == "efWXYZ");

        std::cout
            << "[PASS] Buffer compaction"
            << std::endl;
    }


    // =================================
    // Test 4: 多次写入
    // =================================

    {
        RpcBuffer buffer(4);

        buffer.Append("RPC");
        buffer.Append("-");
        buffer.Append("Buffer");

        std::string result =
            buffer.RetrieveAllAsString();

        assert(result == "RPC-Buffer");

        std::cout
            << "[PASS] Multiple append"
            << std::endl;
    }


    // =================================
    // Test 5: Clear
    // =================================

    {
        RpcBuffer buffer;

        buffer.Append("MiniRPC");

        assert(buffer.ReadableBytes() == 7);

        buffer.Clear();

        assert(buffer.ReadableBytes() == 0);

        buffer.Append("OK");

        assert(
            buffer.RetrieveAllAsString()
            == "OK"
        );

        std::cout
            << "[PASS] Clear"
            << std::endl;
    }


    std::cout
        << "\nAll RpcBuffer tests passed."
        << std::endl;

    return 0;
}
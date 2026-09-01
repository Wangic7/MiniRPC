#pragma once

#include <cstddef>
#include <string>
#include <vector>

class RpcBuffer
{
public:
    explicit RpcBuffer(
        std::size_t initial_size = 4096
    );

    std::size_t ReadableBytes() const;

    std::size_t WritableBytes() const;

    const char* Peek() const;

    void Append(
        const void* data,
        std::size_t length
    );

    void Append(
        const std::string& data
    );

    void Retrieve(
        std::size_t length
    );

    std::string RetrieveAsString(
        std::size_t length
    );

    std::string RetrieveAllAsString();

    void Clear();

private:
    void EnsureWritableBytes(
        std::size_t length
    );

private:
    std::vector<char> buffer_;

    std::size_t read_index_;

    std::size_t write_index_;
};
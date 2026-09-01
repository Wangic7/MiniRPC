#include <minirpc/rpc_buffer.h>

#include <algorithm>
#include <cstring>


RpcBuffer::RpcBuffer(
    std::size_t initial_size)
    : buffer_(initial_size),
      read_index_(0),
      write_index_(0)
{
}


std::size_t RpcBuffer::ReadableBytes() const
{
    return write_index_ - read_index_;
}


std::size_t RpcBuffer::WritableBytes() const
{
    return buffer_.size() - write_index_;
}


const char* RpcBuffer::Peek() const
{
    return buffer_.data() + read_index_;
}


void RpcBuffer::Append(
    const void* data,
    std::size_t length)
{
    if (length == 0)
    {
        return;
    }

    EnsureWritableBytes(length);

    std::memcpy(
        buffer_.data() + write_index_,
        data,
        length
    );

    write_index_ += length;
}


void RpcBuffer::Append(
    const std::string& data)
{
    Append(
        data.data(),
        data.size()
    );
}


void RpcBuffer::Retrieve(
    std::size_t length)
{
    if (length >= ReadableBytes())
    {
        Clear();
        return;
    }

    read_index_ += length;
}


std::string RpcBuffer::RetrieveAsString(
    std::size_t length)
{
    length = std::min(
        length,
        ReadableBytes()
    );

    std::string result(
        Peek(),
        length
    );

    Retrieve(length);

    return result;
}


std::string RpcBuffer::RetrieveAllAsString()
{
    return RetrieveAsString(
        ReadableBytes()
    );
}


void RpcBuffer::Clear()
{
    read_index_ = 0;
    write_index_ = 0;
}


void RpcBuffer::EnsureWritableBytes(
    std::size_t length)
{
    if (WritableBytes() >= length)
    {
        return;
    }

    std::size_t readable =
        ReadableBytes();


    // 前面已经消费掉的空间 + 尾部空间足够
    // 则把剩余数据移动到buffer开头
    if (read_index_ + WritableBytes()
        >= length)
    {
        std::memmove(
            buffer_.data(),
            buffer_.data() + read_index_,
            readable
        );

        read_index_ = 0;
        write_index_ = readable;

        return;
    }


    // 空间仍然不够，扩大buffer
    std::size_t new_size =
        std::max(
            buffer_.size() * 2,
            write_index_ + length
        );

    buffer_.resize(new_size);
}
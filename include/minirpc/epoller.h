#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include <sys/epoll.h>


class Epoller
{
public:

bool Remove(int fd);
    explicit Epoller(
        std::size_t max_events = 1024
    );

    ~Epoller();

    Epoller(const Epoller&) = delete;

    Epoller& operator=(
        const Epoller&
    ) = delete;


    bool Add(
        int fd,
        uint32_t events
    );

    bool Modify(
        int fd,
        uint32_t events
    );

    bool Delete(
        int fd
    );

    int Wait(
        int timeout_ms
    );

    const epoll_event& Event(
        std::size_t index
    ) const;


private:
    bool Control(
        int operation,
        int fd,
        uint32_t events
    );


private:
    int epoll_fd_;

    std::vector<epoll_event> events_;
};

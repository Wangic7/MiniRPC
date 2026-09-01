#include <minirpc/epoller.h>

#include <cerrno>
#include <stdexcept>
#include <unistd.h>


Epoller::Epoller(
    std::size_t max_events)
    : epoll_fd_(-1),
      events_(
          max_events == 0
              ? 1
              : max_events)
{
    epoll_fd_ =
        epoll_create1(
            EPOLL_CLOEXEC
        );

    if (epoll_fd_ < 0)
    {
        throw std::runtime_error(
            "epoll_create1 failed"
        );
    }
}


Epoller::~Epoller()
{
    if (epoll_fd_ >= 0)
    {
        close(epoll_fd_);

        epoll_fd_ = -1;
    }
}


bool Epoller::Add(
    int fd,
    uint32_t events)
{
    return Control(
        EPOLL_CTL_ADD,
        fd,
        events
    );
}


bool Epoller::Modify(
    int fd,
    uint32_t events)
{
    return Control(
        EPOLL_CTL_MOD,
        fd,
        events
    );
}


bool Epoller::Delete(
    int fd)
{
    return epoll_ctl(
        epoll_fd_,
        EPOLL_CTL_DEL,
        fd,
        nullptr
    ) == 0;
}


bool Epoller::Control(
    int operation,
    int fd,
    uint32_t events)
{
    epoll_event event{};

    event.events = events;

    event.data.fd = fd;


    return epoll_ctl(
        epoll_fd_,
        operation,
        fd,
        &event
    ) == 0;
}


int Epoller::Wait(
    int timeout_ms)
{
    while (true)
    {
        int ready =
            epoll_wait(
                epoll_fd_,
                events_.data(),
                static_cast<int>(
                    events_.size()
                ),
                timeout_ms
            );


        if (ready < 0 &&
            errno == EINTR)
        {
            continue;
        }


        return ready;
    }
}


const epoll_event& Epoller::Event(
    std::size_t index) const
{
    return events_.at(index);
}

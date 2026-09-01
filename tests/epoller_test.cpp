#include <cassert>
#include <iostream>
#include <string>

#include <unistd.h>
#include <sys/epoll.h>

#include <minirpc/epoller.h>


int main()
{
    int pipe_fd[2];

    int result =
        pipe(pipe_fd);

    assert(result == 0);


    int read_fd =
        pipe_fd[0];

    int write_fd =
        pipe_fd[1];


    Epoller epoller;


    bool added =
        epoller.Add(
            read_fd,
            EPOLLIN
        );

    assert(added);


    const std::string message =
        "MiniRPC epoll test";


    ssize_t written =
        write(
            write_fd,
            message.data(),
            message.size()
        );

    assert(
        written ==
        static_cast<ssize_t>(
            message.size()
        )
    );


    int ready =
        epoller.Wait(1000);

    assert(ready == 1);


    const epoll_event& event =
        epoller.Event(0);


    assert(event.data.fd == read_fd);

    assert(
        event.events & EPOLLIN
    );


    char buffer[128]{};

    ssize_t received =
        read(
            read_fd,
            buffer,
            sizeof(buffer)
        );

    assert(received > 0);


    std::string received_message(
        buffer,
        static_cast<std::size_t>(
            received
        )
    );


    assert(
        received_message ==
        message
    );


    close(read_fd);
    close(write_fd);


    std::cout
        << "[PASS] epoll readable event"
        << std::endl;

    std::cout
        << "Epoller test passed."
        << std::endl;


    return 0;
}
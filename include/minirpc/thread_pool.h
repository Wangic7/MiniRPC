    #pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class ThreadPool
{
public:
    explicit ThreadPool(std::size_t thread_count);

    ~ThreadPool();

    void Submit(std::function<void()> task);

private:
    void WorkerLoop();

private:
    std::vector<std::thread> workers_;

    std::queue<std::function<void()>> tasks_;

    std::mutex mutex_;

    std::condition_variable condition_;

    bool stopping_ = false;
};

//              tasks_
//        ┌────────────────┐
//        │ task1 task2 ... │
//        └───────┬────────┘
//                │
//      condition_variable
//                │
//       ┌────────┼────────┐
//       ▼        ▼        ▼
//    Worker1  Worker2  Worker3 ...
#include <minirpc/thread_pool.h>

#include <utility>

ThreadPool::ThreadPool(
    std::size_t thread_count,
    std::size_t max_queue_size)
    : max_queue_size_(
          max_queue_size == 0
              ? 1
              : max_queue_size)
{
    if (thread_count == 0)
    {
        thread_count = 1;
    }

    workers_.reserve(thread_count);

    for (std::size_t i = 0;
         i < thread_count;
         ++i)
    {
        workers_.emplace_back(
            [this]()
            {
                WorkerLoop();
            }
        );
    }
}

ThreadPool::~ThreadPool()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);

        stopping_ = true;
    }

    condition_.notify_all();

    for (auto& worker : workers_)
    {
        if (worker.joinable())
        {
            worker.join();
        }
    }
}

bool ThreadPool::Submit(
    std::function<void()> task)
{
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (stopping_)
        {
            return false;
        }

        if (tasks_.size() >= max_queue_size_)//队列不能无限增长
        {
            return false;
        }

        tasks_.push(std::move(task));
    }

    condition_.notify_one();

    return true;
}

void ThreadPool::WorkerLoop()
{
    while (true)
    {
        std::function<void()> task;

        {
            std::unique_lock<std::mutex> lock(
                mutex_
            );

            condition_.wait(
                lock,
                [this]()
                {
                    return stopping_
                        || !tasks_.empty();
                }
            );

            if (stopping_ && tasks_.empty())
            {
                return;
            }

            task = std::move(tasks_.front());

            tasks_.pop();
        }

        task();
    }
}
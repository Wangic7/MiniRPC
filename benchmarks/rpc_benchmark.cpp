#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <minirpc/rpc_channel.h>

#include "calculator.pb.h"


double Percentile(
    const std::vector<double>& sorted_data,
    double p)
{
    if (sorted_data.empty())
    {
        return 0.0;
    }

    size_t index =
        static_cast<size_t>(
            std::ceil(
                p * sorted_data.size()
            )
        );

    if (index == 0)
    {
        index = 1;
    }

    return sorted_data[index - 1];
}


int main(int argc, char* argv[])
{
    if (argc != 3)
    {
        std::cerr
            << "Usage: ./rpc_benchmark "
            << "<threads> <total_requests>"
            << std::endl;

        return 1;
    }


    int thread_count =
        std::stoi(argv[1]);

    int total_requests =
        std::stoi(argv[2]);


    if (thread_count <= 0 ||
        total_requests <= 0)
    {
        std::cerr
            << "threads and total_requests "
            << "must be greater than 0"
            << std::endl;

        return 1;
    }


    constexpr int kWarmupPerThread = 50;


    std::vector<double> all_latencies;

    all_latencies.reserve(
        total_requests
    );


    std::mutex result_mutex;


    int total_success = 0;


    // 用来让所有线程尽量同时开始正式测试
    std::atomic<int> ready_threads{0};

    std::atomic<bool> start_flag{false};



    std::vector<std::thread> workers;

    workers.reserve(
        thread_count
    );


    int base_requests =
        total_requests
        /
        thread_count;


    int remainder =
        total_requests
        %
        thread_count;



    std::cout
        << "Preparing benchmark..."
        << std::endl;

    std::cout
        << "Threads  : "
        << thread_count
        << std::endl;

    std::cout
        << "Requests : "
        << total_requests
        << std::endl;



    for (int thread_id = 0;
         thread_id < thread_count;
         ++thread_id)
    {
        int requests_for_thread =
            base_requests;

        if (thread_id < remainder)
        {
            ++requests_for_thread;
        }


        workers.emplace_back(
            [thread_id,
             requests_for_thread,
             &ready_threads,
             &start_flag,
             &all_latencies,
             &total_success,
             &result_mutex]()
            {
                RpcChannel channel(
                    "127.0.0.1",
                    9000
                );


                minirpc::AddRequest request;

                request.set_a(10);
                request.set_b(20);



                // =========================
                // Warmup
                // =========================

                for (int i = 0;
                     i < kWarmupPerThread;
                     ++i)
                {
                    minirpc::AddResponse response;

                    if (!channel.Call(
                            "Calculator",
                            "Add",
                            request,
                            response))
                    {
                        std::cerr
                            << "Warmup failed in thread "
                            << thread_id
                            << std::endl;

                        return;
                    }
                }



                // 告诉主线程：本线程已准备完成
                ready_threads.fetch_add(
                    1,
                    std::memory_order_release
                );


                // 等待统一开始信号
                while (!start_flag.load(
                    std::memory_order_acquire))
                {
                    std::this_thread::yield();
                }



                std::vector<double>
                    local_latencies;

                local_latencies.reserve(
                    requests_for_thread
                );


                int local_success = 0;



                // =========================
                // 正式测试
                // =========================

                for (int i = 0;
                     i < requests_for_thread;
                     ++i)
                {
                    minirpc::AddResponse response;


                    auto begin =
                        std::chrono::steady_clock::now();


                    bool success =
                        channel.Call(
                            "Calculator",
                            "Add",
                            request,
                            response
                        );


                    auto end =
                        std::chrono::steady_clock::now();



                    if (!success)
                    {
                        continue;
                    }


                    if (response.result()
                        != 30)
                    {
                        continue;
                    }


                    double latency_ms =
                        std::chrono::duration<
                            double,
                            std::milli
                        >(
                            end - begin
                        ).count();


                    local_latencies.push_back(
                        latency_ms
                    );


                    ++local_success;
                }



                // =========================
                // 汇总线程结果
                // =========================

                {
                    std::lock_guard<std::mutex>
                        lock(result_mutex);


                    total_success +=
                        local_success;


                    all_latencies.insert(
                        all_latencies.end(),
                        local_latencies.begin(),
                        local_latencies.end()
                    );
                }
            }
        );
    }



    // 等所有线程完成连接和warmup
    while (ready_threads.load(
               std::memory_order_acquire)
           < thread_count)
    {
        std::this_thread::yield();
    }



    std::cout
        << "Starting benchmark..."
        << std::endl;


    auto benchmark_begin =
        std::chrono::steady_clock::now();


    start_flag.store(
        true,
        std::memory_order_release
    );



    for (auto& worker : workers)
    {
        worker.join();
    }



    auto benchmark_end =
        std::chrono::steady_clock::now();



    if (total_success == 0)
    {
        std::cerr
            << "No successful RPC requests."
            << std::endl;

        return 1;
    }



    double total_seconds =
        std::chrono::duration<double>(
            benchmark_end
            -
            benchmark_begin
        ).count();



    double qps =
        total_success
        /
        total_seconds;



    double latency_sum = 0.0;


    for (double latency :
         all_latencies)
    {
        latency_sum += latency;
    }



    double average_latency =
        latency_sum
        /
        all_latencies.size();



    std::sort(
        all_latencies.begin(),
        all_latencies.end()
    );



    double p50 =
        Percentile(
            all_latencies,
            0.50
        );


    double p95 =
        Percentile(
            all_latencies,
            0.95
        );


    double p99 =
        Percentile(
            all_latencies,
            0.99
        );



    std::cout
        << std::fixed
        << std::setprecision(3);


    std::cout
        << "\n===== MiniRPC Concurrent Benchmark ====="
        << std::endl;


    std::cout
        << "Threads         : "
        << thread_count
        << std::endl;


    std::cout
        << "Requests        : "
        << total_requests
        << std::endl;


    std::cout
        << "Success         : "
        << total_success
        << std::endl;


    std::cout
        << "Total time      : "
        << total_seconds
        << " s"
        << std::endl;


    std::cout
        << "QPS             : "
        << qps
        << std::endl;


    std::cout
        << "Average latency : "
        << average_latency
        << " ms"
        << std::endl;


    std::cout
        << "P50 latency     : "
        << p50
        << " ms"
        << std::endl;


    std::cout
        << "P95 latency     : "
        << p95
        << " ms"
        << std::endl;


    std::cout
        << "P99 latency     : "
        << p99
        << " ms"
        << std::endl;


    return 0;
}
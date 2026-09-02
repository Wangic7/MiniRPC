#include <minirpc/logger.h>

#include <atomic>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>

namespace minirpc
{
namespace
{

std::atomic<int>& MinimumLevel()
{
    static std::atomic<int> level{
        static_cast<int>(LogLevel::Info)
    };

    return level;
}

std::mutex& OutputMutex()
{
    static std::mutex mutex;
    return mutex;
}

const char* LevelName(LogLevel level) noexcept
{
    switch (level)
    {
        case LogLevel::Debug:
            return "DEBUG";
        case LogLevel::Info:
            return "INFO";
        case LogLevel::Warn:
            return "WARN";
        case LogLevel::Error:
            return "ERROR";
    }

    return "UNKNOWN";
}

const char* BaseName(const char* file) noexcept
{
    if (file == nullptr)
    {
        return "?";
    }

    const char* base = file;

    for (const char* current = file;
         *current != '\0';
         ++current)
    {
        if (*current == '/' || *current == '\\')
        {
            base = current + 1;
        }
    }

    return base;
}

} // namespace

void Logger::SetLevel(LogLevel level) noexcept
{
    MinimumLevel().store(
        static_cast<int>(level),
        std::memory_order_release
    );
}

LogLevel Logger::Level() noexcept
{
    return static_cast<LogLevel>(
        MinimumLevel().load(
            std::memory_order_acquire
        )
    );
}

bool Logger::IsEnabled(LogLevel level) noexcept
{
    return static_cast<int>(level) >=
        MinimumLevel().load(
            std::memory_order_relaxed
        );
}

void Logger::Write(
    LogLevel level,
    const char* file,
    int line,
    const std::string& message) noexcept
{
    if (!IsEnabled(level))
    {
        return;
    }

    try
    {
        auto now = std::chrono::system_clock::now();
        std::time_t now_time =
            std::chrono::system_clock::to_time_t(now);

        std::tm local_time{};

#if defined(_WIN32)
        localtime_s(&local_time, &now_time);
#else
        localtime_r(&now_time, &local_time);
#endif

        auto milliseconds =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()
            ) % 1000;

        std::lock_guard<std::mutex> lock(OutputMutex());

        std::clog
            << '[' << LevelName(level) << "] "
            << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S")
            << '.' << std::setfill('0') << std::setw(3)
            << milliseconds.count()
            << " [" << BaseName(file) << ':' << line << "] "
            << message
            << std::endl;
    }
    catch (...)
    {
        // Logging must never affect the RPC control flow.
    }
}

LogMessage::LogMessage(
    LogLevel level,
    const char* file,
    int line)
    : level_(level),
      file_(file),
      line_(line),
      enabled_(Logger::IsEnabled(level))
{
}

LogMessage::~LogMessage() noexcept
{
    if (enabled_)
    {
        try
        {
            Logger::Write(
                level_,
                file_,
                line_,
                stream_.str()
            );
        }
        catch (...)
        {
            // A log message must not terminate application code.
        }
    }
}

std::ostream& LogMessage::Stream() noexcept
{
    return stream_;
}

} // namespace minirpc

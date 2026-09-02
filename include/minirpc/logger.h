#pragma once

#include <ostream>
#include <sstream>
#include <string>

namespace minirpc
{

enum class LogLevel
{
    Debug = 0,
    Info,
    Warn,
    Error
};

class Logger
{
public:
    static void SetLevel(LogLevel level) noexcept;
    static LogLevel Level() noexcept;
    static bool IsEnabled(LogLevel level) noexcept;

    static void Write(
        LogLevel level,
        const char* file,
        int line,
        const std::string& message
    ) noexcept;
};

class LogMessage
{
public:
    LogMessage(
        LogLevel level,
        const char* file,
        int line
    );

    ~LogMessage() noexcept;

    LogMessage(const LogMessage&) = delete;
    LogMessage& operator=(const LogMessage&) = delete;

    std::ostream& Stream() noexcept;

private:
    LogLevel level_;
    const char* file_;
    int line_;
    bool enabled_;
    std::ostringstream stream_;
};

} // namespace minirpc

#define LOG_DEBUG() \
    ::minirpc::LogMessage( \
        ::minirpc::LogLevel::Debug, __FILE__, __LINE__).Stream()

#define LOG_INFO() \
    ::minirpc::LogMessage( \
        ::minirpc::LogLevel::Info, __FILE__, __LINE__).Stream()

#define LOG_WARN() \
    ::minirpc::LogMessage( \
        ::minirpc::LogLevel::Warn, __FILE__, __LINE__).Stream()

#define LOG_ERROR() \
    ::minirpc::LogMessage( \
        ::minirpc::LogLevel::Error, __FILE__, __LINE__).Stream()

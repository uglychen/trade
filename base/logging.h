#ifndef LOGGING_H_
#define LOGGING_H_

#include <cassert>
#include <string>
#include <cstring>
#include <sstream>

enum LoggingDestination {
  LOG_NONE                = 0,
  LOG_TO_FILE             = 1 << 0,
  LOG_TO_SYSTEM_DEBUG_LOG = 1 << 1,
  LOG_TO_ALL = LOG_TO_FILE | LOG_TO_SYSTEM_DEBUG_LOG,
  LOG_DEFAULT = LOG_TO_SYSTEM_DEBUG_LOG,
};

enum OldFileDeletionState { DELETE_OLD_LOG_FILE, APPEND_TO_OLD_LOG_FILE };

struct LoggingSettings {
  LoggingSettings();

  LoggingDestination logging_dest;

  const char* log_file;
  OldFileDeletionState delete_old;
};

bool BaseInitLoggingImpl(const LoggingSettings& settings);

inline bool InitLogging(const LoggingSettings& settings) {
  return BaseInitLoggingImpl(settings);
}

void SetMinLogLevel(int level);

int GetMinLogLevel();

bool ShouldCreateLogMessage(int severity);

void SetLogItems(bool enable_process_id, bool enable_thread_id,
                 bool enable_timestamp, bool enable_tickcount);

typedef void (*LogAssertHandlerFunction)(const std::string& str);
void SetLogAssertHandler(LogAssertHandlerFunction handler);

typedef bool (*LogMessageHandlerFunction)(int severity,
    const char* file, int line, size_t message_start, const std::string& str);
void SetLogMessageHandler(LogMessageHandlerFunction handler);
LogMessageHandlerFunction GetLogMessageHandler();

typedef int LogSeverity;
const LogSeverity LOG_INFO = 0;
const LogSeverity LOG_WARNING = 1;
const LogSeverity LOG_ERROR = 2;
const LogSeverity LOG_FATAL = 3;
const LogSeverity LOG_NUM_SEVERITIES = 4;

#ifdef NDEBUG
const LogSeverity LOG_DFATAL = LOG_ERROR;
#else
const LogSeverity LOG_DFATAL = LOG_FATAL;
#endif

#define COMPACT_GOOGLE_LOG_EX_INFO(ClassName, ...) \
  ClassName(__FILE__, __LINE__, LOG_INFO , ##__VA_ARGS__)
#define COMPACT_GOOGLE_LOG_EX_WARNING(ClassName, ...) \
  ClassName(__FILE__, __LINE__, LOG_WARNING , ##__VA_ARGS__)
#define COMPACT_GOOGLE_LOG_EX_ERROR(ClassName, ...) \
  ClassName(__FILE__, __LINE__, LOG_ERROR , ##__VA_ARGS__)
#define COMPACT_GOOGLE_LOG_EX_FATAL(ClassName, ...) \
  ClassName(__FILE__, __LINE__, LOG_FATAL , ##__VA_ARGS__)
#define COMPACT_GOOGLE_LOG_EX_DFATAL(ClassName, ...) \
  ClassName(__FILE__, __LINE__, LOG_DFATAL , ##__VA_ARGS__)

#define COMPACT_GOOGLE_LOG_INFO \
  COMPACT_GOOGLE_LOG_EX_INFO(LogMessage)
#define COMPACT_GOOGLE_LOG_WARNING \
  COMPACT_GOOGLE_LOG_EX_WARNING(LogMessage)
#define COMPACT_GOOGLE_LOG_ERROR \
  COMPACT_GOOGLE_LOG_EX_ERROR(LogMessage)
#define COMPACT_GOOGLE_LOG_FATAL \
  COMPACT_GOOGLE_LOG_EX_FATAL(LogMessage)
#define COMPACT_GOOGLE_LOG_DFATAL \
  COMPACT_GOOGLE_LOG_EX_DFATAL(LogMessage)

#define LOG_IS_ON(severity) \
  (ShouldCreateLogMessage(LOG_##severity))

#define LAZY_STREAM(stream, condition) \
  !(condition) ? (void) 0 : LogMessageVoidify() & (stream)

#define LOG_STREAM(severity) COMPACT_GOOGLE_LOG_ ## severity.stream()

#define LOG(severity) LAZY_STREAM(LOG_STREAM(severity), LOG_IS_ON(severity))
#define LOG_IF(severity, condition) \
  LAZY_STREAM(LOG_STREAM(severity), LOG_IS_ON(severity) && (condition))

#define LOG_ASSERT(condition) \
  LOG_IF(FATAL, !(condition)) << "Assert failed: " #condition ". "

extern std::ostream* g_swallow_stream;

#define EAT_STREAM_PARAMETERS \
  true ? (void) 0 : LogMessageVoidify() & (*g_swallow_stream)

#if defined(NDEBUG)
#define ENABLE_DLOG 0
#else
#define ENABLE_DLOG 1
#endif

#if ENABLE_DLOG

#define DLOG_IS_ON(severity) LOG_IS_ON(severity)
#define DLOG(severity) LOG(severity)
#define DLOG_IF(severity, condition) LOG_IF(severity, condition)
#define DLOG_ASSERT(condition) LOG_ASSERT(condition)

#else  // ENABLE_DLOG

#define DLOG_IS_ON(severity) false
#define DLOG(severity) EAT_STREAM_PARAMETERS
#define DLOG_IF(severity, condition) EAT_STREAM_PARAMETERS
#define DLOG_ASSERT(condition) EAT_STREAM_PARAMETERS

#endif  // ENABLE_DLOG

enum { DEBUG_MODE = ENABLE_DLOG };

#undef ENABLE_DLOG

#undef assert
#define assert(x) DLOG_ASSERT(x)

class LogMessage {
 public:
  LogMessage(const char* file, int line, LogSeverity severity);

  ~LogMessage();

  std::ostream& stream() { return stream_; }

  LogSeverity severity() { return severity_; }
  std::string str() { return stream_.str(); }

 private:
  void Init(const char* file, int line);

  LogSeverity severity_;
  std::ostringstream stream_;
  size_t message_start_;

  const char* file_;
  const int line_;
};

class LogMessageVoidify {
 public:
  LogMessageVoidify() { }
  void operator&(std::ostream&) { }
};

void CloseLogFile();

std::ostream& operator<<(std::ostream& out, const wchar_t* wstr);
inline std::ostream& operator<<(std::ostream& out, const std::wstring& wstr) {
  return out << wstr.c_str();
}

#endif  // LOGGING_H_

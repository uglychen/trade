#include "logging.h"

#include <sys/time.h>
#include <time.h>
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <string>
#include <iomanip>
#include <ostream>

const char* const log_severity_names[LOG_NUM_SEVERITIES] = {
  "INFO", "WARNING", "ERROR", "FATAL" };

const char* log_severity_name(int severity) {
  if (severity >= 0 && severity < LOG_NUM_SEVERITIES)
    return log_severity_names[severity];
  return "UNKNOWN";
}

int g_min_log_level = 0;

LoggingDestination g_logging_destination = LOG_DEFAULT;

const int kAlwaysPrintErrorLevel = LOG_ERROR;

std::string* g_log_file_name = NULL;
std::string* g_log_date = NULL;

FILE* g_log_file = NULL;

bool g_log_process_id = false;
bool g_log_thread_id = false;
bool g_log_timestamp = true;
bool g_log_tickcount = false;

LogAssertHandlerFunction log_assert_handler = NULL;
LogMessageHandlerFunction log_message_handler = NULL;

int32_t CurrentProcessId() {
  return getpid();
}

uint64_t TickCount() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);

  uint64_t absolute_micro = static_cast<int64_t>(ts.tv_sec) * 1000000 +
                            static_cast<int64_t>(ts.tv_nsec) / 1000;

  return absolute_micro;
}

void DeleteFilePath(const std::string& log_name) {
  unlink(log_name.c_str());
}

std::string GetDefaultLogFile() {
  return std::string("debug.log");
}

std::string GetCurrentDate() {
  char buffer[16];
  struct tm gmt;
  time_t sec;
  timeval tv;

  gettimeofday(&tv, NULL);
  sec = tv.tv_sec;
  localtime_r(&sec, &gmt);
  snprintf(buffer, sizeof(buffer), "%4d%02d%02d", gmt.tm_year + 1900, gmt.tm_mon + 1, gmt.tm_mday);

  return std::string(buffer);
}

bool InitializeLogFileHandle() {
  std::string date = GetCurrentDate();
  if (g_log_date && *g_log_date != date) {
    CloseLogFile();
    delete g_log_date;
    g_log_date = NULL;
  }

  if (g_log_file)
    return true;

  if (!g_log_file_name) {
    g_log_file_name = new std::string(GetDefaultLogFile());
  }

  if (!g_log_date) {
    g_log_date = new std::string(date);
  }

  if ((g_logging_destination & LOG_TO_FILE) != 0) {
    char file_name[1024];
    snprintf(file_name, sizeof(file_name), "%s-%s", g_log_file_name->c_str(), g_log_date->c_str());
    g_log_file = fopen(file_name, "a");
    if (g_log_file == NULL)
      return false;
  }

  return true;
}

void CloseLogFile() {
  if (!g_log_file)
    return;

  fclose(g_log_file);
  g_log_file = NULL;
}

std::ostream* g_swallow_stream;

LoggingSettings::LoggingSettings()
    : logging_dest(LOG_DEFAULT),
      log_file(NULL),
      delete_old(APPEND_TO_OLD_LOG_FILE) {}

bool BaseInitLoggingImpl(const LoggingSettings& settings) {
  g_logging_destination = settings.logging_dest;

  if ((g_logging_destination & LOG_TO_FILE) == 0)
    return true;

  CloseLogFile();

  if (!g_log_file_name)
    g_log_file_name = new std::string();
  *g_log_file_name = settings.log_file;
  if (settings.delete_old == DELETE_OLD_LOG_FILE)
    DeleteFilePath(*g_log_file_name);

  return InitializeLogFileHandle();
}

void SetMinLogLevel(int level) {
  g_min_log_level = std::min(LOG_FATAL, level);
}

int GetMinLogLevel() {
  return g_min_log_level;
}

bool ShouldCreateLogMessage(int severity) {
  if (severity < g_min_log_level)
    return false;

  return g_logging_destination != LOG_NONE || log_message_handler ||
         severity >= kAlwaysPrintErrorLevel;
}

void SetLogItems(bool enable_process_id, bool enable_thread_id,
                 bool enable_timestamp, bool enable_tickcount) {
  g_log_process_id = enable_process_id;
  g_log_thread_id = enable_thread_id;
  g_log_timestamp = enable_timestamp;
  g_log_tickcount = enable_tickcount;
}

void SetLogAssertHandler(LogAssertHandlerFunction handler) {
  log_assert_handler = handler;
}

void SetLogMessageHandler(LogMessageHandlerFunction handler) {
  log_message_handler = handler;
}

LogMessageHandlerFunction GetLogMessageHandler() {
  return log_message_handler;
}

LogMessage::LogMessage(const char* file, int line, LogSeverity severity)
    : severity_(severity), file_(file), line_(line) {
  Init(file, line);
}

LogMessage::~LogMessage() {
  stream_ << std::endl;
  std::string str_newline(stream_.str());

  if (log_message_handler &&
    log_message_handler(severity_, file_, line_,
                        message_start_, str_newline)) {
    return;
  }

  if ((g_logging_destination & LOG_TO_SYSTEM_DEBUG_LOG) != 0) {
    fwrite(str_newline.data(), str_newline.size(), 1, stderr);
    fflush(stderr);
  } else if (severity_ >= kAlwaysPrintErrorLevel) {
    fwrite(str_newline.data(), str_newline.size(), 1, stderr);
    fflush(stderr);
  }

  if ((g_logging_destination & LOG_TO_FILE) != 0) {
    if (InitializeLogFileHandle()) {
      fwrite(str_newline.data(), str_newline.size(), 1, g_log_file);
      fflush(g_log_file);
    }
  }

  if (severity_ == LOG_FATAL) {
    if (log_assert_handler) {
      log_assert_handler(std::string(stream_.str()));
    }
  }
}

void LogMessage::Init(const char* file, int line) {
  std::string filename(file);
  size_t last_slash_pos = filename.find_last_of("\\/");
  if (last_slash_pos != std::string::npos)
    filename.substr(last_slash_pos + 1);

  stream_ <<  '[';
  if (g_log_timestamp) {
    timeval tv;
    gettimeofday(&tv, NULL);
    time_t t = tv.tv_sec;
    struct tm local_time = {0};
    localtime_r(&t, &local_time);
    struct tm* tm_time = &local_time;
    stream_ << std::setfill('0')
            << std::setw(2) << 1 + tm_time->tm_mon
            << std::setw(2) << tm_time->tm_mday
            << '/'
            << std::setw(2) << tm_time->tm_hour
            << std::setw(2) << tm_time->tm_min
            << std::setw(2) << tm_time->tm_sec
            << '.'
            << std::setw(6) << tv.tv_usec
            << ':';
  }
  if (g_log_tickcount)
    stream_ << TickCount() << ':';
  if (severity_ >= 0)
    stream_ << log_severity_name(severity_);

  stream_ << ":" << filename << "(" << line << ")] ";

  message_start_ = stream_.str().length();
}

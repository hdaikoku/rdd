//
// Created by Harunobu Daikoku on 2016/11/29.
//

#ifndef LOGGER_LOGGER_H
#define LOGGER_LOGGER_H

#include "spdlog/spdlog.h"

class Logger {
 public:
  Logger(const std::string &log_tag)
      : logger_(std::make_shared<spdlog::logger>(log_tag, spdlog::sinks::stderr_sink_mt::instance())) {
    logger_->set_pattern("[%l] %C/%m/%d %T %n: %v");
#ifdef DEBUG
    logger_->set_level(spdlog::level::debug);
#endif //DEBUG
  }

 protected:
  void LogError(const std::string &msg) const {
    logger_->error(msg);
  }

  void LogError(int error_num) const {
    logger_->error(spdlog::details::os::errno_str(error_num));
  }

  void LogInfo(const std::string &msg) const {
    logger_->info(msg);
  }

  void LogDebug(const std::string &msg) const {
    logger_->debug(msg);
  }

  const std::string &GetLogTag() const {
    return logger_->name();
  }

 private:
  std::shared_ptr<spdlog::logger> logger_;
};

#endif //LOGGER_LOGGER_H

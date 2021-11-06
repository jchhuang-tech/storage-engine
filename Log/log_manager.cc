/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II Course Project, Fall 2021
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 */
#include <fcntl.h>
#include <unistd.h>
#include "log_manager.h"

namespace yase {

LogManager *LogManager::instance = nullptr;

LogManager::LogManager(const char *log_filename, uint32_t logbuf_kb) {
  // Initialize the log manager:
  // 1. Create a log file according to the given parameters; if a the file
  //    already exists, truncate it.
  // 2. Allocate a log buffer according to the given parameter. Note the size is
  //    given in kilobytes (1024 bytes).
  // 3. Initialize durable, current LSN and log buffer offset and other member
  //    variables properly
  // 4. Any error here is fatal.
  //
  // TODO: Your implementation.
  int ret = open(log_filename, (O_CREAT | O_RDWR | O_TRUNC), (S_IRWXU | S_IRWXG | S_IRWXO));
  LOG_IF(FATAL, ret < 0) << "error: open failed";
  fd = ret;
  logbuf_size = logbuf_kb * 1024;
  logbuf = (char*)malloc(logbuf_size);
  logbuf_offset = 0;

  durable_lsn = 0;
  current_lsn = 0;
};

LogManager::~LogManager() {
  // Flush the log buffer, and deallocate any dynamically allocated memory (if
  // any).
  //
  // TODO: Your implementation.
  ssize_t ret = pwrite(fd, logbuf, logbuf_size, logbuf_offset); // not sure about this
  LOG_IF(FATAL, ret < 0) << "error";

  ret = fsync(fd);
  LOG_IF(FATAL, ret < 0) << "error";
  
  free(logbuf);
  ret = close(fd);
  LOG_IF(FATAL, ret < 0) << "error";
}

bool LogManager::LogInsert(RID rid, const char *record, uint32_t length) {
  // TODO: Your implementation.
  return false;
}

bool LogManager::LogUpdate(RID rid, const char *record, uint32_t length) {
  // TODO: Your implementation.
  return false;
}

bool LogManager::LogDelete(RID rid) {
  // TODO: Your implementation.
  return false;
}

bool LogManager::LogCommit(uint64_t tid) {
  // TODO: Your implementation.
  return false;
}

bool LogManager::LogAbort(uint64_t tid) {
  // TODO: Your implementation.
  return false;
}

bool LogManager::LogEnd(uint64_t tid) {
  // TODO: Your implementation.
  return false;
}

bool LogManager::Flush() {
  // 1. Flush the log buffer to log file when holding the log buffer latch.
  // 2. After flushing, adjust the durable LSN to be the same as current LSN, and
  //    reset the log buffer offset so future log record space reservations can
  //    proceed.
  //
  // TODO: Your implementation.
  return false;
}

}  // namespace yase

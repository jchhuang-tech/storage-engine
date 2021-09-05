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
#include "basefile.h"

namespace yase {

// Initialize valid file ID to start from 1
std::atomic<uint16_t> BaseFile::next_file_id(1);

BaseFile::BaseFile(std::string name) {
  // Example error handling code:
  // - Using glog to throw a fatal error if ret is less than 0 with a message "error"
  //   LOG_IF(FATAL, ret < 0) << "error";
  // - Using abort system call:
  //   if (ret < 0) {
  //     abort();
  //   }
  //
  // TODO: Your implementation
}

BaseFile::~BaseFile() {
  // TODO: Your implementation
}

bool BaseFile::FlushPage(PageId pid, void *page) {
  // TODO: Your implementation
  return false;
}

bool BaseFile::LoadPage(PageId pid, void *out_buf) {
  // TODO: Your implementation
  return false;
}

PageId BaseFile::CreatePage() {
  // TODO: Your implementation
  PageId pid;
  return pid;
}

}  // namespace yase

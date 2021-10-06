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
#include <cstring> // memcpy

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
  file_id = next_file_id.fetch_add(1, std::memory_order_relaxed);

  page_count = 0;

  int ret = open(name.c_str(), (O_CREAT | O_RDWR | O_TRUNC), (S_IRWXU | S_IRWXG | S_IRWXO));
  LOG_IF(FATAL, ret < 0) << "error";
  fd = ret;
}

BaseFile::~BaseFile() {
  // TODO: Your implementation
  int ret = fsync(fd);
  LOG_IF(FATAL, ret < 0) << "error";

  ret = close(fd);
  LOG_IF(FATAL, ret < 0) << "error";
}

bool BaseFile::FlushPage(PageId pid, void *page) {
  // TODO: Your implementation
  if(!pid.IsValid()){
    return false;
  }
  off_t offset = pid.GetPageNum() * PAGE_SIZE;
  ssize_t ret = pwrite(fd, page, PAGE_SIZE, offset);
  if (ret < 0) {
    return false;
  }
  ret = fsync(fd);
  if (ret < 0) {
    return false;
  }
  return true;
}

bool BaseFile::LoadPage(PageId pid, void *out_buf) {
  // TODO: Your implementation
  // LOG(ERROR) << "LoadPage 1 " << strerror(errno);
  if (!pid.IsValid()){
    return false;
  }
  // LOG(ERROR) << "LoadPage 2 " << strerror(errno);
  off_t offset = pid.GetPageNum() * PAGE_SIZE;
  // LOG(ERROR) << "LoadPage 3 " << strerror(errno);
  ssize_t ret = pread(fd, out_buf, PAGE_SIZE, offset);
  // LOG(ERROR) << "LoadPage 4 " << strerror(errno);
  if (ret != PAGE_SIZE) {
    return false;
  }
  // LOG(ERROR) << "LoadPage 5 " << strerror(errno);
  return true;
}

PageId BaseFile::CreatePage() {
  // TODO: Your implementation
  PageId pid;
  pid = PageId(file_id, page_count.fetch_add(1, std::memory_order_relaxed));
  unsigned char buffer[PAGE_SIZE] = {0};
  std::memcpy(buffer, &(pid.value), sizeof(pid.value));
  if (!FlushPage(pid, buffer)){
    return PageId();
  }
  return pid;
}

}  // namespace yase

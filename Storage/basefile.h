/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II Course Project, Fall 2021
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 */
#pragma once

#include <atomic>
#include <mutex>
#include "../yase_internal.h"

namespace yase {

// Low-level primitives for reading and writing pages
class BaseFile {
 public:
  BaseFile() {}
  BaseFile(std::string name);
  ~BaseFile();

  // Write a page to storage
  bool FlushPage(PageId pid, void *page);

  // Load a page from storage
  bool LoadPage(PageId pid, void *out_buf);

  // Create a new page in the file permanently, returns the ID of the new page
  PageId CreatePage();

  // Return the ID of this file
  inline uint16_t GetId() { return file_id; }

  // Return the number of created pages
  inline uint32_t GetPageCount() { return page_count; }

  //Return the file descriptor
  inline int GetFd() { return fd; }

 protected:
  // ID of this file
  uint16_t file_id;

  // Number of pages the file currently has, including page and dir pages
  std::atomic<uint32_t> page_count;

  // File descriptor of the underlying file
  int fd;

  // Static global file ID counter
  static std::atomic<uint16_t> next_file_id;
};

}  // namespace yase

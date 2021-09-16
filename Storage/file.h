/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II, Fall 2021
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 */
#pragma once

#include "../yase_internal.h"
#include "basefile.h"
#include "page.h"

namespace yase {

// Underlying structure of Table to read/write data
class File : public BaseFile {
 public:
  File(std::string name, uint16_t record_size);
  ~File();

  // Allocate a new data page
  // Returns the Page ID of the allocated page
  // If no page is allocated, return an invalid PageId
  PageId AllocatePage();

  // Deallocate an existing page
  // @pid: ID of the page to be deallocated
  // Returns true/false if the page is deallocated/already deallocated
  bool DeallocatePage(PageId pid);

  // Return a pointer to the dir basefile object
  inline BaseFile *GetDir() { return &dir; }

  // Return true if the specified page is allocated (i.e., "exists")
  bool PageExists(PageId pid);

  // Try to find a previously-deallocated page, and allocate it
  // Returns invalid PageId if no such page is found.
  PageId ScavengePage();

 private:
  // BaseFile for managing directory pages
  BaseFile dir;

  // Record size supported by data pages in this file
  uint16_t record_size;
};

}  // namespace yase

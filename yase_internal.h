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

#include <glog/logging.h>

namespace yase {

// Page ID - a 64-bit integer
struct PageId {
  // Represents an Invalid Page ID
  static const uint64_t kInvalidValue = ~uint64_t{0};

  // Structure of the Page ID:
  // ---16 bits---|---24 bits---|---24 bits---|
  //    File ID   |   Page Num  |    Unused   |
  uint64_t value;

  // Constructors
  PageId() : value(kInvalidValue) {}
  PageId(uint16_t file_id, uint32_t page_num) {
    value = (uint64_t) file_id << 48u | (uint64_t) page_num << 24u;
  }
  explicit PageId(uint64_t value) : value(value) {
    LOG_IF(FATAL, (value & 0xffffffu) != 0) << "invalid page_id";
  }

  inline bool IsValid() { return value != kInvalidValue; }
  inline uint32_t GetPageNum() const { return (value << 16u) >> 40u; }
  inline uint16_t GetFileID() const { return value >> 48u; }
  inline bool operator()(const PageId &l, const PageId &r) const { return l.value < r.value; }
};

// Record ID - a 64-bit integer
struct RID {
  // Represents an invalid RID
  static const uint64_t kInvalidValue = ~uint64_t{0};

  // Structure of the RID:
  // ---16 bits---|---24 bits---|---24 bits---|
  //    File ID   |   Page Num  |   Slot Num  |
  uint64_t value;

  // Constructors
  explicit RID(uint64_t value) : value(value) {}
  explicit RID() : value(kInvalidValue) {}
  explicit RID(PageId page_id, uint32_t slot_num) { value = page_id.value | slot_num; }

  inline PageId GetPageId() { return PageId{value & (~(uint64_t) 0xffffffu)}; }
  inline uint32_t GetSlotNum() { return (value << 40u) >> 40u; }
  inline bool IsValid() { return value != kInvalidValue; }

  static inline RID MakeRid(PageId page_id, uint32_t slot_num) {
    return RID(page_id.value | slot_num);
  }
};
}  // namespace yase

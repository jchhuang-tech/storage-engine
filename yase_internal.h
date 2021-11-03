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
#include <list>
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

struct Transaction {
  static std::atomic<uint64_t> ts_counter;
  const static uint32_t kStateCommitted = 1;
  const static uint32_t kStateInProgress = 2;
  const static uint32_t kStateAborted = 3;
  const static uint64_t kInvalidTimestamp = ~uint64_t{0};

  // Transaction timestamp. Smaller == older.
  std::atomic<uint64_t> timestamp;

  // List of the RID of records which the transaction currently locked.
  // Other implementations may maintain a list of LockRequest references;
  // for this project we use RIDs.
  std::list<RID> locks;

  // State of the transaction, must be one of the kState* values
  uint32_t state;

  Transaction() : timestamp(ts_counter.fetch_add(1)), state(kStateInProgress) {}
  ~Transaction() {}

  // Abort the transaction
  // Returns the timestamp of this transaction.
  // If the operation fails, return invalid timestamp (kInvalidTimestamp)
  uint64_t Abort();

  // Commit the transaction
  // Returns true/false if the operation succeeded/failed.
  bool Commit();

  inline uint64_t GetTimestamp() { return timestamp; }
  inline bool IsAborted() { return state == kStateAborted; }
  inline bool IsCommitted() { return state == kStateCommitted; }
  inline bool IsInProgress() { return state == kStateInProgress; }
};
}  // namespace yase

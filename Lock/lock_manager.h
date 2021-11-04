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
#include <algorithm>
#include <list>
#include <memory>
#include <map>
#include <mutex>
#include <unordered_map>

#include "../yase_internal.h"

namespace yase {

struct LockRequest {
  // Lock modes
  enum Mode {
    NL, // Not locked
    XL, // Exclusive lock
    SH, // Shared lock
  };

  // Requested mode
  uint64_t mode;

  // The transaction that's requesting the lock
  Transaction *requester;

  // Whethert the lock is granted to [requester]
  std::atomic<bool> granted;

  LockRequest() : mode(Mode::NL), requester(nullptr), granted(false) {}
  LockRequest(Transaction *t) : mode(Mode::NL), requester(t), granted(false) {}
  LockRequest(Transaction *t, Mode m, bool granted) : mode(m), requester(t), granted(granted) {}
  ~LockRequest() {}
};

struct LockHead {
  // Current mode of the lock
  LockRequest::Mode current_mode;

  // Request queue
  std::list<LockRequest> requests;

  // Latch protecting the lock head (inluding the request queue)
  std::mutex latch;

  LockHead() : current_mode(LockRequest::Mode::NL) {}
  ~LockHead() {}
};

class LockManager {
 private:
  static LockManager *instance;

 public:
  enum DeadlockPolicy {
    NoWait,
    WaitDie,
  };

  inline static LockManager *Get() { return instance; }
  inline static void Initialize(DeadlockPolicy policy) {
    LockManager::instance = new LockManager(policy);
  }
  inline static void Uninitialize() {
    delete LockManager::instance;
    LockManager::instance = nullptr;
  }

  LockManager(DeadlockPolicy policy);
  LockManager(LockManager const &) = delete;
  void operator=(LockManager const &) = delete;
  ~LockManager();
  inline DeadlockPolicy GetDeadlockPolicy() { return ddl_policy; }
  inline std::unordered_map<uint64_t, LockHead *> &GetLockTableRef() { return lock_table; }

  // Request to lock a record
  // @tx: the transaction that is trying to lock a record
  // @rid: reference to the RID of the record to be locked
  // @mode: requested lock mode, must be XL or SH
  // Returns true if the lock is acquired, false otherwise
  bool AcquireLock(Transaction *tx, RID &rid, LockRequest::Mode mode);

  // Release a lock being held by the calling transaction
  // @tx: the transaction that is trying to release the lock on a record
  // @rid: reference to the RID of the locked record
  // Returns true if the release operation succeeded, false otherwise
  bool ReleaseLock(Transaction *tx, RID &rid);

 private:
  // Lock table that maps RIDs (RID.value) to lock heads
  std::unordered_map<uint64_t, LockHead *> lock_table;

  // Latch protecting the lock table
  std::mutex latch;

  // Deadlock prevention policy
  DeadlockPolicy ddl_policy;
};

}  // namespace yase

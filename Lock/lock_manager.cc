/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II Course Project, Fall 2021
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 */
#include <Lock/lock_manager.h>
#include <Log/log_manager.h>

namespace yase {

// Timestamp counter
std::atomic<uint64_t> Transaction::ts_counter(0);

// Static global lock manager instance
LockManager *LockManager::instance = nullptr;

LockManager::LockManager(DeadlockPolicy policy) {
  // Initialize the lock manager with the specified deadlock handling policy
  //
  // TODO: Your implementation
}

LockManager::~LockManager() {
  // Free all the dynamically allocated lock heads
  //
  // Note: another design is to remove the lock head from the lock table whenever
  // there is no lock holders. You may follow either approach.
  //
  // TODO: Your implementation
}

bool LockManager::AcquireLock(Transaction *tx, RID &rid, LockRequest::Mode mode) {
  // 1. Take the latch that's protecting the lock table
  // 2. Look up the lock head for the given RID in the lock table:
  //    (a) If it exists in the lock table, release the lock table latch and
  //        enqueue (note: need to take the latch in lock head)
  //    (b) If it doesn't exist, create a new item in the lock table and the lock
  //        is granted. Set the proper fields in the lock head, release lock
  //        table latch and return true.
  //    (c) Lock table latch should be released after latching the lock head
  // 3. Check lock compatibility to see if the lock can be granted by looking at
  //    the predecessor in the request queue. 
  //    (a) If the predecessor has acquired the lock in a compatible mode, grant
  //        the lock.
  //    (b) Otherwise, if the *wait-die* policy is specified, use the wait-die
  //        algorithm to determine if the transaction given is allowed to wait, if
  //        not, remove tx from the queue. If the *no-wait* policy is specified,
  //        give up the request and return false directly (the caller then can 
  //        choose to abort the transaction).
  // 4. Release the lock head latch
  // 5. Add the lock request to the transactions list of lock requests if the
  //    lock is granted.
  // 6. Return true/false if the lock is granted/not granted.
  //
  // Note: you may find there are ways to optimize the above procedure, e.g., by
  // delaying the enqueue until you find the transaction is allowed to wait. You
  // are allowed to implement such optimizations; the above procedure is for your
  // reference to clarify the steps needed for this function.
  //
  // TODO: Your implementation
  return false;
}

bool LockManager::ReleaseLock(Transaction *tx, RID &rid) {
  // 1. Latch the lock table and look up the given RID
  // 2. Unlatch the lock table, latch the lock head, traverse the lock queue to
  //    remove the node belonging to the given transaction
  // 3. Check and (if allowed) grant the next requester the lock
  // 4. Unlatch the lock head and remove the lock from list of locks in
  //    transation 
  //
  // Note - Important error handling cases:
  // 1. If the RID doesn't exist in the lock table, return false directly.
  // 2. If the RID is not locked, return false.
  // 3. Return true only if the release operation succeeded.
  //
  // TODO: Your implementation
  return false;
}

bool Transaction::Commit() {
  // 1. Insert commit log record and flush log buffer
  // 2. Release all the locks held by this transaction
  // 3. Set transaction state to "committed" and return true
  //
  // TODO: Your implementation
  return false;
}

uint64_t Transaction::Abort() {
  // 1. Insert abortlog record and flush log buffer
  // 2. Iterate all the locks and release each of them
  // 3. Set transaction state to "aborted"
  // 4. Return the transaction's timestamp
  //
  // TODO: Your implementation
  return 0;
}

}  // namespace yase

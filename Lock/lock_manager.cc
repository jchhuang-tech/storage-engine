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
  ddl_policy = policy;
}

LockManager::~LockManager() {
  // Free all the dynamically allocated lock heads
  //
  // Note: another design is to remove the lock head from the lock table whenever
  // there is no lock holders. You may follow either approach.
  //
  // TODO: Your implementation
  for (auto i : lock_table){
    delete i.second;
  }
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
  if (!tx){
    return false;
  }

  if (mode == LockRequest::NL){
    return true;
  }

  latch.lock();
  if (lock_table.count(rid.value)){
    latch.unlock();
    struct LockHead* lock_head = lock_table[rid.value];
    lock_head->latch.lock();
    // if the queue is empty, enqueue and grant it
    if (lock_head->requests.empty()){
      lock_head->requests.emplace_back(tx, mode, true);
      lock_head->current_mode = mode;
      lock_head->latch.unlock();
      tx->locks.push_back(rid);
      return true;
    }
    // if the same transaction has other requests with high permissions in the queue, don't enqueue
    for (auto const& i : lock_head->requests) {
      if (i.requester == tx && (i.mode == mode || i.mode == LockRequest::XL)){
        lock_head->latch.unlock();
        return i.granted;
      }
    }

    LockRequest* pred_req = &lock_head->requests.back();
    lock_head->requests.emplace_back(tx, mode, false);
    LockRequest* cur_req = &lock_head->requests.back();
    if (pred_req->granted && pred_req->mode != LockRequest::XL && cur_req->mode != LockRequest::XL){
      cur_req->granted = true;
      lock_head->current_mode = mode;
      lock_head->latch.unlock();
      tx->locks.push_back(rid);
      return true;
    } else {
      if (ddl_policy == WaitDie) {
        if (cur_req->requester->timestamp < pred_req->requester->timestamp) { // higher priority than predecessor
          lock_head->latch.unlock();
          while (!cur_req->granted){
            // busy wait
          }
          lock_head->latch.lock();
          lock_head->current_mode = mode;
          lock_head->latch.unlock();
          tx->locks.push_back(rid);
          return true;
        } else { // lower priority
          lock_head->requests.pop_back();
          lock_head->latch.unlock();
          return false;
        }
      } else {
        lock_head->requests.pop_back();
        lock_head->latch.unlock();
        return false;
      }
    }
  } else { // doesn't exist in table
    struct LockHead* lock_head = new LockHead();
    lock_head->latch.lock();
    lock_head->requests.emplace_back(tx, mode, true);
    lock_head->current_mode = mode;
    tx->locks.push_back(rid);
    lock_table[rid.value] = lock_head;
    lock_head->latch.unlock();
    latch.unlock();
    return true;
  }
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
  if (!tx){
    return false;
  }
  
  latch.lock();
  // if RID is not in lock table
  if (lock_table.count(rid.value) == 0){
    latch.unlock();
    return false;
  }
  struct LockHead* lock_head = lock_table[rid.value];
  latch.unlock();
  lock_head->latch.lock();
  // if RID is not locked
  if (lock_head->current_mode == LockRequest::NL){
    lock_head->latch.unlock();
    return false;
  }

  bool is_rid_tx_locked = false;
  for (auto tx_it = tx->locks.begin(); tx_it != tx->locks.end(); tx_it++){
    if (tx_it->value == rid.value){
      is_rid_tx_locked = true;
      break;
    }
  }
  if (!is_rid_tx_locked){
    lock_head->latch.unlock();
    return false;
  }
    
  for (auto cur_it = lock_head->requests.begin(); cur_it != lock_head->requests.end(); cur_it++){
    if (cur_it->requester == tx && cur_it->granted){
      auto next_it = std::next(cur_it, 1);
      if (next_it != lock_head->requests.end()){
        if (cur_it->mode == LockRequest::XL && next_it->mode == LockRequest::XL){
          next_it->granted = true;
        } else if (cur_it->mode == LockRequest::XL && next_it->mode == LockRequest::SH){
          for (auto it2 = next_it; it2 != lock_head->requests.end(); it2++){
            if (it2->mode == LockRequest::XL){
              break;
            } else {
              it2->granted = true;
            }
          }
        } else if (cur_it->mode == LockRequest::SH && next_it->mode == LockRequest::XL){
          if (cur_it == lock_head->requests.begin()){
            next_it->granted = true;
          }
        }
      }
      lock_head->requests.erase(cur_it);
      break; // erasing the iterator invalidates the iterator so we need to break
    }
  }
  if (lock_head->requests.empty()){
    lock_head->current_mode = LockRequest::NL;
  }
  
  lock_head->latch.unlock();
  for (auto tx_it = tx->locks.begin(); tx_it != tx->locks.end(); tx_it++){
    if (tx_it->value == rid.value){
      tx->locks.erase(tx_it);
      break;
    }
  }
  return true;
}

bool Transaction::Commit() {
  // 1. Insert commit log record and flush log buffer
  // 2. Release all the locks held by this transaction
  // 3. Set transaction state to "committed" and return true
  //
  // TODO: Your implementation
  LogManager* log_manager = LogManager::Get();
  if (!log_manager){
    return false;
  }
  bool ret = false;
  ret = log_manager->LogCommit(timestamp);
  if (!ret){
    return false;
  }
  log_manager->logbuf_latch.lock();
  ret = log_manager->Flush();
  log_manager->logbuf_latch.unlock();
  if (!ret){
    return false;
  }
  ret = log_manager->LogEnd(timestamp);
  if (!ret){
    return false;
  }
  while (!locks.empty()){
    ret = LockManager::Get()->ReleaseLock(this, *locks.begin());
    if (!ret){
      return false;
    }
  }
  state = kStateCommitted;
  return true;
}

uint64_t Transaction::Abort() {
  // 1. Insert abortlog record and flush log buffer
  // 2. Iterate all the locks and release each of them
  // 3. Set transaction state to "aborted"
  // 4. Return the transaction's timestamp
  //
  // TODO: Your implementation
  LogManager* log_manager = LogManager::Get();
  if (!log_manager){
    return kInvalidTimestamp;
  }
  bool ret = false;
  ret = log_manager->LogAbort(timestamp);
  if (!ret){
    return kInvalidTimestamp;
  }
  log_manager->logbuf_latch.lock();
  ret = log_manager->Flush();
  log_manager->logbuf_latch.unlock();
  if (!ret){
    return kInvalidTimestamp;
  }
  ret = log_manager->LogEnd(timestamp);
  if (!ret){
    return kInvalidTimestamp;
  }
  while (!locks.empty()){
    ret = LockManager::Get()->ReleaseLock(this, *locks.begin());
    if (!ret){
      return kInvalidTimestamp;
    }
  }
  state = kStateAborted;
  return timestamp;
}

}  // namespace yase

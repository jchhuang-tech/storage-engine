/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II Course Project, Fall 2021
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 *
 * Benchmarking framework implementation.
 */

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "simple_bench.h"
#include <random>

// Command line arguments defined using gflags: 
DEFINE_uint64(threads, 8, "Number of worker threads");
DEFINE_uint64(seconds, 10, "Number of seconds to run the benchmark");
DEFINE_uint64(table_size, 10000, "Number of records to load in table");
DEFINE_uint64(bpool_pages, 50, "Number of buffer pool pages");
DEFINE_uint64(logbuf_kb, 1024, "Size of log buffer in KB");
DEFINE_string(logfile, "simple_bench.log", "Log file path and name");
DEFINE_string(tablefile, "simple_bench.data", "Table file path and name");

// Transaction mix
DEFINE_uint64(point_read_pct, 40, "Update percentage");
DEFINE_uint64(read_update_pct, 40, "Insert percentage");
DEFINE_uint64(scan_update_pct, 20, "Insert percentage");

namespace yase {

SimpleBench::SimpleBench() : PerformanceTest(FLAGS_threads, FLAGS_seconds) {
  // 1. Initialize benchmark including the base Performance Test class
  // 2. Create a table with the specified table file name (FLAGS_tablefile) that
  //    stores 8-byte records
  // 3. Create an in-memory skiplist index that supports 8-byte keys
  //
  // Note: use the provided [index] and [table] defined in pointers SimpleBench
  // structure.
  //
  // TODO: Your implementation
  table = new Table(FLAGS_tablefile, 8);
  index = new SkipList(8);
}

SimpleBench::~SimpleBench() {
  // Deallocate any dynamically allocated memory
  //
  // TODO: Your implementation
  delete table;
  delete index;
}

void SimpleBench::Load() {
  // Populate the table with the specified number of records (by the table_size
  // parameter). For each record, also setup the key-RID mapping in index. For
  // simplicity, the n-th record (count from 1) in the table should have a key
  // of n.
  //
  // For any error that happened during the load phase, abort the program and
  // output an error message.
  //
  // TODO: Your implementation
  for (uint64_t i = 1; i <= FLAGS_table_size; i++) {
    RID rid = table->Insert((char*)&i);
    LOG_IF(ERROR, !rid.IsValid()) << "error";
    bool ret = index->Insert((char*)&i, rid);
    LOG_IF(ERROR, !ret) << "error";
  }
}

void SimpleBench::WorkerRun(uint32_t thread_id) {
  // 1. Mark self as ready using the thread start barrier
  // 2. Wait (busy-spin) for other threads to become ready using the benchmark start barrier
  // 3. Do real work until [shutdown] is set, by picking one of the three transactions to
  //    run following the percentage specified by [point_read_pct],
  //    [read_update_pct] and [scan_update_pct]. The three parameters should add
  //    up to 100.
  // 4. For each committed/aborted transaction, increment the
  //    [ncommits]/[naborts] stats using [thread_id]
  //
  // TODO: Your implementation
  this->thread_start_barrier++;
  while (!this->bench_start_barrier) {
    // busy spin
  }

  while (!shutdown) {
    uint64_t rand_num = rand() % 100 + 1;
    if (rand_num <= FLAGS_point_read_pct) {
      TxPointRead() ? ncommits[thread_id]++ : naborts[thread_id]++;
    } else if (rand_num > FLAGS_point_read_pct && rand_num <= FLAGS_point_read_pct + FLAGS_read_update_pct) {
      TxReadUpdate() ? ncommits[thread_id]++ : naborts[thread_id]++;
    } else if (rand_num > FLAGS_point_read_pct + FLAGS_read_update_pct ) {
      TxScanUpdate() ? ncommits[thread_id]++ : naborts[thread_id]++;
    }
  }
}

bool SimpleBench::TxPointRead() {
  // Transaction profile:
  // 0. Start a transaction
  // 1. Read ten randomly-chosen records from the underlying table using index
  //    and table interfaces. 
  // 2. Commit the transaction
  //
  // Notes:
  // 1. If there an error occured in the above steps, abort the transaction and
  //    return false
  // 2. Follow S2PL when reading/modifying recods.
  //
  // TODO: Your implementation
  yase::Transaction t;
  auto *lockmgr = yase::LockManager::Get();

  char* out_buf = (char*)malloc(8);
  bool success = true;
  for (int i = 0; i < 10; i++) {
    uint64_t rand_key = rand() % FLAGS_table_size + 1;
    RID rid = index->Search((char*)&rand_key);
    if (!rid.IsValid()) {
      success = false;
      break;
    }
    bool ret = lockmgr->AcquireLock(&t, rid, LockRequest::Mode::SH);
    if (!ret) {
      success = false;
      break;
    }
    ret = table->Read(rid, out_buf);
    if (!ret) {
      success = false;
      break;
    }
  }

  free(out_buf);
  if (success) {
    bool t_ret = t.Commit();
    LOG_IF(ERROR, !t_ret) << "error";
    return true;
  } else {
    bool t_ret = t.Abort();
    LOG_IF(ERROR, !t_ret) << "error";
    return false;
  }
}

bool SimpleBench::TxReadUpdate() {
  // Transaction profile:
  // Read and update 10 randomly-chosen records from the underlying table. 
  // 0. Start a transaction
  // 1. Randomly pick a key
  // 2. Read the record represented by the key (using the index and table
  //    interfaces)
  // 3. Update the record by incrementing the content by one (interpret it as
  //    interger) 
  // 4. Write the record back
  // 5. Repeat 1-4 for ten times.
  // 6. Commit the transaction
  //
  // Notes:
  // 1. If there an error occured in the above steps, abort the transaction and
  //    return false
  // 2. Follow S2PL when reading/modifying recods.
  //
  // TODO: Your implementation
  yase::Transaction t;
  auto *lockmgr = yase::LockManager::Get();

  char* out_buf = (char*)malloc(8);
  bool success = true;
  for (int i = 0; i < 10; i++) {
    uint64_t rand_key = rand() % FLAGS_table_size + 1;
    RID rid = index->Search((char*)&rand_key);
    if (!rid.IsValid()) {
      success = false;
      break;
    }
    bool ret = lockmgr->AcquireLock(&t, rid, LockRequest::Mode::XL);
    if (!ret) {
      success = false;
      break;
    }
    ret = table->Read(rid, out_buf);
    if (!ret) {
      success = false;
      break;
    }
    uint64_t new_value = *(uint64_t*)out_buf + 1;
    ret = table->Update(rid, (char*)&new_value);
    if (!ret) {
      success = false;
      break;
    }
  }

  free(out_buf);
  if (success) {
    bool t_ret = t.Commit();
    LOG_IF(ERROR, !t_ret) << "error";
    return true;
  } else {
    bool t_ret = t.Abort();
    LOG_IF(ERROR, !t_ret) << "error";
    return false;
  }
}

bool SimpleBench::TxScanUpdate() {
  // Transaction profile:
  // Scan a range of randomly chosen keys, and update randomly chosen keys in
  // the scan result.
  // 0. Start a transaction
  // 1. Randomly pick a start key in the range of loaded records, and scan
  //    length (up to 20); issue an inclusive scan using the index
  // 2. If the returned number of records is less than the requested in step 1,
  //    update all the returned records, otherwise randomly pick five records
  //    from the scan result. The update should be done by incrementing the
  //    content by one (interpret it as interger) modify it and write it back.
  // 3. Commit the transaction
  //
  // Notes:
  // 1. If there an error occured in the above steps, abort the transaction and
  //    return false
  // 2. Follow S2PL when reading/modifying recods.
  // 3. Memory allocated as part of the scan operation should be recycled
  //    properly.
  //
  // TODO: Your implementation
  yase::Transaction t;
  auto *lockmgr = yase::LockManager::Get();

  uint64_t rand_key = rand() % 10000 + 1;
  uint32_t rand_n_keys = rand() % 20 + 1;
  std::vector<std::pair<char *, RID> > out_records;

  char* out_buf = (char*)malloc(8);
  bool success = true;
  index->ForwardScan((char*)&rand_key, rand_n_keys, true, &out_records);
  if (out_records.size() < rand_n_keys) {
    for (uint64_t i = 0; i < out_records.size(); i++) {
      RID rid = out_records[i].second;
      if (!rid.IsValid()) {
        success = false;
        break;
      }
      bool ret = lockmgr->AcquireLock(&t, rid, LockRequest::Mode::XL);
      if (!ret) {
        success = false;
        break;
      }
      ret = table->Read(rid, out_buf);
      if (!ret) {
        success = false;
        break;
      }
      uint64_t new_value = *(uint64_t*)out_buf + 1;
      ret = table->Update(rid, (char*)&new_value);
      if (!ret) {
        success = false;
        break;
      }
    }
  } else {
    for (uint64_t i = 0; i < 5; i++) {
      uint64_t pick = rand() % out_records.size();
      RID rid = out_records[pick].second;
      if (!rid.IsValid()) {
        success = false;
        break;
      }
      bool ret = lockmgr->AcquireLock(&t, rid, LockRequest::Mode::XL);
      if (!ret) {
        success = false;
        break;
      }
      ret = table->Read(rid, out_buf);
      if (!ret) {
        success = false;
        break;
      }
      uint64_t new_value = *(uint64_t*)out_buf + 1;
      ret = table->Update(rid, (char*)&new_value);
      if (!ret) {
        success = false;
        break;
      }
    }
  }

  free(out_buf);
  for (auto &r : out_records) {
    free(r.first);
  }
  
  if (success) {
    bool t_ret = t.Commit();
    LOG_IF(ERROR, !t_ret) << "error";
    return true;
  } else {
    bool t_ret = t.Abort();
    LOG_IF(ERROR, !t_ret) << "error";
    return false;
  }
}

}  // namespace yase

int main(int argc, char **argv) {
  ::google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Initialize buffer manager, lock manager and the log manager
  yase::BufferManager::Initialize(FLAGS_bpool_pages);
  yase::LockManager::Initialize(yase::LockManager::WaitDie);
  yase::LogManager::Initialize(FLAGS_logfile.c_str(), FLAGS_logbuf_kb);

  std::cout << "Setup: " << std::endl;
  std::cout << "  threads: " << FLAGS_threads << std::endl;
  std::cout << "  duration (s): " << FLAGS_seconds << "s" << std::endl;
  std::cout << "  buffer pool size (pages): " << FLAGS_bpool_pages << std::endl;
  std::cout << "  log buffer size (KB): " << FLAGS_logbuf_kb << std::endl;
  std::cout << "Transaction mix (%): " << std::endl;
  std::cout << "  point-read : " << FLAGS_point_read_pct << std::endl;
  std::cout << "  read-update: " << FLAGS_read_update_pct << std::endl;
  std::cout << "  scan-update: " << FLAGS_scan_update_pct << std::endl;

  yase::SimpleBench test;
  test.Load();
  test.Run();

  // Uninitialize buffer manager, lock manager and log manager
  yase::LockManager::Uninitialize();
  yase::BufferManager::Uninitialize();
  yase::LogManager::Uninitialize();

  return 0;
}


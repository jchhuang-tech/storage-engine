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

// Command line arguments defined using gflags: 
DEFINE_uint64(threads, 1, "Number of worker threads");
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

SimpleBench::SimpleBench() {
  // 1. Initialize benchmark including the base Performance Test class
  // 2. Create a table with the specified table file name (FLAGS_tablefile) that
  //    stores 8-byte records
  // 3. Create an in-memory skiplist index that supports 8-byte keys
  //
  // Note: use the provided [index] and [table] defined in pointers SimpleBench
  // structure.
  //
  // TODO: Your implementation
}

SimpleBench::~SimpleBench() {
  // Deallocate any dynamically allocated memory
  //
  // TODO: Your implementation
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
  return false;
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
  return false;
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
  return false;
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


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

#include <iostream>
#include <thread>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include <Index/skiplist.h>
#include <Storage/buffer_manager.h>
#include <Storage/table.h>
#include <Lock/lock_manager.h>
#include <Log/log_manager.h>

namespace yase {

struct PerformanceTest {
  // Constructor
  // @threads: number of benchmark worker threads
  // @seconds: benchmark duration in seconds
  PerformanceTest(uint32_t threads, uint32_t seconds);

  // Destructor
  ~PerformanceTest();

  // Virtual method interface for worker threads
  virtual void WorkerRun(uint32_t thread_id) = 0;

  // Entrance function to run a benchmark
  void Run();

  // One entry per thread to record the number of finished operations
  std::vector<uint64_t> ncommits;
  std::vector<uint64_t> naborts;

  // Benchmark start barrier: worker threads can only proceed if set to true
  std::atomic<bool> bench_start_barrier;

  // Thread start barrier: a counter of "ready-to-start" threads
  std::atomic<uint32_t> thread_start_barrier;

  // Whether the benchmark should stop and worker threads should shutdown 
  std::atomic<bool> shutdown;

  // List of all worker threads
  std::vector<std::thread *> workers;

  // Number of worker threads
  uint32_t nthreads;

  // Benchmark duration in seconds
  uint32_t seconds;
};

}  // namespace yase

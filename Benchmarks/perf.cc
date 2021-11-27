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

#include <iomanip>
#include "perf.h"

namespace yase {

PerformanceTest::PerformanceTest(uint32_t threads, uint32_t seconds) {
  // Initialize environment using passed in parameters and default values
  // TODO: Your implementation
}

PerformanceTest::~PerformanceTest() {
  // Destructor - nothing needed here
}

void PerformanceTest::Run() {
  // Entrance function to run benchmarks using worker threads.
  // 1. Start threads and initialize the commit/abort stats for each thread to 0
  // 2. Wait (busy-spin) for all threads to become ready using the thread start barrier
  // 3. Allow worker threads to go ahead (toggle benchmark start barrier)
  // 4. Sleep for the benchmark duration
  // 5. Issue a 'stop' signal to all threads using the shutdown variable and join all threads
  //
  // TODO: Your implementation

  // 6. Show stats
  std::cout << "=====================" << std::endl;
  std::cout << "Thread,Commits/s,Aborts/s: " << std::endl;

  std::cout << std::fixed;
  std::cout << std::setprecision(3);

  uint32_t total_commits = 0, total_aborts = 0;
  for (uint32_t i = 0; i < ncommits.size(); ++i) {
    std::cout << i << "," << ncommits[i] / (float)seconds << "," << naborts[i] / (float)seconds;
    total_commits += ncommits[i];
    total_aborts += naborts[i];
    std::cout << std::endl;
  }

  std::cout << "---------------------" << std::endl;
  std::cout << "All," << total_commits / (float)seconds << "," << total_aborts / (float)seconds << std::endl;
}

}  // namespace yase

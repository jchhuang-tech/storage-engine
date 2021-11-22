/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II Course Project, Fall 2021
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 *
 * A simple YCSB-like benchmark.
 */

#include "perf.h"

namespace yase {

struct SimpleBench : public PerformanceTest {
  // A transaction that reads records (point read)
  // Returns true if committed, false otherwise
  bool TxPointRead();

  // A transaction that does equal number of point reads and updates
  // Returns true if committed, false otherwise
  bool TxReadUpdate();

  // A trnsaction that does one scan and some updates
  // Returns true if committed, false otherwise
  bool TxScanUpdate();

  // Load the table to prepare for running benchmark tests
  void Load();

  // Benchmark worker function
  void WorkerRun(uint32_t thread_id) override;

  // Constructor
  SimpleBench();

  // Destructor
  ~SimpleBench();

  // Index structure
  SkipList *index;

  // Actual table structure
  Table *table;
};

}  // namespace yase


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

#include <gtest/gtest_prod.h>

#include "../yase_internal.h"

namespace yase {

// Skip list node (tower)
struct SkipListNode {
  static const uint8_t kInvalidLevels = 0;

  // Tower height
  uint32_t nlevels;

  // Payload (RID)
  RID rid;

  // Pointer to the next node. The i-th element represents the (i+1)th level
  SkipListNode *next[SKIP_LIST_MAX_LEVEL];

  // Key (must be the last field of this struct)
  char key[0];

  SkipListNode(uint32_t nlevels, RID rid) : nlevels(nlevels), rid(rid) {}
  SkipListNode() : nlevels(kInvalidLevels) {}
  ~SkipListNode() {}
};

// Skip list that maps keys to RIDs
class SkipList {
 public:
  // Constructor - create a skip list
  SkipList(uint32_t key_size);

  // Destructor
  ~SkipList();

  // Insert a key - RID mapping into the skip list index
  // @key: pointer to the key
  // @rid: RID of the record represented by the key (the "data entry")
  bool Insert(const char *key, RID rid);

  // Search for a key in the skip list
  // @key: pointer to the key
  // Returns the data entry corresponding to the search key; returns Invalid RID
  // if the key is not found
  RID Search(const char *key);

  // Delete a key from the index
  // @key: pointer to the key
  // Returns true if the index entry is successfully deleted, false if the key
  // does not exist
  bool Delete(const char *key);

  // Update the data entry with a new rid
  // @key: target key
  // @rid: new RID to be written
  // Returns true if the update was successful
  bool Update(const char *key, RID rid);

  // Scan and return multiple keys/payloads
  // @start_key: start (smallest) key of the scan operation
  // @nkeys: number of keys to scan
  // @inclusive: whether the result (nkeys) includes [start_key] (and the RID)
  // @out_records: pointer to a vector to stores the scan result (RIDs)
  void ForwardScan(const char *start_key, uint32_t nkeys, bool inclusive,
                   std::vector<std::pair<char *, RID> > *out_records);

  // Traverse the skip list to find a key
  // @key: pointer to the target key
  // @out_pred_nodes: a vector (provided by the caller) to store the predecessor
  // nodes during traversal), sorted by reverse-height order, i.e.,
  // out_pred_nodes[0] should point to the height-level predecessor (the head
  // dummy node), and out_pred_nodes[1] should point to the second height tower
  // and so on.
  // Returns the node containing the provided key if the key exists; otherwise
  // returns nullptr.
  SkipListNode *Traverse(const char *key, std::vector<SkipListNode*> *out_pred_nodes = nullptr);

  // Create a new skip list node (tower)
  // @levels: height of this tower
  // @key: pointer to the key
  // @rid: data entry (RID)
  SkipListNode *NewNode(uint32_t levels, const char *key, RID rid);

 private:
  FRIEND_TEST(SkipListTest, Init);
  FRIEND_TEST(SkipListTest, NewNode);
  FRIEND_TEST(SkipListTest, SortedList);
  FRIEND_TEST(SkipListTest, ForwardScanInclusive);
  FRIEND_TEST(SkipListTest, ForwardScanNonInclusive);
  FRIEND_TEST(SkipListTest, Hidden1);
  FRIEND_TEST(SkipListTest, Hidden2);
  FRIEND_TEST(SkipListTest, Hidden3);
  FRIEND_TEST(SkipListTest, Hidden4);

  // key size supported
  uint32_t key_size;

  // Dummy head tower
  SkipListNode head;

  // Dummy tail tower
  SkipListNode tail;

  // Current height of the skip list
  uint32_t height;
};

}  // namespace yase

/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II Course Project, Fall 2021
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 *
 * Test cases for skip list.
 */

#include <thread>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <Index/pskiplist.h>

namespace yase {

class PSkipListTest : public ::testing::Test {
 protected:
  PSkipList *slist;

  void SetUp() override {
    slist = nullptr;
  }

  void TearDown() override {
    if (slist) {
      delete slist;
    }
    slist = nullptr;
  }

  void NewPSkipList(uint32_t key_size) {
                                                              LOG(ERROR);
    std::string name ("pskiplist");
    slist = new PSkipList(name, key_size);
                                                              LOG(ERROR);
  }
};

// Empty list with pointers properly set up
TEST_F(PSkipListTest, Init) {
                                                              LOG(ERROR);
  NewPSkipList(8);
                                                              LOG(ERROR);

  ASSERT_EQ(slist->key_size, 8);
  PSkipListNode* head_node = (PSkipListNode*) malloc(sizeof(PSkipListNode) + slist->key_size);
  slist->table.Read(slist->head, head_node);
  PSkipListNode* tail_node = (PSkipListNode*) malloc(sizeof(PSkipListNode) + slist->key_size);
  slist->table.Read(slist->tail, tail_node);
  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; ++i) {
    ASSERT_EQ(head_node->next[i].value, slist->tail.value);
    ASSERT_EQ(tail_node->next[i].value, RID().value);
  }
  free(head_node);
  free(tail_node);
  ASSERT_EQ(slist->height, 1);
}

TEST_F(PSkipListTest, NewNodeTooHigh) {
  NewPSkipList(8);
  RID rid(0xfeedbeef);
  std::string key("testkey");

  // Level greater than max, should fail
  RID node_rid = slist->NewNode(100, key.c_str(), rid);
  ASSERT_FALSE(node_rid.IsValid());
  // free(node);
}

TEST_F(PSkipListTest, NewNode) {
  NewPSkipList(8);
  RID rid(0xfeedbeef);
  std::string key("testkey1");

  // This one should succeed
  RID node_rid = slist->NewNode(4, key.c_str(), rid);
  ASSERT_TRUE(node_rid.IsValid());

  // Check level and RID
  PSkipListNode* node = (PSkipListNode*) malloc(sizeof(PSkipListNode) + slist->key_size);
  slist->table.Read(node_rid, node);
  ASSERT_EQ(node->nlevels, 4);
  ASSERT_EQ(node->rid.value, rid.value);

  // Check key
  int cmp = memcmp(key.c_str(), node->key, 8);
  ASSERT_EQ(cmp, 0);

  // Check next pointers
  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; ++i) {
    ASSERT_EQ(node->next[i].value, RID().value);
  }

  free(node);
}

// Insert one key
TEST_F(PSkipListTest, SingleInsertSearch) {
  NewPSkipList(8);
  RID rid(0xfeedbeef);
  std::string key("testkeyk");

  bool success = slist->Insert(key.c_str(), rid);
  ASSERT_TRUE(success);

  // Read it back
  RID read_rid = slist->Search(key.c_str());
  ASSERT_EQ(read_rid.value, rid.value);
}

// Insert an existed key
TEST_F(PSkipListTest, InsertExisted) {
  NewPSkipList(8);
  RID rid(0xfeedbeef);
  std::string key("testkeyk");

  bool success = slist->Insert(key.c_str(), rid);
  ASSERT_TRUE(success);

  // Insert again
  success = slist->Insert(key.c_str(), rid);
  ASSERT_FALSE(success);
}

TEST_F(PSkipListTest, SearchNonExist) {
  NewPSkipList(8);
  std::string key("11111111");

  // Empty, should turn up nothing
  RID rid = slist->Search(key.c_str());
  ASSERT_FALSE(rid.IsValid());

  // Insert than search for a different key
  RID insert_rid(0xfeedbeef);
  std::string search_key("11111112");
  rid = slist->Search(search_key.c_str());
  ASSERT_FALSE(rid.IsValid());
}

TEST_F(PSkipListTest, Update) {
  NewPSkipList(8);
  std::string key("11111111");

  // Empty, should fail
  RID rid1(1);
  bool success = slist->Update(key.c_str(), rid1);
  ASSERT_FALSE(success);

  // Insert
  success = slist->Insert(key.c_str(), rid1);
  ASSERT_TRUE(success);
  auto rid11 = slist->Search(key.c_str());
  ASSERT_EQ(rid1.value, rid11.value);

  // Now update, should succeed
  RID rid2(2);
  success = slist->Update(key.c_str(), rid2);
  ASSERT_TRUE(success);

  // Search should return rid2
  auto rid22 = slist->Search(key.c_str());
  ASSERT_EQ(rid22.value, rid2.value);
}

// Check all nodes are sorted
TEST_F(PSkipListTest, SortedList) {
  NewPSkipList(8);
  static const uint64_t kKeys = 1024;

  for (uint64_t k = 1; k <= kKeys; ++k) {
    RID rid(k);
    bool success = slist->Insert((const char *)&k, rid);
    ASSERT_TRUE(success);
  }

  RID curr = slist->head;
  PSkipListNode* curr_node = (PSkipListNode*) malloc(sizeof(PSkipListNode) + slist->key_size);
  slist->table.Read(curr, curr_node);
  ASSERT_NE(curr_node->next[0].value, RID().value);
  ASSERT_NE(curr_node->next[0].value, slist->tail.value);

  uint64_t nkeys = 0;
  curr = curr_node->next[0];
  slist->table.Read(curr, curr_node);
  char *prev_key = nullptr;
  while (curr.value != slist->tail.value) {
    if (!prev_key) {
      ASSERT_EQ(nkeys, 0);
    } else {
      int cmp = memcmp(prev_key, curr_node->key, 8);
      ASSERT_LE(cmp, 0);
    }
    prev_key = curr_node->key;
    ASSERT_GE(curr_node->nlevels, 1);
    curr = curr_node->next[0];
    slist->table.Read(curr, curr_node);
    ++nkeys;
  }
  PSkipListNode* tail_node = (PSkipListNode*) malloc(sizeof(PSkipListNode) + slist->key_size);
  ASSERT_EQ(slist->tail.value, curr.value);
  ASSERT_EQ(tail_node->next[0].value, RID().value);
  ASSERT_EQ(nkeys, kKeys);
  free(curr_node);
  free(tail_node);
}

TEST_F(PSkipListTest, InsertSearchDelete) {
  NewPSkipList(8);

  // Insert 100 keys
  static const uint64_t kKeys = 100;
  for (uint64_t k = 1; k <= kKeys; ++k) {
    RID rid(k * 2);
    bool success = slist->Insert((const char *)&k, rid);
    ASSERT_TRUE(success);
  }
  LOG(INFO) << "Insert succeeded";

  // Read the inserted keys back
  for (uint64_t k = 1; k <= kKeys; ++k) {
    RID r = slist->Search((const char *)&k);
    ASSERT_EQ(r.value, RID(k * 2).value);
  }

  LOG(INFO) << "Read succeeded";

  // Delete some inserted keys
  for (uint64_t k = 0; k < kKeys; ++k) {
    if (k % 2) {
      bool success = slist->Delete((const char *)&k);
      ASSERT_TRUE(success);
      // Read should fail now
      RID r = slist->Search((char *)&k);
      ASSERT_FALSE(r.IsValid());
    }
  }
}

TEST_F(PSkipListTest, ForwardScanInclusive) {
  NewPSkipList(8);

  static const uint64_t kKeys = 200;
  uint64_t start_key = 1;
  for (uint64_t i = 1; i <= kKeys; ++i) {
    yase::RID rid(i);
    bool success = slist->Insert((const char *)&i, rid);
    ASSERT_TRUE(success);
  }

  std::vector<std::pair<char *, RID> > result;
  static const uint32_t nkeys = 10;
  slist->ForwardScan((const char *)&start_key, nkeys, true, &result);
  ASSERT_EQ(result.size(), nkeys);

  for (uint32_t i = 1; i < result.size(); ++i) {
    auto &prev = result[i - 1];
    auto &r = result[i];
    int cmp = memcmp(prev.first, r.first, slist->key_size);
    ASSERT_LT(cmp, 0);
  }

  // Recycle memory allocated for keys
  for (auto &r : result) {
    free(r.first);
  }
}

TEST_F(PSkipListTest, ForwardScanNonInclusive) {
  NewPSkipList(8);

  static const uint64_t kKeys = 6;
  for (uint64_t i = 1; i <= kKeys; ++i) {
    yase::RID rid(i);
    bool success = slist->Insert((const char *)&i, rid);
    ASSERT_TRUE(success);
  }

  std::vector<std::pair<char *, RID> > result;
  static const uint32_t nkeys = 30;

  // Non-existant, small start_key, scanning more than the list has: 
  // should return all keys
  uint64_t start_key = 0;
  slist->ForwardScan((const char *)&start_key, nkeys, true, &result);
  ASSERT_EQ(result.size(), kKeys);

  for (uint32_t i = 1; i < result.size(); ++i) {
    auto &prev = result[i - 1];
    auto &r = result[i];
    int cmp = memcmp(prev.first, r.first, slist->key_size);
    ASSERT_LT(cmp, 0);
  }

  // Recycle memory allocated for keys
  for (auto &r : result) {
    free(r.first);
  }
}

void InsertSearch(uint32_t thread_id, yase::PSkipList *slist) {
  static const uint64_t kKeys = 100;
  for (uint64_t k = 0; k < kKeys; ++k) {
    uint64_t key = k;
    key = k * 4 + thread_id;
    yase::RID rid(key);
    bool success = slist->Insert((char *)&key, rid);
    ASSERT_TRUE(success);

    // Issue a search for non-existant key
    key = 400 + k;
    rid = slist->Search((char *)&key);
    ASSERT_FALSE(rid.IsValid());
  }
}

// New tests with four threads inserting non-overlapping keys
TEST_F(PSkipListTest, ConcurrentInsertSearch) {
  // Initialize a skip list that supports 8-byte key
  NewPSkipList(8);

  static const uint32_t kThreads = 4;
  std::vector<std::thread *> threads;
  for (uint32_t i = 0; i < kThreads; ++i) {
    threads.push_back(new std::thread(InsertSearch, i, slist));
  }

  // Wait for all threads to finish
  for (auto &t : threads) {
    t->join();
    delete t;
  }
}

// Long keys
TEST_F(PSkipListTest, LongKeys) {
  // 20-byte keys
  NewPSkipList(20);

  auto insert = [&](uint32_t thread_id, PSkipList *slist) {
    static const uint64_t kKeys = 100;
    for (uint64_t k = 0; k < kKeys; ++k) {
      char key[20];
      // Use the high-order bits for thread id
      *(uint32_t*)&key[16] = thread_id;
      // Remaining as a counter
      *(uint64_t*)&key[8] = k;
      *(uint64_t*)&key[0] = k;
      yase::RID rid(k);
      bool success = slist->Insert(key, rid);
      ASSERT_TRUE(success);
    }
  };

  static const uint32_t kThreads = 4;
  std::vector<std::thread *> threads;
  for (uint32_t i = 0; i < kThreads; ++i) {
    threads.push_back(new std::thread(insert, i, slist));
  }

  // Wait for all threads to finish
  for (auto &t : threads) {
    t->join();
    delete t;
  }

  // Read all keys back
  for (uint32_t i = 0; i < kThreads; ++i) {
    static const uint64_t kKeys = 100;
    for (uint64_t k = 0; k < kKeys; ++k) {
      char key[20];
      // Use the high-order bits for thread id
      *(uint32_t*)&key[16] = i;
      // Remaining as a counter
      *(uint64_t*)&key[8] = k;
      *(uint64_t*)&key[0] = k;
      auto rid = slist->Search(key);
      ASSERT_TRUE(rid.value == k);
    }
  };
}

}  // namespace yase

int main(int argc, char **argv) {
  ::google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

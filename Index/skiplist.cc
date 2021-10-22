/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II, Fall 2021
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 */

#include <random>
#include "skiplist.h"
#include <cmath>
#include <utility>


namespace yase {

SkipList::SkipList(uint32_t key_size) {
  // Initialize skip list with necessary parameters:
  // 1. ey_size as the argument passed in
  // 2. Initial height to be 1
  // 3. Initialize each level to be an empty linked list 
  //    (i.e., heads pointing to tails, tails pointing to null)
  // 4. Initialize latches
  //
  // TODO: Your implementation
  this->key_size = key_size;
  this->height = 1;
  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; i++){
    head.next[i] = &tail;
    tail.next[i] = nullptr;
  }
}

SkipList::~SkipList() {
  // Deallocate all the towers allocated in memory and destroy latches
  //
  // TODO: Your implementation

  SkipListNode* cur = &head;
  while (cur){
    if (cur == &head || cur == &tail){
      cur = cur->next[0];
      continue;
    }
    SkipListNode* old = cur;
    cur = cur->next[0];
    free(old);
  }
}

SkipListNode *SkipList::NewNode(uint32_t levels, const char *key, RID rid) {
  // 1. Use malloc to allocate the a node, including SkipListNode itself and the
  //    key that follows (i.e., starts at SkipListNode.key).
  // 2. Use placement new and the SkipListNode(uint32_t nlevels, RID rid)
  //    constructor to initialize the node
  // 3. Copy the key to the node's key area (represented by the "key" variable)
  //
  // TODO: Your implementation
  if (levels > SKIP_LIST_MAX_LEVEL || levels <= 0) {
    return nullptr;
  }

  SkipListNode* node = (SkipListNode*) malloc(sizeof(SkipListNode) + key_size);
  new (node) SkipListNode(levels, rid);
  // // // LOG(ERROR) << "key: " << key << ", size of key: " << key_size;
  memcpy(node->key, key, key_size);

  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; i++) {
    node->next[i] = nullptr;
  }

  return node;
}

SkipListNode *SkipList::Traverse(const char *key, std::vector<SkipListNode*> *out_pred_nodes) {
  // Start from the head dummy tower to reach the specified key. If the key
  // exists, return the tower that represents the key, otherwise return nullptr
  // to indicate the key does not exist.
  //
  // Keep predecessor nodes (i.e., whenever the search path "makes a turn") in the
  // user-provided vector. Note that out_pred_nodes might be nullptr if the user
  // is Read() or Update for which the predecessor nodes won't be useful.
  //
  // TODO: Your implementation
  int i = SKIP_LIST_MAX_LEVEL - 1;
  SkipListNode* cur = &head;
  SkipListNode* pred = cur; // POTENTIAL BUGS HERE!
  while (cur != &tail){
                                                                                                // LOG(ERROR) << "ಠ_ಠ " << i;
                                                                                                // LOG(ERROR) << "cur == &head: " << (cur == &head);
    if (memcmp(cur->key, key, key_size) == 0 && cur != &head){ // cur key == key
                                                                                                // LOG(ERROR) << "cur key == key";
      if (out_pred_nodes){
        out_pred_nodes->push_back(pred);
      }
      return cur;
    } 
    if (memcmp(cur->next[i]->key, key, key_size) > 0 || cur->next[i] == &tail){ // next key > key
                                                                                                // LOG(ERROR) << "next key > key";
      if (out_pred_nodes){
        out_pred_nodes->push_back(cur);
      }
      if (i > 0){
                                                                                                // LOG(ERROR) << "go down";
        // go down, repeat
        i--;
      } else {
                                                                                                // LOG(ERROR) << "i==0, return nullptr";
        return nullptr;
      }
    } else if (memcmp(cur->next[i]->key, key, key_size) <= 0){ // next key <= key
                                                                                                // LOG(ERROR) << "next key <= key";
      // go right, repeat
      pred = cur;
      cur = cur->next[i];
    }
  }
  return nullptr;
}

bool SkipList::Insert(const char *key, RID rid) {
  // Use the Traverse function to reach the insert position:
  // 1. If Traverse returns a valid node, then the key already exists in the skip
  //    list and return false;
  // 2. Otherwise continue to:
  //    (a) determine the height of the new tower
  //    (b) build up the tower from bottom up using the vector passed in to the
  //        Traverse function: it should contain all the predecessors at each
  //        level
  //    (c) return true/false to indicate a successful/failed insert
  //
  // TODO: Your implementation
  std::vector<SkipListNode*> out_pred_nodes;
  SkipListNode* node = Traverse(key, &out_pred_nodes);
  if (node){
    return false;
  }

  std::random_device rd;
  std::mt19937 gen(rd()); 
  // std::uniform_int_distribution<> rand(0, pow(2, SKIP_LIST_MAX_LEVEL) - 1);
  // uint32_t new_tower_height = rand(gen);

  std::uniform_int_distribution<> rand(0, 1);
  uint32_t new_tower_height = 1;
  while (rand(gen) != 0 && new_tower_height < SKIP_LIST_MAX_LEVEL){
    new_tower_height++;
  }
  // uint32_t new_tower_height = ffz(random() & ((1UL << SKIP_LIST_MAX_LEVEL) - 1)); // source: CMPT 454 lecture notes 
  // // LOG(ERROR) << "new tower height: " << new_tower_height;
  height = std::max(height, new_tower_height);
  
  // // LOG(ERROR) << "insert 1";
  SkipListNode* new_node = NewNode(new_tower_height, key, rid);
  // // LOG(ERROR) << "insert 7";
  for (uint32_t i=0; i<new_tower_height; i++){
    // // LOG(ERROR) << "out_pred_nodes.size(): " << out_pred_nodes.size();
    SkipListNode* pred = out_pred_nodes.back();
    out_pred_nodes.pop_back();
    SkipListNode* next_node = pred->next[i];
    pred->next[i] = new_node;
    new_node->next[i] = next_node;
  }
  // // LOG(ERROR) << "insert 8";
  return true;
}

RID SkipList::Search(const char *key) {
  // Use the Traverse function to locate the key.
  // Return the RID (i.e., payload) if the key is found; otherwise return invalid RID.
  //
  // TODO: Your implementation
  // // LOG(ERROR) << "search 1";
  SkipListNode* node = Traverse(key);
  // // LOG(ERROR) << "search 2";
  if (!node){
    // // LOG(ERROR) << "search 3";
    return RID();
  }
  // // LOG(ERROR) << "search 4";
  return node->rid;
}

bool SkipList::Update(const char *key, RID rid) {
  // Invoke the Traverse function to obtain the target tower and update its
  // payload RID. 
  //
  // Return true if the key is found and the RID is updated,
  // otherwise return false.
  //
  // TODO: Your implementation
  SkipListNode* node = Traverse(key);
  if (!node){
    return false;
  }
  node->rid = rid;
  return true;
}

bool SkipList::Delete(const char *key) {
  // Similar to Insert(), use the Traverse function to obtain the target node
  // that contains the provided key and the predecessor nodes, then unlink the
  // returned node at all levels.
  //
  // The unlinked tower should be freed.
  //
  // Return true if the operation succeeeded, false if the key is not found.
  //
  // TODO: Your implementation
  std::vector<SkipListNode*> out_pred_nodes;
                                      // LOG(ERROR) << "ಠ_ಠ";
  SkipListNode* node = Traverse(key, &out_pred_nodes);
                                      // LOG(ERROR) << "ಠ_ಠ";
  if (!node){
                                      // LOG(ERROR) << "ಠ_ಠ";
    return false;
  }
                                      // LOG(ERROR) << "ಠ_ಠ";
  // LOG(ERROR) << "ಠ_ಠ";
  SkipListNode* cur = out_pred_nodes.back();
                                      // LOG(ERROR) << "ಠ_ಠ";
  // out_pred_nodes.pop_back();

  int i = node->nlevels - 1;
  // LOG(ERROR) << i;
  while (i >= 0){
    // LOG(ERROR) << i;
    if (cur->next[i] == node){
      // LOG(ERROR) << i;
      cur->next[i] = node->next[i];
      // LOG(ERROR) << i;
      i--;
      // LOG(ERROR) << i;
    } else {
      // LOG(ERROR) << i;
      cur = cur->next[i];
      // LOG(ERROR) << i;
    }
    // LOG(ERROR) << i;
  }
  free(node);
  return true;
}

void SkipList::ForwardScan(const char *start_key, uint32_t nkeys, bool inclusive,
                           std::vector<std::pair<char *, RID> > *out_records) {
  // 1. Use the Traverse function to locate the start key and a stack of
  //    predecessor nodes.
  // 2. Scan from the start key and push results in the given result set
  //    (out_records vector).
  //
  // Notes: 
  // 1. Return directly if [out_records] is null or nkeys is 0.
  // 2. If inclusive is set to fales, the start_key record should be excluded
  //    from the result set.
  // 3. Results (elements in [out_records]) should be sorted in ascending key
  //    order
  // 4. The keys stored in the result vector should be copies of keys in skip
  //    list nodes using dynamically allocated memory. Caller of ForwardScan 
  //    is responsible for deallocating the keys after use. 
  // 5. A start key of null means scanning from the smallest record.
  //
  // TODO: Your implementation
  if (!out_records || nkeys == 0){
    return;
  }
  SkipListNode* cur = Traverse(start_key);
  if (!start_key || !cur){
                                                                                                // LOG(ERROR) << "ಠ_ಠ";
    cur = head.next[0];
  }
                                                                                                // LOG(ERROR) << "ಠ_ಠ";
  // std::pair <char *, RID> p;
  // if (!inclusive || cur == &head){
  if (!inclusive){
                                                                                                // LOG(ERROR) << "ಠ_ಠ";
    cur = cur->next[0];
  }
                                                                                                // LOG(ERROR) << "ಠ_ಠ";
  uint32_t i = 0;
  while (cur != &tail && i < nkeys){
                                                                                                // LOG(ERROR) << "ಠ_ಠ " << i;
                                                                                                // LOG(ERROR) << cur->nlevels;
    // // LOG(ERROR) << cur->key;
    char* key_copy = (char *)malloc(key_size);
    memcpy(key_copy, cur->key, key_size);
    std::pair<char *, RID> p = std::make_pair(key_copy, cur->rid);
    cur = cur->next[0];
    out_records->push_back(p);
    i++;
  }
}

}  // namespace yase

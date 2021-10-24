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
#include "pskiplist.h"
#include <cmath>
#include <utility>


namespace yase {

PSkipList::PSkipList(std::string name, uint32_t key_size) : table(name, sizeof(PSkipListNode) + key_size) {
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
  // PSkipListNode* head_node = (PSkipListNode*) malloc(sizeof(PSkipListNode) + key_size);
  // table.Read(head, head_node);
  // PSkipListNode* tail_node = (PSkipListNode*) malloc(sizeof(PSkipListNode) + key_size);
  // table.Read(tail, tail_node);
  PSkipListNode head_node;
  head = table.Insert((char*)&head_node);
  PSkipListNode tail_node;
  tail = table.Insert((char*)&tail_node);
  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; i++){
    head_node.next[i].value = tail.value;
    tail_node.next[i].value = RID().value;
    pthread_rwlock_init(latches + i, NULL);
  }
  table.Update(head, (char*)&head_node);
  table.Update(tail, (char*)&tail_node);
}

PSkipList::~PSkipList() {
  // Deallocate all the towers allocated in memory and destroy latches
  //
  // TODO: Your implementation
  RID cur_rid = head;
  PSkipListNode* cur = (PSkipListNode*) malloc(sizeof(PSkipListNode) + key_size);
  table.Read(cur_rid, cur);
  while (cur_rid.IsValid()){
    RID old = cur_rid;
    cur_rid = cur->next[0];
    table.Read(cur_rid, cur);
    table.Delete(old);
  }

  free(cur);

  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; i++){
    pthread_rwlock_destroy(latches + i);
  }

  // delete table;
}

RID PSkipList::NewNode(uint32_t levels, const char *key, RID rid) {
  // 1. Use malloc to allocate the a node, including PSkipListNode itself and the
  //    key that follows (i.e., starts at PSkipListNode.key).
  // 2. Use placement new and the PSkipListNode(uint32_t nlevels, RID rid)
  //    constructor to initialize the node
  // 3. Copy the key to the node's key area (represented by the "key" variable)
  //
  // TODO: Your implementation
  if (levels > SKIP_LIST_MAX_LEVEL || levels <= 0) {
    return RID();
  }

  PSkipListNode* node = (PSkipListNode*) malloc(sizeof(PSkipListNode) + key_size);
  new (node) PSkipListNode(levels, rid);
  memcpy(node->key, key, key_size);

  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; i++) {
    node->next[i] = RID();
  }
  RID node_rid = table.Insert((char*)node);
  free(node);
  return node_rid;
}

RID PSkipList::Traverse(const char *key, std::vector<RID> *out_pred_nodes) {
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
  RID cur_rid = head;
  PSkipListNode* cur = (PSkipListNode*) malloc(sizeof(PSkipListNode) + key_size);
  table.Read(cur_rid, cur);
  RID pred_rid = cur_rid; // POTENTIAL BUGS HERE!

  PSkipListNode* next = (PSkipListNode*) malloc(sizeof(PSkipListNode) + key_size);
  table.Read(cur->next[i], next);

  while (cur_rid.value != tail.value){
    table.Read(cur_rid, cur);
    if (memcmp(cur->key, key, key_size) == 0 && cur_rid.value != head.value){ // cur key == key
      if (out_pred_nodes){
        out_pred_nodes->push_back(pred_rid);
      }
      return cur_rid;
    } 
    table.Read(cur->next[i], next);
    if (memcmp(next->key, key, key_size) > 0 || cur->next[i].value == tail.value){ // next key > key
      if (out_pred_nodes){
        out_pred_nodes->push_back(cur_rid);
      }
      if (i > 0){
        // go down, repeat
        i--;
      } else {
        return RID();
      }
    } else if (memcmp(next->key, key, key_size) <= 0){ // next key <= key
      // go right, repeat
      pred_rid = cur_rid;
      cur_rid = cur->next[i];
    }
  }
  return RID();
}

bool PSkipList::Insert(const char *key, RID rid) {
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

  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; i++){
    if (i >= new_tower_height){
      pthread_rwlock_rdlock(latches + i);
    } else {
      pthread_rwlock_wrlock(latches + i);
    }
  }

  std::vector<RID> out_pred_nodes;
  RID node_rid = Traverse(key, &out_pred_nodes);
  
  if (node_rid.IsValid()){
    for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; i++){
      pthread_rwlock_unlock(latches + i);
    }
    return false;
  }
  
  RID new_node_rid = NewNode(new_tower_height, key, rid);
  PSkipListNode* new_node = (PSkipListNode*) malloc(sizeof(PSkipListNode) + key_size);
  table.Read(new_node_rid, new_node);

  PSkipListNode* pred = (PSkipListNode*) malloc(sizeof(PSkipListNode) + key_size);

  PSkipListNode* next_node = (PSkipListNode*) malloc(sizeof(PSkipListNode) + key_size);

  for (uint32_t i=0; i<new_tower_height; i++){
    RID pred_rid = out_pred_nodes.back();
    out_pred_nodes.pop_back();
    table.Read(pred_rid, pred);

    // PSkipListNode* pred = (PSkipListNode*) malloc(sizeof(PSkipListNode) + key_size);
    RID next_node_rid = pred->next[i];
    // PSkipListNode* next_node = (PSkipListNode*) malloc(sizeof(PSkipListNode) + key_size);
    table.Read(next_node_rid, next_node);

    pred->next[i] = new_node_rid;
    table.Update(pred_rid, (char*)pred);

    new_node->next[i] = next_node_rid;
    table.Update(new_node_rid, (char*)new_node);
  }
  height = std::max(height, new_tower_height);

  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; i++){
    pthread_rwlock_unlock(latches + i);
  }

  free(new_node);
  free(pred);
  free(next_node);

  return true;
}

RID PSkipList::Search(const char *key) {
  // Use the Traverse function to locate the key.
  // Return the RID (i.e., payload) if the key is found; otherwise return invalid RID.
  //
  // TODO: Your implementation
  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; i++){
    pthread_rwlock_rdlock(latches + i);
  }

  RID node_rid = Traverse(key);
  
  if (!node_rid.IsValid()){
    for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; i++){
      pthread_rwlock_unlock(latches + i);
    }
    return RID();
  }

  PSkipListNode* node = (PSkipListNode*) malloc(sizeof(PSkipListNode) + key_size);
  table.Read(node_rid, node);
  RID ret = node->rid;

  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; i++){
    pthread_rwlock_unlock(latches + i);
  }

  free(node);

  return ret;
}

bool PSkipList::Update(const char *key, RID rid) {
  // Invoke the Traverse function to obtain the target tower and update its
  // payload RID. 
  //
  // Return true if the key is found and the RID is updated,
  // otherwise return false.
  //
  // TODO: Your implementation
  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; i++){
    if (i == 0){
      pthread_rwlock_wrlock(latches + i);
    } else {
      pthread_rwlock_rdlock(latches + i);
    }
  }

  RID node_rid = Traverse(key);
  PSkipListNode* node = (PSkipListNode*) malloc(sizeof(PSkipListNode) + key_size);
  table.Read(node_rid, node);
  
  if (!node_rid.IsValid()){
    for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; i++){
      pthread_rwlock_unlock(latches + i);
    }
    return false;
  }

  node->rid = rid;

  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; i++){
    pthread_rwlock_unlock(latches + i);
  }
  return true;
}

bool PSkipList::Delete(const char *key) {
  // Similar to Insert(), use the Traverse function to obtain the target node
  // that contains the provided key and the predecessor nodes, then unlink the
  // returned node at all levels.
  //
  // The unlinked tower should be freed.
  //
  // Return true if the operation succeeeded, false if the key is not found.
  //
  // TODO: Your implementation
  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; i++){
    pthread_rwlock_wrlock(latches + i);
  }

  std::vector<RID> out_pred_nodes;
  RID node_rid = Traverse(key, &out_pred_nodes);
  PSkipListNode* node = (PSkipListNode*) malloc(sizeof(PSkipListNode) + key_size);
  table.Read(node_rid, node);
  
  if (!node_rid.IsValid()){
    for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; i++){
      pthread_rwlock_unlock(latches + i);
    }
    return false;
  }

  RID cur_rid = out_pred_nodes.back();
  PSkipListNode* cur = (PSkipListNode*) malloc(sizeof(PSkipListNode) + key_size);
  table.Read(cur_rid, cur);
  int i = node->nlevels - 1;
  while (i >= 0){
    if (cur->next[i].value == node_rid.value){
      cur->next[i] = node->next[i];
      table.Update(cur_rid, (char*)cur);
      i--;
    } else {
      cur_rid = cur->next[i];
      table.Read(cur_rid, cur);
    }
  }
  table.Delete(node_rid);
  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; i++){
    pthread_rwlock_unlock(latches + i);
  }
  return true;
}

void PSkipList::ForwardScan(const char *start_key, uint32_t nkeys, bool inclusive,
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
  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; i++){
    pthread_rwlock_rdlock(latches + i);
  }
  RID cur_rid = Traverse(start_key);
  PSkipListNode* cur = (PSkipListNode*) malloc(sizeof(PSkipListNode) + key_size);
  table.Read(cur_rid, cur);
  if (!start_key || !cur_rid.IsValid()){
    PSkipListNode* head_node = (PSkipListNode*) malloc(sizeof(PSkipListNode) + key_size);
    table.Read(head, head_node);

    cur_rid = head_node->next[0];
    table.Read(cur_rid, cur);
  }

  if (!inclusive && start_key){
    cur_rid = cur->next[0];
    table.Read(cur_rid, cur);
  }
  uint32_t i = 0;
  while (cur_rid.value != tail.value && i < nkeys){
    char* key_copy = (char *)malloc(key_size);
    memcpy(key_copy, cur->key, key_size);
    std::pair<char *, RID> p = std::make_pair(key_copy, cur->rid);
    cur_rid = cur->next[0];
    table.Read(cur_rid, cur);
    out_records->push_back(p);
    i++;
  }
  for (uint32_t i = 0; i < SKIP_LIST_MAX_LEVEL; i++){
    pthread_rwlock_unlock(latches + i);
  }
}

}  // namespace yase

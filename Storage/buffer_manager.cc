/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II Course Project, Fall 2021
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 */
#include "buffer_manager.h"

namespace yase {

BufferManager *BufferManager::instance = nullptr;

// Initialize a new buffer manager
BufferManager::BufferManager(uint32_t page_count) {
  // Allocate and initialize memory for page frames
  // 1. Initialize the page_count member variable
  // 2. malloc the desired amount of memory (specified by page_count) 
  //    and store the address in member variable page_frames
  // 3. Clear the allocated memory to 0
  // 4. Use placement new to initialize each frame
  latch.lock();
  this->page_count = page_count;
  page_frames = (Page*)calloc(page_count, sizeof(Page));
  // page_frames = (Page*)malloc(page_count * sizeof(Page));
  // memset(page_frames, 0, page_count * sizeof(Page));
  // page_frames = new Page();
  for (unsigned int i=0; i<page_count; i++){
    // new (page_frames + i) Page();
    Page* initialized_page = new (page_frames + i) Page();
    lru_queue.push_back(initialized_page);
  }
  latch.unlock();

}

BufferManager::~BufferManager() {
  // Flush all dirty pages and free page frames
  latch.lock();

  for (unsigned int i=0; i<page_count; i++){
    Page* page = page_frames + i;
    page->Latch();
    if (page->IsDirty()){
      PageId page_id = page->GetPageId();
      uint16_t bf_id = page_id.GetFileID();
      file_map[bf_id]->FlushPage(page_id, page->page_data);
      page->SetDirty(false);
      page->Unlatch();
    } else {
      page->Unlatch();
    }
  }
  free(page_frames);
  latch.unlock();
}

Page* BufferManager::PinPage(PageId page_id) {
  // Pin a page in the buffer pool:
  // 1. Check if the page already exists in the buffer pool (using the 
  //    PageID - Page* mapping). If so, return it.
  // 2. If not, find a free page frame [p] using LRU and:
  //    (a) setup the PageID - [p] mapping
  //    (b) load the page from storage using the FileID - BaseFile* mapping.
  //    frame and set up the PageID-Page Frame mapping
  //
  // Notes: 
  // 1. Before returning, make sure increment the page's pin_count (using
  // Page::IncPinCount function) and record page_id in the Page structure.
  //
  // 2. The pinned page should not appear in the LRU queue.
  //
  // 3. Errors such as invalid page IDs should be handled. If there is any error
  //    at any step, return nullptr.
  LOG_IF(ERROR, lru_queue.size() < 5) << "THERE ARE " << lru_queue.size() << " FREE PAGE FRAMES IN THE BUFFER POOL";
  bool ret = false;
  if (!page_id.IsValid()){
    return nullptr;
  }

  latch.lock();
  if (page_map.count(page_id)){ // if page_id is in page_map (the buffer pool)
    Page* pinned_page = page_map[page_id];
    if (!pinned_page) {
      latch.unlock();
      return nullptr;
    }
    pinned_page->Latch();
    pinned_page->IncPinCount();

    // if the page is in the LRU queue, remove it from the queue
    std::list<Page*>::iterator page_it = std::find(lru_queue.begin(), lru_queue.end(), pinned_page);
    pinned_page->Unlatch();
    if (page_it != lru_queue.end()){
      lru_queue.erase(page_it);
    }
    latch.unlock();

    return pinned_page;

  } else { // if page_id is not in page_map (the buffer pool)
    Page* page_buffer = nullptr;
    LOG_IF(ERROR, lru_queue.size() < 5) << "THERE ARE " << lru_queue.size() << " FREE PAGE FRAMES IN THE BUFFER POOL";
    if (page_map.size() >= page_count) { // if the buffer pool is full, evict a page, and load the new page into that page frame
      page_buffer = EvictPage();
      if (!page_buffer){
        latch.unlock();
        return nullptr;
      }
    } else { // if the buffer pool is not full, find an empty page frame in it, and load the new page into that page
      // for (unsigned int i=0; i<page_count; i++){
      //   if (page_frames[i].GetPinCount() == 0){
      //     page_buffer = page_frames + i;
      //     std::list<Page*>::iterator page_it = std::find(lru_queue.begin(), lru_queue.end(), page_buffer);
      //     if (page_it != lru_queue.end()){
      //       lru_queue.erase(page_it);
      //     }
      //   }
      // }
      if (lru_queue.empty()) {
        latch.unlock();
        return nullptr;
      }
      page_buffer = lru_queue.front();
      lru_queue.pop_front();
      if (!page_buffer) {
        latch.unlock();
        return nullptr;
      }
    }

    LOG_IF(ERROR, lru_queue.size() < 5) << "THERE ARE " << lru_queue.size() << " FREE PAGE FRAMES IN THE BUFFER POOL";
    uint16_t bf_id = page_id.GetFileID();
    BaseFile* bf = file_map[bf_id];
    if (!bf){
      latch.unlock();
      return nullptr;
    }
    page_buffer->Latch();
    ret = bf->LoadPage(page_id, page_buffer->page_data);
    page_buffer->Unlatch();
    if (!ret){
      latch.unlock();
      return nullptr;
    }
    page_buffer->Latch();
    page_map[page_id] = page_buffer;
    page_buffer->pin_count = 1;
    page_buffer->page_id = page_id;
    page_buffer->Unlatch();
    latch.unlock();
    
    return page_buffer;
  }

  // The following return statement tries to silent compiler warning so the base
  // code compiles, you may need to revise/remove it in your implementation
  return nullptr;
}

void BufferManager::UnpinPage(Page *page) {
  // 1. Unpin the provided page by decrementing the pin count. The page should
  // stay in the buffer pool until another operation evicts it later.
  // 2. The page should be added to the LRU queue when its pin count becomes 0.
  //
  // Note: you may assume page is non-null.
  LOG_IF(ERROR, lru_queue.size() < 5) << "THERE ARE " << lru_queue.size() << " FREE PAGE FRAMES IN THE BUFFER POOL";
  if (!page){
    return;
  }
  latch.lock();
  page->Latch();
  page->DecPinCount();
  if (page->pin_count == 0){
    lru_queue.push_back(page);
  }
  page->Unlatch();
  latch.unlock();
  LOG_IF(ERROR, lru_queue.size() < 5) << "THERE ARE " << lru_queue.size() << " FREE PAGE FRAMES IN THE BUFFER POOL";
}

void BufferManager::RegisterFile(BaseFile *bf) {
  // Setup a mapping from [bf]'s file ID to [bf]
  latch.lock();
  file_map[bf->GetId()] = bf;
  latch.unlock();
}

Page* BufferManager::EvictPage() {
  bool ret = false;
  if (lru_queue.empty()) {
    return nullptr;
  }
  Page* evicted_page = lru_queue.front();
  lru_queue.pop_front();
  if (!evicted_page) {
    return nullptr;
  }
  evicted_page->Latch();
  PageId evicted_page_id = evicted_page->GetPageId();
  if (!evicted_page_id.IsValid()){
    evicted_page->Unlatch();
    return nullptr;
  }
  uint16_t evicted_bf_id = evicted_page_id.GetFileID();
  if (evicted_page->IsDirty()){
    ret = file_map[evicted_bf_id]->FlushPage(evicted_page_id, evicted_page->page_data);
    evicted_page->SetDirty(false);
    evicted_page->Unlatch();
    if (!ret){
      return nullptr;
    }
  } else {
    evicted_page->Unlatch();
  }
  page_map.erase(evicted_page_id);
  return evicted_page; // we are going to load the new page in here
}

}  // namespace yase
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
  this->page_count = page_count;
  page_frames = (Page*)calloc(page_count, sizeof(Page));
  // page_frames = (Page*)malloc(page_count * sizeof(Page));
  // memset(page_frames, 0, page_count * sizeof(Page));
  // page_frames = new Page();
  for (unsigned int i=0; i<page_count; i++){
    new (page_frames + i) Page(); // segmentation fault
  }
}

BufferManager::~BufferManager() {
  // Flush all dirty pages and free page frames

  Page* new_page = new Page();
  for (unsigned int i=0; i<page_count; i++){
    if (page_frames[i].IsDirty()){
      PageId page_id = page_frames[i].GetPageId();
      uint16_t bf_id = page_id.GetFileID();
      file_map[bf_id]->FlushPage(page_id, new_page);
      // uint16_t bf_id = page_frames[i].GetPageId().GetFileID();
      // file_map[bf_id]->FlushPage(page_frames[i].GetPageId(), new_page);
    }
  }
  free(page_frames);
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

  // if (page_map.count(page_id)){
  //   page_map[page_id]->IncPinCount();
  //   return page_map[page_id];
  // } else {
  //   // page_map[page_id] = ;

  // }



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
  // page->DecPinCount();
  // if (page->pin_count == 0){
    
  // }
}

void BufferManager::RegisterFile(BaseFile *bf) {
  // Setup a mapping from [bf]'s file ID to [bf]
  file_map[bf->GetId()] = bf;
}

}  // namespace yase
bf;
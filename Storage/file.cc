/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II, Fall 2021
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 */
#include "basefile.h"
#include "buffer_manager.h"
#include "page.h"

#include <string.h>

namespace yase {

File::File(std::string name, uint16_t record_size) : BaseFile(name), dir(name + ".dir") {
  // 1. Initialize the structure as needed; in particular the directory
  //    BaseFile should be named as "name.dir"
  // 2. Use BufferManager::RegisterFile to register this file's both 
  //    BaseFiles with the buffer manager.
  //
  // TODO: Your implementation
  this->record_size = record_size;
  BufferManager* bm = BufferManager::Get();
  bm->RegisterFile(this);
  bm->RegisterFile(&dir);
}

File::~File() {
  // Nothing needed here
}

PageId File::AllocatePage() {
  // Allocate a new data page:
  // 1. Check if any page was deallocated and can be reused (using ScavengePage).
  //    If so return the page returned by ScavengePage.
  //
  // 2. Otherwise continue to create a new data page:
  //    (a) Check and allocate a directory page if needed
  //    (b) Mark allocation in the directory page
  //    (c) Initialize the new data page
  //
  // Notes:
  // 1 .Page access should be done through the "PinPage" interface provided by the 
  //    buffer pool, for example:
  //
  //      auto *bm = BufferManager::Get();
  //      Page *p = bm->PinPage(page_id);
  //      ... access page p ...
  //
  //    You should not directly use LoadPage/FlushPage interfaces to access data
  //    pages in the File class; all accesses must go through the buffer manager.
  //    Basically this means to follow the following steps:
  //
  // 2. After a page is pinned, before/after access, the page also needs to be 
  //    latched/unlatched to handle concurrecy correctly, using the Latch/Unlatch
  //    functions provided by Page.
  //
  // 3. After modifying a page (directory or data page), it needs to be marked as
  //    "dirty" - check out Page::SetDirty for details.
  //
  // Hint: 
  // 1. The "Page" structure represents a "page frame" in the buffer pool and it 
  //    can contain either a data page (DataPage) or a directory page (DirectoryPage).
  //    See the definition of Page in buffer_manager.h for details. See also page.h 
  //    for definitions of DataPage and DirectoryPage.
  //
  // 2. A directory page just contains an array of directory entries, each of which
  //    records the information about a data page. The n-th entry would correspond
  //    to the n-th data page (i.e., page number n - 1). You can locate the directory
  //    entry for a particular page by dividing page size by the number of entries per
  //    directory page. Functions implemented in table.cc use a similar approach, you
  //    may adapt the approach used there.
  //
  // 3. To use the buffer manager, you need to use BufferManager::Get() to obtain the
  //    instance. See BufferManager class definition in buffermanager.h for details.
  //
  // TODO: Your implementation
  PageId pid = ScavengePage();
  if (pid.IsValid()) { // if there is a entry in the dir page that is created but not allocated
    return pid;
  }

  BufferManager* bm = BufferManager::Get();

  // create data page first, and use the data page num 
  // to get the dir page num and see if it's larger than the
  // number of dir pages created, if so, create new dir page
  PageId data_pid = CreatePage();
  Page* data_page = bm->PinPage(data_pid);
  DataPage new_data_page_datap = DataPage(record_size);
  memcpy(data_page->page_data, &new_data_page_datap, sizeof(DataPage));
  data_page->SetDirty(true);
  bm->UnpinPage(data_page);
  uint32_t data_page_num = data_pid.GetPageNum();

  int entries_per_dir_page = PAGE_SIZE / sizeof(struct DirectoryPage::Entry);
  uint32_t dir_page_num = data_page_num / entries_per_dir_page;
  int entry_num = data_page_num % entries_per_dir_page;

  if (dir_page_num >= dir.GetPageCount()) {
    dir.CreatePage();
  }

  PageId dir_pid = PageId(dir.GetId(), dir_page_num);
  Page* dir_page = bm->PinPage(dir_pid);
  DirectoryPage* dir_page_dirp = dir_page->GetDirPage();

  dir_page_dirp->entries[entry_num].created = true;
  dir_page->SetDirty(true);
  dir_page_dirp->entries[entry_num].allocated = true;
  dir_page_dirp->entries[entry_num].free_slots = DataPage::GetCapacity(record_size);
  bm->UnpinPage(dir_page);

  return data_pid;
  // return PageId();
}

bool File::DeallocatePage(PageId data_pid) {
  // Mark the data page as deallocated in its corresponding directory page entry 
  //
  // TODO: Your implementation
  // Mark the page as deallocated in directory page

  if (!data_pid.IsValid()) {
    return false;
  }

  uint32_t data_page_num = data_pid.GetPageNum();
  if (data_page_num >= GetPageCount()) {
    return false;
  }
  int entries_per_dir_page = PAGE_SIZE / sizeof(struct DirectoryPage::Entry);
  uint32_t dir_page_num = data_page_num / entries_per_dir_page;
  int entry_num = data_page_num % entries_per_dir_page;

  BufferManager* bm = BufferManager::Get();
  PageId dir_pid = PageId(dir.GetId(), dir_page_num);
  Page* dir_page = bm->PinPage(dir_pid);
  DirectoryPage* dir_page_dirp = dir_page->GetDirPage();

  if (dir_page_dirp->entries[entry_num].allocated) {
    dir_page_dirp->entries[entry_num].allocated = false;
    dir_page->SetDirty(true);
    bm->UnpinPage(dir_page);
    return true;
  }
  bm->UnpinPage(dir_page);

  return false;
}

bool File::PageExists(PageId pid) {
  // Pin the directory page corresponding to the specifid page (pid) and check whether 
  // the page is in the "allocated" state.

  BufferManager* bm = BufferManager::Get();
  uint32_t data_page_num = pid.GetPageNum();
  int entries_per_dir_page = PAGE_SIZE / sizeof(struct DirectoryPage::Entry);
  uint32_t dir_page_num = data_page_num / entries_per_dir_page;
  int entry_num = data_page_num % entries_per_dir_page;
  PageId dir_pid = PageId(dir.GetId(), dir_page_num);
  Page* dir_page = bm->PinPage(dir_pid);
  DirectoryPage* dir_page_dirp = dir_page->GetDirPage();

  if (dir_page_dirp->entries[entry_num].allocated){
    bm->UnpinPage(dir_page);
    return true;
  } else {
    bm->UnpinPage(dir_page);
    return false;
  }
}

PageId File::ScavengePage() {
  // Loop over all directory pages, and for each page, check if any entry
  // represents a "created" but not "allocatd" page. If so, allocate it by
  // setting the proper directory entry details:
  // 1. Mark it as "allocated"
  // 2. Initialize the number of free slots
  //
  // Return the page ID of the allocated data page; an invalid page ID should
  // be returned if no page is scavenged.
  //
  // Note: remember to mark the modified directory page as "dirty" and 
  //       properly pin/unpin/latch/unlatch it.
  //
  // TODO: Your implementation
  BufferManager* bm = BufferManager::Get();
  for (unsigned int i=0; i<dir.GetPageCount(); i++) {
    PageId dir_pid = PageId(dir.GetId(), i);
    Page* dir_page = bm->PinPage(dir_pid);
    DirectoryPage* dir_page_dirp = dir_page->GetDirPage();
    int entries_per_dir_page = PAGE_SIZE / sizeof(struct DirectoryPage::Entry);
    for (int j=0; j<entries_per_dir_page; j++)
      if (dir_page_dirp->entries[j].created && !dir_page_dirp->entries[j].allocated) {
        dir_page_dirp->entries[j].allocated = true;
        dir_page->SetDirty(true);
        dir_page_dirp->entries[j].free_slots = DataPage::GetCapacity(record_size);
        PageId data_pid = PageId(this->GetId(), i * entries_per_dir_page + j);
        bm->UnpinPage(dir_page);
        // TODO: add latch/unlatch
        return data_pid;
      }
    bm->UnpinPage(dir_page);
  }

  return PageId();
}

}  // namespace yase

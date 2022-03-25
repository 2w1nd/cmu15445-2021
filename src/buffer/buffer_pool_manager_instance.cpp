//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
    pages_[i].page_id_ = INVALID_PAGE_ID;
    pages_[i].is_dirty_ = false;
    pages_[i].pin_count_ = 0;
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (page_table_.count(page_id) == 0U) {
    return false;
  }
  FlushPg(page_id);
  pages_[page_table_[page_id]].is_dirty_ = false; // 将该页写入磁盘，记得将其更新为干净页
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for (const auto &item : page_table_) {
    FlushPgImp(item.first);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t frame_id = FindFreshPage();
  if (frame_id == -1) {
    return nullptr;
  }
  pages_[frame_id].pin_count_ ++;
  pages_[frame_id].page_id_ = AllocatePage();
  page_table_[pages_[frame_id].page_id_] = frame_id;

  *page_id = pages_[frame_id].GetPageId();
  memset(pages_[frame_id].GetData(), 0, PAGE_SIZE); // 为数据分配内存
  return &pages_[frame_id];
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  frame_id_t frame_id = FindPage(page_id);
  if (frame_id != -1) {
    replacer_->Pin(frame_id);
    pages_[frame_id].pin_count_ ++;
    pages_[frame_id].is_dirty_ = true;
    return &pages_[frame_id];
  }
  frame_id = FindFreshPage();
  if (frame_id == -1) {
    return nullptr;
  }
  page_table_[page_id] = frame_id;
  replacer_->Pin(page_table_[page_id]);
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ ++;
  pages_[frame_id].is_dirty_ = false;
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  return &pages_[frame_id];
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  DeallocatePage(page_id);
  frame_id_t frame_id = FindPage(page_id);
  if (frame_id != -1) {
    if (pages_[frame_id].GetPinCount() != 0) {
      return false;
    }
    page_table_.erase(page_id);
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].is_dirty_ = false;
    memset(pages_[frame_id].GetData(), 0, PAGE_SIZE);
    free_list_.push_back(frame_id);
  }
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  frame_id_t frame_id = FindPage(page_id);
  if (frame_id == -1 || pages_[frame_id].pin_count_ == 0) {
    return false;
  }
  pages_[frame_id].is_dirty_ = is_dirty;
  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->Unpin(frame_id);
    FlushPg(page_id);
  }
  return true;
}



page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

void BufferPoolManagerInstance::FlushPg(page_id_t page_id) {
  frame_id_t frame_id = FindPage(page_id);
  if (pages_[frame_id].IsDirty()) { // 如果该页是脏页
    disk_manager_->WritePage(frame_id, pages_[frame_id].GetData());
  }
}

frame_id_t BufferPoolManagerInstance::FindPage(page_id_t page_id) {
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.cend()) { // 如果找到
    return iter->second;
  }
  return -1;
}

frame_id_t BufferPoolManagerInstance::FindFreshPage() {
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    return frame_id;
  }
  if (replacer_->Victim(&frame_id)) {
    auto page_id = pages_[frame_id].page_id_;
    FlushPgImp(page_id);  // 将当前弹出的页面先刷到磁盘里
    page_table_.erase(page_id);
    pages_[frame_id].is_dirty_ = false;
    return frame_id;
  }
  return -1;
}

}  // namespace bustub

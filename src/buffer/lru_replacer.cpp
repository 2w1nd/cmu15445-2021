//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { pages_num_ = num_pages; }

LRUReplacer::~LRUReplacer() {
  map_frames_.clear();
  list_unpinned_.clear();
}

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (map_frames_.empty()) {
    return false;
  }
  *frame_id = list_unpinned_.back();
  list_unpinned_.pop_back();
  map_frames_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (map_frames_.count(frame_id) == 0U) {
    return;
  }
  auto p = map_frames_[frame_id];
  list_unpinned_.erase(p);
  map_frames_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (map_frames_.count(frame_id) != 0U) {
    return;
  }
  if (map_frames_.size() == pages_num_) {
    list_unpinned_.pop_back();
    map_frames_.erase(frame_id);
  }
  list_unpinned_.push_front(frame_id);
  map_frames_.emplace(frame_id, list_unpinned_.begin());
}

size_t LRUReplacer::Size() { return map_frames_.size(); }

}  // namespace bustub

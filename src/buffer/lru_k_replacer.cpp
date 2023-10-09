//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <exception>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0U) {
    return false;
  }
  for (auto i = history_list_.rbegin(); i != history_list_.rend(); i++) {
    auto frame = *i;  // be cautious the erase method will modify the iterator
    if (is_evictable_[frame]) {
      *frame_id = frame;
      curr_size_--;
      access_num_[frame] = 0;
      is_evictable_[frame] = false;
      // history_list_.erase(hashmap_[frame]);  // need positive iterator instead of history_list_.erase(i);
      history_list_.remove(frame);
      hashmap_.erase(frame);
      return true;
    }
  }
  for (auto i = cache_list_.rbegin(); i != cache_list_.rend(); i++) {
    auto frame = *i;
    if (is_evictable_[frame]) {
      *frame_id = frame;
      curr_size_--;
      access_num_[frame] = 0;
      is_evictable_[frame] = false;
      // cache_list_.erase(hashmap_[frame]);  // need positive iterator instead of history_list_.erase(i);
      cache_list_.remove(frame);
      hashmap_.erase(frame);
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {  // size_t to int cast
    throw std::exception();
  }
  size_t num = ++access_num_[frame_id];
  if (num == 1) {
    history_list_.push_front(frame_id);
    hashmap_[frame_id] = history_list_.begin();
  } else if (num == k_) {
    history_list_.erase(hashmap_[frame_id]);
    cache_list_.push_front(frame_id);
    hashmap_[frame_id] = cache_list_.begin();
  } else if (num > k_) {
    cache_list_.erase(hashmap_[frame_id]);
    cache_list_.push_front(frame_id);
    hashmap_[frame_id] = cache_list_.begin();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {  // size_t to int cast
    throw std::exception();
  }
  if (access_num_[frame_id] == 0) {
    return;
  }
  if (!is_evictable_[frame_id] && set_evictable) {
    curr_size_++;
  } else if (is_evictable_[frame_id] && !set_evictable) {
    curr_size_--;
  }
  is_evictable_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {  // size_t to int cast
    throw std::exception();
  }
  size_t num = access_num_[frame_id];
  if (num == 0) {
    return;
  }
  if (!is_evictable_[frame_id]) {
    throw std::exception();
  }
  if (num < k_) {
    history_list_.remove(frame_id);
  } else {
    cache_list_.remove(frame_id);
  }
  curr_size_--;
  hashmap_.erase(frame_id);
  access_num_[frame_id] = 0;
  is_evictable_[frame_id] = false;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub

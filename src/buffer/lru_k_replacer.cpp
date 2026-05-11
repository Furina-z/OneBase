#include "onebase/buffer/lru_k_replacer.h"
#include "onebase/common/exception.h"

namespace onebase {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : max_frames_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);

  bool found_inf = false;
  bool found_finite = false;

  frame_id_t inf_victim = INVALID_FRAME_ID;
  frame_id_t finite_victim = INVALID_FRAME_ID;

  size_t earliest_first_access = 0;
  size_t max_k_distance = 0;

  for (const auto &[fid, entry] : entries_) {
    if (!entry.is_evictable_) {
      continue;
    }

    if (entry.history_.empty()) {
      continue;
    }

    // Case 1: fewer than k accesses => backward k-distance = +infinity.
    // Among these pages, evict the one with the earliest first access.
    if (entry.history_.size() < k_) {
      size_t first_access = entry.history_.front();

      if (!found_inf || first_access < earliest_first_access) {
        found_inf = true;
        earliest_first_access = first_access;
        inf_victim = fid;
      }

      continue;
    }

    // Case 2: at least k accesses => finite backward k-distance.
    // Evict the one with the largest backward k-distance.
    size_t k_distance = current_timestamp_ - entry.history_.front();

    if (!found_finite || k_distance > max_k_distance) {
      found_finite = true;
      max_k_distance = k_distance;
      finite_victim = fid;
    }
  }

  frame_id_t victim = INVALID_FRAME_ID;

  if (found_inf) {
    victim = inf_victim;
  } else if (found_finite) {
    victim = finite_victim;
  } else {
    return false;
  }

  entries_.erase(victim);
  curr_size_--;

  *frame_id = victim;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);

  auto &entry = entries_[frame_id];

  entry.history_.push_back(current_timestamp_);
  current_timestamp_++;

  while (entry.history_.size() > k_) {
    entry.history_.pop_front();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);

  auto it = entries_.find(frame_id);
  if (it == entries_.end()) {
    return;
  }

  auto &entry = it->second;

  if (entry.is_evictable_ && !set_evictable) {
    curr_size_--;
  } else if (!entry.is_evictable_ && set_evictable) {
    curr_size_++;
  }

  entry.is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);

  auto it = entries_.find(frame_id);
  if (it == entries_.end()) {
    return;
  }

  if (!it->second.is_evictable_) {
    throw NotImplementedException("LRUKReplacer::Remove: frame is not evictable");
  }

  entries_.erase(it);
  curr_size_--;
}

auto LRUKReplacer::Size() const -> size_t {
  return curr_size_;
}

}  // namespace onebase
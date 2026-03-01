#include "onebase/buffer/lru_k_replacer.h"
#include "onebase/common/exception.h"

namespace onebase {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : max_frames_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // TODO(student): Implement LRU-K eviction policy
  // - Find the frame with the largest backward k-distance
  // - Among frames with fewer than k accesses, evict the one with earliest first access
  // - Only consider evictable frames
  throw NotImplementedException("LRUKReplacer::Evict");
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  // TODO(student): Record a new access for frame_id at current timestamp
  // - If frame_id is new, create an entry
  // - Add current_timestamp_ to the frame's history
  // - Increment current_timestamp_
  throw NotImplementedException("LRUKReplacer::RecordAccess");
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // TODO(student): Set whether a frame is evictable
  // - Update curr_size_ accordingly
  throw NotImplementedException("LRUKReplacer::SetEvictable");
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // TODO(student): Remove a frame from the replacer
  // - The frame must be evictable; throw if not
  throw NotImplementedException("LRUKReplacer::Remove");
}

auto LRUKReplacer::Size() const -> size_t {
  // TODO(student): Return the number of evictable frames
  throw NotImplementedException("LRUKReplacer::Size");
}

}  // namespace onebase

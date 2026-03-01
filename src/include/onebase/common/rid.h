#pragma once

#include "onebase/common/types.h"

#include <cstdint>
#include <functional>
#include <string>

namespace onebase {

class RID {
   public:
    RID() = default;
    RID(page_id_t page_id, slot_offset_t slot_num) : page_id_(page_id), slot_num_(slot_num) {}

    auto GetPageId() const -> page_id_t { return page_id_; }
    auto GetSlotNum() const -> slot_offset_t { return slot_num_; }

    auto operator==(const RID &other) const -> bool {
        return page_id_ == other.page_id_ && slot_num_ == other.slot_num_;
    }

    auto operator!=(const RID &other) const -> bool { return !(*this == other); }

    auto ToString() const -> std::string;

   private:
    page_id_t page_id_{INVALID_PAGE_ID};
    slot_offset_t slot_num_{0};
};

}  // namespace onebase

namespace std {
template <>
struct hash<onebase::RID> {
    auto operator()(const onebase::RID &rid) const -> size_t {
        return hash<int64_t>()(static_cast<int64_t>(rid.GetPageId()) << 16 | rid.GetSlotNum());
    }
};
}  // namespace std

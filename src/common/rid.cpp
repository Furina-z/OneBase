#include "onebase/common/rid.h"

#include <fmt/format.h>

namespace onebase {

auto RID::ToString() const -> std::string { return fmt::format("({}, {})", page_id_, slot_num_); }

}  // namespace onebase

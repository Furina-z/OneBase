#pragma once
#include <cstring>
#include <optional>
#include "onebase/common/config.h"
#include "onebase/common/rid.h"
#include "onebase/common/types.h"

namespace onebase {

class TablePage {
 public:
  static constexpr size_t SIZE_TABLE_PAGE_HEADER = 16;
  static constexpr size_t SIZE_TUPLE_SLOT = 4;

  void Init(page_id_t page_id);

  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto GetNumTuples() const -> uint32_t;

  auto InsertTuple(const char *tuple_data, uint32_t tuple_size) -> std::optional<uint16_t>;
  void GetTuple(uint16_t slot_num, char *dest, uint32_t *out_size) const;
  auto GetTupleSize(uint16_t slot_num) const -> uint32_t;
  void DeleteTuple(uint16_t slot_num);
  auto UpdateTuple(uint16_t slot_num, const char *new_data, uint32_t new_size) -> bool;

  auto GetFreeSpaceRemaining() const -> uint32_t;

 private:
  auto GetFreeSpacePointer() const -> uint32_t;
  void SetFreeSpacePointer(uint32_t fsp);
  void SetNumTuples(uint32_t num_tuples);

  auto GetTupleOffsetAtSlot(uint16_t slot_num) const -> uint16_t;
  auto GetTupleLengthAtSlot(uint16_t slot_num) const -> uint16_t;
  void SetTupleOffsetAtSlot(uint16_t slot_num, uint16_t offset);
  void SetTupleLengthAtSlot(uint16_t slot_num, uint16_t length);

  char data_[ONEBASE_PAGE_SIZE];
};

}  // namespace onebase

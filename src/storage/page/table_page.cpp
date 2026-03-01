#include "onebase/storage/page/table_page.h"
#include <cstring>
#include "onebase/common/config.h"

namespace onebase {

void TablePage::Init(page_id_t page_id) {
  std::memset(data_, 0, ONEBASE_PAGE_SIZE);
  *reinterpret_cast<page_id_t *>(data_) = page_id;
  *reinterpret_cast<uint32_t *>(data_ + 4) = 0;
  SetFreeSpacePointer(ONEBASE_PAGE_SIZE);
}

auto TablePage::GetNextPageId() const -> page_id_t {
  return *reinterpret_cast<const page_id_t *>(data_);
}

void TablePage::SetNextPageId(page_id_t next_page_id) {
  *reinterpret_cast<page_id_t *>(data_) = next_page_id;
}

auto TablePage::GetNumTuples() const -> uint32_t {
  return *reinterpret_cast<const uint32_t *>(data_ + 4);
}

void TablePage::SetNumTuples(uint32_t num_tuples) {
  *reinterpret_cast<uint32_t *>(data_ + 4) = num_tuples;
}

auto TablePage::GetFreeSpacePointer() const -> uint32_t {
  return *reinterpret_cast<const uint32_t *>(data_ + 8);
}

void TablePage::SetFreeSpacePointer(uint32_t fsp) {
  *reinterpret_cast<uint32_t *>(data_ + 8) = fsp;
}

auto TablePage::GetTupleOffsetAtSlot(uint16_t slot_num) const -> uint16_t {
  auto slot_offset = SIZE_TABLE_PAGE_HEADER + slot_num * SIZE_TUPLE_SLOT;
  return *reinterpret_cast<const uint16_t *>(data_ + slot_offset);
}

auto TablePage::GetTupleLengthAtSlot(uint16_t slot_num) const -> uint16_t {
  auto slot_offset = SIZE_TABLE_PAGE_HEADER + slot_num * SIZE_TUPLE_SLOT + 2;
  return *reinterpret_cast<const uint16_t *>(data_ + slot_offset);
}

void TablePage::SetTupleOffsetAtSlot(uint16_t slot_num, uint16_t offset) {
  auto slot_offset = SIZE_TABLE_PAGE_HEADER + slot_num * SIZE_TUPLE_SLOT;
  *reinterpret_cast<uint16_t *>(data_ + slot_offset) = offset;
}

void TablePage::SetTupleLengthAtSlot(uint16_t slot_num, uint16_t length) {
  auto slot_offset = SIZE_TABLE_PAGE_HEADER + slot_num * SIZE_TUPLE_SLOT + 2;
  *reinterpret_cast<uint16_t *>(data_ + slot_offset) = length;
}

auto TablePage::GetFreeSpaceRemaining() const -> uint32_t {
  auto slot_end = SIZE_TABLE_PAGE_HEADER + GetNumTuples() * SIZE_TUPLE_SLOT;
  return GetFreeSpacePointer() - slot_end;
}

auto TablePage::InsertTuple(const char *tuple_data, uint32_t tuple_size) -> std::optional<uint16_t> {
  if (GetFreeSpaceRemaining() < tuple_size + SIZE_TUPLE_SLOT) {
    return std::nullopt;
  }
  auto num_tuples = GetNumTuples();
  auto fsp = GetFreeSpacePointer();
  auto new_fsp = fsp - tuple_size;
  SetFreeSpacePointer(new_fsp);
  std::memcpy(data_ + new_fsp, tuple_data, tuple_size);
  SetTupleOffsetAtSlot(num_tuples, static_cast<uint16_t>(new_fsp));
  SetTupleLengthAtSlot(num_tuples, static_cast<uint16_t>(tuple_size));
  SetNumTuples(num_tuples + 1);
  return static_cast<uint16_t>(num_tuples);
}

void TablePage::GetTuple(uint16_t slot_num, char *dest, uint32_t *out_size) const {
  auto offset = GetTupleOffsetAtSlot(slot_num);
  auto length = GetTupleLengthAtSlot(slot_num);
  std::memcpy(dest, data_ + offset, length);
  if (out_size) { *out_size = length; }
}

auto TablePage::GetTupleSize(uint16_t slot_num) const -> uint32_t {
  return GetTupleLengthAtSlot(slot_num);
}

void TablePage::DeleteTuple(uint16_t slot_num) {
  SetTupleLengthAtSlot(slot_num, 0);
  SetTupleOffsetAtSlot(slot_num, 0);
}

auto TablePage::UpdateTuple(uint16_t slot_num, const char *new_data, uint32_t new_size) -> bool {
  auto old_size = GetTupleLengthAtSlot(slot_num);
  if (new_size > old_size && (new_size - old_size) > GetFreeSpaceRemaining()) {
    return false;
  }
  DeleteTuple(slot_num);
  auto fsp = GetFreeSpacePointer();
  auto new_fsp = fsp - new_size;
  SetFreeSpacePointer(new_fsp);
  std::memcpy(data_ + new_fsp, new_data, new_size);
  SetTupleOffsetAtSlot(slot_num, static_cast<uint16_t>(new_fsp));
  SetTupleLengthAtSlot(slot_num, static_cast<uint16_t>(new_size));
  return true;
}

}  // namespace onebase

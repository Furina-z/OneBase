#include "onebase/storage/table/table_heap.h"
#include "onebase/storage/page/table_page.h"

namespace onebase {

TableHeap::TableHeap(BufferPoolManager *bpm) : bpm_(bpm) {
  page_id_t page_id;
  auto *page = bpm_->NewPage(&page_id);
  if (page == nullptr) {
    throw std::runtime_error("Cannot allocate page for table heap");
  }
  first_page_id_ = page_id;
  auto *table_page = reinterpret_cast<TablePage *>(page->GetData());
  table_page->Init(INVALID_PAGE_ID);
  bpm_->UnpinPage(page_id, true);
}

auto TableHeap::InsertTuple(const Tuple &tuple) -> std::optional<RID> {
  auto page_id = first_page_id_;
  while (page_id != INVALID_PAGE_ID) {
    auto *page = bpm_->FetchPage(page_id);
    if (page == nullptr) { return std::nullopt; }
    auto *table_page = reinterpret_cast<TablePage *>(page->GetData());
    auto slot = table_page->InsertTuple(tuple.GetData(), tuple.GetLength());
    if (slot.has_value()) {
      bpm_->UnpinPage(page_id, true);
      return RID(page_id, slot.value());
    }
    auto next_page_id = table_page->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      page_id_t new_page_id;
      auto *new_page = bpm_->NewPage(&new_page_id);
      if (new_page == nullptr) {
        bpm_->UnpinPage(page_id, false);
        return std::nullopt;
      }
      auto *new_table_page = reinterpret_cast<TablePage *>(new_page->GetData());
      new_table_page->Init(INVALID_PAGE_ID);
      table_page->SetNextPageId(new_page_id);
      bpm_->UnpinPage(page_id, true);
      auto new_slot = new_table_page->InsertTuple(tuple.GetData(), tuple.GetLength());
      bpm_->UnpinPage(new_page_id, true);
      if (new_slot.has_value()) {
        return RID(new_page_id, new_slot.value());
      }
      return std::nullopt;
    }
    bpm_->UnpinPage(page_id, false);
    page_id = next_page_id;
  }
  return std::nullopt;
}

auto TableHeap::GetTuple(const RID &rid) -> Tuple {
  auto *page = bpm_->FetchPage(rid.GetPageId());
  if (page == nullptr) {
    throw std::runtime_error("Cannot fetch page for GetTuple");
  }
  auto *table_page = reinterpret_cast<TablePage *>(page->GetData());
  char buffer[ONEBASE_PAGE_SIZE];
  uint32_t size = 0;
  table_page->GetTuple(rid.GetSlotNum(), buffer, &size);
  bpm_->UnpinPage(rid.GetPageId(), false);
  Tuple tuple;
  tuple.DeserializeFrom(buffer, size);
  tuple.SetRID(rid);
  return tuple;
}

auto TableHeap::UpdateTuple(const RID &rid, const Tuple &new_tuple) -> bool {
  auto *page = bpm_->FetchPage(rid.GetPageId());
  if (page == nullptr) { return false; }
  auto *table_page = reinterpret_cast<TablePage *>(page->GetData());
  bool success = table_page->UpdateTuple(rid.GetSlotNum(), new_tuple.GetData(), new_tuple.GetLength());
  bpm_->UnpinPage(rid.GetPageId(), success);
  return success;
}

void TableHeap::DeleteTuple(const RID &rid) {
  auto *page = bpm_->FetchPage(rid.GetPageId());
  if (page == nullptr) { return; }
  auto *table_page = reinterpret_cast<TablePage *>(page->GetData());
  table_page->DeleteTuple(rid.GetSlotNum());
  bpm_->UnpinPage(rid.GetPageId(), true);
}

// --- Iterator ---

TableHeap::Iterator::Iterator(TableHeap *table_heap, RID rid)
    : table_heap_(table_heap), rid_(rid) {}

auto TableHeap::Iterator::operator*() -> Tuple {
  return table_heap_->GetTuple(rid_);
}

auto TableHeap::Iterator::operator++() -> Iterator & {
  auto page_id = rid_.GetPageId();
  auto slot = static_cast<uint16_t>(rid_.GetSlotNum() + 1);

  while (page_id != INVALID_PAGE_ID) {
    auto *page = table_heap_->bpm_->FetchPage(page_id);
    if (page == nullptr) {
      break;
    }
    auto *table_page = reinterpret_cast<TablePage *>(page->GetData());
    auto num_tuples = table_page->GetNumTuples();

    // Find the next valid (non-deleted) slot on this page
    while (slot < num_tuples) {
      if (table_page->GetTupleSize(slot) > 0) {
        table_heap_->bpm_->UnpinPage(page_id, false);
        rid_ = RID(page_id, slot);
        return *this;
      }
      slot++;
    }

    // No more valid slots on this page — move to the next page
    auto next_page_id = table_page->GetNextPageId();
    table_heap_->bpm_->UnpinPage(page_id, false);
    page_id = next_page_id;
    slot = 0;
  }

  rid_ = RID(INVALID_PAGE_ID, 0);
  return *this;
}

auto TableHeap::Iterator::operator==(const Iterator &other) const -> bool {
  return rid_ == other.rid_;
}

auto TableHeap::Iterator::operator!=(const Iterator &other) const -> bool {
  return !(*this == other);
}

auto TableHeap::Begin() -> Iterator {
  auto page_id = first_page_id_;
  while (page_id != INVALID_PAGE_ID) {
    auto *page = bpm_->FetchPage(page_id);
    if (page == nullptr) {
      break;
    }
    auto *table_page = reinterpret_cast<TablePage *>(page->GetData());
    auto num_tuples = table_page->GetNumTuples();

    // Find first valid (non-deleted) slot
    for (uint16_t slot = 0; slot < num_tuples; slot++) {
      if (table_page->GetTupleSize(slot) > 0) {
        bpm_->UnpinPage(page_id, false);
        return Iterator(this, RID(page_id, slot));
      }
    }

    auto next = table_page->GetNextPageId();
    bpm_->UnpinPage(page_id, false);
    page_id = next;
  }
  return End();
}

auto TableHeap::End() -> Iterator {
  return Iterator(this, RID(INVALID_PAGE_ID, 0));
}

}  // namespace onebase

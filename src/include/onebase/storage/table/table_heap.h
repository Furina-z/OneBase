#pragma once
#include <memory>
#include <optional>
#include "onebase/buffer/buffer_pool_manager.h"
#include "onebase/common/rid.h"
#include "onebase/storage/table/tuple.h"

namespace onebase {

class TableHeap {
 public:
  explicit TableHeap(BufferPoolManager *bpm);
  ~TableHeap() = default;

  auto InsertTuple(const Tuple &tuple) -> std::optional<RID>;
  auto GetTuple(const RID &rid) -> Tuple;
  auto UpdateTuple(const RID &rid, const Tuple &new_tuple) -> bool;
  void DeleteTuple(const RID &rid);

  auto GetFirstPageId() const -> page_id_t { return first_page_id_; }

  class Iterator {
   public:
    Iterator(TableHeap *table_heap, RID rid);
    auto operator*() -> Tuple;
    auto operator++() -> Iterator &;
    auto operator==(const Iterator &other) const -> bool;
    auto operator!=(const Iterator &other) const -> bool;
    auto GetRID() const -> RID { return rid_; }

   private:
    TableHeap *table_heap_;
    RID rid_;
  };

  auto Begin() -> Iterator;
  auto End() -> Iterator;

 private:
  BufferPoolManager *bpm_;
  page_id_t first_page_id_{INVALID_PAGE_ID};
};

}  // namespace onebase

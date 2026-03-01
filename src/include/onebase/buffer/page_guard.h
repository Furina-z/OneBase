#pragma once
#include "onebase/buffer/buffer_pool_manager.h"
#include "onebase/storage/page/page.h"

namespace onebase {

class BasicPageGuard {
 public:
  BasicPageGuard() = default;
  BasicPageGuard(BufferPoolManager *bpm, Page *page);
  BasicPageGuard(BasicPageGuard &&that) noexcept;
  auto operator=(BasicPageGuard &&that) -> BasicPageGuard &;
  ~BasicPageGuard();

  BasicPageGuard(const BasicPageGuard &) = delete;
  auto operator=(const BasicPageGuard &) -> BasicPageGuard & = delete;

  auto GetPageId() const -> page_id_t;
  auto GetData() const -> const char *;
  auto GetDataMut() -> char *;
  template <class T>
  auto As() const -> const T * { return reinterpret_cast<const T *>(GetData()); }
  template <class T>
  auto AsMut() -> T * { return reinterpret_cast<T *>(GetDataMut()); }
  auto IsDirty() const -> bool;
  void Drop();

 private:
  BufferPoolManager *bpm_{nullptr};
  Page *page_{nullptr};
  bool is_dirty_{false};
};

class ReadPageGuard {
 public:
  ReadPageGuard() = default;
  ReadPageGuard(BufferPoolManager *bpm, Page *page);
  ReadPageGuard(ReadPageGuard &&that) noexcept;
  auto operator=(ReadPageGuard &&that) -> ReadPageGuard &;
  ~ReadPageGuard();

  ReadPageGuard(const ReadPageGuard &) = delete;
  auto operator=(const ReadPageGuard &) -> ReadPageGuard & = delete;

  auto GetPageId() const -> page_id_t;
  auto GetData() const -> const char *;
  template <class T>
  auto As() const -> const T * { return reinterpret_cast<const T *>(GetData()); }
  void Drop();

 private:
  BufferPoolManager *bpm_{nullptr};
  Page *page_{nullptr};
};

class WritePageGuard {
 public:
  WritePageGuard() = default;
  WritePageGuard(BufferPoolManager *bpm, Page *page);
  WritePageGuard(WritePageGuard &&that) noexcept;
  auto operator=(WritePageGuard &&that) -> WritePageGuard &;
  ~WritePageGuard();

  WritePageGuard(const WritePageGuard &) = delete;
  auto operator=(const WritePageGuard &) -> WritePageGuard & = delete;

  auto GetPageId() const -> page_id_t;
  auto GetData() const -> const char *;
  auto GetDataMut() -> char *;
  template <class T>
  auto As() const -> const T * { return reinterpret_cast<const T *>(GetData()); }
  template <class T>
  auto AsMut() -> T * { return reinterpret_cast<T *>(GetDataMut()); }
  void Drop();

 private:
  BufferPoolManager *bpm_{nullptr};
  Page *page_{nullptr};
};

}  // namespace onebase

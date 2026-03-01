#include "onebase/buffer/page_guard.h"
#include "onebase/common/exception.h"

namespace onebase {

// --- BasicPageGuard ---

BasicPageGuard::BasicPageGuard(BufferPoolManager *bpm, Page *page)
    : bpm_(bpm), page_(page) {}

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) -> BasicPageGuard & {
  // TODO(student): Implement move assignment
  // - Drop current page if held, then take ownership of `that`
  throw NotImplementedException("BasicPageGuard::operator=(BasicPageGuard&&)");
}

BasicPageGuard::~BasicPageGuard() { Drop(); }

auto BasicPageGuard::GetPageId() const -> page_id_t {
  return page_ ? page_->GetPageId() : INVALID_PAGE_ID;
}

auto BasicPageGuard::GetData() const -> const char * { return page_->GetData(); }
auto BasicPageGuard::GetDataMut() -> char * {
  is_dirty_ = true;
  return page_->GetData();
}

auto BasicPageGuard::IsDirty() const -> bool { return is_dirty_; }

void BasicPageGuard::Drop() {
  if (page_ == nullptr) { return; }
  // TODO(student): Unpin the page via BPM and reset state
  // - Call bpm_->UnpinPage(page_id, is_dirty_) if page_ is not null
  // - Set bpm_ and page_ to nullptr
  throw NotImplementedException("BasicPageGuard::Drop");
}

// --- ReadPageGuard ---

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page)
    : bpm_(bpm), page_(page) {
  if (page_) { page_->RLatch(); }
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) -> ReadPageGuard & {
  // TODO(student): Implement move assignment (drop current, take that)
  throw NotImplementedException("ReadPageGuard::operator=(ReadPageGuard&&)");
}

ReadPageGuard::~ReadPageGuard() { Drop(); }

auto ReadPageGuard::GetPageId() const -> page_id_t {
  return page_ ? page_->GetPageId() : INVALID_PAGE_ID;
}

auto ReadPageGuard::GetData() const -> const char * { return page_->GetData(); }

void ReadPageGuard::Drop() {
  if (page_ == nullptr) { return; }
  // TODO(student): Release read latch, unpin page, reset state
  throw NotImplementedException("ReadPageGuard::Drop");
}

// --- WritePageGuard ---

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page)
    : bpm_(bpm), page_(page) {
  if (page_) { page_->WLatch(); }
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

auto WritePageGuard::operator=(WritePageGuard &&that) -> WritePageGuard & {
  // TODO(student): Implement move assignment (drop current, take that)
  throw NotImplementedException("WritePageGuard::operator=(WritePageGuard&&)");
}

WritePageGuard::~WritePageGuard() { Drop(); }

auto WritePageGuard::GetPageId() const -> page_id_t {
  return page_ ? page_->GetPageId() : INVALID_PAGE_ID;
}

auto WritePageGuard::GetData() const -> const char * { return page_->GetData(); }
auto WritePageGuard::GetDataMut() -> char * { return page_->GetData(); }

void WritePageGuard::Drop() {
  if (page_ == nullptr) { return; }
  // TODO(student): Release write latch, unpin page (dirty=true), reset state
  throw NotImplementedException("WritePageGuard::Drop");
}

}  // namespace onebase

#include <gtest/gtest.h>
#include <cstring>
#include <string>
#include <thread>

#include "eval/grading.h"

#include "onebase/buffer/buffer_pool_manager.h"
#include "onebase/buffer/lru_k_replacer.h"
#include "onebase/buffer/page_guard.h"
#include "onebase/storage/disk/disk_manager.h"

namespace onebase {

// ============================================================
// LRU-K Replacer Tests (30 pts)
// ============================================================

GRADED_TEST(Lab1Replacer, RecordAccessAndSize, 5) {
  LRUKReplacer replacer(7, 2);

  // Record accesses for frames 1..5
  replacer.RecordAccess(1);
  replacer.RecordAccess(2);
  replacer.RecordAccess(3);
  replacer.RecordAccess(4);
  replacer.RecordAccess(5);

  // Nothing is evictable yet
  EXPECT_EQ(replacer.Size(), 0u);

  // Set frames 1, 2, 3 evictable
  replacer.SetEvictable(1, true);
  replacer.SetEvictable(2, true);
  replacer.SetEvictable(3, true);
  EXPECT_EQ(replacer.Size(), 3u);

  // Un-evict frame 2
  replacer.SetEvictable(2, false);
  EXPECT_EQ(replacer.Size(), 2u);
}

GRADED_TEST(Lab1Replacer, BasicEvict, 5) {
  LRUKReplacer replacer(7, 2);

  replacer.RecordAccess(1);
  replacer.RecordAccess(1);  // frame 1: 2 accesses (== k)
  replacer.RecordAccess(2);  // frame 2: 1 access (< k, +inf distance)

  replacer.SetEvictable(1, true);
  replacer.SetEvictable(2, true);
  EXPECT_EQ(replacer.Size(), 2u);

  frame_id_t frame;
  EXPECT_TRUE(replacer.Evict(&frame));
  // Frame 2 has < k accesses, so +inf distance, evicted first
  EXPECT_EQ(frame, 2);
  EXPECT_EQ(replacer.Size(), 1u);

  EXPECT_TRUE(replacer.Evict(&frame));
  EXPECT_EQ(frame, 1);
  EXPECT_EQ(replacer.Size(), 0u);

  // No more evictable frames
  EXPECT_FALSE(replacer.Evict(&frame));
}

GRADED_TEST(Lab1Replacer, EvictByInfDistance, 5) {
  // Among frames with < k accesses (all +inf distance), evict earliest first access
  LRUKReplacer replacer(7, 3);  // k=3

  replacer.RecordAccess(1);  // 1 access, earliest
  replacer.RecordAccess(2);  // 1 access
  replacer.RecordAccess(3);  // 1 access, latest
  replacer.RecordAccess(2);  // 2 accesses, still < k

  replacer.SetEvictable(1, true);
  replacer.SetEvictable(2, true);
  replacer.SetEvictable(3, true);

  frame_id_t frame;
  // All have +inf distance. Among those, frame 1 had earliest first access
  EXPECT_TRUE(replacer.Evict(&frame));
  EXPECT_EQ(frame, 1);

  // Frame 2 had earlier first access than frame 3
  EXPECT_TRUE(replacer.Evict(&frame));
  EXPECT_EQ(frame, 2);

  EXPECT_TRUE(replacer.Evict(&frame));
  EXPECT_EQ(frame, 3);
}

GRADED_TEST(Lab1Replacer, EvictByKDistance, 10) {
  LRUKReplacer replacer(7, 2);  // k=2

  // Sequence: frame1 access, frame2 access, frame3 access, then each 2nd access
  // Frame 1: accesses at ts 0, 1  -> k-dist = current - 0
  // Frame 2: accesses at ts 2, 3  -> k-dist = current - 2
  // Frame 3: accesses at ts 4, 5  -> k-dist = current - 4
  replacer.RecordAccess(1);  // ts 0
  replacer.RecordAccess(1);  // ts 1 (frame 1 now has k=2 accesses)
  replacer.RecordAccess(2);  // ts 2
  replacer.RecordAccess(2);  // ts 3 (frame 2 now has k=2 accesses)
  replacer.RecordAccess(3);  // ts 4
  replacer.RecordAccess(3);  // ts 5 (frame 3 now has k=2 accesses)

  replacer.SetEvictable(1, true);
  replacer.SetEvictable(2, true);
  replacer.SetEvictable(3, true);

  frame_id_t frame;
  // All have exactly k accesses. Backward k-distance for each:
  // Frame 1: current_ts(6) - history.front(0) = 6 (largest)
  // Frame 2: 6 - 2 = 4
  // Frame 3: 6 - 4 = 2 (smallest)
  // Evict frame 1 (largest k-distance)
  EXPECT_TRUE(replacer.Evict(&frame));
  EXPECT_EQ(frame, 1);

  EXPECT_TRUE(replacer.Evict(&frame));
  EXPECT_EQ(frame, 2);

  EXPECT_TRUE(replacer.Evict(&frame));
  EXPECT_EQ(frame, 3);
}

GRADED_TEST(Lab1Replacer, SetEvictableAndRemove, 5) {
  LRUKReplacer replacer(7, 2);

  replacer.RecordAccess(0);
  replacer.RecordAccess(1);
  replacer.SetEvictable(0, true);
  replacer.SetEvictable(1, false);
  EXPECT_EQ(replacer.Size(), 1u);

  // Evict should only get frame 0 (frame 1 is non-evictable)
  frame_id_t frame;
  EXPECT_TRUE(replacer.Evict(&frame));
  EXPECT_EQ(frame, 0);
  EXPECT_EQ(replacer.Size(), 0u);

  // Now set frame 1 evictable, then Remove it
  replacer.SetEvictable(1, true);
  EXPECT_EQ(replacer.Size(), 1u);
  replacer.Remove(1);
  EXPECT_EQ(replacer.Size(), 0u);

  // Nothing left to evict
  EXPECT_FALSE(replacer.Evict(&frame));
}

// ============================================================
// Buffer Pool Manager Tests (40 pts)
// ============================================================

static auto MakeDbName(const char *tag) -> std::string {
  return std::string("__eval_bpm_") + tag + ".db";
}

GRADED_TEST(Lab1BPM, NewPageBasic, 5) {
  auto db = MakeDbName("new");
  DiskManager dm(db);
  BufferPoolManager bpm(10, &dm);

  page_id_t pid0, pid1, pid2;
  auto *p0 = bpm.NewPage(&pid0);
  auto *p1 = bpm.NewPage(&pid1);
  auto *p2 = bpm.NewPage(&pid2);

  ASSERT_NE(p0, nullptr);
  ASSERT_NE(p1, nullptr);
  ASSERT_NE(p2, nullptr);
  EXPECT_EQ(pid0, 0);
  EXPECT_EQ(pid1, 1);
  EXPECT_EQ(pid2, 2);

  bpm.UnpinPage(pid0, false);
  bpm.UnpinPage(pid1, false);
  bpm.UnpinPage(pid2, false);

  dm.ShutDown();
  std::remove(db.c_str());
}

GRADED_TEST(Lab1BPM, FetchPageBasic, 5) {
  auto db = MakeDbName("fetch");
  DiskManager dm(db);
  BufferPoolManager bpm(10, &dm);

  page_id_t pid;
  auto *page = bpm.NewPage(&pid);
  ASSERT_NE(page, nullptr);

  // Write data
  std::snprintf(page->GetData(), 256, "hello_onebase");
  bpm.UnpinPage(pid, true);

  // Fetch the same page
  auto *fetched = bpm.FetchPage(pid);
  ASSERT_NE(fetched, nullptr);
  EXPECT_STREQ(fetched->GetData(), "hello_onebase");
  bpm.UnpinPage(pid, false);

  dm.ShutDown();
  std::remove(db.c_str());
}

GRADED_TEST(Lab1BPM, UnpinAndEvict, 10) {
  auto db = MakeDbName("evict");
  DiskManager dm(db);
  BufferPoolManager bpm(3, &dm);  // small pool

  page_id_t pids[3];
  for (int i = 0; i < 3; i++) {
    auto *p = bpm.NewPage(&pids[i]);
    ASSERT_NE(p, nullptr);
  }

  // Pool is full, NewPage should fail
  page_id_t extra;
  EXPECT_EQ(bpm.NewPage(&extra), nullptr);

  // Unpin all
  for (int i = 0; i < 3; i++) {
    bpm.UnpinPage(pids[i], false);
  }

  // Now we should be able to allocate a new page (evicts one)
  auto *p = bpm.NewPage(&extra);
  EXPECT_NE(p, nullptr);
  bpm.UnpinPage(extra, false);

  dm.ShutDown();
  std::remove(db.c_str());
}

GRADED_TEST(Lab1BPM, DirtyPagePersistence, 10) {
  auto db = MakeDbName("dirty");
  DiskManager dm(db);
  BufferPoolManager bpm(3, &dm);

  // Create page and write data
  page_id_t pid;
  auto *page = bpm.NewPage(&pid);
  ASSERT_NE(page, nullptr);
  std::snprintf(page->GetData(), 256, "persist_test");
  bpm.UnpinPage(pid, true);  // Mark dirty

  // Fill the pool to force eviction
  page_id_t dummy_pids[3];
  for (int i = 0; i < 3; i++) {
    bpm.NewPage(&dummy_pids[i]);
  }
  for (int i = 0; i < 3; i++) {
    bpm.UnpinPage(dummy_pids[i], false);
  }

  // Fetch original page — should read from disk
  auto *fetched = bpm.FetchPage(pid);
  ASSERT_NE(fetched, nullptr);
  EXPECT_STREQ(fetched->GetData(), "persist_test");
  bpm.UnpinPage(pid, false);

  dm.ShutDown();
  std::remove(db.c_str());
}

GRADED_TEST(Lab1BPM, DeletePage, 5) {
  auto db = MakeDbName("del");
  DiskManager dm(db);
  BufferPoolManager bpm(3, &dm);

  page_id_t pids[3];
  for (int i = 0; i < 3; i++) {
    auto *p = bpm.NewPage(&pids[i]);
    ASSERT_NE(p, nullptr);
  }

  // Pool is full
  page_id_t extra;
  EXPECT_EQ(bpm.NewPage(&extra), nullptr);

  // Unpin and delete page 0
  bpm.UnpinPage(pids[0], false);
  EXPECT_TRUE(bpm.DeletePage(pids[0]));

  // Now we can allocate a new page using the freed frame
  auto *p = bpm.NewPage(&extra);
  EXPECT_NE(p, nullptr);
  bpm.UnpinPage(extra, false);

  bpm.UnpinPage(pids[1], false);
  bpm.UnpinPage(pids[2], false);

  dm.ShutDown();
  std::remove(db.c_str());
}

GRADED_TEST(Lab1BPM, FlushPage, 5) {
  auto db = MakeDbName("flush");
  DiskManager dm(db);
  BufferPoolManager bpm(10, &dm);

  page_id_t pid;
  auto *page = bpm.NewPage(&pid);
  ASSERT_NE(page, nullptr);
  std::snprintf(page->GetData(), 256, "flush_data");

  // Flush while still pinned
  EXPECT_TRUE(bpm.FlushPage(pid));
  bpm.UnpinPage(pid, false);

  // Verify data persisted: read directly from disk
  char buf[PAGE_SIZE] = {};
  dm.ReadPage(pid, buf);
  EXPECT_STREQ(buf, "flush_data");

  dm.ShutDown();
  std::remove(db.c_str());
}

// ============================================================
// PageGuard Tests (30 pts)
// ============================================================

GRADED_TEST(Lab1Guard, BasicGuardUnpin, 10) {
  auto db = MakeDbName("guard_basic");
  DiskManager dm(db);
  BufferPoolManager bpm(3, &dm);

  page_id_t pid;
  auto *page = bpm.NewPage(&pid);
  ASSERT_NE(page, nullptr);

  {
    BasicPageGuard guard(&bpm, page);
    EXPECT_EQ(guard.GetPageId(), pid);
    // Destructor should call Drop() which unpins
  }

  // Page should be unpinned now — fill the pool + allocate should work
  page_id_t pids[2];
  for (int i = 0; i < 2; i++) {
    auto *p = bpm.NewPage(&pids[i]);
    ASSERT_NE(p, nullptr);
  }
  // Pool has 3 slots, 2 pinned + 1 unpinned (from guard drop)
  page_id_t extra;
  auto *p = bpm.NewPage(&extra);
  EXPECT_NE(p, nullptr);  // Should evict the unpinned page

  bpm.UnpinPage(pids[0], false);
  bpm.UnpinPage(pids[1], false);
  bpm.UnpinPage(extra, false);

  dm.ShutDown();
  std::remove(db.c_str());
}

GRADED_TEST(Lab1Guard, ReadGuardLatch, 10) {
  auto db = MakeDbName("guard_read");
  DiskManager dm(db);
  BufferPoolManager bpm(3, &dm);

  page_id_t pid;
  auto *page = bpm.NewPage(&pid);
  ASSERT_NE(page, nullptr);

  {
    // ReadPageGuard constructor acquires RLatch
    ReadPageGuard guard(&bpm, page);
    EXPECT_EQ(guard.GetPageId(), pid);
    // Destructor releases RLatch + unpins
  }

  // After guard dropped, we should be able to take a write latch
  auto *fetched = bpm.FetchPage(pid);
  ASSERT_NE(fetched, nullptr);
  fetched->WLatch();
  fetched->WUnlatch();
  bpm.UnpinPage(pid, false);

  dm.ShutDown();
  std::remove(db.c_str());
}

GRADED_TEST(Lab1Guard, WriteGuardLatch, 5) {
  auto db = MakeDbName("guard_write");
  DiskManager dm(db);
  BufferPoolManager bpm(3, &dm);

  page_id_t pid;
  auto *page = bpm.NewPage(&pid);
  ASSERT_NE(page, nullptr);

  {
    // WritePageGuard constructor acquires WLatch
    WritePageGuard guard(&bpm, page);
    EXPECT_EQ(guard.GetPageId(), pid);
    std::snprintf(guard.GetDataMut(), 256, "write_guard_data");
    // Destructor releases WLatch + unpins (dirty)
  }

  // After drop, verify data and that latch was released
  auto *fetched = bpm.FetchPage(pid);
  ASSERT_NE(fetched, nullptr);
  EXPECT_STREQ(fetched->GetData(), "write_guard_data");
  fetched->RLatch();
  fetched->RUnlatch();
  bpm.UnpinPage(pid, false);

  dm.ShutDown();
  std::remove(db.c_str());
}

GRADED_TEST(Lab1Guard, MoveSemantics, 5) {
  auto db = MakeDbName("guard_move");
  DiskManager dm(db);
  BufferPoolManager bpm(3, &dm);

  page_id_t pid;
  auto *page = bpm.NewPage(&pid);
  ASSERT_NE(page, nullptr);

  BasicPageGuard guard1(&bpm, page);
  EXPECT_EQ(guard1.GetPageId(), pid);

  // Move construct
  BasicPageGuard guard2(std::move(guard1));
  EXPECT_EQ(guard1.GetPageId(), INVALID_PAGE_ID);
  EXPECT_EQ(guard2.GetPageId(), pid);

  // Move assign
  BasicPageGuard guard3;
  guard3 = std::move(guard2);
  EXPECT_EQ(guard2.GetPageId(), INVALID_PAGE_ID);
  EXPECT_EQ(guard3.GetPageId(), pid);

  guard3.Drop();

  dm.ShutDown();
  std::remove(db.c_str());
}

}  // namespace onebase

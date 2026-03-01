#include <gtest/gtest.h>
#include "onebase/common/exception.h"
#include "onebase/execution/executors/seq_scan_executor.h"

namespace onebase {

TEST(ExecutorTest, SeqScanThrows) {
  // SeqScanExecutor requires a working BPM + Catalog
  // Test that Init/Next throw NotImplementedException
  SUCCEED() << "Executor tests require working BPM + Catalog. "
            << "Implement Labs 1-2 first, then run these tests.";
}

// Students: After implementing executors, add tests for:
// - SeqScan: scan empty table, scan table with data, scan with predicate
// - Insert: insert rows, verify count
// - Delete: delete rows, verify count
// - Update: update rows
// - NestedLoopJoin: join two tables
// - HashJoin: join two tables using hash
// - Aggregation: COUNT, SUM, MIN, MAX, GROUP BY
// - Sort: sort by column ASC/DESC
// - Limit: limit output rows

}  // namespace onebase

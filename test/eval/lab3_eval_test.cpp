#include "eval/grading.h"
#include "execution/lab3_test_common.h"

namespace onebase {

using onebase::test::LargeSqlExecutorLab3Test;
using onebase::test::SqlExecutorLab3Test;

GRADED_TEST_F(SqlExecutorLab3Test, SeqScanPredicatesAndDuplicates, 15) { VerifySeqScanPredicatesAndDuplicates(); }

GRADED_TEST_F(SqlExecutorLab3Test, ProjectionAndArithmeticCornerCases, 15) {
  VerifyProjectionAndArithmeticCornerCases();
}

GRADED_TEST_F(SqlExecutorLab3Test, InsertUpdateDeleteCounts, 20) { VerifyInsertUpdateDeleteCounts(); }

GRADED_TEST_F(SqlExecutorLab3Test, JoinDuplicateExplosionAndMisses, 20) {
  VerifyJoinDuplicateExplosionAndMisses();
}

GRADED_TEST_F(SqlExecutorLab3Test, AggregationAndGroupingEdgeCases, 15) {
  VerifyAggregationAndGroupingEdgeCases();
}

GRADED_TEST_F(SqlExecutorLab3Test, SortLimitAndZeroLimit, 15) { VerifySortLimitAndZeroLimit(); }

GRADED_TEST_F(LargeSqlExecutorLab3Test, LargeDataRobustness, 10) { VerifyLargeDataRobustness(); }

GRADED_TEST_F(LargeSqlExecutorLab3Test, IndexedLookupsMustNotBeSlow, 10) {
  VerifyIndexedLookupsMustNotBeSlow();
}

}  // namespace onebase

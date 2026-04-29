#include "lab3_test_common.h"

namespace onebase {

using onebase::test::LargeSqlExecutorLab3Test;
using onebase::test::SqlExecutorLab3Test;

TEST_F(SqlExecutorLab3Test, SeqScanPredicatesAndDuplicates) { VerifySeqScanPredicatesAndDuplicates(); }

TEST_F(SqlExecutorLab3Test, ProjectionAndArithmeticCornerCases) { VerifyProjectionAndArithmeticCornerCases(); }

TEST_F(SqlExecutorLab3Test, InsertUpdateDeleteCounts) { VerifyInsertUpdateDeleteCounts(); }

TEST_F(SqlExecutorLab3Test, JoinDuplicateExplosionAndMisses) { VerifyJoinDuplicateExplosionAndMisses(); }

TEST_F(SqlExecutorLab3Test, AggregationAndGroupingEdgeCases) { VerifyAggregationAndGroupingEdgeCases(); }

TEST_F(SqlExecutorLab3Test, SortLimitAndZeroLimit) { VerifySortLimitAndZeroLimit(); }

TEST_F(LargeSqlExecutorLab3Test, LargeDataRobustness) { VerifyLargeDataRobustness(); }

TEST_F(LargeSqlExecutorLab3Test, IndexedLookupsMustNotBeSlow) { VerifyIndexedLookupsMustNotBeSlow(); }

}  // namespace onebase

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "eval/grading.h"
#include "eval/sql_test_client.h"

#include "onebase/catalog/column.h"
#include "onebase/catalog/schema.h"
#include "onebase/type/type_id.h"
#include "onebase/type/value.h"

namespace onebase {

namespace {

using onebase::eval::SqlTestClient;
using onebase::eval::Table;

auto SortRows(std::vector<std::vector<std::string>> rows) -> std::vector<std::vector<std::string>> {
  std::sort(rows.begin(), rows.end());
  return rows;
}

auto ExpectRows(const Table &table, const std::vector<std::string> &expected_headers,
                std::vector<std::vector<std::string>> expected_rows) -> void {
  EXPECT_EQ(table.GetHeaders(), expected_headers);
  EXPECT_EQ(SortRows(table.GetRows()), SortRows(std::move(expected_rows)));
}

auto ExpectRowsIgnoringHeaders(const Table &table,
                               std::vector<std::vector<std::string>> expected_rows) -> void {
  EXPECT_EQ(SortRows(table.GetRows()), SortRows(std::move(expected_rows)));
}

}  // namespace

class SqlExecutorEvalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    client_ = std::make_unique<SqlTestClient>("lab3", 128);

    client_->CreateTable("base",
                         Schema({Column("id", TypeId::INTEGER), Column("val", TypeId::INTEGER)}));
    client_->CreateTable("probe",
                         Schema({Column("id", TypeId::INTEGER), Column("tag", TypeId::INTEGER)}));
    client_->CreateTable("copy",
                         Schema({Column("id", TypeId::INTEGER), Column("val", TypeId::INTEGER)}));
    client_->CreateTable("empty",
                         Schema({Column("id", TypeId::INTEGER), Column("val", TypeId::INTEGER)}));

    client_->SeedTable("base",
                       {{Value(TypeId::INTEGER, 0), Value(TypeId::INTEGER, 0)},
                        {Value(TypeId::INTEGER, 1), Value(TypeId::INTEGER, 10)},
                        {Value(TypeId::INTEGER, 1), Value(TypeId::INTEGER, 11)},
                        {Value(TypeId::INTEGER, 2), Value(TypeId::INTEGER, 20)},
                        {Value(TypeId::INTEGER, 3), Value(TypeId::INTEGER, -30)},
                        {Value(TypeId::INTEGER, 4), Value(TypeId::INTEGER, 40)},
                        {Value(TypeId::INTEGER, 5), Value(TypeId::INTEGER, 50)},
                        {Value(TypeId::INTEGER, 6), Value(TypeId::INTEGER, 60)},
                        {Value(TypeId::INTEGER, 7), Value(TypeId::INTEGER, 70)},
                        {Value(TypeId::INTEGER, 8), Value(TypeId::INTEGER, 80)},
                        {Value(TypeId::INTEGER, 9), Value(TypeId::INTEGER, 90)}});

    client_->SeedTable("probe",
                       {{Value(TypeId::INTEGER, 1), Value(TypeId::INTEGER, 100)},
                        {Value(TypeId::INTEGER, 1), Value(TypeId::INTEGER, 101)},
                        {Value(TypeId::INTEGER, 3), Value(TypeId::INTEGER, 300)},
                        {Value(TypeId::INTEGER, 7), Value(TypeId::INTEGER, 700)},
                        {Value(TypeId::INTEGER, 9), Value(TypeId::INTEGER, 900)},
                        {Value(TypeId::INTEGER, 10), Value(TypeId::INTEGER, 1000)}});
  }

  std::unique_ptr<SqlTestClient> client_;
};

class LargeSqlExecutorEvalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    client_ = std::make_unique<SqlTestClient>("lab3_large", 256);

    client_->CreateTable(
        "facts_idx",
        Schema({Column("id", TypeId::INTEGER), Column("bucket", TypeId::INTEGER),
                Column("payload", TypeId::INTEGER)}));
    client_->CreateTable(
        "facts_scan",
        Schema({Column("id", TypeId::INTEGER), Column("bucket", TypeId::INTEGER),
                Column("payload", TypeId::INTEGER)}));
    client_->CreateTable("dim", Schema({Column("id", TypeId::INTEGER), Column("label", TypeId::INTEGER)}));

    std::vector<std::vector<Value>> facts_rows;
    facts_rows.reserve(kFactRows);
    for (int i = 0; i < kFactRows; ++i) {
      int key = i % kKeyCardinality;
      int bucket = i % kBucketCardinality;
      int payload = i * 17 - 5000;
      facts_rows.push_back({Value(TypeId::INTEGER, key), Value(TypeId::INTEGER, bucket),
                            Value(TypeId::INTEGER, payload)});
    }
    client_->SeedTable("facts_idx", facts_rows);
    client_->SeedTable("facts_scan", facts_rows);
    client_->ExecuteCommand("CREATE INDEX idx_facts_idx_id ON facts_idx (id)");

    std::vector<std::vector<Value>> dim_rows;
    dim_rows.reserve(kBucketCardinality);
    for (int i = 0; i < kBucketCardinality; ++i) {
      dim_rows.push_back({Value(TypeId::INTEGER, i), Value(TypeId::INTEGER, i * 100)});
    }
    client_->SeedTable("dim", dim_rows);
  }

  static constexpr int kFactRows = 30000;
  static constexpr int kKeyCardinality = 997;
  static constexpr int kBucketCardinality = 37;

  std::unique_ptr<SqlTestClient> client_;
};

// ============================================================
// Sequential scan and filters (15 pts)
// ============================================================

GRADED_TEST_F(SqlExecutorEvalTest, SeqScanPredicatesAndDuplicates, 15) {
  auto all_rows = client_->ExecuteQuery("SELECT * FROM base");
  EXPECT_EQ(all_rows.GetRowCount(), 11u);

  auto empty_rows = client_->ExecuteQuery("SELECT * FROM base WHERE id < 0");
  EXPECT_EQ(empty_rows.GetRowCount(), 0u);

  auto duplicate_rows = client_->ExecuteQuery("SELECT * FROM base WHERE id = 1");
  ExpectRows(duplicate_rows, {"id", "val"}, {{"1", "10"}, {"1", "11"}});

  auto bounded_rows = client_->ExecuteQuery("SELECT * FROM base WHERE id >= 3 AND id <= 5");
  ExpectRows(bounded_rows, {"id", "val"}, {{"3", "-30"}, {"4", "40"}, {"5", "50"}});
}

// ============================================================
// Projection and expressions (15 pts)
// ============================================================

GRADED_TEST_F(SqlExecutorEvalTest, ProjectionAndArithmeticCornerCases, 15) {
  auto projected = client_->ExecuteQuery("SELECT id, val + 1, val - id FROM base WHERE id >= 3 AND id <= 5");
  ASSERT_EQ(projected.GetRowCount(), 3u);
  ExpectRowsIgnoringHeaders(projected, {{"3", "-29", "-33"}, {"4", "41", "36"}, {"5", "51", "45"}});

  auto reordered = client_->ExecuteQuery("SELECT val, id FROM base WHERE id = 0 OR id = 9");
  EXPECT_EQ(reordered.GetRowCount(), 2u);
  ExpectRowsIgnoringHeaders(reordered, {{"0", "0"}, {"90", "9"}});
}

// ============================================================
// DML row counts and duplicate-sensitive updates (20 pts)
// ============================================================

GRADED_TEST_F(SqlExecutorEvalTest, InsertUpdateDeleteCounts, 20) {
  auto inserted = client_->ExecuteQuery("INSERT INTO copy SELECT * FROM base WHERE id <= 2");
  ASSERT_EQ(inserted.GetRowCount(), 1u);
  EXPECT_EQ(inserted.GetRow(0)[0], "4");

  auto updated = client_->ExecuteQuery("UPDATE copy SET val = val + 100 WHERE id = 1");
  ASSERT_EQ(updated.GetRowCount(), 1u);
  EXPECT_EQ(updated.GetRow(0)[0], "2");

  auto deleted_none = client_->ExecuteQuery("DELETE FROM copy WHERE id < 0");
  ASSERT_EQ(deleted_none.GetRowCount(), 1u);
  EXPECT_EQ(deleted_none.GetRow(0)[0], "0");

  auto deleted = client_->ExecuteQuery("DELETE FROM copy WHERE id = 0 OR id = 2");
  ASSERT_EQ(deleted.GetRowCount(), 1u);
  EXPECT_EQ(deleted.GetRow(0)[0], "2");

  auto remaining = client_->ExecuteQuery("SELECT * FROM copy");
  ExpectRows(remaining, {"id", "val"}, {{"1", "110"}, {"1", "111"}});
}

// ============================================================
// Joins (20 pts)
// ============================================================

GRADED_TEST_F(SqlExecutorEvalTest, JoinDuplicateExplosionAndMisses, 20) {
  auto joined = client_->ExecuteQuery("SELECT * FROM base INNER JOIN probe ON base.id = probe.id");
  ASSERT_EQ(joined.GetRowCount(), 7u);

  ExpectRowsIgnoringHeaders(joined, {{"1", "10", "1", "100"},
                                     {"1", "10", "1", "101"},
                                     {"1", "11", "1", "100"},
                                     {"1", "11", "1", "101"},
                                     {"3", "-30", "3", "300"},
                                     {"7", "70", "7", "700"},
                                     {"9", "90", "9", "900"}});

  auto empty_join = client_->ExecuteQuery("SELECT * FROM base INNER JOIN probe ON base.id = probe.id WHERE base.id = 999");
  EXPECT_EQ(empty_join.GetRowCount(), 0u);
}

// ============================================================
// Aggregation and grouping (15 pts)
// ============================================================

GRADED_TEST_F(SqlExecutorEvalTest, AggregationAndGroupingEdgeCases, 15) {
  auto agg = client_->ExecuteQuery("SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM base WHERE id >= 3");
  ASSERT_EQ(agg.GetRowCount(), 1u);
  EXPECT_EQ(agg.GetRow(0)[0], "7");
  EXPECT_EQ(agg.GetRow(0)[1], "360");
  EXPECT_EQ(agg.GetRow(0)[2], "-30");
  EXPECT_EQ(agg.GetRow(0)[3], "90");

  auto grouped = client_->ExecuteQuery("SELECT id / 2, COUNT(*) FROM base GROUP BY id / 2");
  ASSERT_EQ(grouped.GetRowCount(), 5u);

  std::unordered_map<int, int> counts;
  for (const auto &row : grouped.GetRows()) {
    counts[std::stoi(row[0])] = std::stoi(row[1]);
  }

  EXPECT_EQ(counts[0], 3);
  EXPECT_EQ(counts[1], 2);
  EXPECT_EQ(counts[2], 2);
  EXPECT_EQ(counts[3], 2);
  EXPECT_EQ(counts[4], 2);
}

// ============================================================
// Sort and limit (15 pts)
// ============================================================

GRADED_TEST_F(SqlExecutorEvalTest, SortLimitAndZeroLimit, 15) {
  auto sorted = client_->ExecuteQuery("SELECT id, val FROM base ORDER BY val DESC LIMIT 4");
  ExpectRows(sorted, {"id", "val"}, {{"9", "90"}, {"8", "80"}, {"7", "70"}, {"6", "60"}});

  auto zero_limit = client_->ExecuteQuery("SELECT id FROM base ORDER BY id LIMIT 0");
  EXPECT_EQ(zero_limit.GetRowCount(), 0u);

  auto no_match = client_->ExecuteQuery("SELECT * FROM base WHERE id < 0 ORDER BY val DESC");
  EXPECT_EQ(no_match.GetRowCount(), 0u);
}

// ============================================================
// Large-data robustness and index latency (20 pts)
// ============================================================

GRADED_TEST_F(LargeSqlExecutorEvalTest, LargeDataRobustness, 10) {
  auto clustered = client_->ExecuteQuery("SELECT * FROM facts_idx WHERE id = 123");
  EXPECT_EQ(clustered.GetRowCount(), 30u);

  auto empty = client_->ExecuteQuery("SELECT * FROM facts_scan WHERE id = 123456");
  EXPECT_EQ(empty.GetRowCount(), 0u);

  auto grouped = client_->ExecuteQuery("SELECT bucket, COUNT(*) FROM facts_scan WHERE id < 80 GROUP BY bucket");
  ASSERT_EQ(grouped.GetRowCount(), 37u);

  std::unordered_map<int, int> bucket_counts;
  for (const auto &row : grouped.GetRows()) {
    bucket_counts[std::stoi(row[0])] = std::stoi(row[1]);
  }
  EXPECT_EQ(bucket_counts[0], 3);
  EXPECT_EQ(bucket_counts[1], 3);
  EXPECT_EQ(bucket_counts[2], 3);
  EXPECT_EQ(bucket_counts[3], 3);
}

GRADED_TEST_F(LargeSqlExecutorEvalTest, IndexedLookupsMustNotBeSlow, 10) {
  using clock = std::chrono::steady_clock;

  auto indexed_plan = client_->OptimizeQuery(client_->BindQuery("SELECT * FROM facts_idx WHERE id = 123"));
  if (indexed_plan->GetType() != PlanType::INDEX_SCAN) {
    GTEST_SKIP() << "INDEX_SCAN is not active yet";
  }

  auto scan_plan = client_->OptimizeQuery(client_->BindQuery("SELECT * FROM facts_scan WHERE id = 123"));

  auto measure_us = [&](const AbstractPlanNodeRef &plan, int iterations) -> std::pair<long long, size_t> {
    auto warmup = client_->ExecutePlan(plan);
    size_t total_rows = warmup.GetRowCount();

    auto start = clock::now();
    for (int i = 0; i < iterations; ++i) {
      total_rows += client_->ExecutePlan(plan).GetRowCount();
    }
    auto end = clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    return {elapsed, total_rows};
  };

  constexpr int kIterations = 12;
  auto [indexed_us, indexed_rows] = measure_us(indexed_plan, kIterations);
  auto [scan_us, scan_rows] = measure_us(scan_plan, kIterations);

  EXPECT_EQ(indexed_rows, scan_rows);
  EXPECT_LT(indexed_us, 4000000);
  EXPECT_LT(indexed_us * 4, scan_us + 1);
}

}  // namespace onebase
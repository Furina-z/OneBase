#include <gtest/gtest.h>
#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "eval/grading.h"

#include "onebase/binder/binder.h"
#include "onebase/buffer/buffer_pool_manager.h"
#include "onebase/catalog/catalog.h"
#include "onebase/catalog/column.h"
#include "onebase/catalog/schema.h"
#include "onebase/execution/execution_engine.h"
#include "onebase/execution/executor_context.h"
#include "onebase/execution/plans/plan_nodes.h"
#include "onebase/optimizer/optimizer.h"
#include "onebase/storage/disk/disk_manager.h"
#include "onebase/storage/table/tuple.h"
#include "onebase/type/type_id.h"
#include "onebase/type/value.h"

namespace onebase {

class ExecutorEvalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_name_ = "__eval_exec_" + std::to_string(reinterpret_cast<uintptr_t>(this)) + ".db";
    disk_manager_ = new DiskManager(db_name_);
    bpm_ = new BufferPoolManager(64, disk_manager_);
    catalog_ = new Catalog(bpm_);
    exec_ctx_ = new ExecutorContext(catalog_, bpm_);
    engine_ = new ExecutionEngine(exec_ctx_);

    // Create test_1(id:INT, val:INT) with 10 rows: (0,0), (1,10), ..., (9,90)
    Schema schema1({Column("id", TypeId::INTEGER), Column("val", TypeId::INTEGER)});
    table1_info_ = catalog_->CreateTable("test_1", schema1);
    for (int i = 0; i < 10; i++) {
      table1_info_->table_->InsertTuple(Tuple({Value(TypeId::INTEGER, i), Value(TypeId::INTEGER, i * 10)}));
    }

    // Create test_2(id:INT, data:INT) with 5 rows: (0,100), (2,200), (4,400), (6,600), (8,800)
    Schema schema2({Column("id", TypeId::INTEGER), Column("data", TypeId::INTEGER)});
    table2_info_ = catalog_->CreateTable("test_2", schema2);
    for (int i = 0; i < 5; i++) {
      table2_info_->table_->InsertTuple(
          Tuple({Value(TypeId::INTEGER, i * 2), Value(TypeId::INTEGER, (i * 2) * 100)}));
    }
  }

  void TearDown() override {
    delete engine_;
    delete exec_ctx_;
    delete catalog_;
    delete bpm_;
    disk_manager_->ShutDown();
    delete disk_manager_;
    std::remove(db_name_.c_str());
  }

  // Bind SQL -> plan tree (no optimization)
  auto Bind(const std::string &sql) -> AbstractPlanNodeRef {
    Binder binder(catalog_);
    return binder.BindQuery(sql);
  }

  // Bind + optimize SQL -> plan tree
  auto BindAndOptimize(const std::string &sql) -> AbstractPlanNodeRef {
    auto plan = Bind(sql);
    Optimizer optimizer;
    return optimizer.Optimize(plan);
  }

  // Execute plan and return result tuples
  auto Execute(const AbstractPlanNodeRef &plan) -> std::vector<Tuple> {
    std::vector<Tuple> result;
    engine_->Execute(plan, &result);
    return result;
  }

  // Full pipeline: SQL -> bind -> optimize -> execute
  auto RunSQL(const std::string &sql) -> std::vector<Tuple> {
    return Execute(BindAndOptimize(sql));
  }

  std::string db_name_;
  DiskManager *disk_manager_{nullptr};
  BufferPoolManager *bpm_{nullptr};
  Catalog *catalog_{nullptr};
  ExecutorContext *exec_ctx_{nullptr};
  ExecutionEngine *engine_{nullptr};
  TableInfo *table1_info_{nullptr};
  TableInfo *table2_info_{nullptr};
};

// ============================================================
// Sequential Scan (15 pts)
// ============================================================

GRADED_TEST_F(ExecutorEvalTest, SeqScanEmpty, 5) {
  Schema schema({Column("a", TypeId::INTEGER)});
  catalog_->CreateTable("empty_tbl", schema);

  auto result = RunSQL("SELECT * FROM empty_tbl");
  EXPECT_EQ(result.size(), 0u);
}

GRADED_TEST_F(ExecutorEvalTest, SeqScanAll, 5) {
  auto result = RunSQL("SELECT * FROM test_1");
  EXPECT_EQ(result.size(), 10u);

  // Collect all id values
  const auto *schema = &table1_info_->schema_;
  std::vector<int> ids;
  for (auto &t : result) {
    ids.push_back(t.GetValue(schema, 0).GetAsInteger());
  }
  std::sort(ids.begin(), ids.end());
  for (int i = 0; i < 10; i++) {
    EXPECT_EQ(ids[i], i);
  }
}

GRADED_TEST_F(ExecutorEvalTest, SeqScanPredicate, 5) {
  // SELECT * FROM test_1 WHERE id > 5
  auto result = RunSQL("SELECT * FROM test_1 WHERE id > 5");

  EXPECT_EQ(result.size(), 4u);  // ids 6, 7, 8, 9
  const auto *schema = &table1_info_->schema_;
  for (auto &t : result) {
    EXPECT_GT(t.GetValue(schema, 0).GetAsInteger(), 5);
  }
}

// ============================================================
// Insert (10 pts)
// ============================================================

GRADED_TEST_F(ExecutorEvalTest, InsertAndVerify, 5) {
  // Create source and destination tables
  Schema schema({Column("x", TypeId::INTEGER), Column("y", TypeId::INTEGER)});
  auto *src = catalog_->CreateTable("ins_src", schema);
  auto *dst = catalog_->CreateTable("ins_dst", schema);

  src->table_->InsertTuple(Tuple({Value(TypeId::INTEGER, 100), Value(TypeId::INTEGER, 200)}));
  src->table_->InsertTuple(Tuple({Value(TypeId::INTEGER, 300), Value(TypeId::INTEGER, 400)}));

  // INSERT INTO ins_dst SELECT * FROM ins_src
  Execute(Bind("INSERT INTO ins_dst SELECT * FROM ins_src"));

  // Verify destination has 2 rows
  auto result = RunSQL("SELECT * FROM ins_dst");
  EXPECT_EQ(result.size(), 2u);
}

GRADED_TEST_F(ExecutorEvalTest, InsertCount, 5) {
  Schema schema({Column("x", TypeId::INTEGER)});
  auto *src = catalog_->CreateTable("cnt_src", schema);
  auto *dst = catalog_->CreateTable("cnt_dst", schema);

  for (int i = 0; i < 5; i++) {
    src->table_->InsertTuple(Tuple({Value(TypeId::INTEGER, i)}));
  }

  auto result = Execute(Bind("INSERT INTO cnt_dst SELECT * FROM cnt_src"));

  ASSERT_EQ(result.size(), 1u);
  EXPECT_EQ(result[0].GetValue(0).GetAsInteger(), 5);
}

// ============================================================
// Delete (5 pts)
// ============================================================

GRADED_TEST_F(ExecutorEvalTest, DeleteAndVerify, 5) {
  // DELETE FROM test_1 WHERE id < 3
  auto del_result = Execute(Bind("DELETE FROM test_1 WHERE id < 3"));

  ASSERT_EQ(del_result.size(), 1u);
  EXPECT_EQ(del_result[0].GetValue(0).GetAsInteger(), 3);  // deleted 3 rows

  // Verify remaining
  auto result = RunSQL("SELECT * FROM test_1");
  EXPECT_EQ(result.size(), 7u);
  const auto *schema = &table1_info_->schema_;
  for (auto &t : result) {
    EXPECT_GE(t.GetValue(schema, 0).GetAsInteger(), 3);
  }
}

// ============================================================
// Update (5 pts)
// ============================================================

GRADED_TEST_F(ExecutorEvalTest, UpdateAndVerify, 5) {
  // UPDATE test_1 SET val = val + 1
  auto upd_result = Execute(Bind("UPDATE test_1 SET val = val + 1"));
  ASSERT_EQ(upd_result.size(), 1u);
  EXPECT_EQ(upd_result[0].GetValue(0).GetAsInteger(), 10);  // updated 10 rows

  // Verify
  auto result = RunSQL("SELECT * FROM test_1");
  EXPECT_EQ(result.size(), 10u);
  const auto *schema = &table1_info_->schema_;
  for (auto &t : result) {
    int id = t.GetValue(schema, 0).GetAsInteger();
    int val = t.GetValue(schema, 1).GetAsInteger();
    EXPECT_EQ(val, id * 10 + 1);
  }
}

// ============================================================
// Nested Loop Join (10 pts)
// ============================================================

GRADED_TEST_F(ExecutorEvalTest, NestedLoopJoin, 10) {
  // Bind without optimization -> should produce NLJ
  auto plan = Bind("SELECT * FROM test_1 INNER JOIN test_2 ON test_1.id = test_2.id");
  EXPECT_EQ(plan->GetType(), PlanType::NESTED_LOOP_JOIN);

  auto result = Execute(plan);

  // Matching ids: 0, 2, 4, 6, 8
  EXPECT_EQ(result.size(), 5u);

  // Verify each joined row
  for (auto &t : result) {
    int left_id = t.GetValue(0).GetAsInteger();
    int left_val = t.GetValue(1).GetAsInteger();
    int right_id = t.GetValue(2).GetAsInteger();
    int right_data = t.GetValue(3).GetAsInteger();
    EXPECT_EQ(left_id, right_id);
    EXPECT_EQ(left_val, left_id * 10);
    EXPECT_EQ(right_data, right_id * 100);
  }
}

// ============================================================
// Hash Join (10 pts) -- optimizer converts NLJ to HashJoin
// ============================================================

GRADED_TEST_F(ExecutorEvalTest, HashJoin, 10) {
  // Bind + optimize -> should convert NLJ to HashJoin
  auto plan = BindAndOptimize("SELECT * FROM test_1 INNER JOIN test_2 ON test_1.id = test_2.id");
  EXPECT_EQ(plan->GetType(), PlanType::HASH_JOIN);

  auto result = Execute(plan);

  EXPECT_EQ(result.size(), 5u);

  std::vector<int> joined_ids;
  for (auto &t : result) {
    joined_ids.push_back(t.GetValue(0).GetAsInteger());
  }
  std::sort(joined_ids.begin(), joined_ids.end());
  EXPECT_EQ(joined_ids, (std::vector<int>{0, 2, 4, 6, 8}));
}

// ============================================================
// Aggregation (15 pts)
// ============================================================

GRADED_TEST_F(ExecutorEvalTest, AggCountSum, 5) {
  auto result = RunSQL("SELECT COUNT(*), SUM(val) FROM test_1");

  ASSERT_EQ(result.size(), 1u);
  EXPECT_EQ(result[0].GetValue(0).GetAsInteger(), 10);   // COUNT(*)
  EXPECT_EQ(result[0].GetValue(1).GetAsInteger(), 450);  // SUM(0+10+20+...+90)
}

GRADED_TEST_F(ExecutorEvalTest, AggMinMax, 5) {
  auto result = RunSQL("SELECT MIN(val), MAX(val) FROM test_1");

  ASSERT_EQ(result.size(), 1u);
  EXPECT_EQ(result[0].GetValue(0).GetAsInteger(), 0);   // MIN
  EXPECT_EQ(result[0].GetValue(1).GetAsInteger(), 90);  // MAX
}

GRADED_TEST_F(ExecutorEvalTest, AggGroupBy, 5) {
  // GROUP BY id / 5: group 0 (ids 0-4), group 1 (ids 5-9)
  auto result = RunSQL("SELECT id / 5, COUNT(*) FROM test_1 GROUP BY id / 5");
  EXPECT_EQ(result.size(), 2u);

  // Sort by group key for deterministic checking
  std::sort(result.begin(), result.end(), [](const Tuple &a, const Tuple &b) {
    return a.GetValue(0).GetAsInteger() < b.GetValue(0).GetAsInteger();
  });

  EXPECT_EQ(result[0].GetValue(0).GetAsInteger(), 0);  // group 0
  EXPECT_EQ(result[0].GetValue(1).GetAsInteger(), 5);   // count 5
  EXPECT_EQ(result[1].GetValue(0).GetAsInteger(), 1);  // group 1
  EXPECT_EQ(result[1].GetValue(1).GetAsInteger(), 5);   // count 5
}

// ============================================================
// Sort (10 pts)
// ============================================================

GRADED_TEST_F(ExecutorEvalTest, SortAscDesc, 10) {
  // Sort test_1 by val descending
  auto result = RunSQL("SELECT * FROM test_1 ORDER BY val DESC");
  ASSERT_EQ(result.size(), 10u);

  const auto *schema = &table1_info_->schema_;
  // First tuple should have val=90, last should have val=0
  EXPECT_EQ(result[0].GetValue(schema, 1).GetAsInteger(), 90);
  EXPECT_EQ(result[9].GetValue(schema, 1).GetAsInteger(), 0);

  // Verify strictly descending order
  for (size_t i = 1; i < result.size(); i++) {
    EXPECT_GE(result[i - 1].GetValue(schema, 1).GetAsInteger(), result[i].GetValue(schema, 1).GetAsInteger());
  }
}

// ============================================================
// Limit (5 pts)
// ============================================================

GRADED_TEST_F(ExecutorEvalTest, LimitRows, 5) {
  auto result = RunSQL("SELECT * FROM test_1 LIMIT 3");
  EXPECT_EQ(result.size(), 3u);
}

// ============================================================
// Sort + Limit (5 pts)
// ============================================================

GRADED_TEST_F(ExecutorEvalTest, SortPlusLimit, 5) {
  auto result = RunSQL("SELECT * FROM test_1 ORDER BY val DESC LIMIT 3");

  ASSERT_EQ(result.size(), 3u);
  const auto *schema = &table1_info_->schema_;
  EXPECT_EQ(result[0].GetValue(schema, 1).GetAsInteger(), 90);
  EXPECT_EQ(result[1].GetValue(schema, 1).GetAsInteger(), 80);
  EXPECT_EQ(result[2].GetValue(schema, 1).GetAsInteger(), 70);
}

// ============================================================
// Projection (10 pts)
// ============================================================

GRADED_TEST_F(ExecutorEvalTest, ProjectionColumns, 5) {
  // SELECT id FROM test_1
  auto plan = BindAndOptimize("SELECT id FROM test_1");
  // Verify plan tree has a projection node at the top
  EXPECT_EQ(plan->GetType(), PlanType::PROJECTION);

  auto result = Execute(plan);
  EXPECT_EQ(result.size(), 10u);

  // Each result tuple should have exactly 1 column (id)
  for (auto &t : result) {
    EXPECT_EQ(t.GetValues().size(), 1u);
  }

  // Collect ids and verify
  std::vector<int> ids;
  for (auto &t : result) {
    ids.push_back(t.GetValue(0).GetAsInteger());
  }
  std::sort(ids.begin(), ids.end());
  for (int i = 0; i < 10; i++) {
    EXPECT_EQ(ids[i], i);
  }
}

GRADED_TEST_F(ExecutorEvalTest, ProjectionExpr, 5) {
  // SELECT id, val + 1 FROM test_1
  auto result = RunSQL("SELECT id, val + 1 FROM test_1");
  EXPECT_EQ(result.size(), 10u);

  // Each tuple has 2 columns
  for (auto &t : result) {
    EXPECT_EQ(t.GetValues().size(), 2u);
  }

  // Verify: second column should be id*10 + 1
  for (auto &t : result) {
    int id = t.GetValue(0).GetAsInteger();
    int computed_val = t.GetValue(1).GetAsInteger();
    EXPECT_EQ(computed_val, id * 10 + 1);
  }
}

}  // namespace onebase

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "eval/grading.h"
#include "eval/sql_test_client.h"
#include "storage/b_plus_tree_test_common.h"

#include "onebase/catalog/column.h"
#include "onebase/catalog/schema.h"
#include "onebase/type/type_id.h"

namespace onebase {

namespace {

using onebase::eval::SqlTestClient;
using onebase::eval::Table;

auto SortRows(std::vector<std::vector<std::string>> rows) -> std::vector<std::vector<std::string>> {
  std::sort(rows.begin(), rows.end());
  return rows;
}

auto ExpectTableEquals(const Table &table, const std::vector<std::string> &expected_headers,
                       std::vector<std::vector<std::string>> expected_rows) -> void {
  EXPECT_EQ(table.GetHeaders(), expected_headers);
  EXPECT_EQ(SortRows(table.GetRows()), SortRows(std::move(expected_rows)));
}

}  // namespace

class IndexSqlEvalTest : public ::testing::Test {
 protected:
  void SetUp() override { client_ = std::make_unique<SqlTestClient>("lab2"); }

  std::unique_ptr<SqlTestClient> client_;
};

class BPlusTreeEvalTest : public onebase::test::BPlusTreeLab2Test {};

// ============================================================
// Index DDL / SHOW integration (40 pts)
// ============================================================

GRADED_TEST_F(IndexSqlEvalTest, CreateShowDropAndSchema, 15) {
  client_->CreateTable(
      "orders",
      Schema({Column("order_id", TypeId::INTEGER), Column("customer_id", TypeId::INTEGER),
              Column("region_id", TypeId::INTEGER), Column("created_at", TypeId::INTEGER)}));
  client_->CreateTable(
      "audit",
      Schema({Column("audit_id", TypeId::INTEGER), Column("customer_id", TypeId::INTEGER),
              Column("note", TypeId::VARCHAR)}));

  EXPECT_EQ(client_->ExecuteQuery("SHOW INDEX").GetRowCount(), 0u);

  EXPECT_EQ(client_->ExecuteCommand("CREATE INDEX idx_orders_customer ON orders (customer_id)"),
            "CREATE INDEX idx_orders_customer");
  EXPECT_EQ(client_->ExecuteCommand("CREATE INDEX idx_orders_region_created ON orders (region_id, created_at)"),
            "CREATE INDEX idx_orders_region_created");
  EXPECT_EQ(client_->ExecuteCommand("CREATE INDEX idx_audit_customer ON audit (customer_id)"),
            "CREATE INDEX idx_audit_customer");

  auto indexes = client_->ExecuteQuery("SHOW INDEXES");
  ExpectTableEquals(indexes, {"index_name", "table_name", "columns"},
                    {{"idx_orders_customer", "orders", "customer_id"},
                     {"idx_orders_region_created", "orders", "region_id,created_at"},
                     {"idx_audit_customer", "audit", "customer_id"}});

  auto orders_schema = client_->ExecuteQuery("SHOW orders");
  ExpectTableEquals(orders_schema, {"column_name", "column_type"},
                    {{"order_id", "order_id:INTEGER"},
                     {"customer_id", "customer_id:INTEGER"},
                     {"region_id", "region_id:INTEGER"},
                     {"created_at", "created_at:INTEGER"}});

  EXPECT_EQ(client_->ExecuteCommand("DROP INDEX idx_orders_customer"), "DROP INDEX");
  indexes = client_->ExecuteQuery("SHOW INDEX");
  ExpectTableEquals(indexes, {"index_name", "table_name", "columns"},
                    {{"idx_orders_region_created", "orders", "region_id,created_at"},
                     {"idx_audit_customer", "audit", "customer_id"}});
}

GRADED_TEST_F(IndexSqlEvalTest, RejectInvalidDefinitionsAndMissingObjects, 10) {
  client_->CreateTable("users",
                       Schema({Column("id", TypeId::INTEGER), Column("name", TypeId::VARCHAR),
                               Column("age", TypeId::INTEGER)}));

  EXPECT_EQ(client_->ExecuteCommand("CREATE INDEX idx_users_id ON users (id)"),
            "CREATE INDEX idx_users_id");

  EXPECT_THROW(client_->ExecuteCommand("CREATE INDEX idx_users_id ON users (id)"), std::runtime_error);
  EXPECT_THROW(client_->ExecuteCommand("CREATE INDEX idx_missing_table ON missing_table (id)"),
               std::runtime_error);
  EXPECT_THROW(client_->ExecuteCommand("CREATE INDEX idx_missing_column ON users (missing)"),
               std::runtime_error);
  EXPECT_THROW(client_->ExecuteCommand("DROP INDEX missing_index"), std::runtime_error);
  EXPECT_EQ(client_->ExecuteCommand("DROP INDEX IF EXISTS missing_index"), "DROP INDEX (0 rows)");
}

GRADED_TEST_F(IndexSqlEvalTest, AmbiguousIndexNamesAcrossTables, 10) {
  client_->CreateTable("left_side", Schema({Column("id", TypeId::INTEGER), Column("payload", TypeId::INTEGER)}));
  client_->CreateTable("right_side", Schema({Column("id", TypeId::INTEGER), Column("payload", TypeId::INTEGER)}));

  EXPECT_EQ(client_->ExecuteCommand("CREATE INDEX idx_shared ON left_side (id)"), "CREATE INDEX idx_shared");
  EXPECT_EQ(client_->ExecuteCommand("CREATE INDEX idx_shared ON right_side (id)"), "CREATE INDEX idx_shared");

  auto indexes = client_->ExecuteQuery("SHOW INDEX");
  ExpectTableEquals(indexes, {"index_name", "table_name", "columns"},
                    {{"idx_shared", "left_side", "id"}, {"idx_shared", "right_side", "id"}});

  EXPECT_THROW(client_->ExecuteCommand("DROP INDEX idx_shared"), std::runtime_error);
}

GRADED_TEST_F(IndexSqlEvalTest, ShowTablesAndEmptyIndexViewStayStable, 5) {
  client_->CreateTable("empty_view",
                       Schema({Column("flag", TypeId::BOOLEAN), Column("title", TypeId::VARCHAR)}));
  client_->CreateTable("wide_view",
                       Schema({Column("c1", TypeId::INTEGER), Column("c2", TypeId::INTEGER),
                               Column("c3", TypeId::INTEGER), Column("c4", TypeId::INTEGER),
                               Column("c5", TypeId::INTEGER)}));

  auto tables = client_->ExecuteQuery("SHOW TABLES");
  ExpectTableEquals(tables, {"table_name"}, {{"empty_view"}, {"wide_view"}});

  auto empty_indexes = client_->ExecuteQuery("SHOW INDEXES");
  ExpectTableEquals(empty_indexes, {"index_name", "table_name", "columns"}, {});
}

// ============================================================
// Direct B+ tree behavior (60 pts)
// ============================================================

GRADED_TEST_F(BPlusTreeEvalTest, InsertLookupAndDuplicateHandling, 15) {
  VerifyInsertLookupAndDuplicateHandling();
}

GRADED_TEST_F(BPlusTreeEvalTest, InsertionsSplitAndIterationIsOrdered, 15) {
  VerifyInsertionsSplitAndIterationIsOrdered();
}

GRADED_TEST_F(BPlusTreeEvalTest, BeginFromKeyAndSparseLookupsWork, 15) {
  VerifyBeginFromKeyAndSparseLookupsWork();
}

GRADED_TEST_F(BPlusTreeEvalTest, DeleteMaintainsCorrectnessAndCanEmptyTree, 15) {
  VerifyDeleteMaintainsCorrectnessAndCanEmptyTree();
}

}  // namespace onebase

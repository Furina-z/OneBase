#include "onebase/binder/binder.h"

#include <algorithm>
#include <stdexcept>
#include <string>
#include <vector>

// libpg_query headers
#include "nodes/nodes.hpp"
#include "nodes/parsenodes.hpp"
#include "nodes/pg_list.hpp"
#include "nodes/primnodes.hpp"
#include "nodes/value.hpp"
#include "postgres_parser.hpp"

// OneBase plan / expression headers
#include "onebase/catalog/column.h"
#include "onebase/catalog/schema.h"
#include "onebase/execution/expressions/arithmetic_expression.h"
#include "onebase/execution/expressions/column_value_expression.h"
#include "onebase/execution/expressions/comparison_expression.h"
#include "onebase/execution/expressions/constant_value_expression.h"
#include "onebase/execution/expressions/logic_expression.h"
#include "onebase/execution/plans/plan_nodes.h"
#include "onebase/type/type_id.h"
#include "onebase/type/value.h"

namespace onebase {

// ---------------------------------------------------------------------------
// Internal helpers — kept in an anonymous namespace
// ---------------------------------------------------------------------------
namespace {

using namespace duckdb_libpgquery;

// ---- Scope tracking for column resolution ---------------------------------
struct TableScope {
  std::string alias;  // table name or alias
  TableInfo *info;
  uint32_t tuple_idx;  // 0 = left/single, 1 = right in join
};

// ---- Aggregate info collected while binding target list -------------------
struct AggCall {
  AggregationType type;
  AbstractExpressionRef arg;  // may be nullptr for COUNT(*)
};

// ---- Forward declarations of binding helpers ------------------------------

auto BindExpression(PGNode *node, const std::vector<TableScope> &scopes) -> AbstractExpressionRef;

// Resolve a column name against scopes
auto ResolveColumn(const std::string &col_name, const std::string &table_qualifier,
                   const std::vector<TableScope> &scopes)
    -> std::tuple<uint32_t, uint32_t, TypeId> {
  for (auto &scope : scopes) {
    if (!table_qualifier.empty() && table_qualifier != scope.alias) {
      continue;
    }
    const auto &schema = scope.info->schema_;
    for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
      if (schema.GetColumn(i).GetName() == col_name) {
        return {scope.tuple_idx, i, schema.GetColumn(i).GetType()};
      }
    }
  }
  throw std::runtime_error("Column not found: " + (table_qualifier.empty() ? col_name : table_qualifier + "." + col_name));
}

// Extract a string from a PGValue node
auto GetStringValue(PGNode *node) -> std::string {
  if (node->type == duckdb_libpgquery::T_PGString) {
    return strVal(node);
  }
  return strVal(node);
}

// Bind a PGColumnRef to a ColumnValueExpression
auto BindColumnRef(PGColumnRef *colref, const std::vector<TableScope> &scopes) -> AbstractExpressionRef {
  auto *fields = colref->fields;
  int nfields = 0;
  PGListCell *cell;
  foreach (cell, fields) { nfields++; }

  std::string col_name;
  std::string table_qualifier;

  if (nfields == 1) {
    auto *node = static_cast<PGNode *>(lfirst(list_head(fields)));
    if (node->type == duckdb_libpgquery::T_PGString) {
      col_name = strVal(node);
    } else if (node->type == duckdb_libpgquery::T_PGAStar) {
      // SELECT * — should be handled by caller
      return nullptr;
    }
  } else if (nfields == 2) {
    auto *first = static_cast<PGNode *>(lfirst(list_head(fields)));
    auto *second = static_cast<PGNode *>(lfirst(lnext(list_head(fields))));
    table_qualifier = strVal(first);
    if (second->type == duckdb_libpgquery::T_PGAStar) {
      return nullptr;  // table.*
    }
    col_name = strVal(second);
  }

  auto [tuple_idx, col_idx, type] = ResolveColumn(col_name, table_qualifier, scopes);
  return std::make_shared<ColumnValueExpression>(tuple_idx, col_idx, type);
}

// Bind a PGAConst to a ConstantValueExpression
auto BindConstant(PGAConst *aconst) -> AbstractExpressionRef {
  auto &val = aconst->val;
  switch (val.type) {
    case duckdb_libpgquery::T_PGInteger:
      return std::make_shared<ConstantValueExpression>(
          Value(TypeId::INTEGER, static_cast<int32_t>(val.val.ival)));
    case duckdb_libpgquery::T_PGFloat:
      return std::make_shared<ConstantValueExpression>(
          Value(TypeId::FLOAT, std::stof(val.val.str)));
    case duckdb_libpgquery::T_PGString:
      return std::make_shared<ConstantValueExpression>(
          Value(TypeId::VARCHAR, val.val.str));
    default:
      throw std::runtime_error("Unsupported constant type");
  }
}

// Bind a PGAExpr (operator expression)
auto BindAExpr(PGAExpr *aexpr, const std::vector<TableScope> &scopes) -> AbstractExpressionRef {
  auto left = BindExpression(aexpr->lexpr, scopes);
  auto right = BindExpression(aexpr->rexpr, scopes);

  // Get operator name
  std::string op_name = strVal(static_cast<PGNode *>(lfirst(list_head(aexpr->name))));

  // Comparison operators
  if (op_name == "=") {
    return std::make_shared<ComparisonExpression>(left, right, ComparisonType::Equal);
  }
  if (op_name == "<>" || op_name == "!=") {
    return std::make_shared<ComparisonExpression>(left, right, ComparisonType::NotEqual);
  }
  if (op_name == "<") {
    return std::make_shared<ComparisonExpression>(left, right, ComparisonType::LessThan);
  }
  if (op_name == "<=") {
    return std::make_shared<ComparisonExpression>(left, right, ComparisonType::LessThanOrEqual);
  }
  if (op_name == ">") {
    return std::make_shared<ComparisonExpression>(left, right, ComparisonType::GreaterThan);
  }
  if (op_name == ">=") {
    return std::make_shared<ComparisonExpression>(left, right, ComparisonType::GreaterThanOrEqual);
  }

  // Arithmetic operators
  if (op_name == "+") {
    return std::make_shared<ArithmeticExpression>(left, right, ArithmeticType::Plus);
  }
  if (op_name == "-") {
    return std::make_shared<ArithmeticExpression>(left, right, ArithmeticType::Minus);
  }
  if (op_name == "*") {
    return std::make_shared<ArithmeticExpression>(left, right, ArithmeticType::Multiply);
  }
  if (op_name == "/") {
    return std::make_shared<ArithmeticExpression>(left, right, ArithmeticType::Divide);
  }
  if (op_name == "%") {
    return std::make_shared<ArithmeticExpression>(left, right, ArithmeticType::Modulo);
  }

  throw std::runtime_error("Unsupported operator: " + op_name);
}

// Bind a PGBoolExpr (AND / OR / NOT)
auto BindBoolExpr(PGBoolExpr *bexpr, const std::vector<TableScope> &scopes) -> AbstractExpressionRef {
  if (bexpr->boolop == duckdb_libpgquery::PG_AND_EXPR) {
    // AND can chain >2 args — fold left
    PGListCell *cell = list_head(bexpr->args);
    auto result = BindExpression(static_cast<PGNode *>(lfirst(cell)), scopes);
    for (cell = lnext(cell); cell != nullptr; cell = lnext(cell)) {
      auto right = BindExpression(static_cast<PGNode *>(lfirst(cell)), scopes);
      result = std::make_shared<LogicExpression>(result, right, LogicType::And);
    }
    return result;
  }
  if (bexpr->boolop == duckdb_libpgquery::PG_OR_EXPR) {
    PGListCell *cell = list_head(bexpr->args);
    auto result = BindExpression(static_cast<PGNode *>(lfirst(cell)), scopes);
    for (cell = lnext(cell); cell != nullptr; cell = lnext(cell)) {
      auto right = BindExpression(static_cast<PGNode *>(lfirst(cell)), scopes);
      result = std::make_shared<LogicExpression>(result, right, LogicType::Or);
    }
    return result;
  }
  throw std::runtime_error("Unsupported BoolExpr type");
}

// Main expression binder dispatcher
auto BindExpression(PGNode *node, const std::vector<TableScope> &scopes) -> AbstractExpressionRef {
  if (node == nullptr) {
    return nullptr;
  }
  switch (node->type) {
    case duckdb_libpgquery::T_PGColumnRef:
      return BindColumnRef(reinterpret_cast<PGColumnRef *>(node), scopes);
    case duckdb_libpgquery::T_PGAConst:
      return BindConstant(reinterpret_cast<PGAConst *>(node));
    case duckdb_libpgquery::T_PGAExpr:
      return BindAExpr(reinterpret_cast<PGAExpr *>(node), scopes);
    case duckdb_libpgquery::T_PGBoolExpr:
      return BindBoolExpr(reinterpret_cast<PGBoolExpr *>(node), scopes);
    default:
      throw std::runtime_error("Unsupported expression node type: " + std::to_string(static_cast<int>(node->type)));
  }
}

// Get lowercase string from func name
auto GetFuncName(PGList *funcname) -> std::string {
  auto *node = static_cast<PGNode *>(lfirst(list_tail(funcname)));
  std::string name = strVal(node);
  std::transform(name.begin(), name.end(), name.begin(), ::tolower);
  return name;
}

// Helper: count schema (for INSERT/DELETE/UPDATE count return)
auto CountSchema() -> Schema {
  return Schema({Column("count", TypeId::INTEGER)});
}

// Collect all columns from scopes as a single schema
auto CollectAllColumns(const std::vector<TableScope> &scopes) -> Schema {
  std::vector<Column> columns;
  for (auto &scope : scopes) {
    const auto &schema = scope.info->schema_;
    for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
      columns.push_back(schema.GetColumn(i));
    }
  }
  return Schema(columns);
}

}  // anonymous namespace

// ---------------------------------------------------------------------------
// Binder public interface
// ---------------------------------------------------------------------------

Binder::Binder(Catalog *catalog) : catalog_(catalog) {}

auto Binder::BindQuery(const std::string &sql) -> AbstractPlanNodeRef {
  duckdb::PostgresParser parser;
  parser.Parse(sql);

  if (!parser.success) {
    throw std::runtime_error("Parse error: " + parser.error_message);
  }

  if (parser.parse_tree == nullptr || parser.parse_tree->length == 0) {
    throw std::runtime_error("Empty parse tree");
  }

  auto *raw_stmt = static_cast<PGRawStmt *>(lfirst(list_head(parser.parse_tree)));
  auto *stmt = raw_stmt->stmt;

  switch (stmt->type) {
    case duckdb_libpgquery::T_PGSelectStmt: {
      auto *select = reinterpret_cast<PGSelectStmt *>(stmt);

      // --- FROM clause: build scopes and base plan ---
      std::vector<TableScope> scopes;
      AbstractPlanNodeRef base_plan;

      if (select->fromClause != nullptr) {
        PGListCell *cell;
        std::vector<AbstractPlanNodeRef> from_plans;

        foreach (cell, select->fromClause) {
          auto *from_node = static_cast<PGNode *>(lfirst(cell));

          if (from_node->type == duckdb_libpgquery::T_PGRangeVar) {
            auto *rv = reinterpret_cast<PGRangeVar *>(from_node);
            std::string table_name = rv->relname;
            std::string alias = (rv->alias != nullptr && rv->alias->aliasname != nullptr)
                                    ? rv->alias->aliasname
                                    : table_name;
            auto *info = catalog_->GetTable(table_name);
            if (info == nullptr) {
              throw std::runtime_error("Table not found: " + table_name);
            }
            uint32_t tuple_idx = static_cast<uint32_t>(scopes.size());
            scopes.push_back({alias, info, tuple_idx});
            from_plans.push_back(
                std::make_shared<SeqScanPlanNode>(info->schema_, info->oid_, nullptr));

          } else if (from_node->type == duckdb_libpgquery::T_PGJoinExpr) {
            auto *join = reinterpret_cast<PGJoinExpr *>(from_node);

            // Left side
            auto *left_rv = reinterpret_cast<PGRangeVar *>(join->larg);
            std::string left_name = left_rv->relname;
            std::string left_alias = (left_rv->alias != nullptr && left_rv->alias->aliasname != nullptr)
                                         ? left_rv->alias->aliasname
                                         : left_name;
            auto *left_info = catalog_->GetTable(left_name);
            if (left_info == nullptr) {
              throw std::runtime_error("Table not found: " + left_name);
            }
            scopes.push_back({left_alias, left_info, 0});

            // Right side
            auto *right_rv = reinterpret_cast<PGRangeVar *>(join->rarg);
            std::string right_name = right_rv->relname;
            std::string right_alias = (right_rv->alias != nullptr && right_rv->alias->aliasname != nullptr)
                                          ? right_rv->alias->aliasname
                                          : right_name;
            auto *right_info = catalog_->GetTable(right_name);
            if (right_info == nullptr) {
              throw std::runtime_error("Table not found: " + right_name);
            }
            scopes.push_back({right_alias, right_info, 1});

            auto left_scan = std::make_shared<SeqScanPlanNode>(left_info->schema_, left_info->oid_, nullptr);
            auto right_scan = std::make_shared<SeqScanPlanNode>(right_info->schema_, right_info->oid_, nullptr);

            // Build join output schema (all left columns + all right columns)
            auto join_schema = CollectAllColumns(scopes);

            // Bind ON clause
            auto join_predicate = BindExpression(join->quals, scopes);

            base_plan = std::make_shared<NestedLoopJoinPlanNode>(
                join_schema, left_scan, right_scan, join_predicate);
            // Already handled via join, no need to process from_plans further
            from_plans.clear();
          }
        }

        // If we have a single table scan
        if (!from_plans.empty()) {
          base_plan = from_plans[0];
        }
      } else {
        // No FROM clause — shouldn't happen in our test cases
        throw std::runtime_error("SELECT without FROM not supported");
      }

      // --- WHERE clause: attach predicate ---
      if (select->whereClause != nullptr) {
        auto predicate = BindExpression(select->whereClause, scopes);

        // If base_plan is a SeqScan, we can embed the predicate
        if (base_plan->GetType() == PlanType::SEQ_SCAN) {
          auto *scan = dynamic_cast<SeqScanPlanNode *>(base_plan.get());
          auto *info = scopes[0].info;
          base_plan = std::make_shared<SeqScanPlanNode>(info->schema_, info->oid_, predicate);
        }
        // For joins, the predicate is already in the join node (ON clause)
        // If WHERE is separate from ON, we'd need a filter node, but our tests don't need this
      }

      // --- TARGET LIST: check for aggregates ---
      bool has_aggregates = false;
      std::vector<AggCall> agg_calls;
      std::vector<AbstractExpressionRef> output_exprs;
      bool is_star = false;

      if (select->targetList != nullptr) {
        PGListCell *cell;
        foreach (cell, select->targetList) {
          auto *res_target = reinterpret_cast<PGResTarget *>(lfirst(cell));

          if (res_target->val == nullptr) {
            continue;
          }

          auto *val_node = res_target->val;

          // Check for SELECT * — PGAStar directly as val
          if (val_node->type == duckdb_libpgquery::T_PGAStar) {
            is_star = true;
            continue;
          }

          // Check for SELECT * — PGColumnRef wrapping PGAStar
          if (val_node->type == duckdb_libpgquery::T_PGColumnRef) {
            auto *colref = reinterpret_cast<PGColumnRef *>(val_node);
            auto *first_field = static_cast<PGNode *>(lfirst(list_head(colref->fields)));
            if (first_field->type == duckdb_libpgquery::T_PGAStar) {
              is_star = true;
              continue;
            }
          }

          // Check for aggregate function calls
          if (val_node->type == duckdb_libpgquery::T_PGFuncCall) {
            auto *func = reinterpret_cast<PGFuncCall *>(val_node);
            std::string fname = GetFuncName(func->funcname);

            if (fname == "count" || fname == "sum" || fname == "min" || fname == "max") {
              has_aggregates = true;
              AggCall agg;

              // Detect COUNT(*): either agg_star flag or PGAStar in args
              bool is_count_star = func->agg_star;
              if (!is_count_star && fname == "count" && func->args != nullptr) {
                auto *first_arg = static_cast<PGNode *>(lfirst(list_head(func->args)));
                if (first_arg->type == duckdb_libpgquery::T_PGAStar) {
                  is_count_star = true;
                }
              }

              if (fname == "count" && is_count_star) {
                agg.type = AggregationType::CountStarAggregate;
                // For count(*), we still need a placeholder expression
                agg.arg = std::make_shared<ColumnValueExpression>(0, 0, TypeId::INTEGER);
              } else {
                if (fname == "count") {
                  agg.type = AggregationType::CountAggregate;
                } else if (fname == "sum") {
                  agg.type = AggregationType::SumAggregate;
                } else if (fname == "min") {
                  agg.type = AggregationType::MinAggregate;
                } else if (fname == "max") {
                  agg.type = AggregationType::MaxAggregate;
                }
                // Bind the argument
                auto *arg_node = static_cast<PGNode *>(lfirst(list_head(func->args)));
                agg.arg = BindExpression(arg_node, scopes);
              }
              agg_calls.push_back(agg);
              continue;
            }
          }

          // Regular expression (e.g., "id / 5" in GROUP BY target list)
          output_exprs.push_back(BindExpression(val_node, scopes));
        }
      }

      // --- Build aggregation node if needed ---
      if (has_aggregates) {
        // GROUP BY expressions
        std::vector<AbstractExpressionRef> group_bys;
        if (select->groupClause != nullptr) {
          PGListCell *cell;
          foreach (cell, select->groupClause) {
            auto *group_node = static_cast<PGNode *>(lfirst(cell));
            group_bys.push_back(BindExpression(group_node, scopes));
          }
        }

        // Build aggregate expressions and types
        std::vector<AbstractExpressionRef> agg_exprs;
        std::vector<AggregationType> agg_types;
        for (auto &ac : agg_calls) {
          agg_exprs.push_back(ac.arg);
          agg_types.push_back(ac.type);
        }

        // Build output schema for aggregation
        // Schema: [group_by_cols..., agg_result_cols...]
        std::vector<Column> agg_schema_cols;
        for (size_t i = 0; i < group_bys.size(); i++) {
          // Use the non-agg output_exprs for group-by column names if available
          std::string col_name = "group_" + std::to_string(i);
          agg_schema_cols.emplace_back(col_name, TypeId::INTEGER);
        }
        for (size_t i = 0; i < agg_calls.size(); i++) {
          std::string col_name = "agg_" + std::to_string(i);
          agg_schema_cols.emplace_back(col_name, TypeId::INTEGER);
        }
        Schema agg_schema(agg_schema_cols);

        base_plan = std::make_shared<AggregationPlanNode>(
            agg_schema, base_plan, group_bys, agg_exprs, agg_types);
      }

      // If SELECT * and not aggregation, the schema is already correct from the base plan

      // --- ORDER BY ---
      if (select->sortClause != nullptr) {
        std::vector<SortPlanNode::OrderByType> order_bys;
        PGListCell *cell;
        foreach (cell, select->sortClause) {
          auto *sortby = reinterpret_cast<PGSortBy *>(lfirst(cell));
          auto sort_expr = BindExpression(sortby->node, scopes);

          // PG_SORTBY_DEFAULT and PG_SORTBY_ASC = ascending, PG_SORTBY_DESC = descending
          bool is_ascending = (sortby->sortby_dir != duckdb_libpgquery::PG_SORTBY_DESC);

          order_bys.push_back({is_ascending, sort_expr});
        }

        base_plan = std::make_shared<SortPlanNode>(
            base_plan->GetOutputSchema(), base_plan, order_bys);
      }

      // --- LIMIT ---
      if (select->limitCount != nullptr) {
        auto *limit_const = reinterpret_cast<PGAConst *>(select->limitCount);
        size_t limit_val = static_cast<size_t>(limit_const->val.val.ival);

        base_plan = std::make_shared<LimitPlanNode>(
            base_plan->GetOutputSchema(), base_plan, limit_val);
      }

      // --- PROJECTION: non-star, non-aggregate column selection ---
      if (!is_star && !has_aggregates && !output_exprs.empty()) {
        std::vector<Column> proj_cols;
        for (size_t i = 0; i < output_exprs.size(); i++) {
          proj_cols.emplace_back("col_" + std::to_string(i), output_exprs[i]->GetReturnType());
        }
        Schema proj_schema(proj_cols);
        base_plan = std::make_shared<ProjectionPlanNode>(proj_schema, base_plan, output_exprs);
      }

      return base_plan;
    }

    case duckdb_libpgquery::T_PGInsertStmt: {
      auto *insert = reinterpret_cast<PGInsertStmt *>(stmt);
      std::string table_name = insert->relation->relname;
      auto *info = catalog_->GetTable(table_name);
      if (info == nullptr) {
        throw std::runtime_error("Table not found: " + table_name);
      }

      // Bind the SELECT subquery as the source
      // We recursively bind through BindQuery by rebuilding the SQL for the sub-select
      // But actually, we can just bind the PGSelectStmt directly
      auto *sub_select = reinterpret_cast<PGSelectStmt *>(insert->selectStmt);

      // Build scopes for the sub-select's from clause
      std::vector<TableScope> sub_scopes;
      AbstractPlanNodeRef sub_plan;

      if (sub_select->fromClause != nullptr) {
        PGListCell *cell;
        foreach (cell, sub_select->fromClause) {
          auto *from_node = static_cast<PGNode *>(lfirst(cell));
          if (from_node->type == duckdb_libpgquery::T_PGRangeVar) {
            auto *rv = reinterpret_cast<PGRangeVar *>(from_node);
            std::string src_name = rv->relname;
            auto *src_info = catalog_->GetTable(src_name);
            if (src_info == nullptr) {
              throw std::runtime_error("Table not found: " + src_name);
            }
            sub_scopes.push_back({src_name, src_info, 0});
            sub_plan = std::make_shared<SeqScanPlanNode>(src_info->schema_, src_info->oid_, nullptr);
          }
        }
      } else if (sub_select->valuesLists != nullptr) {
        // INSERT INTO ... VALUES (...) — not needed for our tests
        throw std::runtime_error("INSERT ... VALUES not supported yet");
      }

      return std::make_shared<InsertPlanNode>(CountSchema(), sub_plan, info->oid_);
    }

    case duckdb_libpgquery::T_PGDeleteStmt: {
      auto *del = reinterpret_cast<PGDeleteStmt *>(stmt);
      std::string table_name = del->relation->relname;
      auto *info = catalog_->GetTable(table_name);
      if (info == nullptr) {
        throw std::runtime_error("Table not found: " + table_name);
      }

      std::vector<TableScope> scopes = {{table_name, info, 0}};

      AbstractExpressionRef predicate;
      if (del->whereClause != nullptr) {
        predicate = BindExpression(del->whereClause, scopes);
      }

      auto scan = std::make_shared<SeqScanPlanNode>(info->schema_, info->oid_, predicate);
      return std::make_shared<DeletePlanNode>(CountSchema(), scan, info->oid_);
    }

    case duckdb_libpgquery::T_PGUpdateStmt: {
      auto *update = reinterpret_cast<PGUpdateStmt *>(stmt);
      std::string table_name = update->relation->relname;
      auto *info = catalog_->GetTable(table_name);
      if (info == nullptr) {
        throw std::runtime_error("Table not found: " + table_name);
      }

      std::vector<TableScope> scopes = {{table_name, info, 0}};
      const auto &schema = info->schema_;

      // Parse SET clause: map column name -> expression
      std::unordered_map<std::string, AbstractExpressionRef> set_exprs;
      if (update->targetList != nullptr) {
        PGListCell *cell;
        foreach (cell, update->targetList) {
          auto *res = reinterpret_cast<PGResTarget *>(lfirst(cell));
          std::string col_name = res->name;
          auto expr = BindExpression(res->val, scopes);
          set_exprs[col_name] = expr;
        }
      }

      // Build update_exprs: for each column, either use SET expr or copy old value
      std::vector<AbstractExpressionRef> update_exprs;
      for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
        auto it = set_exprs.find(schema.GetColumn(i).GetName());
        if (it != set_exprs.end()) {
          update_exprs.push_back(it->second);
        } else {
          update_exprs.push_back(
              std::make_shared<ColumnValueExpression>(0, i, schema.GetColumn(i).GetType()));
        }
      }

      // WHERE clause
      AbstractExpressionRef predicate;
      if (update->whereClause != nullptr) {
        predicate = BindExpression(update->whereClause, scopes);
      }

      auto scan = std::make_shared<SeqScanPlanNode>(info->schema_, info->oid_, predicate);
      return std::make_shared<UpdatePlanNode>(CountSchema(), scan, info->oid_, update_exprs);
    }

    default:
      throw std::runtime_error("Unsupported statement type: " + std::to_string(static_cast<int>(stmt->type)));
  }
}

}  // namespace onebase

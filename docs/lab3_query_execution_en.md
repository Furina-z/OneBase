# Lab 3: Query Execution

## 1. Overview

The goal of this lab is to implement the **Query Execution Engine** for the OneBase database. You will implement 11 query executors that enable the database to execute SQL queries, including sequential scan, index scan, insert, delete, update, join, aggregation, sort, limit, and projection.

You will implement the following executors:

| # | Executor | File |
|---|----------|------|
| 1 | Sequential Scan | `src/execution/executors/seq_scan_executor.cpp` |
| 2 | Index Scan | `src/execution/executors/index_scan_executor.cpp` |
| 3 | Insert | `src/execution/executors/insert_executor.cpp` |
| 4 | Delete | `src/execution/executors/delete_executor.cpp` |
| 5 | Update | `src/execution/executors/update_executor.cpp` |
| 6 | Nested Loop Join | `src/execution/executors/nested_loop_join_executor.cpp` |
| 7 | Hash Join | `src/execution/executors/hash_join_executor.cpp` |
| 8 | Aggregation | `src/execution/executors/aggregation_executor.cpp` |
| 9 | Sort | `src/execution/executors/sort_executor.cpp` |
| 10 | Limit | `src/execution/executors/limit_executor.cpp` |
| 11 | Projection | `src/execution/executors/projection_executor.cpp` |

## 2. Background

### 2.1 Volcano Iterator Model

OneBase uses the **Volcano iterator model**. Each executor implements two methods:

```cpp
class AbstractExecutor {
    virtual void Init() = 0;                           // Initialize/reset the executor
    virtual auto Next(Tuple *tuple, RID *rid) -> bool = 0;  // Get the next row
};
```

**Workflow:**
```
SELECT id FROM students WHERE age > 20 ORDER BY id LIMIT 5;

          ┌────────────┐
          │ Projection │  ← Project: select only the id column
          └─────┬──────┘
                │  Next()
          ┌─────▼─────┐
          │   Limit   │  ← Limit output row count
          └─────┬─────┘
                │  Next()
          ┌─────▼─────┐
          │   Sort    │  ← Materialize all child results, sort
          └─────┬─────┘
                │  Next()
          ┌─────▼─────┐
          │  SeqScan  │  ← Scan students table, filter age > 20
          └───────────┘
```

Each executor obtains input rows by calling its child executor's `Next()`, one row at a time, forming a bottom-up "pull" model.

### 2.2 Pipeline Breakers

Most executors are **streaming** — they process one row at a time (e.g., SeqScan, Limit, Projection). However, some executors need to **materialize all child data** before producing output:

| Type | Executor | Reason |
|------|----------|--------|
| Streaming | SeqScan, Insert, Delete, Update, Limit, Projection | Process row by row |
| Materializing | Sort, Aggregation | Sort needs all data, aggregation needs grouping |
| Materializing | Hash Join | Build phase needs all left table data |
| Materializing | Nested Loop Join | For each left row, re-scan the right table |

### 2.3 Key Interfaces

**Catalog:**
```cpp
auto GetTable(table_oid_t oid) -> TableInfo *;
auto GetTableIndexes(const std::string &table_name) -> std::vector<IndexInfo *>;
```

**Table Heap:**
```cpp
auto InsertTuple(const Tuple &tuple) -> std::optional<RID>;
void UpdateTuple(const Tuple &new_tuple, const RID &rid);
void DeleteTuple(const RID &rid);
auto GetTuple(const RID &rid) -> Tuple;
auto Begin() -> Iterator;
auto End() -> Iterator;
```

**Expression Evaluation:**
```cpp
// Single-table expression (for SeqScan, Sort, Projection, etc.)
auto Evaluate(const Tuple *tuple, const Schema *schema) -> Value;

// Two-table expression (for Joins)
auto EvaluateJoin(const Tuple *left, const Schema *left_schema,
                   const Tuple *right, const Schema *right_schema) -> Value;
```

**ExecutorContext:**
```cpp
auto GetCatalog() -> Catalog *;
auto GetBufferPoolManager() -> BufferPoolManager *;
auto GetTransaction() -> Transaction *;
```

### 2.4 Tuple Construction

Ways to create a new Tuple:
```cpp
// Construct from a list of Values
std::vector<Value> values = {Value(TypeId::INTEGER, 42), Value(TypeId::VARCHAR, "hello")};
Tuple new_tuple(std::move(values));

// Construct via expression evaluation
const auto &exprs = plan_->GetUpdateExpressions();
std::vector<Value> new_values;
for (const auto &expr : exprs) {
    new_values.push_back(expr->Evaluate(&old_tuple, &schema));
}
Tuple updated(std::move(new_values));
```

## 3. Your Tasks

**Recommended implementation order:** SeqScan → Insert → Delete → Update → NLJ → HashJoin → Aggregation → Sort → Limit → Projection → IndexScan

Each executor requires implementing the `Init()` and `Next()` methods.

### Task 1: SeqScan Executor ★★☆

Sequentially scan all rows in a table, applying an optional predicate for filtering.

### Task 2: Insert Executor ★★☆

Retrieve rows from the child executor, insert them into the target table, and update all related indexes. `Next()` is called only once, returning the number of inserted rows.

### Task 3: Delete Executor ★★☆

Retrieve rows from the child executor and delete them from the target table. `Next()` is called only once, returning the number of deleted rows.

### Task 4: Update Executor ★★★

Retrieve old rows from the child executor, compute new values using update expressions, and perform the update. `Next()` is called only once, returning the number of updated rows.

### Task 5: Nested Loop Join ★★★

Implement nested loop join. For each row in the left table, scan all rows in the right table, outputting row combinations that satisfy the join predicate.

### Task 6: Hash Join ★★★

Implement hash join. The build phase constructs a hash table from the left table; the probe phase scans the right table row by row, looking up matches via the hash table.

### Task 7: Aggregation ★★★★

Implement grouped aggregation, supporting COUNT(\*), COUNT, SUM, MIN, MAX. Handle GROUP BY grouping and default values for empty input.

### Task 8: Sort Executor ★★☆

Materialize all child data and sort by ORDER BY expressions. Support multi-column sorting and ascending/descending order.

### Task 9: Limit Executor ★☆☆

Limit the number of output rows, passing through the first N rows and then stopping.

### Task 10: Projection Executor ★☆☆

Project each row from the child executor by evaluating a list of projection expressions, producing a new Tuple containing only the selected columns/expressions. Used for `SELECT col1, col2, expr...` queries (non-`SELECT *` column selection).

### Task 11: Index Scan Executor ★★☆

Use the B+ tree index to scan rows that match the conditions.

## 4. Implementation Guide

### 4.1 SeqScan Executor

```
Init():
  Retrieve table_info from catalog (via plan->GetTableOid())
  Initialize the table_heap iterator: iter_ = table_heap->Begin()

Next(tuple, rid):
  while iter_ != table_heap->End():
    *tuple = *iter_     // Get the current row
    *rid = tuple->GetRID()
    ++iter_             // Advance the iterator
    // If there is a predicate, check if it is satisfied
    if plan_->GetPredicate() != nullptr:
      auto value = plan_->GetPredicate()->Evaluate(tuple, &output_schema)
      if !value.GetAsBoolean():
        continue        // Not satisfied, skip
    return true
  return false          // Scan complete
```

### 4.2 Insert Executor

```
Init():
  child_executor_->Init()
  has_inserted_ = false

Next(tuple, rid):
  if has_inserted_: return false    // Execute only once
  has_inserted_ = true

  Retrieve table_info and indexes from catalog
  int count = 0
  Tuple child_tuple; RID child_rid;
  while child_executor_->Next(&child_tuple, &child_rid):
    auto new_rid = table_heap->InsertTuple(child_tuple)
    // Update all indexes
    for each index in indexes:
      index->InsertEntry(child_tuple, *new_rid)
    count++

  *tuple = Tuple({Value(TypeId::INTEGER, count)})  // Return inserted row count
  return true
```

### 4.3 Delete Executor

Similar to Insert, but calls `DeleteTuple` and `DeleteEntry`.

### 4.4 Update Executor

```
Init():
  child_executor_->Init()
  has_updated_ = false

Next(tuple, rid):
  if has_updated_: return false
  has_updated_ = true

  int count = 0
  while child_executor_->Next(&child_tuple, &child_rid):
    // Construct new row using update expressions
    std::vector<Value> new_values;
    for (const auto &expr : plan_->GetUpdateExpressions()):
      new_values.push_back(expr->Evaluate(&child_tuple, &child_schema))
    Tuple new_tuple(std::move(new_values))
    table_heap->UpdateTuple(new_tuple, child_rid)
    // Update indexes: delete old entry + insert new entry
    count++

  *tuple = Tuple({Value(TypeId::INTEGER, count)})
  return true
```

### 4.5 Nested Loop Join

**Option 1: Streaming Implementation** (Recommended but more complex)
```
Init():
  left_executor_->Init()
  has_left_tuple_ = left_executor_->Next(&left_tuple_, &left_rid_)
  right_executor_->Init()

Next(tuple, rid):
  while has_left_tuple_:
    while right_executor_->Next(&right_tuple, &right_rid):
      // Check join predicate (note: use EvaluateJoin)
      if predicate == nullptr || predicate->EvaluateJoin(...).GetAsBoolean():
        // Merge columns from left and right rows
        *tuple = merged Tuple
        return true
    // Right table exhausted, get next left row, reset right table
    has_left_tuple_ = left_executor_->Next(&left_tuple_, &left_rid_)
    right_executor_->Init()
  return false
```

**Option 2: Materialize All Results** (Simpler)
```
Init():
  Materialize all matching row pairs into result_tuples_
  cursor_ = 0

Next(): Retrieve from result_tuples_[cursor_++]
```

### 4.6 Hash Join

```
Init():
  // Build phase: construct hash table from left table
  left_executor_->Init()
  hash_table_.clear()
  while left_executor_->Next(&tuple, &rid):
    auto key = plan_->GetLeftKeyExpression()->Evaluate(&tuple, &left_schema)
    hash_table_[key.ToString()].push_back(tuple)

  // Prepare probe phase
  right_executor_->Init()
  result_tuples_.clear()
  cursor_ = 0

  // Probe phase: scan right table, match via hash table
  while right_executor_->Next(&right_tuple, &right_rid):
    auto key = plan_->GetRightKeyExpression()->Evaluate(&right_tuple, &right_schema)
    auto it = hash_table_.find(key.ToString())
    if it != hash_table_.end():
      for each left_tuple in it->second:
        Merge columns of left_tuple and right_tuple
        result_tuples_.push_back(merged)

Next():
  if cursor_ >= result_tuples_.size(): return false
  *tuple = result_tuples_[cursor_++]
  return true
```

### 4.7 Aggregation

```
Init():
  child_executor_->Init()
  // Materialize all child data, group by group-by key
  groups: HashMap<string, AggState>

  while child_executor_->Next(&child_tuple, &child_rid):
    // Compute group key
    group_key = Concatenate all group_by expression results

    if group_key is a new group:
      Initialize aggregate values
    else:
      Update aggregate values

  // Aggregation type handling:
  //   CountStar: +1 (count every row)
  //   Count: +1 when non-NULL
  //   Sum: Accumulate
  //   Min: Take smaller value
  //   Max: Take larger value

  // Construct results: [group_by_vals..., agg_vals...]
  for each group:
    result_tuples_.push_back(combine group values and aggregate values)

  // Special case: when there is no GROUP BY and input is empty, still return one row with default values
  //   COUNT(*) / COUNT → 0,  SUM/MIN/MAX → NULL
```

### 4.8 Sort Executor

```
Init():
  child_executor_->Init()
  sorted_tuples_.clear()
  cursor_ = 0

  // Materialize all child data
  while child_executor_->Next(&tuple, &rid):
    sorted_tuples_.push_back(tuple)

  // Use std::sort with a custom comparator
  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(),
    [&order_bys, schema](const Tuple &a, const Tuple &b) {
      for each (is_ascending, expr) in order_bys:
        val_a = expr->Evaluate(&a, schema)
        val_b = expr->Evaluate(&b, schema)
        if val_a == val_b: continue
        if is_ascending: return val_a < val_b
        else: return val_a > val_b
      return false  // Completely equal
    })

Next():
  if cursor_ >= sorted_tuples_.size(): return false
  *tuple = sorted_tuples_[cursor_++]
  return true
```

### 4.9 Limit Executor

```
Init():
  child_executor_->Init()
  count_ = 0

Next(tuple, rid):
  if count_ >= plan_->GetLimit(): return false
  if !child_executor_->Next(tuple, rid): return false
  count_++
  return true
```

### 4.10 Projection Executor

The projection executor retrieves rows from the child executor, evaluates a set of expressions on each row, and produces a new row containing only the desired columns or computed results.

**Plan node interface:**
```cpp
class ProjectionPlanNode {
    auto GetExpressions() const -> const std::vector<AbstractExpressionRef> &;
    auto GetChildPlan() const -> const AbstractPlanNodeRef &;
};
```

**Pseudocode:**
```
Init():
  child_executor_->Init()

Next(tuple, rid):
  Tuple child_tuple; RID child_rid;
  if !child_executor_->Next(&child_tuple, &child_rid):
    return false

  // Evaluate each projection expression
  const auto &exprs = plan_->GetExpressions()
  const auto &child_schema = child_executor_->GetOutputSchema()
  std::vector<Value> values
  for (const auto &expr : exprs):
    values.push_back(expr->Evaluate(&child_tuple, &child_schema))

  *tuple = Tuple(std::move(values))
  *rid = child_rid
  return true
```

**Example:**
```sql
SELECT id, val + 1 FROM students;
```
The above query generates a ProjectionPlanNode with two projection expressions:
- Expression 0: `ColumnValueExpression(0, 0, INTEGER)` — reads column 0 (id)
- Expression 1: `ArithmeticExpression(ColumnValueExpression(0, 1, INTEGER), ConstantValueExpression(1), Plus)` — computes val + 1

### 4.11 Index Scan Executor

Use the B+ tree iterator to scan. Retrieve the index info from the Catalog, use the index's `Begin()` to iterate over all RIDs, then fetch the complete Tuple from the table heap via the RID.

## 5. Build & Test

```bash
# Build the project
cd build && cmake --build . -j$(nproc)

# Run Lab 3 related tests
ctest --test-dir build -R executor_test --output-on-failure

# Run all tests
ctest --test-dir build --output-on-failure
```

## 6. Common Mistakes

1. **Evaluate vs EvaluateJoin**: For single-table operations (SeqScan predicates, Sort keys, Projection expressions), use `Evaluate(tuple, schema)`. For two-table operations (NLJ/HashJoin predicates), use `EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema)`.

2. **Insert/Delete/Update return only once**: These executors should execute all operations on the first `Next()` call and return the affected row count; subsequent calls return `false`. Use a flag like `has_inserted_` for control.

3. **Aggregation with empty input**: Without GROUP BY, even if input is empty, return one row with default aggregate values (COUNT returns 0, SUM/MIN/MAX return NULL). With GROUP BY, empty input should produce no rows.

4. **Hash Join key type**: Using `Value::ToString()` as the hash key is the simplest approach.

5. **NLJ right table reset**: After processing each left table row, call `right_executor_->Init()` to reinitialize the right table scan.

6. **Index updates**: Insert and Delete operations must update both the table and all related indexes.

7. **Join result construction**: When joining two rows, merge all columns from the left and right tables into a new Tuple.

8. **Adding member variables to executor headers**: Some executor header files (NLJ, HashJoin, Aggregation, etc.) may require you to add extra member variables to store intermediate state.

9. **Projection uses child executor's Schema**: The projection executor evaluates against the child executor's output rows, so use `child_executor_->GetOutputSchema()` rather than `plan_->GetOutputSchema()` as the schema parameter for `Evaluate()`.

## 7. Grading

| Component | Points |
|-----------|--------|
| SeqScan Executor | 15 |
| Insert Executor | 10 |
| Delete Executor | 5 |
| Update Executor | 5 |
| Nested Loop Join Executor | 10 |
| Hash Join Executor | 10 |
| Aggregation Executor | 15 |
| Sort Executor | 10 |
| Limit Executor | 5 |
| Projection Executor | 10 |
| Index Scan Executor | 5 |
| **Total** | **100** |

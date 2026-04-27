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

## 4. Implementation Notes

- Follow the Volcano model strictly: `Init()` prepares state, `Next()` returns one tuple at a time unless the operator is explicitly materializing.
- Keep row counts, tuple schemas, and output order consistent with the plan node definition.
- For DML, update the base table and any related index metadata together.
- Use the child executor's output schema when evaluating expressions that operate on child rows.
- For join and aggregation operators, keep intermediate state private to the executor rather than exposing it through the plan.
- Index scan should be driven by the catalog and index metadata, then fetch the corresponding base-table tuples.

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

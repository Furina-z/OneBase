# Lab 3: 查询执行

## 1. 实验概述

本实验的目标是为 OneBase 数据库实现 **查询执行引擎 (Query Execution Engine)**。你将实现 11 个查询执行算子 (Executor)，使数据库能够执行 SQL 查询语句，包括顺序扫描、索引扫描、插入、删除、更新、连接、聚合、排序、Limit 和投影。

你将实现以下执行器：

| # | 执行器 | 文件 |
|---|--------|------|
| 1 | Sequential Scan (顺序扫描) | `src/execution/executors/seq_scan_executor.cpp` |
| 2 | Index Scan (索引扫描) | `src/execution/executors/index_scan_executor.cpp` |
| 3 | Insert (插入) | `src/execution/executors/insert_executor.cpp` |
| 4 | Delete (删除) | `src/execution/executors/delete_executor.cpp` |
| 5 | Update (更新) | `src/execution/executors/update_executor.cpp` |
| 6 | Nested Loop Join (嵌套循环连接) | `src/execution/executors/nested_loop_join_executor.cpp` |
| 7 | Hash Join (哈希连接) | `src/execution/executors/hash_join_executor.cpp` |
| 8 | Aggregation (聚合) | `src/execution/executors/aggregation_executor.cpp` |
| 9 | Sort (排序) | `src/execution/executors/sort_executor.cpp` |
| 10 | Limit (限制行数) | `src/execution/executors/limit_executor.cpp` |
| 11 | Projection (投影) | `src/execution/executors/projection_executor.cpp` |

## 2. 背景知识

### 2.1 Volcano 迭代器模型

OneBase 使用 **Volcano 迭代器模型** (iterator model)，也称为"火山模型"。每个执行器实现两个方法：

```cpp
class AbstractExecutor {
    virtual void Init() = 0;                           // 初始化/重置执行器
    virtual auto Next(Tuple *tuple, RID *rid) -> bool = 0;  // 获取下一行
};
```

**工作流程：**
```
SELECT id FROM students WHERE age > 20 ORDER BY id LIMIT 5;

          ┌────────────┐
          │ Projection │  ← 投影：只选取 id 列
          └─────┬──────┘
                │  Next()
          ┌─────▼─────┐
          │   Limit   │  ← 限制输出行数
          └─────┬─────┘
                │  Next()
          ┌─────▼─────┐
          │   Sort    │  ← 物化全部子结果，排序
          └─────┬─────┘
                │  Next()
          ┌─────▼─────┐
          │  SeqScan  │  ← 扫描 students 表，过滤 age > 20
          └───────────┘
```

每个执行器通过调用子执行器的 `Next()` 来获取输入行，一次一行，形成自底向上的"拉取"(pull) 模式。

### 2.2 Pipeline Breaker（管道阻断）

大多数执行器是**流式**的——一次处理一行数据（如 SeqScan、Limit、Projection）。但有些执行器需要**先物化全部子数据**才能开始输出：

| 类型 | 执行器 | 原因 |
|------|--------|------|
| 流式 | SeqScan, Insert, Delete, Update, Limit, Projection | 逐行处理 |
| 物化 | Sort, Aggregation | 排序需要看全部数据，聚合需要分组 |
| 物化 | Hash Join | Build 阶段需要全部左表数据 |
| 物化 | Nested Loop Join | 对左表每一行要重扫右表 |

### 2.3 关键接口

**Catalog（目录）：**
```cpp
auto GetTable(table_oid_t oid) -> TableInfo *;
auto GetTableIndexes(const std::string &table_name) -> std::vector<IndexInfo *>;
```

**TableHeap（堆表）：**
```cpp
auto InsertTuple(const Tuple &tuple) -> std::optional<RID>;
void UpdateTuple(const Tuple &new_tuple, const RID &rid);
void DeleteTuple(const RID &rid);
auto GetTuple(const RID &rid) -> Tuple;
auto Begin() -> Iterator;
auto End() -> Iterator;
```

**Expression（表达式求值）：**
```cpp
// 单表表达式（用于 SeqScan、Sort、Projection 等）
auto Evaluate(const Tuple *tuple, const Schema *schema) -> Value;

// 双表表达式（用于 Join）
auto EvaluateJoin(const Tuple *left, const Schema *left_schema,
                   const Tuple *right, const Schema *right_schema) -> Value;
```

**ExecutorContext：**
```cpp
auto GetCatalog() -> Catalog *;
auto GetBufferPoolManager() -> BufferPoolManager *;
auto GetTransaction() -> Transaction *;
```

### 2.4 Tuple 构造

创建新 Tuple 的方式：
```cpp
// 从 Value 列表构造
std::vector<Value> values = {Value(TypeId::INTEGER, 42), Value(TypeId::VARCHAR, "hello")};
Tuple new_tuple(std::move(values));

// 通过表达式求值构造更新后的 Tuple
const auto &exprs = plan_->GetUpdateExpressions();
std::vector<Value> new_values;
for (const auto &expr : exprs) {
    new_values.push_back(expr->Evaluate(&old_tuple, &schema));
}
Tuple updated(std::move(new_values));
```

## 3. 你的任务

**建议实现顺序：** SeqScan → Insert → Delete → Update → NLJ → HashJoin → Aggregation → Sort → Limit → Projection → IndexScan

每个执行器需要实现 `Init()` 和 `Next()` 两个方法。

### Task 1: SeqScan Executor ★★☆

顺序扫描表中所有行，并应用可选的谓词 (predicate) 进行过滤。

### Task 2: Insert Executor ★★☆

从子执行器获取待插入的行，插入到目标表中，同时更新所有相关索引。只调用一次 `Next()`，返回插入行数。

### Task 3: Delete Executor ★★☆

从子执行器获取待删除的行，从目标表中删除。只调用一次 `Next()`，返回删除行数。

### Task 4: Update Executor ★★★

从子执行器获取旧行，使用更新表达式计算新值，执行更新。只调用一次 `Next()`，返回更新行数。

### Task 5: Nested Loop Join ★★★

实现嵌套循环连接。对于左表的每一行，扫描右表的全部行，输出满足连接谓词的行组合。

### Task 6: Hash Join ★★★

实现哈希连接。Build 阶段建立左表的哈希表，Probe 阶段逐行扫描右表并通过哈希表查找匹配。

### Task 7: Aggregation ★★★★

实现分组聚合，支持 COUNT(*)、COUNT、SUM、MIN、MAX。需要处理 GROUP BY 分组、空输入的默认值。

### Task 8: Sort Executor ★★☆

物化所有子数据，按照 ORDER BY 表达式排序。支持多列排序和升/降序。

### Task 9: Limit Executor ★☆☆

限制输出行数，透传前 N 行后停止。

### Task 10: Projection Executor ★☆☆

对子执行器输出的每一行进行投影，根据投影表达式列表计算出新的列值，生成只包含所选列/表达式的新 Tuple。用于 `SELECT col1, col2, expr...` 这类非 `SELECT *` 的列选择查询。

### Task 11: Index Scan Executor ★★☆

使用 B+ 树索引扫描符合条件的行。

## 4. 实现指南

### 4.1 SeqScan Executor

```
Init():
  从 catalog 获取 table_info（通过 plan->GetTableOid()）
  初始化 table_heap 的迭代器: iter_ = table_heap->Begin()

Next(tuple, rid):
  while iter_ != table_heap->End():
    *tuple = *iter_     // 获取当前行
    *rid = tuple->GetRID()
    ++iter_             // 前进迭代器
    // 如果有 predicate，检查是否满足
    if plan_->GetPredicate() != nullptr:
      auto value = plan_->GetPredicate()->Evaluate(tuple, &output_schema)
      if !value.GetAsBoolean():
        continue        // 不满足，跳过
    return true
  return false          // 扫描结束
```

### 4.2 Insert Executor

```
Init():
  child_executor_->Init()
  has_inserted_ = false

Next(tuple, rid):
  if has_inserted_: return false    // 只执行一次
  has_inserted_ = true

  从 catalog 获取 table_info 和 indexes
  int count = 0
  Tuple child_tuple; RID child_rid;
  while child_executor_->Next(&child_tuple, &child_rid):
    auto new_rid = table_heap->InsertTuple(child_tuple)
    // 更新所有索引
    for each index in indexes:
      index->InsertEntry(child_tuple, *new_rid)
    count++

  *tuple = Tuple({Value(TypeId::INTEGER, count)})  // 返回插入行数
  return true
```

### 4.3 Delete Executor

与 Insert 类似，但调用 `DeleteTuple` 和 `DeleteEntry`。

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
    // 使用 update expressions 构造新行
    std::vector<Value> new_values;
    for (const auto &expr : plan_->GetUpdateExpressions()):
      new_values.push_back(expr->Evaluate(&child_tuple, &child_schema))
    Tuple new_tuple(std::move(new_values))
    table_heap->UpdateTuple(new_tuple, child_rid)
    // 更新索引: 删除旧条目 + 插入新条目
    count++

  *tuple = Tuple({Value(TypeId::INTEGER, count)})
  return true
```

### 4.5 Nested Loop Join

**方式一：流式实现**（推荐但复杂）
```
Init():
  left_executor_->Init()
  has_left_tuple_ = left_executor_->Next(&left_tuple_, &left_rid_)
  right_executor_->Init()

Next(tuple, rid):
  while has_left_tuple_:
    while right_executor_->Next(&right_tuple, &right_rid):
      // 检查 join 谓词（注意使用 EvaluateJoin）
      if predicate == nullptr || predicate->EvaluateJoin(...).GetAsBoolean():
        // 合并左右行的列
        *tuple = 合并后的 Tuple
        return true
    // 右表扫完，取左表下一行，重置右表
    has_left_tuple_ = left_executor_->Next(&left_tuple_, &left_rid_)
    right_executor_->Init()
  return false
```

**方式二：物化全部结果**（简单）
```
Init():
  物化所有匹配的行对到 result_tuples_ 中
  cursor_ = 0

Next(): 从 result_tuples_[cursor_++] 取出
```

### 4.6 Hash Join

```
Init():
  // Build 阶段：构建左表哈希表
  left_executor_->Init()
  hash_table_.clear()
  while left_executor_->Next(&tuple, &rid):
    auto key = plan_->GetLeftKeyExpression()->Evaluate(&tuple, &left_schema)
    hash_table_[key.ToString()].push_back(tuple)

  // Probe 阶段准备
  right_executor_->Init()
  result_tuples_.clear()
  cursor_ = 0

  // Probe 阶段：扫描右表，通过哈希表匹配
  while right_executor_->Next(&right_tuple, &right_rid):
    auto key = plan_->GetRightKeyExpression()->Evaluate(&right_tuple, &right_schema)
    auto it = hash_table_.find(key.ToString())
    if it != hash_table_.end():
      for each left_tuple in it->second:
        合并 left_tuple 和 right_tuple 的列
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
  // 物化所有子数据，按 group-by key 分组
  groups: HashMap<string, AggState>

  while child_executor_->Next(&child_tuple, &child_rid):
    // 计算 group key
    group_key = 拼接所有 group_by 表达式的结果

    if group_key 是新组:
      初始化聚合值
    else:
      更新聚合值

  // 聚合类型处理:
  //   CountStar: +1（每行都计数）
  //   Count: 非 NULL 时 +1
  //   Sum: 累加
  //   Min: 取较小值
  //   Max: 取较大值

  // 构造结果: [group_by_vals..., agg_vals...]
  for each group:
    result_tuples_.push_back(组合 group values 和 aggregate values)

  // 特殊情况：无 group-by 且输入为空时，仍返回一行默认值
  //   COUNT(*) / COUNT → 0,  SUM/MIN/MAX → NULL
```

### 4.8 Sort Executor

```
Init():
  child_executor_->Init()
  sorted_tuples_.clear()
  cursor_ = 0

  // 物化所有子数据
  while child_executor_->Next(&tuple, &rid):
    sorted_tuples_.push_back(tuple)

  // 使用 std::sort + 自定义比较器
  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(),
    [&order_bys, schema](const Tuple &a, const Tuple &b) {
      for each (is_ascending, expr) in order_bys:
        val_a = expr->Evaluate(&a, schema)
        val_b = expr->Evaluate(&b, schema)
        if val_a == val_b: continue
        if is_ascending: return val_a < val_b
        else: return val_a > val_b
      return false  // 完全相等
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

投影算子从子执行器获取行，对每一行求值一组表达式，从而产生一个只包含所需列或计算结果的新行。

**计划节点接口：**
```cpp
class ProjectionPlanNode {
    auto GetExpressions() const -> const std::vector<AbstractExpressionRef> &;
    auto GetChildPlan() const -> const AbstractPlanNodeRef &;
};
```

**伪代码：**
```
Init():
  child_executor_->Init()

Next(tuple, rid):
  Tuple child_tuple; RID child_rid;
  if !child_executor_->Next(&child_tuple, &child_rid):
    return false

  // 对每个投影表达式求值
  const auto &exprs = plan_->GetExpressions()
  const auto &child_schema = child_executor_->GetOutputSchema()
  std::vector<Value> values
  for (const auto &expr : exprs):
    values.push_back(expr->Evaluate(&child_tuple, &child_schema))

  *tuple = Tuple(std::move(values))
  *rid = child_rid
  return true
```

**示例：**
```sql
SELECT id, val + 1 FROM students;
```
上述查询将生成一个包含两个投影表达式的 ProjectionPlanNode：
- 表达式 0: `ColumnValueExpression(0, 0, INTEGER)` — 读取第 0 列 (id)
- 表达式 1: `ArithmeticExpression(ColumnValueExpression(0, 1, INTEGER), ConstantValueExpression(1), Plus)` — 计算 val + 1

### 4.11 Index Scan Executor

使用 B+ 树迭代器进行扫描。从 Catalog 获取 index info，使用索引的 `Begin()` 遍历所有 RID，然后通过 RID 从 table heap 获取完整的 Tuple。

## 5. 编译与测试

```bash
# 编译项目
cd build && cmake --build . -j$(nproc)

# 运行 Lab 3 相关测试
ctest --test-dir build -R executor_test --output-on-failure

# 运行全部测试
ctest --test-dir build --output-on-failure
```

## 6. 常见错误

1. **Evaluate vs EvaluateJoin**：单表操作（SeqScan 谓词、Sort 排序键、Projection 投影表达式）使用 `Evaluate(tuple, schema)`。双表操作（NLJ/HashJoin 谓词）使用 `EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema)`。

2. **Insert/Delete/Update 只返回一次**：这些执行器应该在第一次 `Next()` 调用时执行所有操作并返回受影响的行数，后续调用返回 `false`。使用 `has_inserted_` 类似的标志控制。

3. **聚合空输入**：没有 GROUP BY 时，即使输入为空也要返回一行默认聚合值（COUNT 返回 0，SUM/MIN/MAX 返回 NULL）。有 GROUP BY 时，空输入应该不返回任何行。

4. **Hash Join 的 key 类型**：使用 `Value::ToString()` 作为哈希 key 是最简单的方式。

5. **NLJ 右表重置**：每处理完一个左表行对，需要调用 `right_executor_->Init()` 重新初始化右表扫描。

6. **索引更新**：Insert 和 Delete 操作必须同时更新表和所有相关索引。

7. **Join 结果构造**：连接两行时，需要将左表和右表的所有列合并成一个新的 Tuple。

8. **需要在执行器头文件中添加成员变量**：部分执行器（NLJ、HashJoin、Aggregation 等）的头文件可能需要你添加额外的成员变量来存储中间状态。

9. **Projection 使用子执行器的 Schema**：投影执行器对子执行器输出的行求值，因此应使用 `child_executor_->GetOutputSchema()` 而非 `plan_->GetOutputSchema()` 作为 `Evaluate()` 的 schema 参数。

## 7. 评分标准

| 组件 | 分值 |
|------|------|
| SeqScan Executor (顺序扫描) | 15 |
| Insert Executor (插入) | 10 |
| Delete Executor (删除) | 5 |
| Update Executor (更新) | 5 |
| Nested Loop Join Executor (嵌套循环连接) | 10 |
| Hash Join Executor (哈希连接) | 10 |
| Aggregation Executor (聚合) | 15 |
| Sort Executor (排序) | 10 |
| Limit Executor (限制行数) | 5 |
| Projection Executor (投影) | 10 |
| Index Scan Executor (索引扫描) | 5 |
| **总计** | **100** |

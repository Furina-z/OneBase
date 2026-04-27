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

## 4. 实现说明

- 严格遵守 Volcano 模型：`Init()` 负责初始化状态，`Next()` 按需返回一行，除非算子明确需要先物化。
- 保持输出行数、schema 和列顺序与计划节点定义一致。
- 对 DML 语句，更新基础表和相关索引元数据要同步完成。
- 对需要计算表达式的算子，始终使用子执行器的输出 schema 进行求值。
- 连接、聚合、排序和索引扫描都应把中间状态限制在执行器内部，不要泄漏到文档层面。

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

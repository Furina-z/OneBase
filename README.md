<div align="center">
  <img src="docs/assets/logo.jpg" alt="OneBase Logo" width="400">
  <h1>OneBase</h1>
  <p><strong>A relational database management system built from scratch in modern C++</strong></p>
  <p>
    <img src="https://img.shields.io/badge/C++-17-blue.svg" alt="C++17">
    <img src="https://img.shields.io/badge/CMake-3.16+-green.svg" alt="CMake 3.16+">
    <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License">
  </p>
</div>

---

## About

OneBase is a lightweight relational database system designed for teaching the internals of a DBMS. It covers the core components that make up a real database engine — from buffer management and disk-based indexing to SQL query execution and transaction concurrency control.

## Architecture

```
                          ┌───────────────────────┐
                          │   SQL Parser          │
                          └──────────┬────────────┘
                                     │
                          ┌──────────▼────────────┐
                          │   Binder & Optimizer  │  SQL → Plan Tree
                          └──────────┬────────────┘
                                     │
                          ┌──────────▼────────────┐
                          │   Execution Engine    │  Volcano Iterator Model
                          │   (11 Executors)      │ 
                          └──────────┬────────────┘
                                     │
              ┌──────────────────────┼──────────────────────┐
              │                      │                      │
   ┌──────────▼───────────┐ ┌────────▼─────────┐ ┌──────────▼────────┐
   │  B+ Tree Index       │ │  Table Heap      │ │  Lock Manager     │
   │                      │ │  (Row Storage)   │ │  (2PL)            │
   └──────────┬───────────┘ └────────┬─────────┘ └───────────────────┘
              │                      │
              └──────────┬───────────┘
                         │
              ┌──────────▼───────────┐
              │  Buffer Pool Manager │
              │  (LRU-K Replacement) │
              └──────────┬───────────┘
                         │
              ┌──────────▼───────────┐
              │  Disk Manager        │  Page I/O
              └──────────────────────┘
```

## Labs

| Lab | Topic | Key Concepts | Components |
|-----|-------|-------------|------------|
| **1** | [Buffer Pool Manager](docs/lab1_buffer_pool_en.md) | Page caching, eviction policies, RAII | LRU-K Replacer, Buffer Pool Manager, Page Guard |
| **2** | [B+ Tree Index](docs/lab2_b_plus_tree_en.md) | Disk-based indexing, tree balancing | Internal/Leaf Pages, Insert/Delete/Search, Iterator |
| **3** | [Query Execution](docs/lab3_query_execution_en.md) | Volcano model, join algorithms | 11 Executors: SeqScan, IndexScan, Insert, Delete, Update, NLJ, HashJoin, Aggregation, Sort, Limit, Projection |
| **4** | [Concurrency Control](docs/lab4_concurrency_control_en.md) | Two-phase locking, lock compatibility | LockShared, LockExclusive, LockUpgrade, Unlock |

Each lab comes with:
- Detailed documentation with pseudocode and diagrams
- Stub files with TODO markers for students to fill in
- Automated graded tests (`test/eval/`)

## Getting Started

### Prerequisites

- **Compiler**: GCC 9+ or Clang 10+ (C++17 support required)
- **CMake**: 3.16 or higher
- **Dependencies**: Google Test, fmt

On Ubuntu/Debian:
```bash
sudo apt install cmake g++ libgtest-dev libfmt-dev
```

On Arch Linux:
```bash
sudo pacman -S cmake gcc gtest fmt
```

On macOS:
```bash
brew install cmake googletest fmt
```

### Build

```bash
mkdir build && cd build
cmake ..
cmake --build . -j$(nproc)
```

### Run Tests

```bash
# Run all tests
ctest --test-dir build --output-on-failure

# Run a specific lab's evaluation tests
ctest --test-dir build -R lab1_eval_test --output-on-failure
ctest --test-dir build -R lab2_eval_test --output-on-failure
ctest --test-dir build -R lab3_eval_test --output-on-failure
ctest --test-dir build -R lab4_eval_test --output-on-failure
```

### Interactive Shell

```bash
./build/bin/onebase_shell
```

## Project Structure

```
OneBase/
├── src/
│   ├── include/onebase/       # Public headers
│   ├── buffer/                # Lab 1: Buffer pool manager
│   ├── storage/               # Lab 2: B+ tree, table heap, disk I/O
│   ├── execution/             # Lab 3: Query executors & expressions
│   ├── concurrency/           # Lab 4: Lock manager
│   ├── binder/                # SQL → plan tree binding
│   ├── optimizer/             # Plan optimization
│   ├── catalog/               # Table & index metadata
│   ├── type/                  # SQL type system (INTEGER, VARCHAR, ...)
│   └── common/                # Shared utilities
├── test/
│   ├── eval/                  # Graded evaluation tests (100 pts each)
│   ├── buffer/                # Unit tests for Lab 1
│   ├── storage/               # Unit tests for Lab 2
│   ├── execution/             # Unit tests for Lab 3
│   └── concurrency/           # Unit tests for Lab 4
├── reference/                 # Reference implementations (lab1–lab4)
├── docs/                      # Lab specifications (English & Chinese)
├── tools/                     # Shell and B+ tree printer
└── third_party/               # libpg_query (SQL parser)
```

## Documentation

Lab documentation is available in both English and Chinese:

| Lab | English | 中文 |
|-----|---------|------|
| 1 - Buffer Pool | [lab1_buffer_pool_en.md](docs/lab1_buffer_pool_en.md) | [lab1_buffer_pool_zh.md](docs/lab1_buffer_pool_zh.md) |
| 2 - B+ Tree | [lab2_b_plus_tree_en.md](docs/lab2_b_plus_tree_en.md) | [lab2_b_plus_tree_zh.md](docs/lab2_b_plus_tree_zh.md) |
| 3 - Query Execution | [lab3_query_execution_en.md](docs/lab3_query_execution_en.md) | [lab3_query_execution_zh.md](docs/lab3_query_execution_zh.md) |
| 4 - Concurrency Control | [lab4_concurrency_control_en.md](docs/lab4_concurrency_control_en.md) | [lab4_concurrency_control_zh.md](docs/lab4_concurrency_control_zh.md) |

## Submission

The submission deadline for this project is **May, 12th**. 
You should fork or clone this repository, complete Task 1-3 and upload the Github link to your submission on Canvas.

## Common Mistakes

Check [here](docs/common_mistakes.md).

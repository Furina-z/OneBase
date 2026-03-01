#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "onebase/binder/binder.h"
#include "onebase/catalog/column.h"
#include "onebase/catalog/schema.h"
#include "onebase/common/logger.h"
#include "onebase/optimizer/optimizer.h"
#include "onebase/server/onebase_instance.h"
#include "onebase/storage/table/tuple.h"

// Pretty-print a result set as an ASCII table.
static void PrintResultTable(const onebase::Schema &schema,
                             const std::vector<onebase::Tuple> &rows) {
  uint32_t col_count = schema.GetColumnCount();

  // Collect header names
  std::vector<std::string> headers;
  headers.reserve(col_count);
  for (uint32_t i = 0; i < col_count; i++) {
    headers.push_back(schema.GetColumn(i).GetName());
  }

  // Stringify all cell values
  std::vector<std::vector<std::string>> str_rows;
  str_rows.reserve(rows.size());
  for (const auto &tuple : rows) {
    std::vector<std::string> vals;
    vals.reserve(col_count);
    for (uint32_t i = 0; i < col_count; i++) {
      vals.push_back(tuple.GetValue(i).ToString());
    }
    str_rows.push_back(std::move(vals));
  }

  // Compute column widths
  std::vector<size_t> widths(col_count, 0);
  for (uint32_t c = 0; c < col_count; c++) {
    widths[c] = headers[c].size();
  }
  for (const auto &row : str_rows) {
    for (uint32_t c = 0; c < col_count && c < row.size(); c++) {
      widths[c] = std::max(widths[c], row[c].size());
    }
  }

  // Build separator
  std::string separator = "+";
  for (uint32_t c = 0; c < col_count; c++) {
    separator += std::string(widths[c] + 2, '-') + "+";
  }

  // Print header
  std::cout << separator << "\n|";
  for (uint32_t c = 0; c < col_count; c++) {
    std::cout << " " << std::left << std::setw(static_cast<int>(widths[c]))
              << headers[c] << " |";
  }
  std::cout << "\n" << separator << "\n";

  // Print rows
  for (const auto &row : str_rows) {
    std::cout << "|";
    for (uint32_t c = 0; c < col_count; c++) {
      std::string val = (c < row.size()) ? row[c] : "";
      std::cout << " " << std::left << std::setw(static_cast<int>(widths[c]))
                << val << " |";
    }
    std::cout << "\n";
  }
  std::cout << separator << "\n";

  // Row count
  std::cout << "(" << rows.size() << " row"
            << (rows.size() != 1 ? "s" : "") << ")" << std::endl;
}

// Execute a SQL statement and display results.
static void ExecuteSQL(onebase::OneBaseInstance &instance, const std::string &sql) {
  using namespace onebase;
  try {
    Binder binder(instance.GetCatalog());
    auto plan = binder.BindQuery(sql);

    Optimizer optimizer;
    plan = optimizer.Optimize(plan);

    std::vector<Tuple> result_set;
    instance.GetExecutionEngine()->Execute(plan, &result_set);

    PrintResultTable(plan->GetOutputSchema(), result_set);
  } catch (const std::exception &e) {
    std::cerr << "ERROR: " << e.what() << std::endl;
  }
}

auto main(int argc, char *argv[]) -> int {
  std::string db_file = "onebase_shell.db";
  if (argc > 1) {
    db_file = argv[1];
  }

  std::cout << "OneBase Interactive Shell" << std::endl;
  std::cout << "Database: " << db_file << std::endl;
  std::cout << "Type 'help' for available commands, 'quit' to exit." << std::endl;

  onebase::OneBaseInstance instance(db_file);

  std::string line;
  std::string query_buf;
  while (true) {
    if (query_buf.empty()) {
      std::cout << "onebase> ";
    } else {
      std::cout << "     -> ";
    }

    if (!std::getline(std::cin, line)) {
      break;
    }
    if (line.empty()) {
      continue;
    }

    // Meta-commands (not part of SQL)
    if (query_buf.empty()) {
      if (line == "quit" || line == "exit" || line == "\\q") {
        std::cout << "Bye!" << std::endl;
        break;
      }
      if (line == "help" || line == "\\h" || line == "\\?") {
        std::cout << "Commands:\n"
                  << "  help / \\h     Show this help\n"
                  << "  tables / \\dt  List all tables\n"
                  << "  quit / \\q     Exit the shell\n"
                  << "\n"
                  << "Enter SQL statements terminated by a semicolon (;).\n"
                  << "Supported: SELECT, INSERT, UPDATE, DELETE\n"
                  << std::endl;
        continue;
      }
      if (line == "tables" || line == "\\dt") {
        auto tables = instance.GetCatalog()->GetAllTables();
        if (tables.empty()) {
          std::cout << "No tables." << std::endl;
        } else {
          std::cout << std::left << std::setw(20) << "Name"
                    << "Schema" << std::endl;
          std::cout << std::string(60, '-') << std::endl;
          for (const auto *table : tables) {
            std::cout << std::left << std::setw(20) << table->name_
                      << table->schema_.ToString() << std::endl;
          }
        }
        continue;
      }
    }

    // Accumulate multi-line SQL until semicolon
    query_buf += line;
    if (query_buf.back() != ';') {
      query_buf += ' ';
      continue;
    }

    // Remove trailing semicolon
    std::string query = query_buf.substr(0, query_buf.size() - 1);
    query_buf.clear();

    // Trim whitespace
    while (!query.empty() && std::isspace(static_cast<unsigned char>(query.front()))) {
      query.erase(query.begin());
    }
    while (!query.empty() && std::isspace(static_cast<unsigned char>(query.back()))) {
      query.pop_back();
    }
    if (query.empty()) {
      continue;
    }

    ExecuteSQL(instance, query);
  }

  return 0;
}

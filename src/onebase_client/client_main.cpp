#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "onebase/server/protocol.h"

// Pretty-print a result table received from the server.
static void PrintResult(const std::string &body) {
  // Parse lines
  std::vector<std::string> lines;
  std::istringstream iss(body);
  std::string line;
  while (std::getline(iss, line)) {
    lines.push_back(line);
  }

  if (lines.empty()) {
    return;
  }

  // First line is the header; last line is the row count summary
  auto header_parts = onebase::ParsePipeSeparated(lines[0]);
  size_t num_cols = header_parts.size();

  // Parse all data rows
  std::vector<std::vector<std::string>> rows;
  for (size_t i = 1; i < lines.size(); i++) {
    if (lines[i].empty() || lines[i][0] == '(') {
      break;  // row-count summary line
    }
    rows.push_back(onebase::ParsePipeSeparated(lines[i]));
  }

  // Compute column widths
  std::vector<size_t> widths(num_cols, 0);
  for (size_t c = 0; c < num_cols; c++) {
    widths[c] = header_parts[c].size();
  }
  for (const auto &row : rows) {
    for (size_t c = 0; c < num_cols && c < row.size(); c++) {
      widths[c] = std::max(widths[c], row[c].size());
    }
  }

  // Print header
  std::string separator = "+";
  for (size_t c = 0; c < num_cols; c++) {
    separator += std::string(widths[c] + 2, '-') + "+";
  }

  std::cout << separator << "\n";
  std::cout << "|";
  for (size_t c = 0; c < num_cols; c++) {
    std::cout << " " << std::left << std::setw(static_cast<int>(widths[c]))
              << header_parts[c] << " |";
  }
  std::cout << "\n" << separator << "\n";

  // Print rows
  for (const auto &row : rows) {
    std::cout << "|";
    for (size_t c = 0; c < num_cols; c++) {
      std::string val = (c < row.size()) ? row[c] : "";
      std::cout << " " << std::left << std::setw(static_cast<int>(widths[c]))
                << val << " |";
    }
    std::cout << "\n";
  }
  std::cout << separator << "\n";

  // Row count summary (last non-empty line)
  for (auto it = lines.rbegin(); it != lines.rend(); ++it) {
    if (!it->empty() && (*it)[0] == '(') {
      std::cout << *it << "\n";
      break;
    }
  }
}

static void PrintUsage(const char *prog) {
  std::cerr << "Usage: " << prog << " [options]\n"
            << "Options:\n"
            << "  -H <host>   Server host (default: 127.0.0.1)\n"
            << "  -p <port>   Server port (default: "
            << onebase::DEFAULT_SERVER_PORT << ")\n"
            << "  -h          Show this help\n";
}

auto main(int argc, char *argv[]) -> int {
  std::string host = "127.0.0.1";
  uint16_t port = onebase::DEFAULT_SERVER_PORT;

  int opt;
  while ((opt = getopt(argc, argv, "H:p:h")) != -1) {
    switch (opt) {
      case 'H':
        host = optarg;
        break;
      case 'p':
        port = static_cast<uint16_t>(std::stoi(optarg));
        break;
      case 'h':
      default:
        PrintUsage(argv[0]);
        return (opt == 'h') ? 0 : 1;
    }
  }

  // Create TCP socket and connect
  int sock_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (sock_fd < 0) {
    perror("socket");
    return 1;
  }

  sockaddr_in server_addr{};
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) <= 0) {
    std::cerr << "Invalid address: " << host << std::endl;
    ::close(sock_fd);
    return 1;
  }

  if (::connect(sock_fd, reinterpret_cast<sockaddr *>(&server_addr),
                sizeof(server_addr)) < 0) {
    std::cerr << "Failed to connect to " << host << ":" << port << std::endl;
    perror("connect");
    ::close(sock_fd);
    return 1;
  }

  std::cout << "OneBase Client" << std::endl;
  std::cout << "Connected to " << host << ":" << port << std::endl;
  std::cout << "Type SQL queries, or \\q to quit." << std::endl;

  // REPL loop
  std::string line;
  std::string query_buf;
  while (true) {
    if (query_buf.empty()) {
      std::cout << "onebase=> ";
    } else {
      std::cout << "onebase-> ";
    }

    if (!std::getline(std::cin, line)) {
      break;
    }

    // Handle meta-commands
    if (line == "\\q" || line == "quit" || line == "exit") {
      onebase::SendMessage(sock_fd, onebase::MessageType::TERMINATE, "");
      break;
    }

    if (line == "\\?" || line == "help") {
      std::cout << "Commands:\n"
                << "  \\q          Quit\n"
                << "  \\?          Show this help\n"
                << "  SQL;        Execute a SQL statement\n"
                << std::endl;
      continue;
    }

    if (line.empty()) {
      continue;
    }

    // Accumulate multi-line queries until we see a semicolon
    query_buf += line;
    if (query_buf.back() != ';') {
      query_buf += ' ';
      continue;
    }

    // Remove trailing semicolon for the server
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

    // Send query to server
    if (!onebase::SendMessage(sock_fd, onebase::MessageType::QUERY, query)) {
      std::cerr << "Connection lost." << std::endl;
      break;
    }

    // Receive response
    onebase::MessageType msg_type{};
    std::string msg_data;
    if (!onebase::RecvMessage(sock_fd, &msg_type, &msg_data)) {
      std::cerr << "Connection lost." << std::endl;
      break;
    }

    switch (msg_type) {
      case onebase::MessageType::RESULT:
        PrintResult(msg_data);
        break;
      case onebase::MessageType::ERROR:
        std::cerr << "ERROR: " << msg_data << std::endl;
        break;
      case onebase::MessageType::COMMAND_COMPLETE:
        std::cout << msg_data << std::endl;
        break;
      default:
        std::cerr << "Unexpected response from server." << std::endl;
        break;
    }
  }

  ::close(sock_fd);
  std::cout << "Disconnected." << std::endl;
  return 0;
}

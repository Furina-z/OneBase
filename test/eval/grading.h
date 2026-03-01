#pragma once
#include <gtest/gtest.h>
#include <cstdio>
#include <map>
#include <string>
#include <vector>

namespace onebase::grading {

// Singleton registry: maps "Suite.Test" -> point value
class ScoreRegistry {
 public:
  static auto Instance() -> ScoreRegistry & {
    static ScoreRegistry inst;
    return inst;
  }
  void Register(const std::string &suite, const std::string &name, int points) {
    points_[suite + "." + name] = points;
  }
  auto GetPoints(const std::string &full_name) const -> int {
    auto it = points_.find(full_name);
    return it != points_.end() ? it->second : 0;
  }

 private:
  ScoreRegistry() = default;
  std::map<std::string, int> points_;
};

// Static registrar — registers a test's point value before main()
struct TestRegistrar {
  TestRegistrar(const char *suite, const char *name, int points) {
    ScoreRegistry::Instance().Register(suite, name, points);
  }
};

// GTest event listener that prints a score summary after all tests
class ScoreListener : public ::testing::EmptyTestEventListener {
 public:
  void OnTestEnd(const ::testing::TestInfo &info) override {
    std::string full = std::string(info.test_suite_name()) + "." + info.name();
    int pts = ScoreRegistry::Instance().GetPoints(full);
    bool passed = info.result()->Passed();
    results_.push_back({full, pts, passed});
  }

  void OnTestProgramEnd(const ::testing::UnitTest & /*unit_test*/) override {
    int earned = 0, total = 0;
    printf("\n");
    printf("+================================================+\n");
    printf("|            Evaluation Results                   |\n");
    printf("+================================================+\n");
    for (auto &r : results_) {
      total += r.max_points;
      int got = r.passed ? r.max_points : 0;
      earned += got;
      printf("|  [%s]  %-26s %3d / %3d pts |\n",
             r.passed ? "PASS" : "FAIL",
             r.test_name.c_str(), got, r.max_points);
    }
    printf("+================================================+\n");
    printf("|  Total Score:                    %3d / %3d     |\n", earned, total);
    printf("+================================================+\n\n");
  }

 private:
  struct Result {
    std::string test_name;
    int max_points;
    bool passed;
  };
  std::vector<Result> results_;
};

}  // namespace onebase::grading

// Macro: define a scored test (no fixture)
#define GRADED_TEST(test_suite, test_name, points)                          \
  static ::onebase::grading::TestRegistrar                                  \
      _reg_##test_suite##_##test_name(#test_suite, #test_name, points);     \
  TEST(test_suite, test_name)

// Macro: define a scored test with fixture
#define GRADED_TEST_F(test_fixture, test_name, points)                      \
  static ::onebase::grading::TestRegistrar                                  \
      _reg_##test_fixture##_##test_name(#test_fixture, #test_name, points); \
  TEST_F(test_fixture, test_name)

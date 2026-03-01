#include <gtest/gtest.h>
#include "eval/grading.h"

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  auto &listeners = ::testing::UnitTest::GetInstance()->listeners();
  listeners.Append(new onebase::grading::ScoreListener());
  return RUN_ALL_TESTS();
}

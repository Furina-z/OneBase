#include "lab1_test_common.h"

namespace onebase {

using onebase::test::LRUKReplacerLab1Test;

TEST_F(LRUKReplacerLab1Test, RecordAccessAndSize) { VerifyRecordAccessAndSize(); }

TEST_F(LRUKReplacerLab1Test, BasicEvict) { VerifyBasicEvict(); }

TEST_F(LRUKReplacerLab1Test, EvictByInfDistance) { VerifyEvictByInfDistance(); }

TEST_F(LRUKReplacerLab1Test, EvictByKDistance) { VerifyEvictByKDistance(); }

TEST_F(LRUKReplacerLab1Test, SetEvictableAndRemove) { VerifySetEvictableAndRemove(); }

}  // namespace onebase

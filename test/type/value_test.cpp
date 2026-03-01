#include <gtest/gtest.h>
#include "onebase/type/value.h"

namespace onebase {

TEST(ValueTest, IntegerBasic) {
  Value v1(TypeId::INTEGER, 42);
  EXPECT_EQ(v1.GetTypeId(), TypeId::INTEGER);
  EXPECT_EQ(v1.GetAsInteger(), 42);
  EXPECT_EQ(v1.ToString(), "42");
}

TEST(ValueTest, FloatBasic) {
  Value v1(TypeId::FLOAT, 3.14f);
  EXPECT_EQ(v1.GetTypeId(), TypeId::FLOAT);
  EXPECT_FLOAT_EQ(v1.GetAsFloat(), 3.14f);
}

TEST(ValueTest, BooleanBasic) {
  Value v_true(TypeId::BOOLEAN, true);
  Value v_false(TypeId::BOOLEAN, false);
  EXPECT_TRUE(v_true.GetAsBoolean());
  EXPECT_FALSE(v_false.GetAsBoolean());
}

TEST(ValueTest, VarcharBasic) {
  Value v1(TypeId::VARCHAR, std::string("hello"));
  EXPECT_EQ(v1.GetTypeId(), TypeId::VARCHAR);
  EXPECT_EQ(v1.ToString(), "hello");
}

TEST(ValueTest, IntegerComparison) {
  Value v1(TypeId::INTEGER, 10);
  Value v2(TypeId::INTEGER, 20);

  auto eq = v1.CompareEquals(v2);
  EXPECT_FALSE(eq.GetAsBoolean());

  auto lt = v1.CompareLessThan(v2);
  EXPECT_TRUE(lt.GetAsBoolean());

  auto gt = v1.CompareGreaterThan(v2);
  EXPECT_FALSE(gt.GetAsBoolean());
}

TEST(ValueTest, IntegerArithmetic) {
  Value v1(TypeId::INTEGER, 10);
  Value v2(TypeId::INTEGER, 3);

  auto sum = v1.Add(v2);
  EXPECT_EQ(sum.GetAsInteger(), 13);

  auto diff = v1.Subtract(v2);
  EXPECT_EQ(diff.GetAsInteger(), 7);

  auto prod = v1.Multiply(v2);
  EXPECT_EQ(prod.GetAsInteger(), 30);

  auto quot = v1.Divide(v2);
  EXPECT_EQ(quot.GetAsInteger(), 3);
}

TEST(ValueTest, Serialization) {
  Value v1(TypeId::INTEGER, 42);
  auto size = v1.GetSerializedSize();
  EXPECT_EQ(size, sizeof(int32_t));

  std::vector<char> buf(size);
  v1.SerializeTo(buf.data());

  Value v2 = Value::DeserializeFrom(buf.data(), TypeId::INTEGER);
  EXPECT_EQ(v2.GetAsInteger(), 42);
}

}  // namespace onebase

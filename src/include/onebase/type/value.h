#pragma once

#include "onebase/type/type_id.h"

#include <cstdint>
#include <string>

namespace onebase {

class Value {
   public:
    Value();
    explicit Value(TypeId type);
    Value(TypeId type, int32_t i);
    Value(TypeId type, float f);
    Value(TypeId type, bool b);
    Value(TypeId type, const char *s);
    Value(TypeId type, const std::string &s);

    auto GetTypeId() const -> TypeId { return type_id_; }

    auto GetAsInteger() const -> int32_t;
    auto GetAsFloat() const -> float;
    auto GetAsBoolean() const -> bool;
    auto GetAsString() const -> std::string;

    // Comparisons (return BOOLEAN Values)
    auto CompareLessThan(const Value &o) const -> Value;
    auto CompareGreaterThan(const Value &o) const -> Value;
    auto CompareLessThanOrEqual(const Value &o) const -> Value;
    auto CompareGreaterThanOrEqual(const Value &o) const -> Value;
    auto CompareEquals(const Value &o) const -> Value;
    auto CompareNotEquals(const Value &o) const -> Value;

    // Arithmetic
    auto Add(const Value &o) const -> Value;
    auto Subtract(const Value &o) const -> Value;
    auto Multiply(const Value &o) const -> Value;
    auto Divide(const Value &o) const -> Value;
    auto Modulo(const Value &o) const -> Value;

    // Logic
    auto And(const Value &o) const -> Value;
    auto Or(const Value &o) const -> Value;
    auto Not() const -> Value;

    // Aliases used by LogicExpression
    auto LogicalAnd(const Value &o) const -> Value { return And(o); }
    auto LogicalOr(const Value &o) const -> Value { return Or(o); }

    // Serialization
    void SerializeTo(char *storage) const;
    static auto DeserializeFrom(const char *storage, TypeId type) -> Value;
    auto GetSerializedSize() const -> uint32_t;

    auto ToString() const -> std::string;
    auto IsNull() const -> bool { return is_null_; }

   private:
    TypeId type_id_{TypeId::INVALID};
    union {
        int32_t integer_;
        float float_;
        bool boolean_;
    };
    std::string varchar_;
    bool is_null_{true};
};

}  // namespace onebase

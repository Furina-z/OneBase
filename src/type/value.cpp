#include "onebase/type/value.h"

#include "onebase/common/config.h"
#include "onebase/common/exception.h"
#include "onebase/common/macros.h"

#include <cstring>
#include <fmt/format.h>

namespace onebase {

Value::Value() : type_id_(TypeId::INVALID), integer_(0), is_null_(true) {}

Value::Value(TypeId type) : type_id_(type), integer_(0), is_null_(true) {}

Value::Value(TypeId type, int32_t i) : type_id_(type), integer_(i), is_null_(false) {}

Value::Value(TypeId type, float f) : type_id_(type), float_(f), is_null_(false) {}

Value::Value(TypeId type, bool b) : type_id_(type), boolean_(b), is_null_(false) {}

Value::Value(TypeId type, const char *s) : type_id_(type), integer_(0), varchar_(s), is_null_(false) {}

Value::Value(TypeId type, const std::string &s) : type_id_(type), integer_(0), varchar_(s), is_null_(false) {}

auto Value::GetAsInteger() const -> int32_t {
    ONEBASE_ASSERT(type_id_ == TypeId::INTEGER, "Value is not an integer");
    return integer_;
}

auto Value::GetAsFloat() const -> float {
    ONEBASE_ASSERT(type_id_ == TypeId::FLOAT, "Value is not a float");
    return float_;
}

auto Value::GetAsBoolean() const -> bool {
    ONEBASE_ASSERT(type_id_ == TypeId::BOOLEAN, "Value is not a boolean");
    return boolean_;
}

auto Value::GetAsString() const -> std::string {
    ONEBASE_ASSERT(type_id_ == TypeId::VARCHAR, "Value is not a varchar");
    return varchar_;
}

// --- Comparison ---

auto Value::CompareLessThan(const Value &o) const -> Value {
    ONEBASE_ASSERT(type_id_ == o.type_id_, "Type mismatch in comparison");
    switch (type_id_) {
        case TypeId::INTEGER:
            return Value(TypeId::BOOLEAN, integer_ < o.integer_);
        case TypeId::FLOAT:
            return Value(TypeId::BOOLEAN, float_ < o.float_);
        case TypeId::VARCHAR:
            return Value(TypeId::BOOLEAN, varchar_ < o.varchar_);
        default:
            UNREACHABLE("Unsupported type for comparison");
    }
}

auto Value::CompareGreaterThan(const Value &o) const -> Value {
    ONEBASE_ASSERT(type_id_ == o.type_id_, "Type mismatch in comparison");
    switch (type_id_) {
        case TypeId::INTEGER:
            return Value(TypeId::BOOLEAN, integer_ > o.integer_);
        case TypeId::FLOAT:
            return Value(TypeId::BOOLEAN, float_ > o.float_);
        case TypeId::VARCHAR:
            return Value(TypeId::BOOLEAN, varchar_ > o.varchar_);
        default:
            UNREACHABLE("Unsupported type for comparison");
    }
}

auto Value::CompareLessThanOrEqual(const Value &o) const -> Value {
    ONEBASE_ASSERT(type_id_ == o.type_id_, "Type mismatch in comparison");
    switch (type_id_) {
        case TypeId::INTEGER:
            return Value(TypeId::BOOLEAN, integer_ <= o.integer_);
        case TypeId::FLOAT:
            return Value(TypeId::BOOLEAN, float_ <= o.float_);
        case TypeId::VARCHAR:
            return Value(TypeId::BOOLEAN, varchar_ <= o.varchar_);
        default:
            UNREACHABLE("Unsupported type for comparison");
    }
}

auto Value::CompareGreaterThanOrEqual(const Value &o) const -> Value {
    ONEBASE_ASSERT(type_id_ == o.type_id_, "Type mismatch in comparison");
    switch (type_id_) {
        case TypeId::INTEGER:
            return Value(TypeId::BOOLEAN, integer_ >= o.integer_);
        case TypeId::FLOAT:
            return Value(TypeId::BOOLEAN, float_ >= o.float_);
        case TypeId::VARCHAR:
            return Value(TypeId::BOOLEAN, varchar_ >= o.varchar_);
        default:
            UNREACHABLE("Unsupported type for comparison");
    }
}

auto Value::CompareEquals(const Value &o) const -> Value {
    ONEBASE_ASSERT(type_id_ == o.type_id_, "Type mismatch in comparison");
    switch (type_id_) {
        case TypeId::INTEGER:
            return Value(TypeId::BOOLEAN, integer_ == o.integer_);
        case TypeId::FLOAT:
            return Value(TypeId::BOOLEAN, float_ == o.float_);
        case TypeId::BOOLEAN:
            return Value(TypeId::BOOLEAN, boolean_ == o.boolean_);
        case TypeId::VARCHAR:
            return Value(TypeId::BOOLEAN, varchar_ == o.varchar_);
        default:
            UNREACHABLE("Unsupported type for comparison");
    }
}

auto Value::CompareNotEquals(const Value &o) const -> Value {
    ONEBASE_ASSERT(type_id_ == o.type_id_, "Type mismatch in comparison");
    switch (type_id_) {
        case TypeId::INTEGER:
            return Value(TypeId::BOOLEAN, integer_ != o.integer_);
        case TypeId::FLOAT:
            return Value(TypeId::BOOLEAN, float_ != o.float_);
        case TypeId::BOOLEAN:
            return Value(TypeId::BOOLEAN, boolean_ != o.boolean_);
        case TypeId::VARCHAR:
            return Value(TypeId::BOOLEAN, varchar_ != o.varchar_);
        default:
            UNREACHABLE("Unsupported type for comparison");
    }
}

// --- Arithmetic ---

auto Value::Add(const Value &o) const -> Value {
    ONEBASE_ASSERT(type_id_ == o.type_id_, "Type mismatch in arithmetic");
    switch (type_id_) {
        case TypeId::INTEGER:
            return Value(TypeId::INTEGER, integer_ + o.integer_);
        case TypeId::FLOAT:
            return Value(TypeId::FLOAT, float_ + o.float_);
        default:
            UNREACHABLE("Unsupported type for addition");
    }
}

auto Value::Subtract(const Value &o) const -> Value {
    ONEBASE_ASSERT(type_id_ == o.type_id_, "Type mismatch in arithmetic");
    switch (type_id_) {
        case TypeId::INTEGER:
            return Value(TypeId::INTEGER, integer_ - o.integer_);
        case TypeId::FLOAT:
            return Value(TypeId::FLOAT, float_ - o.float_);
        default:
            UNREACHABLE("Unsupported type for subtraction");
    }
}

auto Value::Multiply(const Value &o) const -> Value {
    ONEBASE_ASSERT(type_id_ == o.type_id_, "Type mismatch in arithmetic");
    switch (type_id_) {
        case TypeId::INTEGER:
            return Value(TypeId::INTEGER, integer_ * o.integer_);
        case TypeId::FLOAT:
            return Value(TypeId::FLOAT, float_ * o.float_);
        default:
            UNREACHABLE("Unsupported type for multiplication");
    }
}

auto Value::Divide(const Value &o) const -> Value {
    ONEBASE_ASSERT(type_id_ == o.type_id_, "Type mismatch in arithmetic");
    switch (type_id_) {
        case TypeId::INTEGER:
            ONEBASE_ASSERT(o.integer_ != 0, "Division by zero");
            return Value(TypeId::INTEGER, integer_ / o.integer_);
        case TypeId::FLOAT:
            ONEBASE_ASSERT(o.float_ != 0.0f, "Division by zero");
            return Value(TypeId::FLOAT, float_ / o.float_);
        default:
            UNREACHABLE("Unsupported type for division");
    }
}

auto Value::Modulo(const Value &o) const -> Value {
    ONEBASE_ASSERT(type_id_ == o.type_id_, "Type mismatch in arithmetic");
    switch (type_id_) {
        case TypeId::INTEGER:
            ONEBASE_ASSERT(o.integer_ != 0, "Modulo by zero");
            return Value(TypeId::INTEGER, integer_ % o.integer_);
        default:
            UNREACHABLE("Unsupported type for modulo");
    }
}

// --- Logic ---

auto Value::And(const Value &o) const -> Value {
    ONEBASE_ASSERT(type_id_ == TypeId::BOOLEAN && o.type_id_ == TypeId::BOOLEAN, "AND requires boolean");
    return Value(TypeId::BOOLEAN, boolean_ && o.boolean_);
}

auto Value::Or(const Value &o) const -> Value {
    ONEBASE_ASSERT(type_id_ == TypeId::BOOLEAN && o.type_id_ == TypeId::BOOLEAN, "OR requires boolean");
    return Value(TypeId::BOOLEAN, boolean_ || o.boolean_);
}

auto Value::Not() const -> Value {
    ONEBASE_ASSERT(type_id_ == TypeId::BOOLEAN, "NOT requires boolean");
    return Value(TypeId::BOOLEAN, !boolean_);
}

// --- Serialization ---

void Value::SerializeTo(char *storage) const {
    switch (type_id_) {
        case TypeId::BOOLEAN:
            *reinterpret_cast<bool *>(storage) = boolean_;
            break;
        case TypeId::INTEGER:
            *reinterpret_cast<int32_t *>(storage) = integer_;
            break;
        case TypeId::FLOAT:
            *reinterpret_cast<float *>(storage) = float_;
            break;
        case TypeId::VARCHAR: {
            auto len = static_cast<uint32_t>(varchar_.size());
            memcpy(storage, &len, sizeof(uint32_t));
            memcpy(storage + sizeof(uint32_t), varchar_.data(), len);
            break;
        }
        default:
            UNREACHABLE("Unsupported type for serialization");
    }
}

auto Value::DeserializeFrom(const char *storage, TypeId type) -> Value {
    switch (type) {
        case TypeId::BOOLEAN:
            return Value(type, *reinterpret_cast<const bool *>(storage));
        case TypeId::INTEGER:
            return Value(type, *reinterpret_cast<const int32_t *>(storage));
        case TypeId::FLOAT:
            return Value(type, *reinterpret_cast<const float *>(storage));
        case TypeId::VARCHAR: {
            uint32_t len;
            memcpy(&len, storage, sizeof(uint32_t));
            return Value(type, std::string(storage + sizeof(uint32_t), len));
        }
        default:
            UNREACHABLE("Unsupported type for deserialization");
    }
}

auto Value::GetSerializedSize() const -> uint32_t {
    switch (type_id_) {
        case TypeId::BOOLEAN:
            return sizeof(bool);
        case TypeId::INTEGER:
            return sizeof(int32_t);
        case TypeId::FLOAT:
            return sizeof(float);
        case TypeId::VARCHAR:
            return sizeof(uint32_t) + static_cast<uint32_t>(varchar_.size());
        default:
            UNREACHABLE("Unsupported type for GetSerializedSize");
    }
}

auto Value::ToString() const -> std::string {
    if (is_null_) {
        return "NULL";
    }
    switch (type_id_) {
        case TypeId::BOOLEAN:
            return boolean_ ? "true" : "false";
        case TypeId::INTEGER:
            return std::to_string(integer_);
        case TypeId::FLOAT:
            return fmt::format("{:.2f}", float_);
        case TypeId::VARCHAR:
            return varchar_;
        default:
            return "<INVALID>";
    }
}

}  // namespace onebase

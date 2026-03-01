#pragma once

#include <stdexcept>
#include <string>

namespace onebase {

enum class ExceptionType {
    INVALID,
    OUT_OF_RANGE,
    BUFFER_FULL,
    IO_ERROR,
    EXECUTION,
    TRANSACTION,
    LOCK,
    CATALOG,
    TYPE_MISMATCH,
    NOT_IMPLEMENTED,
};

class OneBaseException : public std::runtime_error {
   public:
    explicit OneBaseException(const std::string &msg, ExceptionType type = ExceptionType::INVALID)
        : std::runtime_error(msg), type_(type) {}
    auto GetType() const -> ExceptionType { return type_; }

   private:
    ExceptionType type_;
};

class NotImplementedException : public OneBaseException {
   public:
    explicit NotImplementedException(const std::string &msg)
        : OneBaseException(msg, ExceptionType::NOT_IMPLEMENTED) {}
};

}  // namespace onebase

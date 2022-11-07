// Copyright 2022 nullptr

#pragma once

#include <cmath>
#include <memory>

#include "builder/base.h"
#include "util/hton.h"

namespace pgeon {

struct IdRecv {
  static inline const char* recv(const char* x) { return x; }
};

struct DateRecv {
  static const int32_t kEpoch = 10957;  // 2000-01-01 - 1970-01-01 (days)
  static inline int32_t recv(const char* x) { return unpack_int32(x) + kEpoch; }
};

struct BoolRecv {
  static inline bool recv(const char* x) { return (*x != 0); }
};

struct CharRecv {
  static inline uint8_t recv(const char* x) { return *x; }
};

struct Int2Recv {
  static inline int16_t recv(const char* x) { return unpack_int16(x); }
};

struct Int4Recv {
  static inline int64_t recv(const char* x) { return unpack_int32(x); }
};

struct Int8Recv {
  static inline int64_t recv(const char* x) { return unpack_int64(x); }
};

struct Float4Recv {
  static inline float recv(const char* x) { return unpack_float(x); }
};

struct Float8Recv {
  static inline double recv(const char* x) { return unpack_double(x); }
};

template <class BuilderT, typename RecvT>
class GenericBuilder : public ArrayBuilder {
 private:
  BuilderT* ptr_;

 public:
  GenericBuilder(const SqlTypeInfo& info, const UserOptions&) {
    arrow_builder_ = std::make_unique<BuilderT>();
    ptr_ = reinterpret_cast<BuilderT*>(arrow_builder_.get());
  }

  size_t Append(const char* buf) {
    int32_t len = unpack_int32(buf);
    buf += 4;

    if (len == -1) {
      if constexpr (std::is_base_of<BuilderT, arrow::FloatBuilder>::value ||
                    std::is_base_of<BuilderT, arrow::DoubleBuilder>::value)
        auto status = ptr_->Append(NAN);
      else
        auto status = ptr_->AppendNull();
      return 4;
    }

    auto value = RecvT::recv(buf);
    if constexpr (std::is_base_of<BuilderT, arrow::BinaryBuilder>::value ||
                  std::is_base_of<BuilderT, arrow::StringBuilder>::value ||
                  std::is_base_of<BuilderT, arrow::StringDictionaryBuilder>::value)
      auto status = ptr_->Append(value, len);
    else
      auto status = ptr_->Append(value);

    return 4 + len;
  }
};

}  // namespace pgeon

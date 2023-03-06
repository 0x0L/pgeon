// Copyright 2022 nullptr

#pragma once

#include <cmath>
#include <memory>

#include "builder/base.h"
#include "util/hton.h"

namespace pgeon {

struct BinaryRecv {
  static const char* recv(StreamBuffer& sb, size_t len) { return sb.ReadBinary(len); }
};

struct DateRecv {
  static const int32_t kEpoch = 10957;  // 2000-01-01 - 1970-01-01 (days)
  static inline int32_t recv(StreamBuffer& sb) { return sb.ReadInt32() + kEpoch; }
};

struct BoolRecv {
  static inline bool recv(StreamBuffer& sb) { return (sb.ReadUInt8() != 0); }
};

struct CharRecv {
  static inline uint8_t recv(StreamBuffer& sb) { return sb.ReadUInt8(); }
};

struct Int2Recv {
  static inline int16_t recv(StreamBuffer& sb) { return sb.ReadInt16(); }
};

struct Int4Recv {
  static inline int32_t recv(StreamBuffer& sb) { return sb.ReadInt32(); }
};

struct Int8Recv {
  static inline int64_t recv(StreamBuffer& sb) { return sb.ReadInt64(); }
};

struct Float4Recv {
  static inline float recv(StreamBuffer& sb) {
    auto x = sb.ReadBinary(4);
    return unpack_float(x);
  }
};

struct Float8Recv {
  static inline double recv(StreamBuffer& sb) {
    auto x = sb.ReadBinary(8);
    return unpack_double(x);
  }
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

  arrow::Status Append(StreamBuffer& sb) {
    int32_t len = sb.ReadInt32();

    if (len == -1) {
      if constexpr (std::is_base_of<BuilderT, arrow::FloatBuilder>::value ||
                    std::is_base_of<BuilderT, arrow::DoubleBuilder>::value)
        return ptr_->Append(NAN);
      else
        return ptr_->AppendNull();
    }

    if constexpr (std::is_base_of<BuilderT, arrow::BinaryBuilder>::value ||
                  std::is_base_of<BuilderT, arrow::StringBuilder>::value ||
                  std::is_base_of<BuilderT, arrow::StringDictionaryBuilder>::value ||
                  std::is_base_of<BuilderT, arrow::StringDictionary32Builder>::value) {
      auto value = RecvT::recv(sb, len);
      return ptr_->Append(value, len);
    } else {
      auto value = RecvT::recv(sb);
      return ptr_->Append(value);
    }
  }
};

}  // namespace pgeon

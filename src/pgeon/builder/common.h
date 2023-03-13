// Copyright 2022 nullptr

#pragma once

#include <memory>

#include "pgeon/builder/base.h"

#define APPEND_AND_RETURN_IF_EMPTY(sb, ptr) \
  do {                                      \
    int32_t len = sb->ReadInt32();          \
    if (len == -1) {                        \
      return ptr->AppendNull();             \
    }                                       \
  } while (0)

namespace pgeon {

struct BinaryRecv {
  static const char* recv(StreamBuffer* sb, size_t len) { return sb->ReadBinary(len); }
};

struct DateRecv {
  static const int32_t kEpoch = 10957;  // 2000-01-01 - 1970-01-01 (days)
  static inline int32_t recv(StreamBuffer* sb) { return sb->ReadInt32() + kEpoch; }
};

struct BoolRecv {
  static inline bool recv(StreamBuffer* sb) { return (sb->ReadUInt8() != 0); }
};

struct UInt8Recv {
  static inline uint8_t recv(StreamBuffer* sb) { return sb->ReadUInt8(); }
};

struct Int16Recv {
  static inline int16_t recv(StreamBuffer* sb) { return sb->ReadInt16(); }
};

struct Int32Recv {
  static inline int32_t recv(StreamBuffer* sb) { return sb->ReadInt32(); }
};

struct Int64Recv {
  static inline int64_t recv(StreamBuffer* sb) { return sb->ReadInt64(); }
};

struct Float32Recv {
  static inline float recv(StreamBuffer* sb) { return sb->ReadFloat32(); }
};

struct Float64Recv {
  static inline double recv(StreamBuffer* sb) { return sb->ReadFloat64(); }
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

  arrow::Status Append(StreamBuffer* sb) {
    int32_t len = sb->ReadInt32();
    if (len == -1) {
      // TODO(xav): as an option ?
      // if constexpr (std::is_base_of<BuilderT, arrow::FloatBuilder>::value ||
      //               std::is_base_of<BuilderT, arrow::DoubleBuilder>::value)
      //   return ptr_->Append(NAN);
      // else
      return ptr_->AppendNull();
    }

    if constexpr (std::is_base_of<BuilderT, arrow::BinaryBuilder>::value ||
                  std::is_base_of<BuilderT, arrow::StringBuilder>::value ||
                  std::is_base_of<BuilderT, arrow::StringDictionaryBuilder>::value ||
                  std::is_base_of<BuilderT, arrow::StringDictionary32Builder>::value) {
      return ptr_->Append(RecvT::recv(sb, len), len);
    } else {
      return ptr_->Append(RecvT::recv(sb));
    }
  }
};

}  // namespace pgeon

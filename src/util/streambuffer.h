// Copyright 2022 nullptr

#pragma once

#include <memory>
#include "util/hton.h"

namespace pgeon {

class StreamBuffer {
 private:
  const char* buffer_;

 public:
  StreamBuffer(const char* buffer) : buffer_(buffer) {}

  inline const char* ReadBinary(size_t n) {
    const char* buf = buffer_;
    buffer_ += n;
    return buf;
  }
  inline uint8_t ReadUInt8() {
    const char* buf = ReadBinary(1);
    return *buf;
  }
  inline int16_t ReadInt16() {
    const char* buf = ReadBinary(2);
    return unpack_int16(buf);
  }
  inline int32_t ReadInt32() {
    const char* buf = ReadBinary(4);
    return unpack_int32(buf);
  }
  inline int64_t ReadInt64() {
    const char* buf = ReadBinary(8);
    return unpack_int64(buf);
  }
};

}  // namespace pgeon

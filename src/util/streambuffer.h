// Copyright 2022 nullptr

#pragma once

#ifdef _WIN32
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include <cstring>
#include <sstream>
#include <string>
#include <utility>

#if defined(__linux__)
#include <endian.h>
#define ntohll(x) be64toh(x)
#elif defined(__APPLE__)
#include <libkern/OSByteOrder.h>
#endif

namespace pgeon {

// Source from https://github.com/apache/arrow-adbc/tree/main/c/driver/postgresql
static inline uint16_t LoadNetworkUInt16(const char* buf) {
  uint16_t v = 0;
  std::memcpy(&v, buf, sizeof(uint16_t));
  return ntohs(v);
}

static inline uint32_t LoadNetworkUInt32(const char* buf) {
  uint32_t v = 0;
  std::memcpy(&v, buf, sizeof(uint32_t));
  return ntohl(v);
}

static inline int64_t LoadNetworkUInt64(const char* buf) {
  uint64_t v = 0;
  std::memcpy(&v, buf, sizeof(uint64_t));
  return ntohll(v);
}

static inline int16_t LoadNetworkInt16(const char* buf) {
  return static_cast<int16_t>(LoadNetworkUInt16(buf));
}

static inline int32_t LoadNetworkInt32(const char* buf) {
  return static_cast<int32_t>(LoadNetworkUInt32(buf));
}

static inline int64_t LoadNetworkInt64(const char* buf) {
  return static_cast<int64_t>(LoadNetworkUInt64(buf));
}

static inline float LoadNetworkFloat32(const char* buf) {
  int32_t raw_value = LoadNetworkUInt32(buf);
  float value = 0.0;
  std::memcpy(&value, &raw_value, sizeof(float));
  return value;
}

static inline double LoadNetworkFloat64(const char* buf) {
  int64_t raw_value = LoadNetworkUInt64(buf);
  double value = 0.0;
  std::memcpy(&value, &raw_value, sizeof(double));
  return value;
}

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
    return LoadNetworkInt16(buf);
  }

  inline int32_t ReadInt32() {
    const char* buf = ReadBinary(4);
    return LoadNetworkInt32(buf);
  }

  inline int64_t ReadInt64() {
    const char* buf = ReadBinary(8);
    return LoadNetworkInt64(buf);
  }

  inline float ReadFloat32() {
    const char* buf = ReadBinary(4);
    return LoadNetworkFloat32(buf);
  }

  inline double ReadFloat64() {
    const char* buf = ReadBinary(8);
    return LoadNetworkFloat64(buf);
  }
};

}  // namespace pgeon

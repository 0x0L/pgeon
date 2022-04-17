// Copyright 2022 nullptr

#include <iostream>
#include <memory>

#include "pgeon/builder/common.h"
#include "pgeon/builder/datetime.h"

namespace pgeon {

TimeBuilder::TimeBuilder(const SqlTypeInfo&, const UserOptions&) {
  arrow_builder_ = std::make_unique<arrow::Time64Builder>(
      arrow::time64(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
  ptr_ = (arrow::Time64Builder*)arrow_builder_.get();
}

size_t TimeBuilder::Append(const char* buf) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  if (len == -1) {
    auto status = ptr_->AppendNull();
    return 4;
  }

  auto value = unpack_int64(buf);
  auto status = ptr_->Append(value);

  return 4 + len;
}

TimeTzBuilder::TimeTzBuilder(const SqlTypeInfo&, const UserOptions&) {
  arrow_builder_ = std::make_unique<arrow::Time64Builder>(
      arrow::time64(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
  ptr_ = (arrow::Time64Builder*)arrow_builder_.get();
}

size_t TimeTzBuilder::Append(const char* buf) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  if (len == -1) {
    auto status = ptr_->AppendNull();
    return 4;
  }

  auto value = unpack_int64(buf);
  auto tz = unpack_int32(buf + 8);

  auto status = ptr_->Append(value + tz * 1000000LL);
  return 4 + len;
}

TimestampBuilder::TimestampBuilder(const SqlTypeInfo&, const UserOptions&) {
  // TODO(xav) timezone
  arrow_builder_ = std::make_unique<arrow::TimestampBuilder>(
      arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
  ptr_ = (arrow::TimestampBuilder*)arrow_builder_.get();
}

size_t TimestampBuilder::Append(const char* buf) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  if (len == -1) {
    auto status = ptr_->AppendNull();
    return 4;
  }

  static const int64_t kEpoch = 946684800000000;  // 2000-01-01 - 1970-01-01 (us)
  auto value = unpack_int64(buf) + kEpoch;
  auto status = ptr_->Append(value);

  return 4 + len;
}

IntervalBuilder::IntervalBuilder(const SqlTypeInfo& info, const UserOptions&) {
  arrow_builder_ = std::make_unique<arrow::DurationBuilder>(
      arrow::duration(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
  ptr_ = (arrow::DurationBuilder*)arrow_builder_.get();
}

size_t IntervalBuilder::Append(const char* buf) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  if (len == -1) {
    auto status = ptr_->AppendNull();
    return 4;
  }

  static const int64_t kMicrosecondsPerDay = 24 * 3600 * 1000000LL;
  int64_t usecs = unpack_int64(buf);
  int32_t days = unpack_int32(buf + 8);
  // int32_t months = unpack_int32(cursor + 12);
  auto status = ptr_->Append(usecs + days * kMicrosecondsPerDay);

  return 4 + len;
}

}  // namespace pgeon

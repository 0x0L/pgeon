// Copyright 2022 nullptr

#include <memory>

#include "builder/common.h"
#include "builder/datetime.h"

namespace pgeon {

TimeBuilder::TimeBuilder(const SqlTypeInfo&, const UserOptions&) {
  auto status =
      arrow::MakeBuilder(arrow::default_memory_pool(),
                         arrow::time64(arrow::TimeUnit::MICRO), &arrow_builder_);
  ptr_ = reinterpret_cast<arrow::Time64Builder*>(arrow_builder_.get());
}

arrow::Status TimeBuilder::Append(StreamBuffer& sb) {
  APPEND_AND_RETURN_IF_EMPTY(sb, ptr_);
  return ptr_->Append(sb.ReadInt64());
}

TimeTzBuilder::TimeTzBuilder(const SqlTypeInfo&, const UserOptions&) {
  auto status =
      arrow::MakeBuilder(arrow::default_memory_pool(),
                         arrow::time64(arrow::TimeUnit::MICRO), &arrow_builder_);
  ptr_ = reinterpret_cast<arrow::Time64Builder*>(arrow_builder_.get());
}

arrow::Status TimeTzBuilder::Append(StreamBuffer& sb) {
  APPEND_AND_RETURN_IF_EMPTY(sb, ptr_);
  auto value = sb.ReadInt64();
  auto tz = sb.ReadInt32();
  return ptr_->Append(value + tz * 1000000LL);
}

TimestampBuilder::TimestampBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto type = arrow::timestamp(arrow::TimeUnit::MICRO);
  if (info.typreceive == "timestamptz_recv")
    type = arrow::timestamp(arrow::TimeUnit::MICRO, "utc");

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = reinterpret_cast<arrow::TimestampBuilder*>(arrow_builder_.get());
}

arrow::Status TimestampBuilder::Append(StreamBuffer& sb) {
  APPEND_AND_RETURN_IF_EMPTY(sb, ptr_);
  static const int64_t kEpoch = 946684800000000LL;  // 2000-01-01 - 1970-01-01 (us)
  return ptr_->Append(sb.ReadInt64() + kEpoch);
}

IntervalBuilder::IntervalBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto status = arrow::MakeBuilder(arrow::default_memory_pool(),
                                   arrow::month_day_nano_interval(), &arrow_builder_);
  ptr_ = reinterpret_cast<arrow::MonthDayNanoIntervalBuilder*>(arrow_builder_.get());
}

arrow::Status IntervalBuilder::Append(StreamBuffer& sb) {
  APPEND_AND_RETURN_IF_EMPTY(sb, ptr_);
  // static const int64_t kMicrosecondsPerDay = 24 * 3600 * 1000000LL;
  int64_t nano = sb.ReadInt64() * 1000LL;
  int32_t days = sb.ReadInt32();
  int32_t months = sb.ReadInt32();
  return ptr_->Append({.months = months, .days = days, .nanoseconds = nano});
}

}  // namespace pgeon

// Copyright 2022 nullptr

#pragma once

#include "pgeon/builder/base.h"

namespace pgeon {

class TimeBuilder : public ArrayBuilder {
 private:
  arrow::Time64Builder* ptr_;

 public:
  TimeBuilder(const SqlTypeInfo&, const UserOptions&);
  size_t Append(const char* buf);
};

class TimeTzBuilder : public ArrayBuilder {
 private:
  arrow::Time64Builder* ptr_;

 public:
  TimeTzBuilder(const SqlTypeInfo&, const UserOptions&);
  size_t Append(const char* buf);
};

class TimestampBuilder : public ArrayBuilder {
 private:
  arrow::TimestampBuilder* ptr_;

 public:
  TimestampBuilder(const SqlTypeInfo&, const UserOptions&);
  size_t Append(const char* buf);
};

class IntervalBuilder : public ArrayBuilder {
 private:
  arrow::DurationBuilder* ptr_;

 public:
  IntervalBuilder(const SqlTypeInfo& info, const UserOptions&);
  size_t Append(const char* buf);
};

}  // namespace pgeon

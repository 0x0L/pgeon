// Copyright 2022 nullptr

#pragma once

#include "builder/base.h"

namespace pgeon {

class TimeBuilder : public ArrayBuilder {
 private:
  arrow::Time64Builder* ptr_;

 public:
  TimeBuilder(const SqlTypeInfo&, const UserOptions&);
  arrow::Status Append(StreamBuffer&);
};

class TimeTzBuilder : public ArrayBuilder {
 private:
  arrow::Time64Builder* ptr_;

 public:
  TimeTzBuilder(const SqlTypeInfo&, const UserOptions&);
  arrow::Status Append(StreamBuffer&);
};

class TimestampBuilder : public ArrayBuilder {
 private:
  arrow::TimestampBuilder* ptr_;

 public:
  TimestampBuilder(const SqlTypeInfo&, const UserOptions&);
  arrow::Status Append(StreamBuffer&);
};

class IntervalBuilder : public ArrayBuilder {
 private:
  arrow::MonthDayNanoIntervalBuilder* ptr_;

 public:
  IntervalBuilder(const SqlTypeInfo&, const UserOptions&);
  arrow::Status Append(StreamBuffer&);
};

}  // namespace pgeon

// Copyright 2022 nullptr

#pragma once

#include "pgeon/builder/base.h"

namespace pgeon {

class NullBuilder : public ArrayBuilder {
 private:
  arrow::NullBuilder* ptr_;

 public:
  NullBuilder(const SqlTypeInfo&, const UserOptions&);
  arrow::Status Append(StreamBuffer*);
};

class TidBuilder : public ArrayBuilder {
 private:
  arrow::StructBuilder* ptr_;
  arrow::Int32Builder* block_builder_;
  arrow::Int16Builder* offset_builder_;

 public:
  TidBuilder(const SqlTypeInfo&, const UserOptions&);
  arrow::Status Append(StreamBuffer*);
};

class PgSnapshotBuilder : public ArrayBuilder {
 private:
  arrow::StructBuilder* ptr_;
  arrow::Int64Builder* xmin_builder_;
  arrow::Int64Builder* xmax_builder_;
  arrow::ListBuilder* xip_builder_;
  arrow::Int64Builder* value_builder_;

 public:
  PgSnapshotBuilder(const SqlTypeInfo&, const UserOptions&);
  arrow::Status Append(StreamBuffer*);
};

}  // namespace pgeon

// Copyright 2022 nullptr

#pragma once

#include "pgeon/builder/base.h"

namespace pgeon {

class TsVectorBuilder : public ArrayBuilder {
 private:
  arrow::MapBuilder* ptr_;
  arrow::StringBuilder* key_builder_;
  arrow::ListBuilder* item_builder_;
  arrow::Int32Builder* value_builder_;

 public:
  TsVectorBuilder(const SqlTypeInfo&, const UserOptions&);
  arrow::Status Append(StreamBuffer*);
};

class TsQueryBuilder : public ArrayBuilder {
 private:
  arrow::ListBuilder* ptr_;
  arrow::StructBuilder* value_builder_;
  arrow::Int8Builder* type_builder_;
  arrow::Int8Builder* weight_builder_;
  arrow::Int8Builder* prefix_builder_;
  arrow::StringBuilder* operand_builder_;
  arrow::Int8Builder* oper_builder_;
  arrow::Int16Builder* distance_builder_;

 public:
  TsQueryBuilder(const SqlTypeInfo&, const UserOptions&);
  arrow::Status Append(StreamBuffer*);
};

}  // namespace pgeon

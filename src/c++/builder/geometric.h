// Copyright 2022 nullptr

#pragma once

#include <memory>
#include <vector>

#include "builder/base.h"

namespace pgeon {

class PointBuilder : public ArrayBuilder {
 private:
  arrow::StructBuilder* ptr_;

 public:
  PointBuilder(const SqlTypeInfo& info, const UserOptions&);
  size_t Append(const char* buf);
};

class LineBuilder : public ArrayBuilder {
 private:
  arrow::StructBuilder* ptr_;

 public:
  LineBuilder(const SqlTypeInfo& info, const UserOptions&);
  size_t Append(const char* buf);
};

class BoxBuilder : public ArrayBuilder {
 private:
  arrow::StructBuilder* ptr_;

 public:
  BoxBuilder(const SqlTypeInfo& info, const UserOptions&);
  size_t Append(const char* buf);
};

class CircleBuilder : public ArrayBuilder {
 private:
  arrow::StructBuilder* ptr_;

 public:
  CircleBuilder(const SqlTypeInfo& info, const UserOptions&);
  size_t Append(const char* buf);
};

class PathBuilder : public ArrayBuilder {
 private:
  arrow::StructBuilder* ptr_;
  arrow::BooleanBuilder* is_closed_builder_;
  arrow::ListBuilder* point_list_builder_;
  arrow::StructBuilder* point_builder_;
  arrow::DoubleBuilder* x_builder_;
  arrow::DoubleBuilder* y_builder_;

 public:
  PathBuilder(const SqlTypeInfo& info, const UserOptions&);
  size_t Append(const char* buf);
};

class PolygonBuilder : public ArrayBuilder {
 private:
  arrow::ListBuilder* ptr_;
  arrow::StructBuilder* point_builder_;
  arrow::DoubleBuilder* x_builder_;
  arrow::DoubleBuilder* y_builder_;

 public:
  PolygonBuilder(const SqlTypeInfo& info, const UserOptions&);
  size_t Append(const char* buf);
};

}  // namespace pgeon

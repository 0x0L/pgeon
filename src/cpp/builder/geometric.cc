// Copyright 2022 nullptr

#include "builder/geometric.h"
#include "builder/common.h"

namespace pgeon {

inline size_t AppendFlatDoubleHelper(const char* buf, arrow::StructBuilder* ptr) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  if (len == -1) {
    auto status = ptr->AppendNull();
    return 4;
  }

  auto status = ptr->Append();
  for (size_t i = 0; i < ptr->num_children(); i++) {
    double value = unpack_double(buf);
    buf += 8;

    auto status = ((arrow::DoubleBuilder*)ptr->child(i))->Append(value);
  }

  return 4 + len;
}

PointBuilder::PointBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto type = arrow::struct_({
      arrow::field("x", arrow::float64()),
      arrow::field("y", arrow::float64()),
  });

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = (arrow::StructBuilder*)arrow_builder_.get();
}

size_t PointBuilder::Append(const char* buf) { return AppendFlatDoubleHelper(buf, ptr_); }

LineBuilder::LineBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto type = arrow::struct_({
      arrow::field("A", arrow::float64()),
      arrow::field("B", arrow::float64()),
      arrow::field("C", arrow::float64()),
  });

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = (arrow::StructBuilder*)arrow_builder_.get();
}

size_t LineBuilder::Append(const char* buf) { return AppendFlatDoubleHelper(buf, ptr_); }

BoxBuilder::BoxBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto type = arrow::struct_({
      arrow::field("x1", arrow::float64()),
      arrow::field("y1", arrow::float64()),
      arrow::field("x2", arrow::float64()),
      arrow::field("y2", arrow::float64()),
  });

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = (arrow::StructBuilder*)arrow_builder_.get();
}

size_t BoxBuilder::Append(const char* buf) { return AppendFlatDoubleHelper(buf, ptr_); }

CircleBuilder::CircleBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto type = arrow::struct_({
      arrow::field("x", arrow::float64()),
      arrow::field("y", arrow::float64()),
      arrow::field("r", arrow::float64()),
  });

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = (arrow::StructBuilder*)arrow_builder_.get();
}

size_t CircleBuilder::Append(const char* buf) {
  return AppendFlatDoubleHelper(buf, ptr_);
}

PathBuilder::PathBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto type =
      arrow::struct_({arrow::field("closed", arrow::boolean()),
                      arrow::field("points", arrow::list(arrow::struct_({
                                                 arrow::field("x", arrow::float64()),
                                                 arrow::field("y", arrow::float64()),
                                             })))});

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = (arrow::StructBuilder*)arrow_builder_.get();

  is_closed_builder_ = (arrow::BooleanBuilder*)ptr_->child(0);
  point_list_builder_ = (arrow::ListBuilder*)ptr_->child(1);
  point_builder_ = (arrow::StructBuilder*)point_list_builder_->value_builder();
  x_builder_ = (arrow::DoubleBuilder*)point_builder_->child(0);
  y_builder_ = (arrow::DoubleBuilder*)point_builder_->child(1);
}

size_t PathBuilder::Append(const char* buf) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  if (len == -1) {
    auto status = ptr_->AppendNull();
    return 4;
  }

  auto status = ptr_->Append();

  status = is_closed_builder_->Append(*buf != 0);
  buf += 1;

  int32_t npts = unpack_int32(buf);
  buf += 4;

  // TODO(xav) could it be null ?
  status = point_list_builder_->Append();

  for (size_t i = 0; i < npts; i++) {
    status = point_builder_->Append();

    double x = unpack_double(buf);
    double y = unpack_double(buf + 8);
    buf += 16;

    status = x_builder_->Append(x);
    status = y_builder_->Append(y);
  }

  return 4 + len;
}

PolygonBuilder::PolygonBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto type = arrow::list(arrow::struct_({
      arrow::field("x", arrow::float64()),
      arrow::field("y", arrow::float64()),
  }));

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = (arrow::ListBuilder*)arrow_builder_.get();

  point_builder_ = (arrow::StructBuilder*)ptr_->value_builder();
  x_builder_ = (arrow::DoubleBuilder*)point_builder_->child(0);
  y_builder_ = (arrow::DoubleBuilder*)point_builder_->child(1);
}

size_t PolygonBuilder::Append(const char* buf) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  if (len == -1) {
    auto status = ptr_->AppendNull();
    return 4;
  }

  int32_t npts = unpack_int32(buf);
  buf += 4;

  // TODO(xav) could it be null ?
  auto status = ptr_->Append();

  for (size_t i = 0; i < npts; i++) {
    status = point_builder_->Append();

    double x = unpack_double(buf);
    double y = unpack_double(buf + 8);
    buf += 16;

    status = x_builder_->Append(x);
    status = y_builder_->Append(y);
  }

  return 4 + len;
}

}  // namespace pgeon

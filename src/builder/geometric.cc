// Copyright 2022 nullptr

#include "builder/geometric.h"
#include "builder/common.h"

namespace pgeon {

inline arrow::Status AppendFlatDoubleHelper(StreamBuffer& sb, arrow::StructBuilder* ptr) {
  APPEND_AND_RETURN_IF_EMPTY(sb, ptr);
  ARROW_RETURN_NOT_OK(ptr->Append());
  for (int i = 0; i < ptr->num_children(); i++) {
    ARROW_RETURN_NOT_OK(reinterpret_cast<arrow::DoubleBuilder*>(ptr->child(i))
                            ->Append(Float64Recv::recv(sb)));
  }
  return arrow::Status::OK();
}

PointBuilder::PointBuilder(const SqlTypeInfo& info, const UserOptions&) {
  static const auto& type = arrow::struct_({
      arrow::field("x", arrow::float64()),
      arrow::field("y", arrow::float64()),
  });

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = reinterpret_cast<arrow::StructBuilder*>(arrow_builder_.get());
}

arrow::Status PointBuilder::Append(StreamBuffer& sb) {
  return AppendFlatDoubleHelper(sb, ptr_);
}

LineBuilder::LineBuilder(const SqlTypeInfo& info, const UserOptions&) {
  static const auto& type = arrow::struct_({
      arrow::field("A", arrow::float64()),
      arrow::field("B", arrow::float64()),
      arrow::field("C", arrow::float64()),
  });

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = reinterpret_cast<arrow::StructBuilder*>(arrow_builder_.get());
}

arrow::Status LineBuilder::Append(StreamBuffer& sb) {
  return AppendFlatDoubleHelper(sb, ptr_);
}

BoxBuilder::BoxBuilder(const SqlTypeInfo& info, const UserOptions&) {
  static const auto& type = arrow::struct_({
      arrow::field("x1", arrow::float64()),
      arrow::field("y1", arrow::float64()),
      arrow::field("x2", arrow::float64()),
      arrow::field("y2", arrow::float64()),
  });

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = reinterpret_cast<arrow::StructBuilder*>(arrow_builder_.get());
}

arrow::Status BoxBuilder::Append(StreamBuffer& sb) {
  return AppendFlatDoubleHelper(sb, ptr_);
}

CircleBuilder::CircleBuilder(const SqlTypeInfo& info, const UserOptions&) {
  static const auto& type = arrow::struct_({
      arrow::field("x", arrow::float64()),
      arrow::field("y", arrow::float64()),
      arrow::field("r", arrow::float64()),
  });

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = reinterpret_cast<arrow::StructBuilder*>(arrow_builder_.get());
}

arrow::Status CircleBuilder::Append(StreamBuffer& sb) {
  return AppendFlatDoubleHelper(sb, ptr_);
}

PathBuilder::PathBuilder(const SqlTypeInfo& info, const UserOptions&) {
  static const auto& type =
      arrow::struct_({arrow::field("closed", arrow::boolean()),
                      arrow::field("points", arrow::list(arrow::struct_({
                                                 arrow::field("x", arrow::float64()),
                                                 arrow::field("y", arrow::float64()),
                                             })))});

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = reinterpret_cast<arrow::StructBuilder*>(arrow_builder_.get());
  is_closed_builder_ = reinterpret_cast<arrow::BooleanBuilder*>(ptr_->child(0));
  point_list_builder_ = reinterpret_cast<arrow::ListBuilder*>(ptr_->child(1));
  point_builder_ =
      reinterpret_cast<arrow::StructBuilder*>(point_list_builder_->value_builder());
  x_builder_ = reinterpret_cast<arrow::DoubleBuilder*>(point_builder_->child(0));
  y_builder_ = reinterpret_cast<arrow::DoubleBuilder*>(point_builder_->child(1));
}

arrow::Status PathBuilder::Append(StreamBuffer& sb) {
  APPEND_AND_RETURN_IF_EMPTY(sb, ptr_);
  ARROW_RETURN_NOT_OK(ptr_->Append());
  ARROW_RETURN_NOT_OK(is_closed_builder_->Append(BoolRecv::recv(sb)));

  int32_t npts = sb.ReadInt32();
  ARROW_RETURN_NOT_OK(point_list_builder_->Append());
  for (int32_t i = 0; i < npts; i++) {
    ARROW_RETURN_NOT_OK(point_builder_->Append());
    ARROW_RETURN_NOT_OK(x_builder_->Append(Float64Recv::recv(sb)));
    ARROW_RETURN_NOT_OK(y_builder_->Append(Float64Recv::recv(sb)));
  }
  return arrow::Status::OK();
}

PolygonBuilder::PolygonBuilder(const SqlTypeInfo& info, const UserOptions&) {
  static const auto& type = arrow::list(arrow::struct_({
      arrow::field("x", arrow::float64()),
      arrow::field("y", arrow::float64()),
  }));

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = reinterpret_cast<arrow::ListBuilder*>(arrow_builder_.get());
  point_builder_ = reinterpret_cast<arrow::StructBuilder*>(ptr_->value_builder());
  x_builder_ = reinterpret_cast<arrow::DoubleBuilder*>(point_builder_->child(0));
  y_builder_ = reinterpret_cast<arrow::DoubleBuilder*>(point_builder_->child(1));
}

arrow::Status PolygonBuilder::Append(StreamBuffer& sb) {
  APPEND_AND_RETURN_IF_EMPTY(sb, ptr_);
  ARROW_RETURN_NOT_OK(ptr_->Append());
  int32_t npts = sb.ReadInt32();
  for (int32_t i = 0; i < npts; i++) {
    ARROW_RETURN_NOT_OK(point_builder_->Append());
    ARROW_RETURN_NOT_OK(x_builder_->Append(Float64Recv::recv(sb)));
    ARROW_RETURN_NOT_OK(y_builder_->Append(Float64Recv::recv(sb)));
  }
  return arrow::Status::OK();
}

}  // namespace pgeon

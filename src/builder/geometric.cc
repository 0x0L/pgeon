// Copyright 2022 nullptr

#include "builder/geometric.h"
#include "builder/common.h"

namespace pgeon {

inline arrow::Status AppendFlatDoubleHelper(StreamBuffer& sb, arrow::StructBuilder* ptr) {
  int32_t len = sb.ReadInt32();
  if (len == -1) {
    return ptr->AppendNull();
  }

  auto status = ptr->Append();
  for (int i = 0; i < ptr->num_children(); i++) {
    double value = Float8Recv::recv(sb);
    status = ((arrow::DoubleBuilder*)ptr->child(i))->Append(value);
  }

  return status;
}

PointBuilder::PointBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto type = arrow::struct_({
      arrow::field("x", arrow::float64()),
      arrow::field("y", arrow::float64()),
  });

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = (arrow::StructBuilder*)arrow_builder_.get();
}

arrow::Status PointBuilder::Append(StreamBuffer& sb) {
  return AppendFlatDoubleHelper(sb, ptr_);
}

LineBuilder::LineBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto type = arrow::struct_({
      arrow::field("A", arrow::float64()),
      arrow::field("B", arrow::float64()),
      arrow::field("C", arrow::float64()),
  });

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = (arrow::StructBuilder*)arrow_builder_.get();
}

arrow::Status LineBuilder::Append(StreamBuffer& sb) {
  return AppendFlatDoubleHelper(sb, ptr_);
}

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

arrow::Status BoxBuilder::Append(StreamBuffer& sb) {
  return AppendFlatDoubleHelper(sb, ptr_);
}

CircleBuilder::CircleBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto type = arrow::struct_({
      arrow::field("x", arrow::float64()),
      arrow::field("y", arrow::float64()),
      arrow::field("r", arrow::float64()),
  });

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = (arrow::StructBuilder*)arrow_builder_.get();
}

arrow::Status CircleBuilder::Append(StreamBuffer& sb) {
  return AppendFlatDoubleHelper(sb, ptr_);
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

arrow::Status PathBuilder::Append(StreamBuffer& sb) {
  int32_t len = sb.ReadInt32();
  if (len == -1) {
    return ptr_->AppendNull();
  }

  auto status = ptr_->Append();

  status = is_closed_builder_->Append(BoolRecv::recv(sb));

  int32_t npts = sb.ReadInt32();

  // TODO(xav) could it be null ?
  status = point_list_builder_->Append();

  for (int32_t i = 0; i < npts; i++) {
    status = point_builder_->Append();

    double x = Float8Recv::recv(sb);
    double y = Float8Recv::recv(sb);

    status = x_builder_->Append(x);
    status = y_builder_->Append(y);
  }

  return status;
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

arrow::Status PolygonBuilder::Append(StreamBuffer& sb) {
  int32_t len = sb.ReadInt32();
  if (len == -1) {
    return ptr_->AppendNull();
  }

  int32_t npts = sb.ReadInt32();

  // TODO(xav) could it be null ?
  auto status = ptr_->Append();

  for (int32_t i = 0; i < npts; i++) {
    status = point_builder_->Append();

    double x = Float8Recv::recv(sb);
    double y = Float8Recv::recv(sb);

    status = x_builder_->Append(x);
    status = y_builder_->Append(y);
  }

  return status;
}

}  // namespace pgeon

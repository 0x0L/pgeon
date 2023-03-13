// Copyright 2022 nullptr

#include "pgeon/table_builder.h"

namespace pgeon {

TableBuilder::TableBuilder(const FieldVector& fields) : fields_(fields) {
  arrow::FieldVector arrow_fields;
  for (auto& f : fields) {
    auto& [name, builder] = f;
    builders_.push_back(builder.get());
    arrow_fields.push_back(arrow::field(name, builder->type()));
  }
  schema_ = arrow::schema(arrow_fields);
}

arrow::Status TableBuilder::Append(StreamBuffer* sb) {
  int16_t nfields = sb->ReadInt16();
  if (nfields == -1) return arrow::Status::OK();

  for (int16_t i = 0; i < nfields; i++) {
    ARROW_RETURN_NOT_OK(builders_[i]->Append(sb));
  }
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Table>> TableBuilder::Flush() {
  std::vector<std::shared_ptr<arrow::Array>> arrays(fields_.size());
  std::shared_ptr<arrow::Array> array;
  for (size_t i = 0; i < fields_.size(); i++) {
    ARROW_ASSIGN_OR_RAISE(array, builders_[i]->Flush());
    arrays[i] = array;
  }
  return arrow::Table::Make(schema_, arrays);
}

}  // namespace pgeon

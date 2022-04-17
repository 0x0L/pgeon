// Copyright 2022 nullptr

#include "pgeon/table_builder.h"
#include "pgeon/util/hton.h"

namespace pgeon {

TableBuilder::TableBuilder(const FieldVector& fields) : fields_(fields) {
  arrow::FieldVector schema;
  for (auto& f : fields) {
    auto& [name, builder] = f;
    builders_.push_back(builder.get());
    schema.push_back(arrow::field(name, builder->type()));
  }
  schema_ = arrow::schema(schema);
  // fields_offsets_ = std::vector<int32_t>(fields.size(), 0);
}

int32_t TableBuilder::Append(const char* cursor) {
  const char* cur = cursor;
  int16_t nfields = unpack_int16(cur);
  cur += 2;

  if (nfields == -1) return 2;

  // const char* cur2 = cur;
  // for (size_t i = 1; i < nfields; i++)
  // {
  //     cur2 += unpack_int32(cur2) + 4;
  //     fields_offsets_[i] = cur2 - cur;
  // }

  // #pragma omp parallel for
  for (int i = 0; i < nfields; i++) {
    cur += builders_[i]->Append(cur);
    // builders_[i]->Append(cur + fields_offsets_[i]);
  }
  return cur - cursor;
}

std::shared_ptr<arrow::Table> TableBuilder::Flush() {
  std::vector<std::shared_ptr<arrow::Array>> arrays(fields_.size());
  for (size_t i = 0; i < fields_.size(); i++) {
    arrays[i] = builders_[i]->Flush();
  }

  auto batch = arrow::Table::Make(schema_, arrays);
  return batch;
}

}  // namespace pgeon

// Copyright 2022 nullptr

#include <memory>

#include "builder/common.h"
#include "builder/text_search.h"

namespace pgeon {

TsVectorBuilder::TsVectorBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto status = arrow::MakeBuilder(arrow::default_memory_pool(),
                                   arrow::map(arrow::utf8(), arrow::list(arrow::int32())),
                                   &arrow_builder_);

  ptr_ = reinterpret_cast<arrow::MapBuilder*>(arrow_builder_.get());
  key_builder_ = (arrow::StringBuilder*)ptr_->key_builder();
  item_builder_ = (arrow::ListBuilder*)ptr_->item_builder();
  value_builder_ = (arrow::Int32Builder*)item_builder_->value_builder();
}

size_t TsVectorBuilder::Append(const char* buf) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  if (len == -1) {
    auto status = ptr_->AppendNull();
    return 4;
  }

  auto status = ptr_->Append();

  int32_t size = unpack_int32(buf);

  buf += 4;

  int16_t npos;
  for (size_t i = 0; i < size; i++) {
    int16_t flen = 0;
    while (*(buf + flen) != '\0') flen++;

    key_builder_->Append(buf, flen);
    buf += flen + 1;

    item_builder_->Append();

    npos = unpack_int16(buf);
    buf += 2;

    for (size_t j = 0; j < npos; j++) {
      int16_t pos = unpack_int16(buf);
      buf += 2;

      value_builder_->Append(pos);
    }
  }

  return 4 + len;
}

#define QI_VAL 1
#define QI_OPR 2
#define QI_VALSTOP 3

#define OP_NOT 1
#define OP_AND 2
#define OP_OR 3
#define OP_PHRASE 4 /* highest code, tsquery_cleanup.c */
#define OP_COUNT

TsQueryBuilder::TsQueryBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto type = arrow::list(arrow::struct_({
      arrow::field("type", arrow::int8()),
      arrow::field("weight", arrow::int8()),
      arrow::field("prefix", arrow::int8()),
      arrow::field("operand", arrow::utf8()),
      arrow::field("oper", arrow::int8()),
      arrow::field("distance", arrow::int16()),
  }));

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);

  ptr_ = reinterpret_cast<arrow::ListBuilder*>(arrow_builder_.get());
  value_builder_ = (arrow::StructBuilder*)ptr_->value_builder();
  type_builder_ = (arrow::Int8Builder*)value_builder_->child(0);
  weight_builder_ = (arrow::Int8Builder*)value_builder_->child(1);
  prefix_builder_ = (arrow::Int8Builder*)value_builder_->child(2);
  operand_builder_ = (arrow::StringBuilder*)value_builder_->child(3);
  oper_builder_ = (arrow::Int8Builder*)value_builder_->child(4);
  distance_builder_ = (arrow::Int16Builder*)value_builder_->child(5);
}

size_t TsQueryBuilder::Append(const char* buf) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  if (len == -1) {
    auto status = ptr_->AppendNull();
    return 4;
  }

  auto status = ptr_->Append();

  int32_t size = unpack_int32(buf);
  buf += 4;

  int16_t npos;
  for (size_t i = 0; i < size; i++) {
    value_builder_->Append();

    int8_t type = *buf;
    buf += 1;

    switch (type) {
      case QI_VAL: {
        int8_t weight = *buf;
        int8_t prefix = *buf + 1;
        buf += 2;

        int16_t flen = 0;
        while (*(buf + flen) != '\0') flen++;

        type_builder_->Append(type);
        weight_builder_->Append(weight);
        prefix_builder_->Append(prefix);
        operand_builder_->Append(buf, flen);  // TODO(xav)
        oper_builder_->AppendNull();
        distance_builder_->AppendNull();
        buf += flen + 1;
      } break;

      case QI_OPR: {
        int8_t oper = *buf;
        buf += 1;

        type_builder_->Append(type);
        weight_builder_->AppendNull();
        prefix_builder_->AppendNull();
        operand_builder_->AppendNull();
        oper_builder_->Append(oper);

        if (oper == OP_PHRASE) {
          int16_t distance = unpack_int16(buf);
          buf += 2;

          distance_builder_->Append(distance);
        } else {
          distance_builder_->AppendNull();
        }
      } break;

      default: {
        type_builder_->Append(type);
        weight_builder_->AppendNull();
        prefix_builder_->AppendNull();
        operand_builder_->AppendNull();
        oper_builder_->AppendNull();
        distance_builder_->AppendNull();
      } break;
    }
  }

  return 4 + len;
}

}  // namespace pgeon

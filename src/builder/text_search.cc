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

arrow::Status TsVectorBuilder::Append(StreamBuffer& sb) {
  int32_t len = sb.ReadInt32();
  if (len == -1) {
    return ptr_->AppendNull();
  }

  auto status = ptr_->Append();

  int32_t size = sb.ReadInt32();

  int16_t npos;
  for (int32_t i = 0; i < size; i++) {
    const char* buf = sb.ReadBinary(1);
    const char* start_buf = buf;
    int16_t flen = 0;
    while (*buf != '\0') {
      flen += 1;
      buf = sb.ReadBinary(1);
    }

    status = key_builder_->Append(start_buf, flen);

    status = item_builder_->Append();

    npos = sb.ReadInt16();
    for (int16_t j = 0; j < npos; j++) {
      int16_t pos = sb.ReadInt16();
      status = value_builder_->Append(pos);
    }
  }

  return status;
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

arrow::Status TsQueryBuilder::Append(StreamBuffer& sb) {
  int32_t len = sb.ReadInt32();
  if (len == -1) {
    return ptr_->AppendNull();
  }

  auto status = ptr_->Append();

  int32_t size = sb.ReadInt32();
  int16_t npos;
  for (int32_t i = 0; i < size; i++) {
    status = value_builder_->Append();

    int8_t type = sb.ReadUInt8();

    switch (type) {
      case QI_VAL: {
        int8_t weight = sb.ReadUInt8();
        int8_t prefix = sb.ReadUInt8();

        const char* buf = sb.ReadBinary(1);
        const char* start_buf = buf;
        int16_t flen = 0;
        while (*buf != '\0') {
          flen += 1;
          buf = sb.ReadBinary(1);
        }

        status = type_builder_->Append(type);
        status = weight_builder_->Append(weight);
        status = prefix_builder_->Append(prefix);
        status = operand_builder_->Append(start_buf, flen);  // TODO(xav)
        status = oper_builder_->AppendNull();
        status = distance_builder_->AppendNull();
        buf += flen + 1;
      } break;

      case QI_OPR: {
        int8_t oper = sb.ReadUInt8();

        status = type_builder_->Append(type);
        status = weight_builder_->AppendNull();
        status = prefix_builder_->AppendNull();
        status = operand_builder_->AppendNull();
        status = oper_builder_->Append(oper);

        if (oper == OP_PHRASE) {
          int16_t distance = sb.ReadInt16();
          status = distance_builder_->Append(distance);
        } else {
          status = distance_builder_->AppendNull();
        }
      } break;

      default: {
        status = type_builder_->Append(type);
        status = weight_builder_->AppendNull();
        status = prefix_builder_->AppendNull();
        status = operand_builder_->AppendNull();
        status = oper_builder_->AppendNull();
        status = distance_builder_->AppendNull();
      } break;
    }
  }

  return status;
}

}  // namespace pgeon

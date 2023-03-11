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
  key_builder_ = reinterpret_cast<arrow::StringBuilder*>(ptr_->key_builder());
  item_builder_ = reinterpret_cast<arrow::ListBuilder*>(ptr_->item_builder());
  value_builder_ = reinterpret_cast<arrow::Int32Builder*>(item_builder_->value_builder());
}

arrow::Status TsVectorBuilder::Append(StreamBuffer& sb) {
  APPEND_AND_RETURN_IF_EMPTY(sb, ptr_);
  ARROW_RETURN_NOT_OK(ptr_->Append());

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

    ARROW_RETURN_NOT_OK(key_builder_->Append(start_buf, flen));

    ARROW_RETURN_NOT_OK(item_builder_->Append());
    npos = sb.ReadInt16();
    for (int16_t j = 0; j < npos; j++) {
      ARROW_RETURN_NOT_OK(value_builder_->Append(sb.ReadInt16()));
    }
  }
  return arrow::Status::OK();
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
  value_builder_ = reinterpret_cast<arrow::StructBuilder*>(ptr_->value_builder());
  type_builder_ = reinterpret_cast<arrow::Int8Builder*>(value_builder_->child(0));
  weight_builder_ = reinterpret_cast<arrow::Int8Builder*>(value_builder_->child(1));
  prefix_builder_ = reinterpret_cast<arrow::Int8Builder*>(value_builder_->child(2));
  operand_builder_ = reinterpret_cast<arrow::StringBuilder*>(value_builder_->child(3));
  oper_builder_ = reinterpret_cast<arrow::Int8Builder*>(value_builder_->child(4));
  distance_builder_ = reinterpret_cast<arrow::Int16Builder*>(value_builder_->child(5));
}

arrow::Status TsQueryBuilder::Append(StreamBuffer& sb) {
  APPEND_AND_RETURN_IF_EMPTY(sb, ptr_);
  ARROW_RETURN_NOT_OK(ptr_->Append());

  int32_t size = sb.ReadInt32();
  int16_t npos;
  for (int32_t i = 0; i < size; i++) {
    ARROW_RETURN_NOT_OK(value_builder_->Append());
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

        ARROW_RETURN_NOT_OK(type_builder_->Append(type));
        ARROW_RETURN_NOT_OK(weight_builder_->Append(weight));
        ARROW_RETURN_NOT_OK(prefix_builder_->Append(prefix));
        ARROW_RETURN_NOT_OK(operand_builder_->Append(start_buf, flen));  // TODO(xav)
        ARROW_RETURN_NOT_OK(oper_builder_->AppendNull());
        ARROW_RETURN_NOT_OK(distance_builder_->AppendNull());

        buf += flen + 1;
      } break;

      case QI_OPR: {
        int8_t oper = sb.ReadUInt8();

        ARROW_RETURN_NOT_OK(type_builder_->Append(type));
        ARROW_RETURN_NOT_OK(weight_builder_->AppendNull());
        ARROW_RETURN_NOT_OK(prefix_builder_->AppendNull());
        ARROW_RETURN_NOT_OK(operand_builder_->AppendNull());
        ARROW_RETURN_NOT_OK(oper_builder_->Append(oper));

        if (oper == OP_PHRASE) {
          int16_t distance = sb.ReadInt16();
          ARROW_RETURN_NOT_OK(distance_builder_->Append(distance));
        } else {
          ARROW_RETURN_NOT_OK(distance_builder_->AppendNull());
        }
      } break;

      default: {
        ARROW_RETURN_NOT_OK(type_builder_->Append(type));
        ARROW_RETURN_NOT_OK(weight_builder_->AppendNull());
        ARROW_RETURN_NOT_OK(prefix_builder_->AppendNull());
        ARROW_RETURN_NOT_OK(operand_builder_->AppendNull());
        ARROW_RETURN_NOT_OK(oper_builder_->AppendNull());
        ARROW_RETURN_NOT_OK(distance_builder_->AppendNull());

      } break;
    }
  }
  return arrow::Status::OK();
}

}  // namespace pgeon

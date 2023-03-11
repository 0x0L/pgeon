// Copyright 2022 nullptr

#include <memory>

#include "builder/common.h"
#include "builder/numeric.h"

namespace pgeon {

#define NUMERIC_SIGN_MASK 0xC000
#define NUMERIC_POS 0x0000
#define NUMERIC_NEG 0x4000
#define NUMERIC_NAN 0xC000

#define NBASE 10000
#define HALF_NBASE 5000
#define DEC_DIGITS 4       /* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS 2 /* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS 4

struct _NumericHelper {
  int16_t ndigits;
  int16_t weight; /* weight of first digit */
  int16_t sign;   /* NUMERIC_(POS|NEG|NAN) */
  int16_t dscale; /* display scale */
  int16_t digits[];
};

#define VARHDRSZ ((int32_t)sizeof(int32_t))

NumericBuilder::NumericBuilder(const SqlTypeInfo& info, const UserOptions& options) {
  precision_ = ((info.typmod - VARHDRSZ) >> 16) & 0xffff;
  scale_ = (((info.typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024;

  // undefined precision decimals
  if (precision_ == 0xffff) {
    precision_ = options.default_numeric_precision;
    scale_ = options.default_numeric_scale;
  }

  arrow_builder_ =
      std::make_unique<arrow::Decimal128Builder>(arrow::decimal128(precision_, scale_));
  ptr_ = reinterpret_cast<arrow::Decimal128Builder*>(arrow_builder_.get());
}

arrow::Status NumericBuilder::Append(StreamBuffer& sb) {
  int32_t len = sb.ReadInt32();
  if (len == -1) return ptr_->AppendNull();

  const char* buf = sb.ReadBinary(len);
  auto rawdata = reinterpret_cast<const _NumericHelper*>(buf);

  int16_t ndigits = ntoh16(rawdata->ndigits);
  int16_t weight = ntoh16(rawdata->weight);
  int16_t sign = ntoh16(rawdata->sign);
  int16_t scale = scale_;  // ntoh16(rawdata->dscale);

  arrow::Decimal128 value = 0;
  int16_t d, dig;

  if ((sign & NUMERIC_SIGN_MASK) == NUMERIC_NAN) {
    return ptr_->AppendNull();
  }

  /* makes integer portion first */
  for (d = 0; d <= weight; d++) {
    dig = (d < ndigits) ? ntoh16(rawdata->digits[d]) : 0;
    if (dig < 0 || dig >= NBASE)
      return arrow::Status::IOError("[numeric] digit is out of range");
    value = NBASE * value + dig;
  }

  /* makes floating point portion if any */
  while (scale > 0) {
    dig = (d >= 0 && d < ndigits) ? ntoh16(rawdata->digits[d]) : 0;
    if (dig < 0 || dig >= NBASE)
      return arrow::Status::IOError("[numeric] digit is out of range");

    if (scale >= DEC_DIGITS)
      value = NBASE * value + dig;
    else if (scale == 3)
      value = 1000L * value + dig / 10L;
    else if (scale == 2)
      value = 100L * value + dig / 100L;
    else if (scale == 1)
      value = 10L * value + dig / 1000L;
    else
      return arrow::Status::IOError("[numeric] Unexpected error while parsing");
    scale -= DEC_DIGITS;
    d++;
  }
  /* is it a negative value? */
  if ((sign & NUMERIC_NEG) != 0) value = -value;

  return ptr_->Append(value);
}

MonetaryBuilder::MonetaryBuilder(const SqlTypeInfo& info, const UserOptions& options) {
  precision_ = options.default_numeric_precision;
  scale_ = options.monetary_fractional_precision;

  arrow_builder_ =
      std::make_unique<arrow::Decimal128Builder>(arrow::decimal128(precision_, scale_));
  ptr_ = reinterpret_cast<arrow::Decimal128Builder*>(arrow_builder_.get());
}

arrow::Status MonetaryBuilder::Append(StreamBuffer& sb) {
  int32_t len = sb.ReadInt32();
  if (len == -1) {
    return ptr_->AppendNull();
  }

  arrow::Decimal128 value = sb.ReadInt64();
  return ptr_->Append(value);
}

}  // namespace pgeon

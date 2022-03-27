#include "decoders.h"

namespace pgeon
{

class ArrayBuilder : public ColumnBuilder
{
  private:
    std::shared_ptr<ColumnBuilder> value_builder_;
    arrow::ListBuilder *ptr_;

  public:
    ArrayBuilder(std::shared_ptr<ColumnBuilder> value_builder)
        : value_builder_(value_builder)
    {
        Builder = std::make_unique<arrow::ListBuilder>(
            arrow::default_memory_pool(), std::move(value_builder->Builder));
        ptr_ = (arrow::ListBuilder *)Builder.get();
    }

    size_t Append(const char *buf)
    {
        int32_t len = unpack_int32(buf);
        buf += 4;

        if (len == -1)
        {
            auto status = ptr_->AppendNull();
            return 4;
        }

        int32_t ndim = unpack_int32(buf);
        buf += 4;
        // int32_t hasnull = unpack_int32(buf);
        buf += 4;
        // int32_t element_type = unpack_int32(buf);
        buf += 4;

        // Element will be flattened
        int32_t nitems = 1;
        for (size_t i = 0; i < ndim; i++)
        {
            int32_t dim = unpack_int32(buf);
            buf += 4;
            // int32_t lb = unpack_int32(buf);
            buf += 4;
            nitems *= dim;
        }

        auto status = ptr_->Append();
        for (size_t i = 0; i < nitems; i++)
        {
            buf += value_builder_->Append(buf);
        }

        return 4 + len;
    }
};

std::shared_ptr<ColumnBuilder>
createArrayBuilder(std::shared_ptr<ColumnBuilder> value_builder)
{
    return std::make_shared<ArrayBuilder>(value_builder);
}

class RecordBuilder : public ColumnBuilder
{
  private:
    std::vector<std::shared_ptr<ColumnBuilder>> builders_;
    arrow::StructBuilder *ptr_;
    size_t ncolumns_;

  public:
    RecordBuilder(FieldVector fields) : ncolumns_(fields.size())
    {
        std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders(ncolumns_);
        arrow::FieldVector fv(ncolumns_);
        for (size_t i = 0; i < ncolumns_; i++)
        {
            builders_.push_back(fields[i].second);
            builders[i] = std::move(fields[i].second->Builder);
            fv[i] = arrow::field(fields[i].first, builders[i]->type());
        }

        Builder = std::make_unique<arrow::StructBuilder>(
            arrow::struct_(fv), arrow::default_memory_pool(), builders);

        ptr_ = (arrow::StructBuilder *)Builder.get();
    }

    size_t Append(const char *buf)
    {
        int32_t len = unpack_int32(buf);
        buf += 4;

        if (len == -1)
        {
            auto status = ptr_->AppendNull();
            return 4;
        }

        auto status = ptr_->Append();

        int32_t validcols = unpack_int32(buf);
        buf += 4;

        assert(validcols == ncolumns_);

        for (size_t i = 0; i < ncolumns_; i++)
        {
            // int32_t column_type = unpack_int32(buf);
            buf += 4;

            buf += builders_[i]->Append(buf);
        }

        return 4 + len;
    }
};

std::shared_ptr<ColumnBuilder> createRecordBuilder(FieldVector fields)
{
    return std::make_shared<RecordBuilder>(fields);
}

class TimeBuilder : public ColumnBuilder
{
  private:
    arrow::Time64Builder *ptr_;

  public:
    TimeBuilder()
    {
        Builder = std::make_unique<arrow::Time64Builder>(
            arrow::time64(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
        ptr_ = (arrow::Time64Builder *)Builder.get();
    }

    size_t Append(const char *buf)
    {
        int32_t len = unpack_int32(buf);
        buf += 4;

        if (len == -1)
        {
            auto status = ptr_->AppendNull();
            return 4;
        }

        auto value = unpack_int64(buf);
        auto status = ptr_->Append(value);

        return 4 + len;
    }
};

class TimestampBuilder : public ColumnBuilder
{
  private:
    arrow::TimestampBuilder *ptr_;

  public:
    TimestampBuilder()
    {
        Builder = std::make_unique<arrow::TimestampBuilder>(
            arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
        ptr_ = (arrow::TimestampBuilder *)Builder.get();
    }

    TimestampBuilder(const std::string &timezone)
    {
        Builder = std::make_unique<arrow::TimestampBuilder>(
            arrow::timestamp(arrow::TimeUnit::MICRO, timezone),
            arrow::default_memory_pool());
        ptr_ = (arrow::TimestampBuilder *)Builder.get();
    }

    size_t Append(const char *buf)
    {
        int32_t len = unpack_int32(buf);
        buf += 4;

        if (len == -1)
        {
            auto status = ptr_->AppendNull();
            return 4;
        }

        static const int64_t kEpoch = 946684800000000; // 2000-01-01 - 1970-01-01 (us)
        auto value = unpack_int64(buf) + kEpoch;
        auto status = ptr_->Append(value);

        return 4 + len;
    }
};

#define NUMERIC_SIGN_MASK 0xC000
#define NUMERIC_POS 0x0000
#define NUMERIC_NEG 0x4000
#define NUMERIC_NAN 0xC000

#define NBASE 10000
#define HALF_NBASE 5000
#define DEC_DIGITS 4       /* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS 2 /* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS 4

struct _NumericHelper
{
    int16_t ndigits;
    int16_t weight; /* weight of first digit */
    int16_t sign;   /* NUMERIC_(POS|NEG|NAN) */
    int16_t dscale; /* display scale */
    int16_t digits[];
};

class NumericBuilder : public ColumnBuilder
{
  private:
    arrow::Decimal128Builder *ptr_;

  public:
    NumericBuilder(int precision, int scale)
    {
        Builder = std::make_unique<arrow::Decimal128Builder>(
            arrow::decimal128(precision, scale));
        ptr_ = (arrow::Decimal128Builder *)Builder.get();
    }

    size_t Append(const char *buf)
    {
        int32_t len = unpack_int32(buf);
        buf += 4;

        if (len == -1)
        {
            auto status = ptr_->AppendNull();
            return 4;
        }

        _NumericHelper *rawdata = (_NumericHelper *)buf;

        int ndigits = ntoh16(rawdata->ndigits);
        int weight = ntoh16(rawdata->weight);
        int sign = ntoh16(rawdata->sign);
        int scale = ntoh16(rawdata->dscale);
        __int128_t value = 0;
        int d, dig;

        if ((sign & NUMERIC_SIGN_MASK) == NUMERIC_NAN)
        {
            auto status = ptr_->AppendNull();
            return 4 + len;
        }

        /* makes integer portion first */
        for (d = 0; d <= weight; d++)
        {
            dig = (d < ndigits) ? ntoh16(rawdata->digits[d]) : 0;
            if (dig < 0 || dig >= NBASE)
                printf("Numeric digit is out of range: %d", (int)dig);
            value = NBASE * value + (__int128_t)dig;
        }

        /* makes floating point portion if any */
        while (scale > 0)
        {
            dig = (d >= 0 && d < ndigits) ? ntoh16(rawdata->digits[d]) : 0;
            if (dig < 0 || dig >= NBASE)
                printf("Numeric digit is out of range: %d", (int)dig);

            if (scale >= DEC_DIGITS)
                value = NBASE * value + dig;
            else if (scale == 3)
                value = 1000L * value + dig / 10L;
            else if (scale == 2)
                value = 100L * value + dig / 100L;
            else if (scale == 1)
                value = 10L * value + dig / 1000L;
            else
                printf("internal bug");
            scale -= DEC_DIGITS;
            d++;
        }
        /* is it a negative value? */
        if ((sign & NUMERIC_NEG) != 0)
            value = -value;

        auto status = ptr_->Append((uint8_t *)&value);

        return 4 + len;
    }
};

std::shared_ptr<ColumnBuilder> createNumericBuilder(int precision, int scale)
{
    return std::make_shared<NumericBuilder>(precision, scale);
}

class IntervalBuilder : public ColumnBuilder
{
  private:
    arrow::DurationBuilder *ptr_;

  public:
    IntervalBuilder()
    {
        Builder = std::make_unique<arrow::DurationBuilder>(
            arrow::duration(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
        ptr_ = (arrow::DurationBuilder *)Builder.get();
    }

    size_t Append(const char *buf)
    {
        int32_t len = unpack_int32(buf);
        buf += 4;

        if (len == -1)
        {
            auto status = ptr_->AppendNull();
            return 4;
        }

        static const int64_t kMicrosecondsPerDay = 24 * 3600 * 1000000LL;
        int8_t msecs = unpack_int64(buf);
        int32_t days = unpack_int32(buf + 8);
        // int32_t months = unpack_int32(cursor + 12);
        auto status = ptr_->Append(msecs + days * kMicrosecondsPerDay);

        return 4 + len;
    }
};

class BoxBuilder : public ColumnBuilder
{
  private:
    arrow::StructBuilder *ptr_;
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders_;

  public:
    BoxBuilder()
    {
        auto type = arrow::struct_({
            arrow::field("high.x", arrow::float64()),
            arrow::field("high.y", arrow::float64()),
            arrow::field("low.x", arrow::float64()),
            arrow::field("low.y", arrow::float64()),
        });

        // std::unique_ptr<arrow::ArrayBuilder> out;
        // auto ss = arrow::MakeBuilder(
        //     arrow::default_memory_pool(), type->field(i)->type(), &out);

        field_builders_ = {
            std::make_shared<arrow::DoubleBuilder>(),
            std::make_shared<arrow::DoubleBuilder>(),
            std::make_shared<arrow::DoubleBuilder>(),
            std::make_shared<arrow::DoubleBuilder>(),
        };

        Builder = std::make_unique<arrow::StructBuilder>(
            type, arrow::default_memory_pool(), field_builders_);
        ptr_ = (arrow::StructBuilder *)Builder.get();
    }

    size_t Append(const char *buf)
    {
        int32_t len = unpack_int32(buf);
        buf += 4;

        if (len == -1)
        {
            auto status = ptr_->AppendNull();
            return 4;
        }

        auto status = ptr_->Append();
        for (size_t i = 0; i < field_builders_.size(); i++)
        {
            double value = unpack_double(buf);
            buf += 8;

            auto status =
                ((arrow::DoubleBuilder *)field_builders_[i].get())->Append(value);
        }

        return 4 + len;
    }
};

class InetBuilder : public ColumnBuilder
{
  private:
    arrow::StructBuilder *ptr_;
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders_;

  public:
    InetBuilder()
    {
        auto type = arrow::struct_({
            arrow::field("family", arrow::uint8()),
            arrow::field("bits", arrow::uint8()),
            arrow::field("is_cidr", arrow::boolean()),
            arrow::field("ipaddr", arrow::binary()),
        });

        field_builders_ = {
            std::make_shared<arrow::UInt8Builder>(),
            std::make_shared<arrow::UInt8Builder>(),
            std::make_shared<arrow::BooleanBuilder>(),
            std::make_shared<arrow::BinaryBuilder>(),
        };

        Builder = std::make_unique<arrow::StructBuilder>(
            type, arrow::default_memory_pool(), field_builders_);
        ptr_ = (arrow::StructBuilder *)Builder.get();
    }

    size_t Append(const char *buf)
    {
        int32_t len = unpack_int32(buf);
        buf += 4;

        if (len == -1)
        {
            auto status = ptr_->AppendNull();
            return 4;
        }

        auto status = ptr_->Append();

        status = ((arrow::UInt8Builder *)field_builders_[0].get())->Append(*buf);
        buf += 1;

        status = ((arrow::UInt8Builder *)field_builders_[1].get())->Append(*buf);
        buf += 1;

        status = ((arrow::BooleanBuilder *)field_builders_[2].get())->Append(*buf != 0);
        buf += 1;

        uint8_t nb = *buf;
        buf += 1;

        status = ((arrow::BinaryBuilder *)field_builders_[3].get())->Append(buf, nb);

        return 4 + len;
    }
};

struct IdRecv
{
    static inline const char *recv(const char *x) { return x; }
};

struct DateRecv
{
    static const int32_t kEpoch = 10957; // 2000-01-01 - 1970-01-01 (days)
    static inline int32_t recv(const char *x) { return unpack_int32(x) + kEpoch; }
};

struct BoolRecv
{
    static inline bool recv(const char *x) { return (*x != 0); }
};

struct CharRecv
{
    static inline uint8_t recv(const char *x) { return *x; }
};

struct Int2Recv
{
    static inline int16_t recv(const char *x) { return unpack_int16(x); }
};

struct Int4Recv
{
    static inline int64_t recv(const char *x) { return unpack_int32(x); }
};

struct Int8Recv
{
    static inline int64_t recv(const char *x) { return unpack_int64(x); }
};

struct Float4Recv
{
    static inline float recv(const char *x) { return unpack_float(x); }
};

struct Float8Recv
{
    static inline double recv(const char *x) { return unpack_double(x); }
};

template <class BuilderT, typename RecvT> class GenericBuilder : public ColumnBuilder
{
  private:
    BuilderT *ptr_;

  public:
    GenericBuilder()
    {
        Builder = std::make_unique<BuilderT>();
        ptr_ = (BuilderT *)Builder.get();
    }

    size_t Append(const char *buf)
    {
        int32_t len = unpack_int32(buf);
        buf += 4;

        if (len == -1)
        {
            if constexpr (
                std::is_base_of<BuilderT, arrow::FloatBuilder>::value ||
                std::is_base_of<BuilderT, arrow::DoubleBuilder>::value)
                auto status = ptr_->Append(NAN);
            else
                auto status = ptr_->AppendNull();
            return 4;
        }

        auto value = RecvT::recv(buf);
        if constexpr (
            std::is_base_of<BuilderT, arrow::BinaryBuilder>::value ||
            std::is_base_of<BuilderT, arrow::StringBuilder>::value ||
            std::is_base_of<BuilderT, arrow::StringDictionaryBuilder>::value)
            auto status = ptr_->Append(value, len);
        else
            auto status = ptr_->Append(value);

        return 4 + len;
    }
};

template <class T> std::shared_ptr<ColumnBuilder> create()
{
    return std::make_shared<T>();
}

template <class T> std::shared_ptr<ColumnBuilder> createUTC()
{
    return std::make_shared<T>("utc");
}

std::map<std::string, std::shared_ptr<ColumnBuilder> (*)()> gDecoderFactory = {
    {"bit_recv", &create<GenericBuilder<arrow::BinaryBuilder, IdRecv>>},
    {"boolrecv", &create<GenericBuilder<arrow::BooleanBuilder, BoolRecv>>},
    {"box_recv", &create<BoxBuilder>},
    {"bpcharrecv", &create<GenericBuilder<arrow::StringBuilder, IdRecv>>},
    {"brin_bloom_summary_recv", &create<GenericBuilder<arrow::BinaryBuilder, IdRecv>>},
    {"brin_minmax_multi_summary_recv",
     &create<GenericBuilder<arrow::BinaryBuilder, IdRecv>>},
    {"bytearecv", &create<GenericBuilder<arrow::BinaryBuilder, IdRecv>>},
    {"cash_recv", &create<GenericBuilder<arrow::Int64Builder, Int8Recv>>},
    {"charrecv", &create<GenericBuilder<arrow::UInt8Builder, CharRecv>>},
    {"cidr_recv", &create<InetBuilder>},
    {"cidrecv", &create<GenericBuilder<arrow::Int32Builder, Int4Recv>>},
    // {"circle_recv", &create<Builder>}, center.x center.y radius f8
    {"cstring_recv", &create<GenericBuilder<arrow::BinaryBuilder, IdRecv>>},
    {"date_recv", &create<GenericBuilder<arrow::Date32Builder, DateRecv>>},
    // {"domain_recv", &create<Builder>},
    {"enum_recv", &create<GenericBuilder<arrow::StringDictionary32Builder, IdRecv>>},
    {"float4recv", &create<GenericBuilder<arrow::FloatBuilder, Float4Recv>>},
    {"float8recv", &create<GenericBuilder<arrow::DoubleBuilder, Float8Recv>>},
    // {"hstore_recv", &create<Builder>},
    {"inet_recv", &create<InetBuilder>},
    {"int2recv", &create<GenericBuilder<arrow::Int16Builder, Int2Recv>>},
    // {"int2vectorrecv", &create<Builder>},
    {"int4recv", &create<GenericBuilder<arrow::Int32Builder, Int4Recv>>},
    {"int8recv", &create<GenericBuilder<arrow::Int64Builder, Int8Recv>>},
    {"interval_recv", &create<IntervalBuilder>},
    {"json_recv", &create<GenericBuilder<arrow::StringBuilder, IdRecv>>},
    // {"jsonb_recv", &create<Builder>},
    // {"jsonpath_recv", &create<Builder>},
    // {"line_recv", &create<Builder>},
    // {"lseg_recv", &create<Builder>},
    {"macaddr_recv", &create<GenericBuilder<arrow::BinaryBuilder, IdRecv>>},
    {"macaddr8_recv", &create<GenericBuilder<arrow::BinaryBuilder, IdRecv>>},
    // {"multirange_recv", &create<Builder>},
    {"namerecv", &create<GenericBuilder<arrow::StringBuilder, IdRecv>>},
    // {"numeric_recv", &create<Builder>},
    {"oidrecv", &create<GenericBuilder<arrow::Int32Builder, Int4Recv>>},
    // {"oidvectorrecv", &create<Builder>},
    // {"path_recv", &create<Builder>},
    {"pg_dependencies_recv", &create<GenericBuilder<arrow::BinaryBuilder, IdRecv>>},
    {"pg_lsn_recv", &create<GenericBuilder<arrow::Int64Builder, Int8Recv>>},
    {"pg_mcv_list_recv", &create<GenericBuilder<arrow::BinaryBuilder, IdRecv>>},
    {"pg_ndistinct_recv", &create<GenericBuilder<arrow::BinaryBuilder, IdRecv>>},
    // {"pg_snapshot_recv", &create<Builder>},
    // {"point_recv", &create<Builder>},
    // {"poly_recv", &create<Builder>},
    // {"range_recv", &create<Builder>},
    {"regclassrecv", &create<GenericBuilder<arrow::Int32Builder, Int4Recv>>},
    {"regcollationrecv", &create<GenericBuilder<arrow::Int32Builder, Int4Recv>>},
    {"regconfigrecv", &create<GenericBuilder<arrow::Int32Builder, Int4Recv>>},
    {"regdictionaryrecv", &create<GenericBuilder<arrow::Int32Builder, Int4Recv>>},
    {"regnamespacerecv", &create<GenericBuilder<arrow::Int32Builder, Int4Recv>>},
    {"regoperatorrecv", &create<GenericBuilder<arrow::Int32Builder, Int4Recv>>},
    {"regoperrecv", &create<GenericBuilder<arrow::Int32Builder, Int4Recv>>},
    {"regprocedurerecv", &create<GenericBuilder<arrow::Int32Builder, Int4Recv>>},
    {"regprocrecv", &create<GenericBuilder<arrow::Int32Builder, Int4Recv>>},
    {"regrolerecv", &create<GenericBuilder<arrow::Int32Builder, Int4Recv>>},
    {"regtyperecv", &create<GenericBuilder<arrow::Int32Builder, Int4Recv>>},
    {"textrecv", &create<GenericBuilder<arrow::StringBuilder, IdRecv>>},
    // {"tidrecv", &create<Builder>},
    {"time_recv", &create<TimeBuilder>},
    // {"timetz_recv", &createUTC<TimeBuilder>},  // nope
    {"timestamp_recv", &create<TimestampBuilder>},
    {"timestamptz_recv", &createUTC<TimestampBuilder>},
    // {"tsqueryrecv", &create<Builder>},
    // {"tsvectorrecv", &create<Builder>},
    {"unknownrecv", &create<GenericBuilder<arrow::StringBuilder, IdRecv>>},
    {"uuid_recv", &create<GenericBuilder<arrow::BinaryBuilder, IdRecv>>},
    {"varbit_recv", &create<GenericBuilder<arrow::BinaryBuilder, IdRecv>>},
    {"varcharrecv", &create<GenericBuilder<arrow::StringBuilder, IdRecv>>},
    // {"void_recv", &create<GenericBuilder<arrow::NullBuilder, IdRecv>>},
    {"xid8recv", &create<GenericBuilder<arrow::Int64Builder, Int8Recv>>},
    {"xidrecv", &create<GenericBuilder<arrow::Int32Builder, Int4Recv>>},
    {"xml_recv", &create<GenericBuilder<arrow::StringBuilder, IdRecv>>},
};

} // namespace pgeon

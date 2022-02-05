#include "hton.h"

#include <arrow/api.h>

class FieldReceiver
{
  public:
    std::shared_ptr<arrow::ArrayBuilder> Builder;
    virtual int Parse(const char *buffer) = 0;
};

class ArrayReceiver : public FieldReceiver
{
  private:
    std::shared_ptr<FieldReceiver> value_receiver_;
    arrow::ListBuilder *ptr_;

  public:
    ArrayReceiver(std::shared_ptr<FieldReceiver> value_receiver)
        : value_receiver_(value_receiver)
    {
        Builder = std::make_shared<arrow::ListBuilder>(
            arrow::default_memory_pool(), value_receiver_->Builder);
        ptr_ = (arrow::ListBuilder *)Builder.get();
    }

    int Parse(const char *buffer)
    {
        int32_t flen = unpack_int32(buffer);
        buffer += 4;

        if (flen == -1)
        {
            auto status = ptr_->AppendNull();
            return 4;
        }

        int32_t ndims = unpack_int32(buffer);
        buffer += 4;
        // int32_t hasnulls = unpack_int32(buffer);
        buffer += 4;
        // int32_t elem_oid = unpack_int32(buffer);
        buffer += 4;

        // Element will be flattened
        int32_t total_elem = 1;
        for (size_t i = 0; i < ndims; i++)
        {
            int32_t dim_sz = unpack_int32(buffer);
            buffer += 4;
            // int32_t dim_lb = unpack_int32(buffer);
            buffer += 4;
            total_elem *= dim_sz;
        }

        auto status = ptr_->Append();
        for (size_t i = 0; i < total_elem; i++)
        {
            buffer += value_receiver_->Parse(buffer);
        }

        return 4 + flen;
    }
};

class StructReceiver : public FieldReceiver
{
  private:
    std::vector<std::shared_ptr<FieldReceiver>> receivers_;
    arrow::StructBuilder *ptr_;
    size_t num_fields_;

  public:
    StructReceiver(
        std::vector<std::pair<std::string, std::shared_ptr<FieldReceiver>>> fields)
        : num_fields_(fields.size())
    {
        std::vector<std::shared_ptr<arrow::ArrayBuilder>> receivers(num_fields_);
        std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders(num_fields_);
        arrow::FieldVector fv(num_fields_);
        for (size_t i = 0; i < num_fields_; i++)
        {
            receivers_.push_back(fields[i].second);
            builders[i] = fields[i].second->Builder;
            fv[i] = arrow::field(fields[i].first, builders[i]->type());
        }

        Builder = std::make_shared<arrow::StructBuilder>(
            arrow::struct_(fv), arrow::default_memory_pool(), builders);

        ptr_ = (arrow::StructBuilder *)Builder.get();
    }

    int Parse(const char *buffer)
    {
        int32_t flen = unpack_int32(buffer);
        buffer += 4;

        if (flen == -1)
        {
            auto status = ptr_->AppendNull();
            return 4;
        }

        auto status = ptr_->Append();

        int32_t nvalids = unpack_int32(buffer);
        buffer += 4;

        for (size_t i = 0; i < num_fields_; i++)
        {
            if (i >= nvalids)
            {
                // TODO: check postgres ref code
                status = receivers_[i]->Builder->AppendNull();
                continue;
            }
            // int32_t elem_oid = unpack_int32(buffer);
            buffer += 4;

            buffer += receivers_[i]->Parse(buffer);
        }

        return 4 + flen;
    }
};

struct IdRecv
{
    static inline const char *Recv(const char *x) { return x; }
};

struct TimestampRecv
{
    static const int64_t kEpoch = 946684800000000; // 2000-01-01 - 1970-01-01 (us)
    static inline int64_t Recv(const char *x) { return unpack_int64(x) + kEpoch; }
};

struct DateRecv
{
    static const int32_t kEpoch = 10957; // 2000-01-01 - 1970-01-01 (days)
    static inline int32_t Recv(const char *x) { return unpack_int32(x) + kEpoch; }
};

struct IntervalRecv
{
    static const int64_t kMicrosecondsPerDay = 24 * 3600 * 1000000LL;
    static inline int64_t Recv(const char *x)
    {
        int8_t msecs = unpack_int64(x);
        int32_t days = unpack_int32(x + 8);
        // int32_t months = unpack_int32(cursor + 12);
        return msecs + days * kMicrosecondsPerDay;
    }
};

struct BoolRecv
{
    static inline bool Recv(const char *x) { return (*x != 0); }
};

struct Int2Recv
{
    static inline int16_t Recv(const char *x) { return unpack_int16(x); }
};

struct Int4Recv
{
    static inline int64_t Recv(const char *x) { return unpack_int32(x); }
};

struct Int8Recv
{
    static inline int64_t Recv(const char *x) { return unpack_int64(x); }
};

struct Float4Recv
{
    static inline float Recv(const char *x) { return unpack_float(x); }
};

struct Float8Recv
{
    static inline double Recv(const char *x) { return unpack_double(x); }
};

template <class BuilderT, typename RecvT> class GenericReceiver : public FieldReceiver
{
  private:
    BuilderT *ptr_;

  public:
    GenericReceiver()
    {
        Builder = std::make_shared<BuilderT>();
        ptr_ = (BuilderT *)Builder.get();
    }

    int Parse(const char *buffer)
    {
        int32_t flen = unpack_int32(buffer);
        buffer += 4;

        if (flen == -1)
        {
            if constexpr (
                std::is_base_of<BuilderT, arrow::FloatBuilder>::value ||
                std::is_base_of<BuilderT, arrow::DoubleBuilder>::value)
                auto status = ptr_->Append(NAN);
            else
                auto status = ptr_->AppendNull();
            return 4;
        }

        auto value = RecvT::Recv(buffer);
        if constexpr (
            std::is_base_of<BuilderT, arrow::BinaryBuilder>::value ||
            std::is_base_of<BuilderT, arrow::StringBuilder>::value ||
            std::is_base_of<BuilderT, arrow::StringDictionaryBuilder>::value)
            auto status = ptr_->Append(value, flen);
        else
            auto status = ptr_->Append(value);

        return 4 + flen;
    }
};

using BoolReceiver = GenericReceiver<arrow::BooleanBuilder, BoolRecv>;
using Int2Receiver = GenericReceiver<arrow::Int16Builder, Int2Recv>;
using Int4Receiver = GenericReceiver<arrow::Int32Builder, Int4Recv>;
using Int8Receiver = GenericReceiver<arrow::Int64Builder, Int8Recv>;
using Float4Receiver = GenericReceiver<arrow::FloatBuilder, Float4Recv>;
using Float8Receiver = GenericReceiver<arrow::DoubleBuilder, Float8Recv>;
using BinaryReceiver = GenericReceiver<arrow::BinaryBuilder, IdRecv>;
using TextReceiver = GenericReceiver<arrow::StringBuilder, IdRecv>;
using DictionaryReceiver = GenericReceiver<arrow::StringDictionary32Builder, IdRecv>;
using DateReceiver = GenericReceiver<arrow::Date32Builder, DateRecv>;

class TimestampReceiver : public FieldReceiver
{
  private:
    arrow::TimestampBuilder *ptr_;

  public:
    TimestampReceiver()
    {
        Builder = std::make_shared<arrow::TimestampBuilder>(
            arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
        ptr_ = (arrow::TimestampBuilder *)Builder.get();
    }

    int Parse(const char *buffer)
    {
        int32_t flen = unpack_int32(buffer);
        buffer += 4;

        if (flen == -1)
        {
            auto status = ptr_->AppendNull();
            return 4;
        }

        auto value = TimestampRecv::Recv(buffer);
        auto status = ptr_->Append(value);

        return 4 + flen;
    }
};

class TimeReceiver : public FieldReceiver
{
  private:
    arrow::Time64Builder *ptr_;

  public:
    TimeReceiver()
    {
        Builder = std::make_shared<arrow::Time64Builder>(
            arrow::time64(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
        ptr_ = (arrow::Time64Builder *)Builder.get();
    }

    int Parse(const char *buffer)
    {
        int32_t flen = unpack_int32(buffer);
        buffer += 4;

        if (flen == -1)
        {
            auto status = ptr_->AppendNull();
            return 4;
        }

        auto value = Int8Recv::Recv(buffer);
        auto status = ptr_->Append(value);

        return 4 + flen;
    }
};

class IntervalReceiver : public FieldReceiver
{
  private:
    arrow::DurationBuilder *ptr_;

  public:
    IntervalReceiver()
    {
        Builder = std::make_shared<arrow::DurationBuilder>(
            arrow::duration(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
        ptr_ = (arrow::DurationBuilder *)Builder.get();
    }

    int Parse(const char *buffer)
    {
        int32_t flen = unpack_int32(buffer);
        buffer += 4;

        if (flen == -1)
        {
            auto status = ptr_->AppendNull();
            return 4;
        }

        auto value = IntervalRecv::Recv(buffer);
        auto status = ptr_->Append(value);

        return 4 + flen;
    }
};

class UuidReceiver : public FieldReceiver
{
  private:
    arrow::FixedSizeBinaryBuilder *ptr_;

  public:
    UuidReceiver()
    {
        Builder = std::make_shared<arrow::FixedSizeBinaryBuilder>(
            arrow::fixed_size_binary(16), arrow::default_memory_pool());
        ptr_ = (arrow::FixedSizeBinaryBuilder *)Builder.get();
    }

    int Parse(const char *buffer)
    {
        int32_t flen = unpack_int32(buffer);
        buffer += 4;

        if (flen == -1)
        {
            auto status = ptr_->AppendNull();
            return 4;
        }

        auto status = ptr_->Append(buffer);
        return 4 + flen;
    }
};

class JsonbReceiver : public FieldReceiver
{
  private:
    arrow::BinaryBuilder *ptr_;

  public:
    JsonbReceiver()
    {
        Builder = std::make_shared<arrow::StringBuilder>();
        ptr_ = (arrow::BinaryBuilder *)Builder.get();
    }

    int Parse(const char *buffer)
    {
        int32_t flen = unpack_int32(buffer);
        buffer += 4;

        if (flen == -1)
        {
            auto status = ptr_->AppendNull();
            return 4;
        }

        // int8_t version = (int8_t)(*buffer);
        buffer += 1;

        auto status = ptr_->Append(buffer, flen - 1);

        return 4 + flen;
    }
};

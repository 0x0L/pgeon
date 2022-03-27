
#include "hton.h"

#include <arrow/api.h>

#include <map>

namespace pgeon
{

class ColumnBuilder
{
  public:
    std::unique_ptr<arrow::ArrayBuilder> Builder;
    virtual ~ColumnBuilder(){};

    // General format of a field is
    // int32 length
    // char[length] content if length > -1
    virtual size_t Append(const char *buffer) = 0;

    std::shared_ptr<arrow::DataType> type() { return Builder->type(); };

    std::shared_ptr<arrow::Array> Flush()
    {
        std::shared_ptr<arrow::Array> array;
        auto status = Builder->Finish(&array);
        return array;
    };
};

using Field = std::pair<std::string, std::shared_ptr<ColumnBuilder>>;
using FieldVector = std::vector<Field>;

std::shared_ptr<ColumnBuilder>
createArrayBuilder(std::shared_ptr<ColumnBuilder> value_builder);

std::shared_ptr<ColumnBuilder> createRecordBuilder(FieldVector fields);

std::shared_ptr<ColumnBuilder> createNumericBuilder(int precision, int scale);

extern std::map<std::string, std::shared_ptr<ColumnBuilder> (*)()> gDecoderFactory;

class TableBuilder
{
  private:
    FieldVector fields_;
    std::vector<ColumnBuilder *> builders_;
    std::shared_ptr<arrow::Schema> schema_;
    // std::vector<int32_t> fields_offsets_;

  public:
    TableBuilder(FieldVector &fields) : fields_(fields)
    {
        arrow::FieldVector schema;
        for (auto &f : fields)
        {
            auto &[name, builder] = f;
            builders_.push_back(builder.get());
            schema.push_back(arrow::field(name, builder->type()));
        }
        schema_ = arrow::schema(schema);
        // fields_offsets_ = std::vector<int32_t>(fields.size(), 0);
    }

    int32_t Append(const char *cursor)
    {
        const char *cur = cursor;
        int16_t nfields = unpack_int16(cur);
        cur += 2;

        if (nfields == -1)
            return 2;

        // const char* cur2 = cur;
        // for (size_t i = 1; i < nfields; i++)
        // {
        //     cur2 += unpack_int32(cur2) + 4;
        //     fields_offsets_[i] = cur2 - cur;
        // }

        // #pragma omp parallel for
        for (size_t i = 0; i < nfields; i++)
        {
            cur += builders_[i]->Append(cur);
            // builders_[i]->Append(cur + fields_offsets_[i]);
        }
        return cur - cursor;
    }

    std::shared_ptr<arrow::Table> Flush()
    {
        std::vector<std::shared_ptr<arrow::Array>> arrays(fields_.size());
        for (size_t i = 0; i < fields_.size(); i++)
        {
            arrays[i] = builders_[i]->Flush();
        }

        auto batch = arrow::Table::Make(schema_, arrays);
        return batch;
    }

    // TODO: Flush to batch
};

} // namespace pgeon

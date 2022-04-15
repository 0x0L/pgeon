#include <arrow/api.h>
#include <libpq-fe.h>

#include <map>

namespace pgeon
{

class ColumnBuilder;

using Field = std::pair<std::string, std::shared_ptr<ColumnBuilder>>;
using FieldVector = std::vector<Field>;

struct SqlTypeInfo
{
    // attname, attnum, atttypid, atttypmod, attlen,"
    // 		 "       attbyval, attalign, typtype, typrelid, typelem,"
    // 		 "       nspname, typname"

    int typmod;

    // ListBuilder
    std::shared_ptr<ColumnBuilder> value_builder;

    // StructBuilder
    std::vector<std::pair<std::string, std::shared_ptr<ColumnBuilder>>> field_builders;
};

struct UserOptions
{
    bool string_as_dictionaries = false;

    struct UserOptions static Defaults() { return UserOptions(); }
};

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

extern std::map<
    std::string,
    std::shared_ptr<ColumnBuilder> (*)(const SqlTypeInfo &, const UserOptions &)>
    gDecoderFactory;

class TableBuilder
{
  private:
    FieldVector fields_;
    std::vector<ColumnBuilder *> builders_;
    std::shared_ptr<arrow::Schema> schema_;
    // std::vector<int32_t> fields_offsets_;

  public:
    TableBuilder(FieldVector &fields);

    int32_t Append(const char *cursor);

    std::shared_ptr<arrow::Table> Flush();

    // TODO: Flush to batch
};

std::shared_ptr<TableBuilder> MakeQueryBuilder(PGconn *conn, const char *query);

void CopyQuery(PGconn *conn, const char *query, std::shared_ptr<TableBuilder> builder);

std::shared_ptr<arrow::Table> GetTable(const char *conninfo, const char *query);

} // namespace pgeon

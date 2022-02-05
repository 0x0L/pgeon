#include "decoders.hpp"
#include <iostream>

#include <libpq-fe.h>

using FieldVector = std::vector<std::pair<std::string, std::shared_ptr<FieldReceiver>>>;

FieldVector GetQuerySchema(PGconn *conn, const char *query);

class Builder
{
  private:
    std::vector<FieldReceiver *> fields_;
    std::shared_ptr<arrow::Schema> schema_;

  public:
    Builder(FieldVector &fields)
    {
        arrow::FieldVector fv;
        for (size_t i = 0; i < fields.size(); i++)
        {
            fields_.push_back(fields[i].second.get());
            auto f = arrow::field(fields[i].first, fields_[i]->Builder->type());
            fv.push_back(f);
        }

        schema_ = arrow::schema(fv);
    }

    int32_t Append(const char *cursor)
    {
        const char *cur = cursor;
        int16_t nfields = unpack_int16(cur);
        cur += 2;

        if (nfields == -1)
            return 2;

        for (size_t i = 0; i < nfields; i++)
        {
            auto receiver = fields_[i];
            cur += receiver->Parse(cur);
        }
        return cur - cursor;
    }

    std::shared_ptr<arrow::Table> Flush()
    {
        std::vector<std::shared_ptr<arrow::Array>> arrays(fields_.size());
        for (size_t i = 0; i < fields_.size(); i++)
        {
            auto status = fields_[i]->Builder->Finish(&arrays[i]);
        }

        auto batch = arrow::Table::Make(schema_, arrays);
        return batch;
    }
};

void CopyQuery(PGconn *conn, const char *query, Builder &builder)
{
    auto copy_query = std::string("COPY (") + query + ") TO STDOUT (FORMAT binary)";
    auto res = PQexec(conn, copy_query.c_str());
    if (PQresultStatus(res) != PGRES_COPY_OUT)
        std::cout << "error in copy command: " << PQresultErrorMessage(res)
                  << std::endl;
    PQclear(res);

    char *tuple;
    auto status = PQgetCopyData(conn, &tuple, 0);
    if (status > 0)
    {
        const int kBinaryHeaderSize = 19;
        builder.Append(tuple + kBinaryHeaderSize);
        PQfreemem(tuple);
    }

    while (true)
    {
        status = PQgetCopyData(conn, &tuple, 0);
        if (status < 0)
            break;

        builder.Append(tuple);
        PQfreemem(tuple);
    }

    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        std::cout << "copy command failed: " << PQresultErrorMessage(res) << std::endl;
    PQclear(res);
}

int main(int argc, char const *argv[])
{
    const char *conninfo = "postgresql://localhost/mytests";
    auto conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK)
        std::cout << "failed on PostgreSQL connection: " << PQerrorMessage(conn)
                  << std::endl;

    auto res = PQexec(conn, "BEGIN READ ONLY");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        std::cout << "unable to begin transaction: " << PQresultErrorMessage(res)
                  << std::endl;
    PQclear(res);

    const char *query = "select * from minute_bars";
    // const char *query = "select sum(volume) from minute_bars;"
    auto h = GetQuerySchema(conn, query);
    auto builder = Builder(h);

    CopyQuery(conn, query, builder);
    auto x = builder.Flush();

    // std::cout << x->ToString() << std::endl;

    res = PQexec(conn, "END");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        std::cout << "unable to end transaction: " << PQresultErrorMessage(res)
                  << std::endl;
    PQclear(res);

    PQfinish(conn);

    return 0;
}

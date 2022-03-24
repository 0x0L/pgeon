#include "decoders.h"
#include <libpq-fe.h>

namespace pgeon
{

using ColumnVector = std::vector<std::pair<std::string, Oid>>;

std::shared_ptr<TableBuilder> MakeQueryBuilder(PGconn *conn, const char *query);
void CopyQuery(PGconn *conn, const char *query, std::shared_ptr<TableBuilder> builder);

} // namespace pgeon

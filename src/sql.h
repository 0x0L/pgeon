#include "decoders.h"
#include <libpq-fe.h>

namespace pgeon
{

std::shared_ptr<TableBuilder> MakeQueryBuilder(PGconn *conn, const char *query);

void CopyQuery(PGconn *conn, const char *query, std::shared_ptr<TableBuilder> builder);

} // namespace pgeon

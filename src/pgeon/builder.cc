// Copyright 2022 nullptr

#include <map>

#include "pgeon/builder.h"

#include "pgeon/builder/common.h"
#include "pgeon/builder/datetime.h"
#include "pgeon/builder/geometric.h"
#include "pgeon/builder/nested.h"
#include "pgeon/builder/network.h"
#include "pgeon/builder/numeric.h"
#include "pgeon/builder/stringlike.h"

namespace pgeon {

using BinaryBuilder = GenericBuilder<arrow::BinaryBuilder, IdRecv>;
using StringBuilder = GenericBuilder<arrow::StringBuilder, IdRecv>;

using BooleanBuilder = GenericBuilder<arrow::BooleanBuilder, BoolRecv>;
using Int32Builder = GenericBuilder<arrow::Int32Builder, Int4Recv>;
using Int64Builder = GenericBuilder<arrow::Int64Builder, Int8Recv>;

using FloatBuilder = GenericBuilder<arrow::FloatBuilder, Float4Recv>;
using DoubleBuilder = GenericBuilder<arrow::DoubleBuilder, Float8Recv>;

template <class T>
std::shared_ptr<ArrayBuilder> make(const SqlTypeInfo& info, const UserOptions& options) {
  return std::make_shared<T>(info, options);
}

std::map<std::string,
         std::shared_ptr<ArrayBuilder> (*)(const SqlTypeInfo&, const UserOptions&)>
    gTypeMap = {
        {"anyarray_recv", &make<ListBuilder>},
        {"anycompatiblearray_recv", &make<ListBuilder>},
        {"array_recv", &make<ListBuilder>},
        {"bit_recv", &make<BinaryBuilder>},
        {"boolrecv", &make<BooleanBuilder>},
        {"box_recv", &make<BoxBuilder>},
        {"bpcharrecv", &make<StringBuilder>},
        {"brin_bloom_summary_recv", &make<BinaryBuilder>},
        {"brin_minmax_multi_summary_recv", &make<BinaryBuilder>},
        {"bytearecv", &make<BinaryBuilder>},
        {"cash_recv", &make<Int64Builder>},
        {"charrecv", &make<GenericBuilder<arrow::UInt8Builder, CharRecv>>},
        {"cidr_recv", &make<InetBuilder>},
        {"cidrecv", &make<Int32Builder>},
        {"circle_recv", &make<CircleBuilder>},
        {"cstring_recv", &make<BinaryBuilder>},
        {"date_recv", &make<GenericBuilder<arrow::Date32Builder, DateRecv>>},
        // TODO(xav) this probably needs to get done in MakeBuilder
        // {"domain_recv", &make<Builder>},
        {"enum_recv", &make<GenericBuilder<arrow::StringDictionary32Builder, IdRecv>>},
        {"float4recv", &make<FloatBuilder>},
        {"float8recv", &make<DoubleBuilder>},
        {"hstore_recv", &make<HstoreBuilder>},
        {"inet_recv", &make<InetBuilder>},
        {"int2recv", &make<GenericBuilder<arrow::Int16Builder, Int2Recv>>},
        // {"int2vectorrecv", &make<Builder>},  // should need no implem
        {"int4recv", &make<Int32Builder>},
        {"int8recv", &make<Int64Builder>},
        {"interval_recv", &make<IntervalBuilder>},
        {"json_recv", &make<StringBuilder>},
        {"jsonb_recv", &make<JsonbBuilder>},
        {"jsonpath_recv", &make<JsonbBuilder>},
        {"line_recv", &make<LineBuilder>},
        {"lseg_recv", &make<BoxBuilder>},
        {"macaddr_recv", &make<BinaryBuilder>},
        {"macaddr8_recv", &make<BinaryBuilder>},
        // {"multirange_recv", &make<Builder>},
        {"namerecv", &make<StringBuilder>},
        {"numeric_recv", &make<NumericBuilder>},
        {"oidrecv", &make<Int32Builder>},
        // {"oidvectorrecv", &make<Builder>},  // should need no implem
        {"path_recv", &make<PathBuilder>},
        {"pg_dependencies_recv", &make<BinaryBuilder>},
        {"pg_lsn_recv", &make<Int64Builder>},
        {"pg_mcv_list_recv", &make<BinaryBuilder>},
        {"pg_ndistinct_recv", &make<BinaryBuilder>},
        // format: int4 nxip, u64 xmin, u64 xmax, u64 xip...
        // {"pg_snapshot_recv", &make<Builder>},
        {"point_recv", &make<PointBuilder>},
        {"poly_recv", &make<PolygonBuilder>},
        // {"range_recv", &make<Builder>},
        {"record_recv", &make<StructBuilder>},
        {"regclassrecv", &make<Int32Builder>},
        {"regcollationrecv", &make<Int32Builder>},
        {"regconfigrecv", &make<Int32Builder>},
        {"regdictionaryrecv", &make<Int32Builder>},
        {"regnamespacerecv", &make<Int32Builder>},
        {"regoperatorrecv", &make<Int32Builder>},
        {"regoperrecv", &make<Int32Builder>},
        {"regprocedurerecv", &make<Int32Builder>},
        {"regprocrecv", &make<Int32Builder>},
        {"regrolerecv", &make<Int32Builder>},
        {"regtyperecv", &make<Int32Builder>},
        {"textrecv", &make<StringBuilder>},
        // format: int4 block, int2 offset
        // {"tidrecv", &make<Builder>},
        {"time_recv", &make<TimeBuilder>},
        {"timetz_recv", &make<TimeTzBuilder>},
        {"timestamp_recv", &make<TimestampBuilder>},
        {"timestamptz_recv", &make<TimestampBuilder>},
        // {"tsqueryrecv", &make<Builder>},
        // {"tsvectorrecv", &make<Builder>},
        {"unknownrecv", &make<StringBuilder>},
        {"uuid_recv", &make<BinaryBuilder>},
        {"varbit_recv", &make<BinaryBuilder>},
        {"varcharrecv", &make<StringBuilder>},
        // {"void_recv", &make<GenericBuilder<arrow::NullBuilder, IdRecv>>},
        {"xid8recv", &make<Int64Builder>},
        {"xidrecv", &make<Int32Builder>},
        {"xml_recv", &make<StringBuilder>},
};

std::shared_ptr<ArrayBuilder> MakeBuilder(const std::string& typreceive,
                                          const SqlTypeInfo& info,
                                          const UserOptions& options) {
  return gTypeMap.count(typreceive) == 0 ? gTypeMap["bytearecv"](info, options)
                                         : gTypeMap[typreceive](info, options);
}

}  // namespace pgeon

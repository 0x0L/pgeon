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
using Int32Builder = GenericBuilder<arrow::Int32Builder, Int4Recv>;
using Int64Builder = GenericBuilder<arrow::Int64Builder, Int8Recv>;

using CtorType = std::shared_ptr<ArrayBuilder> (*)(const SqlTypeInfo&,
                                                   const UserOptions&);

template <class T>
std::shared_ptr<ArrayBuilder> make(const SqlTypeInfo& info, const UserOptions& options) {
  return std::make_shared<T>(info, options);
}

std::map<std::string, CtorType> gTypeMap = {
    {"anyarray_recv", &make<ListBuilder>},
    {"anycompatiblearray_recv", &make<ListBuilder>},
    {"array_recv", &make<ListBuilder>},
    {"bit_recv", &make<BinaryBuilder>},
    {"boolrecv", &make<GenericBuilder<arrow::BooleanBuilder, BoolRecv>>},
    {"box_recv", &make<BoxBuilder>},
    {"bpcharrecv", &make<StringBuilder>},
    {"brin_bloom_summary_recv", &make<BinaryBuilder>},
    {"brin_minmax_multi_summary_recv", &make<BinaryBuilder>},
    {"bytearecv", &make<BinaryBuilder>},
    {"cash_recv", &make<Int64Builder>},
    {"charrecv", &make<GenericBuilder<arrow::UInt8Builder, CharRecv>>},
    {"cidr_recv", &make<InetBuilder>},
    {"cidrecv", &make<Int32Builder>},
    // {"circle_recv", &make<Builder>}, center.x center.y radius f8
    {"cstring_recv", &make<BinaryBuilder>},
    {"date_recv", &make<GenericBuilder<arrow::Date32Builder, DateRecv>>},
    // {"domain_recv", &make<Builder>},
    {"enum_recv", &make<GenericBuilder<arrow::StringDictionary32Builder, IdRecv>>},
    {"float4recv", &make<GenericBuilder<arrow::FloatBuilder, Float4Recv>>},
    {"float8recv", &make<GenericBuilder<arrow::DoubleBuilder, Float8Recv>>},
    // {"hstore_recv", &make<Builder>},
    {"inet_recv", &make<InetBuilder>},
    {"int2recv", &make<GenericBuilder<arrow::Int16Builder, Int2Recv>>},
    // {"int2vectorrecv", &make<Builder>},
    {"int4recv", &make<Int32Builder>},
    {"int8recv", &make<Int64Builder>},
    {"interval_recv", &make<IntervalBuilder>},
    {"json_recv", &make<StringBuilder>},
    // {"jsonb_recv", &make<Builder>},
    // {"jsonpath_recv", &make<Builder>},
    // {"line_recv", &make<Builder>},
    // {"lseg_recv", &make<Builder>},
    {"macaddr_recv", &make<BinaryBuilder>},
    {"macaddr8_recv", &make<BinaryBuilder>},
    // {"multirange_recv", &make<Builder>},
    {"namerecv", &make<StringBuilder>},
    {"numeric_recv", &make<NumericBuilder>},
    {"oidrecv", &make<Int32Builder>},
    // {"oidvectorrecv", &make<Builder>},
    // {"path_recv", &make<Builder>},
    {"pg_dependencies_recv", &make<BinaryBuilder>},
    {"pg_lsn_recv", &make<Int64Builder>},
    {"pg_mcv_list_recv", &make<BinaryBuilder>},
    {"pg_ndistinct_recv", &make<BinaryBuilder>},
    // {"pg_snapshot_recv", &make<Builder>},
    // {"point_recv", &make<Builder>},
    // {"poly_recv", &make<Builder>},
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
    // {"tidrecv", &make<Builder>},
    {"time_recv", &make<TimeBuilder>},
    // {"timetz_recv", &makeUTC<TimeBuilder>},  // nope
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

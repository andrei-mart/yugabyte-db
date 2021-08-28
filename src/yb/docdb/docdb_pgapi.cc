//--------------------------------------------------------------------------------------------------
// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//--------------------------------------------------------------------------------------------------

#include "yb/docdb/docdb_pgapi.h"

#include "yb/common/ql_expr.h"
#include "yb/common/schema.h"

#include "yb/gutil/singleton.h"
#include "yb/yql/pggate/ybc_pg_typedefs.h"
#include "yb/yql/pggate/pg_value.h"
#include "yb/yql/pggate/pg_expr.h"

#include "yb/util/result.h"

// This file comes from this directory:
// postgres_build/src/include/catalog
// added as a special include path to CMakeLists.txt
#include "pg_type_d.h" // NOLINT

using yb::pggate::PgValueFromPB;
using yb::pggate::PgValueToPB;

namespace yb {
namespace docdb {

#define PG_RETURN_NOT_OK(status) \
  do { \
    if (status.err_code != 0) { \
      std::string msg; \
      if (status.err_msg != NULL) { \
        msg = std::string(status.err_msg); \
      } else { \
        msg = std::string("Unexpected error while evaluating expression"); \
      } \
      YbgResetMemoryContext(); \
      return STATUS(QLError, msg); \
    } \
  } while(0);


//-----------------------------------------------------------------------------
// Types
//-----------------------------------------------------------------------------

class DocPgTypeAnalyzer {
 public:
  const YBCPgTypeEntity* GetTypeEntity(int32_t type_oid) {
    const auto iter = type_map_.find(type_oid);
    if (iter != type_map_.end()) {
      return iter->second;
    }
    return nullptr;
  }

 private:
  DocPgTypeAnalyzer() {
    // Setup type mapping.
    const YBCPgTypeEntity *type_table;
    int count;

    YbgGetTypeTable(&type_table, &count);
    for (int idx = 0; idx < count; idx++) {
        const YBCPgTypeEntity *type_entity = &type_table[idx];
        type_map_[type_entity->type_oid] = type_entity;
    }
  }

  // Mapping table of YugaByte and PostgreSQL datatypes.
  std::unordered_map<int, const YBCPgTypeEntity *> type_map_;

  friend class Singleton<DocPgTypeAnalyzer>;
  DISALLOW_COPY_AND_ASSIGN(DocPgTypeAnalyzer);
};

//-----------------------------------------------------------------------------
// Expressions/Values
//-----------------------------------------------------------------------------

const YBCPgTypeEntity* DocPgGetTypeEntity(YbgTypeDesc pg_type) {
    return Singleton<DocPgTypeAnalyzer>::get()->GetTypeEntity(pg_type.type_id);
}

Status DocPgPrepareExpr(const std::string& expr_str,
                        std::vector<DocPgParamDesc> params,
                        const Schema *schema,
                        std::map<int, const DocPgVarRef>& var_map,
                        YbgPreparedExpr *expr,
                        DocPgVarRef *ret_type) {
  char *expr_cstring = const_cast<char *>(expr_str.c_str());
  VLOG(1) << "Deserialize " << expr_cstring;
  PG_RETURN_NOT_OK(YbgPrepareExpr(expr_cstring, expr));

  VLOG(1) << "Processing expression parameters";
  // Set the column values (used to resolve scan variables in the expression).
  for (const ColumnId& col_id : schema->column_ids()) {
    bool found = false;
    auto column = schema->column_by_id(col_id);
    SCHECK(column.ok(), InternalError, "Invalid Schema");
    int32_t attno = column->order();
    if (var_map.find(attno) != var_map.end()) {
      VLOG(1) << "Attribute " << attno << " is already processed";
      continue;
    }

    for (int i = 0; i < params.size(); i++) {
      if (attno == params[i].attno) {
        YbgTypeDesc pg_arg_type = {params[i].typid, params[i].typmod};
        const YBCPgTypeEntity *arg_type = DocPgGetTypeEntity(pg_arg_type);
        var_map.emplace(std::piecewise_construct,
                        std::forward_as_tuple(attno),
                        std::forward_as_tuple(col_id.rep(), arg_type, params[i].typmod));
        VLOG(1) << "Attribute " << attno << " has been processed";
        found = true;
        break;
      }
    }

    if (!found) {
      VLOG(1) << "Attribute " << attno << " is not referenced";
    }
  }
  VLOG(1) << "Total attributes referenced: " << var_map.size();

  if (ret_type != nullptr) {
    YbgTypeDesc pg_arg_type = {params[0].typid, params[0].typmod};
    const YBCPgTypeEntity *arg_type = DocPgGetTypeEntity(pg_arg_type);
    *ret_type = DocPgVarRef(0, arg_type, params[0].typmod);
    VLOG(1) << "Processed expression return type";
  }
  return Status::OK();
}

Status DocPgPrepareExprCtx(const QLTableRow& table_row,
                           std::map<int, const DocPgVarRef>& var_map,
                           YbgExprContext *expr_ctx) {
  if (var_map.empty()) {
    return Status::OK();
  }

  int32_t min_attno = var_map.begin()->first;
  int32_t max_attno = var_map.rbegin()->first;

  VLOG(1) << "Allocating expr context: (" << min_attno << ", " << max_attno;
  PG_RETURN_NOT_OK(YbgExprContextCreate(min_attno, max_attno, expr_ctx));

  // Set the column values (used to resolve scan variables in the expression).
  for (auto it = var_map.begin(); it != var_map.end(); it++) {
    const int& attno = it->first;
    const DocPgVarRef& arg_ref = it->second;
    const QLValuePB* val = table_row.GetColumn(arg_ref.var_colid);
    bool is_null = false;
    uint64_t datum = 0;
    RETURN_NOT_OK(PgValueFromPB(arg_ref.var_type, arg_ref.var_type_attrs, *val, &datum, &is_null));
    VLOG(1) << "Adding value for attno " << attno;
    PG_RETURN_NOT_OK(YbgExprContextAddColValue(*expr_ctx, attno, datum, is_null));
  }
  return Status::OK();
}

Status DocPgEvalExpr(YbgPreparedExpr expr,
                     YbgExprContext expr_ctx,
                     uint64_t *datum,
                     bool *is_null) {
  // Evaluate the expression and get the result.
  PG_RETURN_NOT_OK(YbgEvalExpr(expr, expr_ctx, datum, is_null));
  return Status::OK();
}

Status DocPgEvalExpr(YbgPreparedExpr expr,
                     YbgExprContext expr_ctx,
                     const DocPgVarRef& res_type,
                     QLValue* result) {
  // Evaluate the expression and get the result.
  bool is_null = false;
  uint64_t datum;
  RETURN_NOT_OK(DocPgEvalExpr(expr, expr_ctx, &datum, &is_null));
  return PgValueToPB(res_type.var_type, datum, is_null, result);
}

Status ExtractTextArrayFromQLBinaryValue(const QLValuePB& ql_value,
                                         vector<QLValuePB> *const ql_value_vec) {
  PG_RETURN_NOT_OK(YbgPrepareMemoryContext());

  RETURN_NOT_OK(ExtractVectorFromQLBinaryValueHelper(
      ql_value,
      TEXTARRAYOID,
      TEXTOID,
      ql_value_vec));
  PG_RETURN_NOT_OK(YbgResetMemoryContext());
  return Status::OK();
}

// This function expects that YbgPrepareMemoryContext was called by the caller of this function.
Status ExtractVectorFromQLBinaryValueHelper(
  const QLValuePB& ql_value,
  const int array_type,
  const int elem_type,
  vector<QLValuePB> *const result) {

  const uint64_t size = ql_value.binary_value().size();
  char *val = const_cast<char *>(ql_value.binary_value().c_str());

  YbgTypeDesc pg_arg_type {array_type, -1 /* typmod */};
  const YBCPgTypeEntity *arg_type = DocPgGetTypeEntity(pg_arg_type);
  YBCPgTypeAttrs type_attrs {-1 /* typmod */};
  uint64_t datum = arg_type->yb_to_datum(reinterpret_cast<uint8_t *>(val), size, &type_attrs);

  uint64_t *datum_elements;
  int num_elems = 0;
  PG_RETURN_NOT_OK(YbgSplitArrayDatum(datum, elem_type, &datum_elements, &num_elems));
  YbgTypeDesc elem_pg_arg_type {elem_type, -1 /* typmod */};
  const YBCPgTypeEntity *elem_arg_type = DocPgGetTypeEntity(elem_pg_arg_type);
  VLOG(4) << "Number of parsed elements: " << num_elems;
  for (int i = 0; i < num_elems; ++i) {
    QLValuePB ql_val;
    pggate::PgConstant value(elem_arg_type,
                             false /* collate_is_valid_non_c */,
                             nullptr /* collation_sortkey */,
                             datum_elements[i], false /* isNull */);
    RETURN_NOT_OK(value.Eval(&ql_val));
    VLOG(4) << "Parsed value: " << ql_val.string_value();
    result->emplace_back(std::move(ql_val));
  }
  return Status::OK();
}

}  // namespace docdb
}  // namespace yb

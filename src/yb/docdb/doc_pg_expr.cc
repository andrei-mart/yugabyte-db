//--------------------------------------------------------------------------------------------------
// Copyright (c) YugaByte, Inc.
//--------------------------------------------------------------------------------------------------

#include "yb/docdb/doc_pg_expr.h"
#include "yb/docdb/docdb_pgapi.h"
#include "yb/util/bfpg/bfpg.h"

namespace yb {
namespace docdb {

//--------------------------------------------------------------------------------------------------

class DocPgExprExecutor::Private {
  Private() {
    // Create memory context
  }

  ~Private() {
    // Delete memory context
  }

  CHECKED_STATUS PreparePgExprCall(const PgsqlExpressionPB& ql_expr,
                                   const Schema *schema) {
    assert(ql_expr.expr_case() == PgsqlExpressionPB::ExprCase::kTscall);
    const PgsqlBCallPB& tscall = ql_expr.tscall();
    bfpg::TSOpcode tsopcode = static_cast<bfpg::TSOpcode>(tscall.opcode());
    assert(tsopcode == bfpg::TSOpcode::kPgEvalExprCall);
    const std::string& expr_str = tscall.operands(0).value().string_value();
    std::vector<DocPgParamDesc> params;
    int num_params = (tscall.operands_size() - 1) / 3;
    params.reserve(num_params);
    for (int i = 0; i < num_params; i++) {
      int32_t attno = tscall.operands(3*i + 1).value().int32_value();
      int32_t typid = tscall.operands(3*i + 2).value().int32_value();
      int32_t typmod = tscall.operands(3*i + 3).value().int32_value();
      params.emplace_back(attno, typid, typmod);
    }
    YbgPreparedExpr expr;
    RETURN_NOT_OK(DocPgPrepareExpr(expr_str, params, schema, var_map_, &expr, nullptr));
    where_clause_.push_back(expr);
    return Status::OK();
  }

  CHECKED_STATUS PreparePgRowData(const QLTableRow& table_row,
                                  YbgExprContext *expr_ctx) {
    return DocPgPrepareExprCtx(table_row, var_map_, expr_ctx);
  }

  CHECKED_STATUS EvalWhereExprCall(YbgPreparedExpr expr,
                                   YbgExprContext expr_ctx,
                                   bool *result) {
    uint64_t datum;
    bool is_null;
    RETURN_NOT_OK(DocPgEvalExpr(expr, expr_ctx, &datum, &is_null));
    if (is_null || datum == 0) {
      *result = false;
    }
    return Status::OK();
  }

  // MemoryContext request_ctx_
  // MemoryContext row_ctx_
  std::list<YbgPreparedExpr> where_clause_;
  std::map<int, const DocPgVarRef> var_map_;

  friend class DocPgExprExecutor;
};

//--------------------------------------------------------------------------------------------------

CHECKED_STATUS DocPgExprExecutor::AddWhereExpression(const PgsqlExpressionPB& ql_expr) {
  if (private_ == nullptr) {
    private_ = std::make_unique<Private>();
  }

  return private_->PreparePgExprCall(ql_expr, schema_);
}

CHECKED_STATUS DocPgExprExecutor::ExecWhereExpr(const QLTableRow& table_row, bool *result) {
  *result = true;

  if (private_ == nullptr || private_->where_clause_.empty()) {
    return Status::OK();
  }

  YbgExprContext expr_ctx;
  RETURN_NOT_OK(private_->PreparePgRowData(table_row, &expr_ctx));
  for (auto expr : private_->where_clause_) {
    RETURN_NOT_OK(private_->EvalWhereExprCall(expr, expr_ctx, result));
    if (!(*result)) {
      break;
    }
  }
  return Status::OK();
}


}  // namespace docdb
}  // namespace yb

//--------------------------------------------------------------------------------------------------
// Copyright (c) YugaByte, Inc.
//--------------------------------------------------------------------------------------------------

#include "yb/docdb/doc_pg_expr.h"
#include "yb/docdb/docdb_pgapi.h"
#include "yb/util/bfpg/bfpg.h"
#include "yb/util/logging.h"

namespace yb {
namespace docdb {

//--------------------------------------------------------------------------------------------------

class DocPgExprExecutor::Private {
  Private() {
    VLOG(1) << "Creating expression memory context";
    YbgCreateMemoryContext(nullptr,
                           "DocPg Expression Context",
                           &expression_ctx_);
  }

  ~Private() {
    YbgSetCurrentMemoryContext(expression_ctx_, nullptr);
    YbgDeleteMemoryContext();
    VLOG(1) << "Deleted expression memory context";
  }

  CHECKED_STATUS AddColumnRef(const PgsqlColumnRefPB& column_ref,
                              const Schema *schema) {
    assert(row_ctx_ == nullptr);
    ColumnId col_id = ColumnId(column_ref.column_id());
    if (!column_ref.has_typid()) {
      VLOG(1) << "Column reference " << col_id << " has no type information, skipping";
      return Status::OK();
    }
    VLOG(1) << "Column lookup " << col_id;
    auto column = schema->column_by_id(col_id);
    assert(column.ok());
    assert(column->order() == column_ref.attno());
    SCHECK(column.ok(), InternalError, "Invalid Schema");
    SCHECK_EQ(column->order(), column_ref.attno(), InternalError, "Invalid Schema");
    return DocPgAddVarRef(col_id,
                          column_ref.attno(),
                          column_ref.typid(),
                          column_ref.has_typmod() ? column_ref.typmod() : -1,
                          column_ref.has_collid() ? column_ref.collid() : 0,
                          &var_map_);
  }

  CHECKED_STATUS PreparePgExprCall(const PgsqlExpressionPB& ql_expr,
                                   const Schema *schema) {
    assert(row_ctx_ == nullptr);
    assert(ql_expr.expr_case() == PgsqlExpressionPB::ExprCase::kTscall);
    YbgMemoryContext old;
    YbgSetCurrentMemoryContext(expression_ctx_, &old);
    const PgsqlBCallPB& tscall = ql_expr.tscall();
    assert(static_cast<bfpg::TSOpcode>(tscall.opcode()) == bfpg::TSOpcode::kPgEvalExprCall);
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
    RETURN_NOT_OK(DocPgPrepareExpr(expr_str, params, schema, &var_map_, &expr, nullptr));
    where_clause_.push_back(expr);
    YbgSetCurrentMemoryContext(old, nullptr);
    return Status::OK();
  }

  CHECKED_STATUS PreparePgRowData(const QLTableRow& table_row,
                                  YbgExprContext *expr_ctx) {
    YbgMemoryContext old;
    if (row_ctx_ == nullptr) {
      VLOG(1) << "Creating row memory context";
      YbgCreateMemoryContext(expression_ctx_, "DocPg Row Context", &row_ctx_);
      YbgSetCurrentMemoryContext(row_ctx_, &old);
    } else {
      VLOG(1) << "Resetting row memory context";
      YbgSetCurrentMemoryContext(row_ctx_, &old);
      YbgResetMemoryContext();
    }

    RETURN_NOT_OK(DocPgPrepareExprCtx(table_row, var_map_, expr_ctx));

    YbgSetCurrentMemoryContext(old, nullptr);
    return Status::OK();
  }

  CHECKED_STATUS EvalWhereExprCall(YbgPreparedExpr expr,
                                   YbgExprContext expr_ctx,
                                   bool *result) {
    YbgMemoryContext old;
    uint64_t datum;
    bool is_null;
    assert(row_ctx_ != nullptr);
    YbgSetCurrentMemoryContext(row_ctx_, &old);
    RETURN_NOT_OK(DocPgEvalExpr(expr, expr_ctx, &datum, &is_null));
    *result = (!is_null && datum != 0);
    YbgSetCurrentMemoryContext(old, nullptr);
    return Status::OK();
  }

  YbgMemoryContext expression_ctx_ = nullptr;
  YbgMemoryContext row_ctx_ = nullptr;
  std::list<YbgPreparedExpr> where_clause_;
  std::map<int, const DocPgVarRef> var_map_;

  friend class DocPgExprExecutor;
};

void DocPgExprExecutor::private_deleter::operator()(DocPgExprExecutor::Private* ptr) const {
  delete ptr;
}

//--------------------------------------------------------------------------------------------------

CHECKED_STATUS DocPgExprExecutor::AddColumnRef(const PgsqlColumnRefPB& column_ref) {
  if (private_.get() == nullptr) {
    private_.reset(new Private());
  }
  return private_->AddColumnRef(column_ref, schema_);
}

CHECKED_STATUS DocPgExprExecutor::AddWhereExpression(const PgsqlExpressionPB& ql_expr) {
  if (private_.get() == nullptr) {
    private_.reset(new Private());
  }
  return private_->PreparePgExprCall(ql_expr, schema_);
}

CHECKED_STATUS DocPgExprExecutor::ExecWhereExpr(const QLTableRow& table_row, bool *result) {
  *result = true;

  if (private_.get() == nullptr || private_->where_clause_.empty()) {
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

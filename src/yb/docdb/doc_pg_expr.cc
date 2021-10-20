//--------------------------------------------------------------------------------------------------
// Copyright (c) YugaByte, Inc.
//--------------------------------------------------------------------------------------------------

#include "yb/docdb/doc_pg_expr.h"
#include "yb/docdb/docdb_pgapi.h"
#include "yb/util/bfpg/bfpg.h"
#include "yb/util/logging.h"
#include "yb/yql/pggate/pg_value.h"

using yb::pggate::PgValueToPB;

namespace yb {
namespace docdb {

//--------------------------------------------------------------------------------------------------

typedef std::pair<YbgPreparedExpr, DocPgVarRef> DocPgEvalExprData;

class DocPgExprExecutor::Private {
  Private() {
    YbgCreateMemoryContext(nullptr,
                           "DocPg Expression Context",
                           &mem_ctx_);
    VLOG(1) << "Created expression memory context @ " << mem_ctx_;
  }

  ~Private() {
    YbgSetCurrentMemoryContext(mem_ctx_, nullptr);
    VLOG(1) << "Deleting row memory context @ " << row_ctx_;
    VLOG(1) << "Deleting expression memory context @ " << mem_ctx_;
    YbgDeleteMemoryContext();
    VLOG(1) << "Deleted memory contexts";
  }

  CHECKED_STATUS AddColumnRef(const PgsqlColRefPB& column_ref,
                              const Schema *schema) {
    assert(expr_ctx_ == nullptr);
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

  CHECKED_STATUS PreparePgWhereExpr(const PgsqlExpressionPB& ql_expr,
                                    const Schema *schema) {
    YbgPreparedExpr expr;
    RETURN_NOT_OK(prepare_pg_expr_call(ql_expr, schema, &expr, nullptr));
    where_clause_.push_back(expr);
    VLOG(1) << "Condition has been added";
    return Status::OK();
  }

  CHECKED_STATUS PreparePgTargetExpr(const PgsqlExpressionPB& ql_expr,
                                     const Schema *schema) {
    YbgPreparedExpr expr;
    DocPgVarRef expr_type;
    RETURN_NOT_OK(prepare_pg_expr_call(ql_expr, schema, &expr, &expr_type));
    targets_.emplace_back(expr, expr_type);
    VLOG(1) << "Target expression has been added";
    return Status::OK();
  }

  CHECKED_STATUS prepare_pg_expr_call(const PgsqlExpressionPB& ql_expr,
                                      const Schema *schema,
                                      YbgPreparedExpr *expr,
                                      DocPgVarRef *expr_type) {
    YbgMemoryContext old;
    assert(expr_ctx_ == nullptr);
    assert(ql_expr.expr_case() == PgsqlExpressionPB::ExprCase::kTscall);
    const PgsqlBCallPB& tscall = ql_expr.tscall();
    assert(static_cast<bfpg::TSOpcode>(tscall.opcode()) == bfpg::TSOpcode::kPgEvalExprCall);
    assert(tscall.operands_size() == 1);
    const std::string& expr_str = tscall.operands(0).value().string_value();
    YbgSetCurrentMemoryContext(mem_ctx_, &old);
    const Status s = DocPgPrepareExpr(expr_str, expr, expr_type);
    YbgSetCurrentMemoryContext(old, nullptr);
    return s;
  }

  CHECKED_STATUS PreparePgRowData(const QLTableRow& table_row) {
    YbgMemoryContext old;
    assert(mem_ctx_);
    if (row_ctx_ == nullptr) {
      YbgCreateMemoryContext(mem_ctx_, "DocPg Row Context", &row_ctx_);
      VLOG(1) << "Created row memory context @ " << row_ctx_;
      YbgSetCurrentMemoryContext(row_ctx_, nullptr);
    } else {
      VLOG(2) << "Resetting row memory context @ " << row_ctx_;
      YbgSetCurrentMemoryContext(row_ctx_, &old);
      YbgResetMemoryContext();
    }
    if (var_map_.empty()) {
      return Status::OK();
    }
    RETURN_NOT_OK(ensure_expr_context());
    RETURN_NOT_OK(DocPgPrepareExprCtx(table_row, var_map_, expr_ctx_));

    YbgSetCurrentMemoryContext(old, nullptr);
    return Status::OK();
  }

  CHECKED_STATUS ensure_expr_context() {
    if (expr_ctx_ == nullptr) {
      YbgMemoryContext old;
      YbgSetCurrentMemoryContext(mem_ctx_, &old);
      RETURN_NOT_OK(DocPgCreateExprCtx(var_map_, &expr_ctx_));
      YbgSetCurrentMemoryContext(old, nullptr);
    }
    return Status::OK();
  }

  CHECKED_STATUS EvalWhereExprCalls(bool *result) {
    YbgMemoryContext old;
    uint64_t datum;
    bool is_null;
    assert(row_ctx_ != nullptr);
    YbgSetCurrentMemoryContext(row_ctx_, &old);
    *result = true;
    for (auto expr : where_clause_) {
      RETURN_NOT_OK(DocPgEvalExpr(expr, expr_ctx_, &datum, &is_null));
      VLOG(2) << "Condition has been evaluated: " << *result;
      if (is_null || datum == 0) {
        *result = false;
        break;
      }
    }
    YbgSetCurrentMemoryContext(old, nullptr);
    return Status::OK();
  }

  CHECKED_STATUS EvalTargetExprCalls(DocPgResults *results) {
    YbgMemoryContext old;
    if (targets_.empty()) {
      return Status::OK();
    }
    int i = 0;
    assert(targets_.size() == results->size());
    assert(row_ctx_ != nullptr);
    YbgSetCurrentMemoryContext(row_ctx_, &old);
    for (DocPgEvalExprData target : targets_) {
      QLExprResult &result = results->at(i++);
      uint64_t datum;
      bool is_null;
      RETURN_NOT_OK(DocPgEvalExpr(target.first, expr_ctx_, &datum, &is_null));
      RETURN_NOT_OK(PgValueToPB(target.second.var_type,
                                datum,
                                is_null,
                                &result.Writer().NewValue()));
      VLOG(1) << "Target expression has been added";
    }
    YbgSetCurrentMemoryContext(old, nullptr);
    return Status::OK();
  }

  YbgMemoryContext mem_ctx_ = nullptr;
  YbgMemoryContext row_ctx_ = nullptr;
  YbgExprContext expr_ctx_ = nullptr;
  std::list<YbgPreparedExpr> where_clause_;
  std::list<DocPgEvalExprData> targets_;
  std::map<int, const DocPgVarRef> var_map_;

  friend class DocPgExprExecutor;
};

void DocPgExprExecutor::private_deleter::operator()(DocPgExprExecutor::Private* ptr) const {
  delete ptr;
}

//--------------------------------------------------------------------------------------------------

CHECKED_STATUS DocPgExprExecutor::AddColumnRef(const PgsqlColRefPB& column_ref) {
  if (private_.get() == nullptr) {
    private_.reset(new Private());
  }
  return private_->AddColumnRef(column_ref, schema_);
}

CHECKED_STATUS DocPgExprExecutor::AddWhereExpression(const PgsqlExpressionPB& ql_expr) {
  if (private_.get() == nullptr) {
    private_.reset(new Private());
  }
  return private_->PreparePgWhereExpr(ql_expr, schema_);
}

CHECKED_STATUS DocPgExprExecutor::AddTargetExpression(const PgsqlExpressionPB& ql_expr) {
  if (private_.get() == nullptr) {
    private_.reset(new Private());
  }
  return private_->PreparePgTargetExpr(ql_expr, schema_);
}

CHECKED_STATUS DocPgExprExecutor::Exec(const QLTableRow& table_row,
                                       DocPgResults *results,
                                       bool *match) {
  if (private_.get() == nullptr) {
    return Status::OK();
  }

  RETURN_NOT_OK(private_->PreparePgRowData(table_row));
  RETURN_NOT_OK(private_->EvalWhereExprCalls(match));
  if (*match)
    RETURN_NOT_OK(private_->EvalTargetExprCalls(results));
  return Status::OK();
}

}  // namespace docdb
}  // namespace yb

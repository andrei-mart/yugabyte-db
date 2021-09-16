//--------------------------------------------------------------------------------------------------
// Copyright (c) YugaByte, Inc.
//
// This module defines and executes expression-related operations in DocDB.
//--------------------------------------------------------------------------------------------------

#ifndef YB_DOCDB_DOC_PG_EXPR_H_
#define YB_DOCDB_DOC_PG_EXPR_H_

#include "yb/common/schema.h"
#include "yb/util/status.h"

namespace yb {
namespace docdb {

class DocPgExprExecutor {
 public:
  explicit DocPgExprExecutor(const Schema *schema) : schema_(schema) {}
  virtual ~DocPgExprExecutor() {}

  CHECKED_STATUS AddWhereExpression(const PgsqlExpressionPB& ql_expr);

  CHECKED_STATUS ExecWhereExpr(const QLTableRow& table_row, bool *result);

 protected:
  const Schema *schema_;

 private:
  class Private;
  struct private_deleter { void operator()(Private*) const; };
  std::unique_ptr<Private, private_deleter> private_;
};

} // namespace docdb
} // namespace yb


#endif // YB_DOCDB_DOC_PG_EXPR_H_

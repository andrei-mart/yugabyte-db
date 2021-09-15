/*--------------------------------------------------------------------------------------------------
 *
 * ybcplan.c
 *	  Utilities for YugaByte scan.
 *
 * Copyright (c) YugaByte, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 * src/backend/executor/ybcplan.c
 *
 *--------------------------------------------------------------------------------------------------
 */


#include "postgres.h"

#include "optimizer/ybcplan.h"
#include "access/htup_details.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/ybcExpr.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/plannodes.h"
#include "nodes/print.h"
#include "nodes/relation.h"
#include "utils/datum.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

#include "yb/yql/pggate/ybc_pggate.h"
#include "pg_yb_utils.h"

/*
 * Returns true if the following are all true:
 *  - is insert, update, or delete command.
 *  - only one target table.
 *  - there are no ON CONFLICT or WITH clauses.
 *  - source data is a VALUES clause with one value set.
 *  - all values are either constants or bind markers.
 *
 *  Additionally, during execution we will also check:
 *  - not in transaction block.
 *  - is a single-plan execution.
 *  - target table has no triggers.
 *  - target table has no indexes.
 *  And if all are true we will execute this op as a single-row transaction
 *  rather than a distributed transaction.
 */
static bool ModifyTableIsSingleRowWrite(ModifyTable *modifyTable)
{

	/* Support INSERT, UPDATE, and DELETE. */
	if (modifyTable->operation != CMD_INSERT &&
		modifyTable->operation != CMD_UPDATE &&
		modifyTable->operation != CMD_DELETE)
		return false;

	/* Multi-relation implies multi-shard. */
	if (list_length(modifyTable->resultRelations) != 1)
		return false;

	/* ON CONFLICT clause is not supported here yet. */
	if (modifyTable->onConflictAction != ONCONFLICT_NONE)
		return false;

	/* WITH clause is not supported here yet. */
	if (modifyTable->plan.initPlan != NIL)
		return false;

	/* Check the data source, only allow a values clause right now */
	if (list_length(modifyTable->plans) != 1)
		return false;

	switch nodeTag(linitial(modifyTable->plans))
	{
		case T_Result:
		{
			/* Simple values clause: one valueset (single row) */
			Result *values = (Result *)linitial(modifyTable->plans);
			ListCell *lc;
			foreach(lc, values->plan.targetlist)
			{
				TargetEntry *target = (TargetEntry *) lfirst(lc);
				if (!YbCanPushdownExpr(target->expr, NULL))
				{
					return false;
				}
			}
			break;
		}
		case T_ValuesScan:
		{
			/*
			 * Simple values clause: multiple valueset (multi-row).
			 * TODO: Eventually we could inspect hash key values to check
			 *       if single shard and optimize that.
			 *       ---
			 *       In this case we'd need some other way to explicitly filter out
			 *       updates involving primary key - right now we simply rely on
			 *       planner not setting the node to Result.
			 */
			return false;

		}
		default:
			/* Do not support any other data sources. */
			return false;
	}

	/* If all our checks passed return true */
	return true;
}

bool YBCIsSingleRowModify(PlannedStmt *pstmt)
{
	if (pstmt->planTree && IsA(pstmt->planTree, ModifyTable))
	{
		ModifyTable *node = castNode(ModifyTable, pstmt->planTree);
		return ModifyTableIsSingleRowWrite(node);
	}

	return false;
}

/*
 * Returns true if the following are all true:
 *  - is update or delete command.
 *  - source data is a Result node (meaning we are skipping scan and thus
 *    are single row).
 */
bool YBCIsSingleRowUpdateOrDelete(ModifyTable *modifyTable)
{
	/* Support UPDATE and DELETE. */
	if (modifyTable->operation != CMD_UPDATE &&
		modifyTable->operation != CMD_DELETE)
		return false;

	/* Should only have one data source. */
	if (list_length(modifyTable->plans) != 1)
		return false;

	/* Verify the single data source is a Result node. */
	if (!IsA(linitial(modifyTable->plans), Result))
		return false;

	return true;
}

/*
 * Returns true if provided Bitmapset of attribute numbers
 * matches the primary key attribute numbers of the relation.
 * Expects YBGetFirstLowInvalidAttributeNumber to be subtracted from attribute numbers.
 */
bool YBCAllPrimaryKeysProvided(Relation rel, Bitmapset *attrs)
{
	if (bms_is_empty(attrs))
	{
		/*
		 * If we don't explicitly check for empty attributes it is possible
		 * for this function to improperly return true. This is because in the
		 * case where a table does not have any primary key attributes we will
		 * use a hidden RowId column which is not exposed to the PG side, so
		 * both the YB primary key attributes and the input attributes would
		 * appear empty and would be equal, even though this is incorrect as
		 * the YB table has the hidden RowId primary key column.
		 */
		return false;
	}

	Bitmapset *primary_key_attrs = YBGetTablePrimaryKeyBms(rel);

	/* Verify the sets are the same. */
	return bms_equal(attrs, primary_key_attrs);
}

/*
 * Check to see whether a returning expression is supported for single-row modification.
 * For function, only immutable expression is supported.
 */
bool YBCIsSupportedSingleRowModifyReturningExpr(Expr *expr) {
	switch (nodeTag(expr))
	{
		case T_Const:
		case T_Var:
		{
			return true;
		}
		case T_RelabelType:
		{
			/*
			 * RelabelType is a "dummy" type coercion between two binary-
			 * compatible datatypes so we just recurse into its argument.
			 */
			RelabelType *rt = castNode(RelabelType, expr);
			return YBCIsSupportedSingleRowModifyReturningExpr(rt->arg);
		}
		case T_ArrayRef:
		{
			ArrayRef *array_ref = castNode(ArrayRef, expr);
			return YBCIsSupportedSingleRowModifyReturningExpr(array_ref->refexpr);
		}
		case T_FuncExpr:
		case T_OpExpr:
		{
			List         *args = NULL;
			ListCell     *lc = NULL;
			Oid          funcid = InvalidOid;
			HeapTuple    tuple = NULL;

			/* Get the function info. */
			if (IsA(expr, FuncExpr))
			{
				FuncExpr *func_expr = castNode(FuncExpr, expr);
				args = func_expr->args;
				funcid = func_expr->funcid;
			}
			else if (IsA(expr, OpExpr))
			{
				OpExpr *op_expr = castNode(OpExpr, expr);
				args = op_expr->args;
				funcid = op_expr->opfuncid;
			}

			tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for function %u", funcid);
			Form_pg_proc pg_proc = ((Form_pg_proc) GETSTRUCT(tuple));

			/*
			 * Only allow functions that cannot modify the database or do
			 * lookups.
			 */
			if (pg_proc->provolatile != PROVOLATILE_IMMUTABLE)
			{
				ReleaseSysCache(tuple);
				return false;
			}
			ReleaseSysCache(tuple);

			/* Checking all arguments are valid (stable). */
			foreach (lc, args) {
				Expr* expr = (Expr *) lfirst(lc);
				if (!YBCIsSupportedSingleRowModifyReturningExpr(expr))
					return false;
			}
			return true;
		}
		case T_CoerceViaIO:
		{
			CoerceViaIO *coerce_via_IO = castNode(CoerceViaIO, expr);
			return YBCIsSupportedSingleRowModifyReturningExpr(coerce_via_IO->arg);
		}
		case T_FieldSelect:
		{
			FieldSelect *field_select = castNode(FieldSelect, expr);
			return YBCIsSupportedSingleRowModifyReturningExpr(field_select->arg);
		}
		case T_RowExpr:
		{
			ListCell     *lc = NULL;

			RowExpr *row_expr = castNode(RowExpr, expr);
			foreach (lc, row_expr->args) {
				Expr* expr = (Expr *) lfirst(lc);
				if (!YBCIsSupportedSingleRowModifyReturningExpr(expr))
					return false;
			}
			return true;
		}
		case T_ArrayExpr:
		{
			ArrayExpr *array_expr = castNode(ArrayExpr, expr);
			ListCell     *lc = NULL;

			foreach (lc, array_expr->elements) {
				Expr* expr = (Expr *) lfirst(lc);
				if (!YBCIsSupportedSingleRowModifyReturningExpr(expr))
					return false;
			}
			return true;
		}
		default:
			break;
	}

	return false;
}

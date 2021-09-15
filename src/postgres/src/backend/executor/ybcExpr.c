/*--------------------------------------------------------------------------------------------------
 * ybcExpr.c
 *        Routines to construct YBC expression tree.
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
 * IDENTIFICATION
 *        src/backend/executor/ybcExpr.c
 *--------------------------------------------------------------------------------------------------
 */

#include <inttypes.h>

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "utils/datum.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "parser/parse_type.h"
#include "utils/lsyscache.h"
#include "commands/dbcommands.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "utils/syscache.h"
#include "utils/builtins.h"

#include "pg_yb_utils.h"
#include "executor/ybcExpr.h"
#include "catalog/yb_type.h"

YBCPgExpr YBCNewColumnRef(YBCPgStatement ybc_stmt, int16_t attr_num,
						  int attr_typid, int attr_collation,
						  const YBCPgTypeAttrs *type_attrs) {
	YBCPgExpr expr = NULL;
	const YBCPgTypeEntity *type_entity = YbDataTypeFromOidMod(attr_num, attr_typid);
	YBCPgCollationInfo collation_info;
	YBGetCollationInfo(attr_collation, type_entity, 0 /* datum */, true /* is_null */,
					   &collation_info);
	HandleYBStatus(YBCPgNewColumnRef(ybc_stmt, attr_num, type_entity,
									 collation_info.collate_is_valid_non_c,
									 type_attrs, &expr));
	return expr;
}

YBCPgExpr YBCNewConstant(YBCPgStatement ybc_stmt, Oid type_id, Oid collation_id,
						 Datum datum, bool is_null) {
	YBCPgExpr expr = NULL;
	const YBCPgTypeEntity *type_entity = YbDataTypeFromOidMod(InvalidAttrNumber, type_id);
	YBCPgCollationInfo collation_info;
	YBGetCollationInfo(collation_id, type_entity, datum, is_null, &collation_info);
	HandleYBStatus(YBCPgNewConstant(ybc_stmt, type_entity,
									collation_info.collate_is_valid_non_c,
									collation_info.sortkey,
									datum, is_null, &expr));
	return expr;
}

YBCPgExpr YBCNewConstantVirtual(YBCPgStatement ybc_stmt, Oid type_id, YBCPgDatumKind kind) {
	YBCPgExpr expr = NULL;
	const YBCPgTypeEntity *type_entity = YbDataTypeFromOidMod(InvalidAttrNumber, type_id);
	HandleYBStatus(YBCPgNewConstantVirtual(ybc_stmt, type_entity, kind, &expr));
	return expr;
}

Node *yb_expr_instantiate_params_mutator(Node *node, ParamListInfo paramLI)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Param))
	{
		Param *param = castNode(Param, node);
		ParamExternData *prm = NULL;
		ParamExternData prmdata;
		if (paramLI->paramFetch != NULL)
			prm = paramLI->paramFetch(paramLI, param->paramid,
									  true, &prmdata);
		else
			prm = &paramLI->params[param->paramid - 1];

		if (!OidIsValid(prm->ptype) ||
			prm->ptype != param->paramtype ||
			!(prm->pflags & PARAM_FLAG_CONST))
		{
			/* Planner should ensure this does not happen */
			elog(ERROR, "Invalid parameter: %s", nodeToString(param));
		}
		int16		typLen = 0;
		bool		typByVal = false;
		Datum		pval = 0;

		get_typlenbyval(param->paramtype, &typLen, &typByVal);
		if (prm->isnull || typByVal)
			pval = prm->value;
		else
			pval = datumCopy(prm->value, typByVal, typLen);

		return (Node *) makeConst(param->paramtype,
								  param->paramtypmod,
								  param->paramcollid,
								  (int) typLen,
								  pval,
								  prm->isnull,
								  typByVal);
	}
	return expression_tree_mutator(node,
								   yb_expr_instantiate_params_mutator,
								   (void *) paramLI);
}

Expr *YbExprInstantiateParams(Expr* expr, ParamListInfo paramLI)
{
	/* Fast-path if there are no params. */
	if (paramLI == NULL)
		return expr;

	return (Expr *) expression_tree_mutator((Node *) expr,
											yb_expr_instantiate_params_mutator,
											(void *) paramLI);
}

bool yb_can_pushdown_func(Oid funcid)
{
	HeapTuple		tuple;
	Form_pg_proc	pg_proc;

	if (!is_builtin_func(funcid))
	{
		return false;
	}

        /*                                                                                           
         * Check whether this function is on a list of hand-picked functions 
         * safe for pushdown.                   
         */                                                                                          
        for (int i = 0; i < yb_funcs_safe_for_pushdown_count; ++i)                                   
        {                                                                                            
                if (funcid == yb_funcs_safe_for_pushdown[i])                                         
                        return true;                                                                 
        }                                                                                            

	tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for function %u", funcid);
	pg_proc = (Form_pg_proc) GETSTRUCT(tuple);
	if (pg_proc->provolatile != PROVOLATILE_IMMUTABLE)
	{
		ReleaseSysCache(tuple);
		return false;
	}
	if (!YBCPgFindTypeEntity(pg_proc->prorettype) ||
		IsPolymorphicType(pg_proc->prorettype))
	{
		ReleaseSysCache(tuple);
		return false;
	}
	for (int i = 0; i < pg_proc->pronargs; i++)
	{
		Oid typid = pg_proc->proargtypes.values[i];
		if (!YBCPgFindTypeEntity(typid) || IsPolymorphicType(typid))
		{
			ReleaseSysCache(tuple);
			return false;
		}
	}
	ReleaseSysCache(tuple);
	return true;
}

bool YbCanPushdownExpr(Expr *pg_expr, List **params)
{
	switch (pg_expr->type)
	{
		case T_FuncExpr:
		case T_OpExpr:
		{
			Oid				funcid;
			List		   *args = NIL;
			ListCell	   *lc;

			/* Get the (underlying) function info. */
			if (IsA(pg_expr, FuncExpr))
			{
				FuncExpr *func_expr = castNode(FuncExpr, pg_expr);
				funcid = func_expr->funcid;
				args = func_expr->args;
			}
			else if (IsA(pg_expr, OpExpr))
			{
				OpExpr *op_expr = castNode(OpExpr, pg_expr);
				funcid = op_expr->opfuncid;
				args = op_expr->args;
			}

			if (!yb_can_pushdown_func(funcid))
			{
				return false;
			}

			foreach(lc, args)
			{
				Expr *arg = (Expr *) lfirst(lc);
				if (!YbCanPushdownExpr(arg, params))
				{
					return false;
				}
			}
			return true;
		}
		case T_RelabelType:
		{
			RelabelType *rt = castNode(RelabelType, pg_expr);
			return YbCanPushdownExpr(rt->arg, params);
		}
		case T_NullTest:
		{
			NullTest *nt = castNode(NullTest, pg_expr);
			return !nt->argisrow && YbCanPushdownExpr(nt->arg, params);
		}
		case T_BoolExpr:
		{
			BoolExpr *be = castNode(BoolExpr, pg_expr);
			ListCell *lc;
			foreach(lc, be->args)
			{
				Expr *arg = (Expr *) lfirst(lc);
				if (!YbCanPushdownExpr(arg, params))
				{
					return false;
				}
			}
			return true;
		}
		case T_Const:
		{
			return true;
		}
		case T_Param:
		{
			Param *p = castNode(Param, pg_expr);
			return p->paramkind == PARAM_EXTERN &&
					YBCPgFindTypeEntity(p->paramtype);
		}
		case T_Var:
		{
			Var		   *var_expr = castNode(Var, pg_expr);
			AttrNumber	attno = var_expr->varattno;
			if (!YBCPgFindTypeEntity(var_expr->vartype))
			{
				return false;
			}
			if (params)
			{
				ListCell   *lc;
				YBExprParamDesc *param;

				foreach(lc, *params)
				{
					param =	(YBExprParamDesc *) lfirst(lc);
					if (param->attno == attno)
					{
						return true;
					}
				}

				param =	(YBExprParamDesc *) palloc(sizeof(YBExprParamDesc));
				param->attno = attno;
				param->typid = var_expr->vartype;
				param->typmod = var_expr->vartypmod;
				param->collid = var_expr->varcollid;
				*params = lappend(*params, param);
			}
			return true;
		}
		default:
		{
			return false;
		}
	}
}

YBCPgExpr YBCNewEvalSingleParamExprCall(YBCPgStatement ybc_stmt,
										Expr *pg_expr,
										int32_t attno,
										int32_t typid,
										int32_t typmod,
										int32_t collid)
{
	YBExprParamDesc param;
	param.attno = attno;
	param.typid = typid;
	param.typmod = typmod;
	param.collid = collid;
	return YBCNewEvalExprCall(ybc_stmt, pg_expr, list_make1(&param));
}

/*
 * Assuming the first param is the target column, therefore representing both
 * the first argument and return type.
 */
YBCPgExpr YBCNewEvalExprCall(YBCPgStatement ybc_stmt,
							 Expr *pg_expr,
							 List *params)
{
	ListCell *lc;
	YBCPgExpr ybc_expr = NULL;
	YBExprParamDesc *param = (YBExprParamDesc *) linitial(params);
	const YBCPgTypeEntity *type_ent = YbDataTypeFromOidMod(InvalidAttrNumber, param->typid);
	YBCPgCollationInfo collation_info;
	YBGetCollationInfo(param->collid, type_ent, 0 /* Datum */, true /* is_null */,
					   &collation_info);
	YBCPgNewEvalExpr(ybc_stmt, type_ent, collation_info.collate_is_valid_non_c, &ybc_expr);

	Datum expr_datum = CStringGetDatum(nodeToString(pg_expr));
	YBCPgExpr expr = YBCNewConstant(ybc_stmt, CSTRINGOID, C_COLLATION_OID,
									expr_datum , /* IsNull */ false);
	YBCPgOperatorAppendArg(ybc_expr, expr);

	/*
	 * Adding the column type ids and mods to the message since we only have the YQL types in the
	 * DocDB Schema.
	 * TODO(mihnea): Eventually DocDB should know the full YSQL/PG types and we can remove this.
	 */
	foreach(lc, params) {
		param = (YBExprParamDesc *) lfirst(lc);
		Datum attno = Int32GetDatum(param->attno);
		YBCPgExpr attno_expr = YBCNewConstant(ybc_stmt, INT4OID, InvalidOid, attno, false);
		YBCPgOperatorAppendArg(ybc_expr, attno_expr);

		Datum typid = Int32GetDatum(param->typid);
		YBCPgExpr typid_expr = YBCNewConstant(ybc_stmt, INT4OID, InvalidOid, typid, false);
		YBCPgOperatorAppendArg(ybc_expr, typid_expr);

		Datum typmod = Int32GetDatum(param->typmod);
		YBCPgExpr typmod_expr = YBCNewConstant(ybc_stmt, INT4OID, InvalidOid, typmod, false);
		YBCPgOperatorAppendArg(ybc_expr, typmod_expr);
	}
	return ybc_expr;
}

/* ------------------------------------------------------------------------- */
/*  Execution output parameter from Yugabyte */
YbPgExecOutParam *YbCreateExecOutParam()
{
	YbPgExecOutParam *param = makeNode(YbPgExecOutParam);
	param->bfoutput = makeStringInfo();

	/* Not yet used */
	param->status = makeStringInfo();
	param->status_code = 0;

	return param;
}

void YbWriteExecOutParam(YbPgExecOutParam *param, const YbcPgExecOutParamValue *value) {
	appendStringInfoString(param->bfoutput, value->bfoutput);

	/* Not yet used */
	if (value->status)
	{
		appendStringInfoString(param->status, value->status);
		param->status_code = value->status_code;
	}
}

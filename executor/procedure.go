// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/sqlexec"
)

// ProcedureExec create 、drop procedure exec plan.
type ProcedureExec struct {
	baseExecutor
	done          bool
	is            infoschema.InfoSchema
	Statement     ast.StmtNode
	oldSqlMod     string
	procedureInfo *plannercore.ProcedurebodyInfo
	Plan          *plannercore.ProcedurePlan
	outParam      map[string]string
	builder       *plannercore.PlanBuilder
}

// buildCreateProcedure Create a new stored procedure executor.
func (b *executorBuilder) buildCreateProcedure(v *plannercore.CreateProcedure) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity
	e := &ProcedureExec{
		baseExecutor: base,
		Statement:    v.ProcedureInfo,
		is:           b.is,
		done:         false,
		outParam:     make(map[string]string, 10),
	}
	return e
}

// buildCreateProcedure Drop a new stored procedure executor.
func (b *executorBuilder) buildDropProcedure(v *plannercore.DropProcedure) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity
	e := &ProcedureExec{
		baseExecutor: base,
		Statement:    v.Procedure,
		is:           b.is,
		done:         false,
		outParam:     make(map[string]string, 10),
	}
	return e
}

func (e *ProcedureExec) autoNewTxn() bool {
	switch e.Statement.(type) {
	case *ast.ProcedureInfo:
		return true
	case *ast.DropProcedureStmt:
		return true
	}
	return false
}
func (e *ProcedureExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}
	// implicit commit
	if e.autoNewTxn() {
		if err = sessiontxn.NewTxnInStmt(ctx, e.ctx); err != nil {
			return err
		}
		defer func() { e.ctx.GetSessionVars().SetInTxn(false) }()
	}
	switch x := e.Statement.(type) {
	case *ast.ProcedureInfo:
		err = e.createProcedure(ctx, x)
	case *ast.DropProcedureStmt:
		err = e.dropProcedure(ctx, x)
	case *ast.CallStmt:
		err = e.callProcedure(ctx, x)
	}

	e.done = true
	return err
}

// procedureExistsInternal Query whether the stored procedure exists.
func procedureExistsInternal(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name string, db string) (bool, error) {
	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, `SELECT * FROM %n.%n WHERE route_schema=%? AND name=%? AND type= 'PROCEDURE' FOR UPDATE;`, mysql.SystemDB, mysql.Routines, db, name)
	recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return false, err
	}
	req := recordSet.NewChunk(nil)
	err = recordSet.Next(ctx, req)
	var rows int = 0
	if err == nil {
		rows = req.NumRows()
	}
	errClose := recordSet.Close()
	if errClose != nil {
		return false, errClose
	}
	return rows > 0, err
}

// getProcedureinfo read stored procedure content.
func getProcedureinfo(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name string, db string) (*plannercore.ProcedurebodyInfo, error) {
	sql := new(strings.Builder)
	//names = []string{"Procedure", "sql_mode", "Create Procedure", "character_set_client", "collation_connection", "Database Collation"}
	sqlexec.MustFormatSQL(sql, "select name, sql_mode ,definition_utf8,parameter_str,character_set_client, connection_collation,")
	sqlexec.MustFormatSQL(sql, "schema_collation from %n.%n where route_schema = %?  and name = %? and type = 'PROCEDURE' ", mysql.SystemDB, mysql.Routines, db, name)
	recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return nil, err
	}
	if recordSet != nil {
		defer recordSet.Close()
	}

	rows, err := sqlexec.DrainRecordSet(ctx, recordSet, 3)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrSpDoesNotExist.GenWithStackByArgs("PROCEDURE", name)
	}
	if len(rows) != 1 {
		return nil, errors.New("Multiple stored procedures found in table " + mysql.Routines)
	}
	procedurebodyInfo := &plannercore.ProcedurebodyInfo{}
	procedurebodyInfo.Name = rows[0].GetString(0)
	procedurebodyInfo.Procedurebody = " CREATE PROCEDURE `" + rows[0].GetString(0) + "`(" + rows[0].GetString(3) + ") " + rows[0].GetString(2)
	procedurebodyInfo.SqlMode = rows[0].GetSet(1).String()
	procedurebodyInfo.CharacterSetClient = rows[0].GetString(4)
	procedurebodyInfo.CollationConnection = rows[0].GetString(5)
	procedurebodyInfo.ShemaCollation = rows[0].GetString(6)
	return procedurebodyInfo, nil
}

func (e *ShowExec) getRowsProcedure(ctx context.Context, sqlExecutor sqlexec.SQLExecutor) error {
	sql := new(strings.Builder)
	var (
		fieldPatternsLike collate.WildcardPattern
		fieldFilter       string
	)
	if e.Extractor != nil {
		fieldFilter = e.Extractor.Field()
		fieldPatternsLike = e.Extractor.FieldPatternLike()
	}
	//names = []string{"Db", "Name", "Type", "Definer", "Modified", "Created", "Security_type", "Comment", "character_set_client", "collation_connection", "Database Collation"}
	sqlexec.MustFormatSQL(sql, "select route_schema, name, type, definer ,last_altered,created,security_type, comment,")
	sqlexec.MustFormatSQL(sql, "character_set_client, connection_collation,schema_collation from %n.%n ", mysql.SystemDB, mysql.Routines)
	recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return err
	}
	if recordSet != nil {
		defer recordSet.Close()
	}

	rows, err := sqlexec.DrainRecordSet(ctx, recordSet, 1024)
	if err != nil {
		return err
	}

	for _, row := range rows {
		if fieldFilter != "" && row.GetString(1) != fieldFilter {
			continue
		} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(row.GetString(1)) {
			continue
		}
		e.appendRow([]interface{}{row.GetString(0), row.GetString(1), row.GetEnum(2).String(), row.GetString(3), row.GetTime(4), row.GetTime(5), row.GetEnum(6).String(),
			row.GetString(7), row.GetString(8), row.GetString(9), row.GetString(10)})
	}
	return nil
}

func (e *ProcedureExec) newEmptyVars(ctx context.Context, name string, tp *types.FieldType) error {
	var datum types.Datum
	switch tp.GetType() {
	case mysql.TypeTiny, mysql.TypeInt24, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
		datum = types.NewDatum(0)
	case mysql.TypeTimestamp, mysql.TypeDatetime:
		datum = types.NewDatum("2023-02-04 11:11:16")
	case mysql.TypeDate:
		datum = types.NewDatum("2023-02-04")
	case mysql.TypeDuration:
		datum = types.NewDatum("11:11:16")
	case mysql.TypeYear:
		datum = types.NewDatum("2023")
	case mysql.TypeJSON:
		datum = types.NewDatum("null")
	case mysql.TypeEnum:
		datum = types.NewDatum(tp.GetElem(0))
	case mysql.TypeString:
		datum = types.NewDatum("")
	default:
		datum = types.NewDatum("")
	}
	varVar, err := datum.Clone().ConvertTo(e.ctx.GetSessionVars().StmtCtx, tp)
	if err != nil {
		return err
	}
	err = e.ctx.GetSessionVars().NewProcedureVariable(name, varVar, tp)
	if err != nil {
		return err
	}
	return nil
}

func (e *ProcedureExec) checkProcedureVars(ctx context.Context, s *ast.ProcedureInfo) error {
	nameMap := make(map[string]struct{}, len(s.ProcedureParam))
	for _, param := range s.ProcedureParam {
		_, ok := nameMap[param.ParamName]
		if ok {
			return ErrSpDupParam.GenWithStackByArgs(param.ParamName)
		}
		nameMap[param.ParamName] = struct{}{}
		err := e.newEmptyVars(ctx, param.ParamName, param.ParamType)
		if err != nil {
			return err
		}
	}
	err := e.checkProcedureExists(ctx, s.ProcedureBody)
	return err
}

func (e *ProcedureExec) checkProcedureExists(ctx context.Context, stmt ast.StmtNode) error {
	switch x := stmt.(type) {
	case *ast.ProcedureBlock:
		vars := x.ProcedureVars
		for _, varInfo := range vars {
			for _, name := range varInfo.DeclNames {
				err := e.newEmptyVars(ctx, name, varInfo.DeclType)
				if err != nil {
					return err
				}
			}
		}
		stmts := x.ProcedureProcStmts
		for _, stmt := range stmts {
			err := e.checkProcedureExists(ctx, stmt)
			if err != nil {
				return err
			}
		}
	case *ast.SetStmt:
		ast.SetFlag(stmt)
		err := e.execDefalutStmt(ctx, x)
		if err != nil {
			return err
		}
	}
	return nil
}

// createProcedure Save stored procedure content.
func (e *ProcedureExec) createProcedure(ctx context.Context, s *ast.ProcedureInfo) error {
	e.ctx.GetSessionVars().SetInCallProcedure()
	e.ctx.GetSessionVars().NewProcedureVariables()
	defer func() {
		e.ctx.GetSessionVars().OutCallProcedure()
		e.ctx.GetSessionVars().ClearProcedureVariable()
	}()

	procedurceName := s.ProcedureName.Name.String()
	procedurceSchema := s.ProcedureName.Schema
	dbInfo, ok := e.is.SchemaByName(procedurceSchema)
	if !ok {
		return ErrBadDB.GenWithStackByArgs(procedurceSchema)
	}

	err := e.checkProcedureVars(ctx, s)
	if err != nil {
		return err
	}
	parameterStr := s.ProcedureParamStr

	bodyStr := s.ProcedureBody.Text()
	sqlMod := variable.GetSysVar(variable.SQLModeVar)
	if sqlMod == nil {
		return errors.New("unknown system var " + variable.SQLModeVar)
	}
	chs := variable.GetSysVar(variable.CharacterSetClient)
	if chs == nil {
		return errors.New("unknown system var " + variable.CharacterSetClient)
	}
	var userInfo string
	if e.ctx.GetSessionVars().User != nil {
		u := e.ctx.GetSessionVars().User.AuthUsername
		h := e.ctx.GetSessionVars().User.AuthHostname
		userInfo = u + "@" + h
	}

	_, sessionCollation := e.ctx.GetSessionVars().GetCharsetInfo()
	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, "insert into mysql.routines (route_schema, name, type, definition, definition_utf8, parameter_str,")
	sqlexec.MustFormatSQL(sql, "is_deterministic, sql_data_access, security_type, definer, sql_mode, character_set_client, connection_collation, schema_collation, created, last_altered, comment, ")
	sqlexec.MustFormatSQL(sql, " external_language) values (%?, %?, 'PROCEDURE', %?, %?, %?, 0, 'CONTAINS SQL', 'DEFINER', %?, %?, %?, %?, %?, now(), now(),  '', 'SQL') ;", procedurceSchema.String(), procedurceName,
		bodyStr, bodyStr, parameterStr, userInfo, sqlMod.Value, chs.Value, sessionCollation, dbInfo.Collate)
	sysSession, err := e.getSysSession()
	if err != nil {
		return err
	}
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	defer e.releaseSysSession(internalCtx, sysSession)
	sqlExecutor := sysSession.(sqlexec.SQLExecutor)
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}
	exists, err := procedureExistsInternal(internalCtx, sqlExecutor, procedurceName, procedurceSchema.L)
	if err != nil {
		return err
	}
	if exists {
		err = ErrSpAlreadyExists.GenWithStackByArgs("PROCEDURE", procedurceName)
		if s.IfNotExists {
			e.ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
		return err
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return err
	}
	return nil
}

// fetchShowCreateProcdure query stored procedure.
func (e *ShowExec) fetchShowCreateProcdure(ctx context.Context) error {
	if e.Procedure.Schema.O == "" {
		e.Procedure.Schema = model.NewCIStr(e.ctx.GetSessionVars().CurrentDB)
	}
	_, ok := e.is.SchemaByName(e.Procedure.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(e.DBName.O)
	}
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	sysSession, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer e.releaseSysSession(internalCtx, sysSession)
	sqlExecutor := sysSession.(sqlexec.SQLExecutor)
	procedureInfo, err := getProcedureinfo(internalCtx, sqlExecutor, e.Procedure.Name.String(), e.Procedure.Schema.O)
	if err != nil {
		return err
	}
	//names = []string{"Procedure", "sql_mode", "Create Procedure", "character_set_client", "collation_connection", "Database Collation"}
	e.appendRow([]interface{}{procedureInfo.Name, procedureInfo.SqlMode, procedureInfo.Procedurebody, procedureInfo.CharacterSetClient,
		procedureInfo.CollationConnection, procedureInfo.ShemaCollation})
	return nil
}

func (e *ShowExec) fetchShowProcedureStatus(ctx context.Context) error {
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	sysSession, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer e.releaseSysSession(internalCtx, sysSession)
	sqlExecutor := sysSession.(sqlexec.SQLExecutor)
	err = e.getRowsProcedure(internalCtx, sqlExecutor)
	if err != nil {
		return err
	}
	//names = []string{"Db", "Name", "Type", "Definer", "Modified", "Created", "Security_type", "Comment",
	//"character_set_client", "collation_connection", "Database Collation"}
	return nil
}

// dropProcedure delete stored procedure.
func (e *ProcedureExec) dropProcedure(ctx context.Context, s *ast.DropProcedureStmt) error {
	procedurceName := s.ProcedureName.Name.String()
	procedurceSchema := s.ProcedureName.Schema
	_, ok := e.is.SchemaByName(procedurceSchema)
	if !ok {
		return ErrBadDB.GenWithStackByArgs(procedurceSchema)
	}
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	sysSession, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer e.releaseSysSession(internalCtx, sysSession)
	sqlExecutor := sysSession.(sqlexec.SQLExecutor)
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}
	exists, err := procedureExistsInternal(internalCtx, sqlExecutor, procedurceName, procedurceSchema.String())
	if err != nil {
		return err
	}
	if !exists {
		err = ErrSpDoesNotExist.GenWithStackByArgs("PROCEDURE", procedurceName)
		if s.IfExists {
			e.ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}
	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, "delete from %n.%n where route_schema = %?  and name = %? and type = 'PROCEDURE' ", mysql.SystemDB,
		mysql.Routines, procedurceSchema.String(), procedurceName)
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, sql.String()); err != nil {
		return err
	}
	if _, err := sqlExecutor.ExecuteInternal(internalCtx, "commit"); err != nil {
		return err
	}
	return nil
}

// buildCallProcedure generate the execution plan of the call.
func (b *executorBuilder) buildCallProcedure(v *plannercore.CallStmt) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity
	e := &ProcedureExec{
		baseExecutor:  base,
		Statement:     v.Callstmt,
		is:            b.is,
		done:          false,
		oldSqlMod:     v.OldSqlMod,
		procedureInfo: v.ProcedureInfo,
		Plan:          v.Plan,
		outParam:      make(map[string]string, 10),
	}
	return e
}

// parseNode execute ProcedureBaseBody.
func (e *ProcedureExec) parseNode(ctx context.Context, node plannercore.ProcedureBaseBody) (err error) {
	var nameList []string
	switch node.(type) {
	case *plannercore.ProcedureBlock:
		for _, pvar := range node.(*plannercore.ProcedureBlock).Vars {
			names, err := e.createVariable(ctx, pvar)
			if err != nil {
				return err
			}
			nameList = append(nameList, names...)
		}
		defer func() {
			for id := len(nameList) - 1; id >= 0; id-- {
				err = e.ctx.GetSessionVars().DeleteProcedureVariable(nameList[id], true)
			}
		}()
		for _, stmt := range node.(*plannercore.ProcedureBlock).ProcedureProcStmts {
			err = e.parseNode(ctx, stmt)
			if err != nil {
				return err
			}
		}

	//to do:add logical structure.
	case *plannercore.ProcedureSQL:
		err := e.execNode(ctx, node.(*plannercore.ProcedureSQL).Stmt)
		if err != nil {
			return err
		}
	default:
		return errors.Errorf("Call procedure unsupport node %v", node)
	}
	return nil
}

// getDarumVar get expr result.
func (e *ProcedureExec) getDarumVar(sqlType *types.FieldType, defaultVar expression.Expression) (types.Datum, error) {
	var datum types.Datum
	var err error
	if defaultVar == nil {
		datum = types.NewDatum("")
		return datum, nil
	} else {
		datum, err = defaultVar.Eval(chunk.Row{})
		if err != nil {
			return datum, err
		}
	}

	newdatum, err := datum.Clone().ConvertTo(e.ctx.GetSessionVars().StmtCtx, sqlType)
	if err != nil {
		return datum, err
	}
	return newdatum, nil
}

// createVariable Create stored procedure internal variable.
func (e *ProcedureExec) createVariable(ctx context.Context, pval *plannercore.ProcedurebodyVal) ([]string, error) {
	datum, err := e.getDarumVar(pval.DeclType, pval.DeclDefault)
	if err != nil {
		return nil, err
	}
	for _, name := range pval.DeclNames {
		if pval.DeclDefault == nil {
			err = e.newEmptyVars(ctx, name, pval.DeclType)
			if err != nil {
				return nil, err
			}
		} else {
			err = e.ctx.GetSessionVars().NewProcedureVariable(name, datum, pval.DeclType)
			if err != nil {
				return nil, err
			}
		}
	}
	return pval.DeclNames, nil
}

// realizeFunction implement stored procedure execution.
func (e *ProcedureExec) realizeFunction(ctx context.Context, node *plannercore.ProcedureBlock) (err error) {
	var nameList []string
	for _, pvar := range node.Vars {
		names, err := e.createVariable(ctx, pvar)
		if err != nil {
			return err
		}
		nameList = append(nameList, names...)
	}
	defer func() {
		for id := len(nameList) - 1; id >= 0; id-- {
			err = e.ctx.GetSessionVars().DeleteProcedureVariable(nameList[id], true)
		}
	}()
	for _, stmt := range node.ProcedureProcStmts {
		err := e.parseNode(ctx, stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

// execWithResult Execute sql with results.
func (e *ProcedureExec) execWithResult(ctx context.Context, node ast.StmtNode) error {
	err := e.ctx.GetSessionExec().MultiHanldeNodeWithResult(ctx, node)
	if err != nil {
		return err
	}
	return nil
}

// execDefalutStmt Execute sql without results.
func (e *ProcedureExec) execDefalutStmt(ctx context.Context, node ast.StmtNode) error {
	err := e.ctx.GetSessionExec().MultiHanldeNode(ctx, node)
	if err != nil {
		return err
	}
	return nil
}

// execNode implement node execution.
func (e *ProcedureExec) execNode(ctx context.Context, node ast.StmtNode) error {
	ast.SetFlag(node)
	switch x := node.(type) {
	case *ast.SelectStmt:
		if x.SelectIntoOpt != nil && x.SelectIntoOpt.Tp == ast.SelectIntoOutfile {
			err := e.execDefalutStmt(ctx, node)
			if err != nil {
				return err
			}

		} else {
			err := e.execWithResult(ctx, node)
			if err != nil {
				return err
			}
		}

	case *ast.ExplainStmt:
		err := e.execWithResult(ctx, node)
		if err != nil {
			return err
		}

	case *ast.SetOprStmt:
		err := e.execWithResult(ctx, node)
		if err != nil {
			return err
		}
	// TODO: add logic and data
	default:
		err := e.execDefalutStmt(ctx, node)
		if err != nil {
			return err
		}
	}
	return nil
}

// inParam handle input parameters.
func (e *ProcedureExec) inParam(ctx context.Context, param *plannercore.ProcedureParameterVal, inName string) error {
	datum, ok := e.ctx.GetSessionVars().GetUserVarVal(inName)
	if !ok {
		datum = types.NewDatum("")
	}
	newdatum, err := datum.Clone().ConvertTo(e.ctx.GetSessionVars().StmtCtx, param.DeclType)
	if err != nil {
		return err
	}
	err = e.ctx.GetSessionVars().NewProcedureVariable(param.DeclName, newdatum, param.DeclType)
	if err != nil {
		return err
	}
	return nil
}

// callParam handle store procedure parameters.
func (e *ProcedureExec) callParam(ctx context.Context, s *ast.CallStmt) error {
	for i, param := range e.Plan.ProcedureParam {
		switch s.Procedure.Args[i].(type) {
		case *ast.VariableExpr:
			name := s.Procedure.Args[i].(*ast.VariableExpr).Name
			if (param.ParamType == ast.MODE_IN) || (param.ParamType == ast.MODE_INOUT) {
				err := e.inParam(ctx, param, name)
				if err != nil {
					return err
				}
			}
			if param.ParamType == ast.MODE_INOUT {
				_, ok := e.outParam[param.DeclName]
				if ok {
					return ErrSpDupParam.GenWithStackByArgs(param.DeclName)
				}
				e.outParam[param.DeclName] = name
			}
			if param.ParamType == ast.MODE_OUT {
				err := e.newEmptyVars(ctx, param.DeclName, param.DeclType)
				if err != nil {
					return err
				}
				e.outParam[param.DeclName] = name
			}

		// to do: support pseudo-variable in BEFORE trigger
		default:
			datum, err := e.getDarumVar(param.DeclType, param.DeclInput)
			if err != nil {
				return err
			}
			if param.ParamType == ast.MODE_IN {
				err = e.ctx.GetSessionVars().NewProcedureVariable(param.DeclName, datum, param.DeclType)
				if err != nil {
					return err
				}
			}
			if (param.ParamType == ast.MODE_OUT) || (param.ParamType == ast.MODE_INOUT) {
				return ErrSpNotVarArg.GenWithStackByArgs(i+1, s.Procedure.Schema.String()+"."+s.Procedure.FnName.String())
			}
		}
	}
	return nil
}

// outParams handle out parameters.
func (e *ProcedureExec) outParams() error {
	for key, v := range e.outParam {
		varType, datum, notFind, err := e.ctx.GetSessionVars().GetProcedureVariable(key)
		if err != nil {
			return err
		}
		if notFind {
			continue
		}
		e.ctx.GetSessionVars().SetUserVarVal(v, datum)
		e.ctx.GetSessionVars().SetUserVarType(v, varType)
	}
	return nil
}

func (e *ProcedureExec) callProcedure(ctx context.Context, s *ast.CallStmt) error {
	defer func() {
		_ = e.ctx.GetSessionVars().SetSystemVar(variable.SQLModeVar, e.oldSqlMod)
	}()
	mutliStateModeSave := variable.GetSysVar(variable.TiDBMultiStatementMode)
	variable.SetSysVar(variable.TiDBMultiStatementMode, "ON")
	clientCapabilitySave := e.ctx.GetSessionVars().ClientCapability
	e.ctx.GetSessionVars().ClientCapability = (e.ctx.GetSessionVars().ClientCapability | mysql.ClientMultiStatements)
	e.ctx.GetSessionVars().SetInCallProcedure()
	e.ctx.GetSessionVars().NewProcedureVariables()
	defer func() {
		e.ctx.GetSessionVars().ClientCapability = clientCapabilitySave
		variable.SetSysVar(variable.TiDBMultiStatementMode, mutliStateModeSave.Value)
		e.ctx.GetSessionVars().OutCallProcedure()
		e.ctx.GetSessionVars().ClearProcedureVariable()
	}()
	sc := e.ctx.GetSessionVars().StmtCtx
	vars := e.ctx.GetSessionVars()
	sc.TruncateAsWarning = !vars.StrictSQLMode
	sc.DividedByZeroAsWarning = !vars.StrictSQLMode
	sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
	sc.IgnoreZeroInDate = !vars.SQLMode.HasNoZeroInDateMode() || !vars.SQLMode.HasNoZeroDateMode() || !vars.StrictSQLMode || sc.AllowInvalidDate
	err := e.callParam(ctx, s)
	if err != nil {
		return err
	}
	switch x := e.Plan.Procedurebody.(type) {
	case *plannercore.ProcedureBlock:
		err := e.realizeFunction(ctx, x)
		if err != nil {
			return err
		}
	case *plannercore.ProcedureSQL:
		err := e.parseNode(ctx, x)
		if err != nil {
			return err
		}
	default:
		return errors.Errorf("Call procedure unsupport node %v", e.Plan.Procedurebody)
	}
	err = e.outParams()
	if err != nil {
		return err
	}
	return nil
}

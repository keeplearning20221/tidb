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
	"bytes"
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

type ProcedureExec struct {
	baseExecutor
	done          bool
	is            infoschema.InfoSchema
	Statement     ast.StmtNode
	oldSqlMod     string
	procedureInfo *plannercore.ProcedurebodyInfo
	Plan          *plannercore.ProcedurePlan
}

func (b *executorBuilder) buildCreateProcedure(v *plannercore.CreateProcedure) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity
	e := &ProcedureExec{
		baseExecutor: base,
		Statement:    v.ProcedureInfo,
		is:           b.is,
		done:         false,
	}
	return e
}

func (b *executorBuilder) buildDropProcedure(v *plannercore.DropProcedure) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity
	e := &ProcedureExec{
		baseExecutor: base,
		Statement:    v.Procedure,
		is:           b.is,
		done:         false,
	}
	return e
}

func (e *ProcedureExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}
	if err = sessiontxn.NewTxnInStmt(ctx, e.ctx); err != nil {
		return err
	}
	defer func() { e.ctx.GetSessionVars().SetInTxn(false) }()
	switch x := e.Statement.(type) {
	case *ast.ProcedureInfo:
		err = e.CreateProcedure(ctx, x)
	case *ast.DropProcedureStmt:
		err = e.DropProcedure(ctx, x)
	case *ast.CallStmt:
		err = e.CallProcedure(ctx, x)
	}

	e.done = true
	return err
}

// use the same internal executor to read within the same transaction
func procedureExistsInternal(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name string, db string) (bool, error) {
	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, `SELECT * FROM %n.%n WHERE route_schema=%? AND name=%? AND type= 'PROCEDURE' FOR UPDATE;`, mysql.SystemDB, "routines", db, name)
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

func getProcedureinfo(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name string, db string) (*plannercore.ProcedurebodyInfo, error) {
	sql := new(strings.Builder)
	//names = []string{"Procedure", "sql_mode", "Create Procedure", "character_set_client", "collation_connection", "Database Collation"}
	sqlexec.MustFormatSQL(sql, "select name, sql_mode ,definition_utf8,parameter_str,character_set_client, connection_collation,")
	sqlexec.MustFormatSQL(sql, "schema_collation from %n.%n where route_schema = %?  and name = %? and type = 'PROCEDURE' ", mysql.SystemDB, mysql.Routines, db, name)
	l := sql.String()
	recordSet, err := sqlExecutor.ExecuteInternal(ctx, l)
	if err != nil {
		return nil, err
	}
	defer recordSet.Close()
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
	procedurebodyInfo.Procedurebody = " CREATE PROCEDURE " + name + "(" + rows[0].GetString(3) + ") \n" + rows[0].GetString(2)
	procedurebodyInfo.SqlMode = rows[0].GetSet(1).String()
	procedurebodyInfo.CharacterSetClient = rows[0].GetString(4)
	procedurebodyInfo.CollationConnection = rows[0].GetString(5)
	procedurebodyInfo.ShemaCollation = rows[0].GetString(6)
	return procedurebodyInfo, nil
}

func (e *ProcedureExec) CreateProcedure(ctx context.Context, s *ast.ProcedureInfo) error {
	e.ctx.GetSessionVars().SetInCallProcedure()
	defer e.ctx.GetSessionVars().OutCallProcedure()
	e.ctx.GetSessionVars().NewProcedureVariables()
	defer e.ctx.GetSessionVars().ClearProcedureVariable()

	procedurceName := s.ProcedureName.Name.L
	procedurceSchema := s.ProcedureName.Schema
	dbInfo, ok := e.is.SchemaByName(procedurceSchema)
	if !ok {
		return ErrBadDB.GenWithStackByArgs(procedurceSchema)
	}
	var buf bytes.Buffer
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)
	for i, procedureParam := range s.ProcedureParam {
		if i > 0 {
			restoreCtx.WriteKeyWord(",")
		}
		err := procedureParam.Restore(restoreCtx)
		if err != nil {
			return err
		}
	}
	parameterStr := buf.String()
	buf.Reset()

	err := s.ProcedureBody.Restore(restoreCtx)
	if err != nil {
		return err
	}
	bodyStr := buf.String()
	sqlMod := variable.GetSysVar(variable.SQLModeVar)
	if sqlMod == nil {
		return errors.New("unknown system var " + variable.SQLModeVar)
	}
	chs := variable.GetSysVar(variable.CharacterSetClient)
	if chs == nil {
		return errors.New("unknown system var " + variable.CharacterSetClient)
	}
	u := e.ctx.GetSessionVars().User.AuthUsername
	h := e.ctx.GetSessionVars().User.AuthHostname
	userInfo := u + "@" + h
	_, sessionCollation := e.ctx.GetSessionVars().GetCharsetInfo()
	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, "insert into mysql.routines (route_schema, name, type, definition, definition_utf8, parameter_str,")
	sqlexec.MustFormatSQL(sql, "is_deterministic, sql_data_access, security_type, definer, sql_mode, character_set_client, connection_collation, schema_collation, created, last_altered, comment, ")
	sqlexec.MustFormatSQL(sql, " external_language) values (%?, %?, 'PROCEDURE', %?, %?, %?, 0, 'CONTAINS SQL', 'DEFINER', %?, %?, %?, %?, %?, now(), now(),  '', 'SQL') ;", procedurceSchema.L, procedurceName,
		bodyStr, bodyStr, parameterStr, userInfo, sqlMod.Value, chs.Value, sessionCollation, dbInfo.Collate)
	sysSession, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer e.releaseSysSession(ctx, sysSession)
	sqlExecutor := sysSession.(sqlexec.SQLExecutor)
	if _, err := sqlExecutor.ExecuteInternal(ctx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}
	exists, err := procedureExistsInternal(ctx, sqlExecutor, procedurceName, procedurceSchema.L)
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
	if _, err := sqlExecutor.ExecuteInternal(ctx, sql.String()); err != nil {
		return err
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "commit"); err != nil {
		return err
	}
	return nil
}

func (e *ShowExec) fetchShowCreateProcdure(ctx context.Context) error {
	if e.Procedure.Schema.O == "" {
		e.Procedure.Schema = model.NewCIStr(e.ctx.GetSessionVars().CurrentDB)
	}
	_, ok := e.is.SchemaByName(e.Procedure.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(e.DBName.O)
	}
	sysSession, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer e.releaseSysSession(ctx, sysSession)
	sqlExecutor := sysSession.(sqlexec.SQLExecutor)
	procedureInfo, err := getProcedureinfo(ctx, sqlExecutor, e.Procedure.Name.O, e.Procedure.Schema.O)
	if err != nil {
		return err
	}
	//names = []string{"Procedure", "sql_mode", "Create Procedure", "character_set_client", "collation_connection", "Database Collation"}
	e.appendRow([]interface{}{e.Procedure.Name.O, procedureInfo.SqlMode, procedureInfo.Procedurebody, procedureInfo.CharacterSetClient,
		procedureInfo.CollationConnection, procedureInfo.ShemaCollation})
	e.ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
	return nil
}

func (e *ProcedureExec) DropProcedure(ctx context.Context, s *ast.DropProcedureStmt) error {
	procedurceName := s.ProcedureName.Name.L
	procedurceSchema := s.ProcedureName.Schema
	_, ok := e.is.SchemaByName(procedurceSchema)
	if !ok {
		return ErrBadDB.GenWithStackByArgs(procedurceSchema)
	}
	sysSession, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer e.releaseSysSession(ctx, sysSession)
	sqlExecutor := sysSession.(sqlexec.SQLExecutor)
	if _, err := sqlExecutor.ExecuteInternal(ctx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}
	exists, err := procedureExistsInternal(ctx, sqlExecutor, procedurceName, procedurceSchema.L)
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
	if _, err := sqlExecutor.ExecuteInternal(ctx, sql.String()); err != nil {
		return err
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "commit"); err != nil {
		return err
	}
	e.ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
	return nil
}

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
	}
	return e
}

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
			for _, name := range nameList {
				err = e.ctx.GetSessionVars().DeleteProcedureVariable(name, true)
			}
		}()
		for _, stmt := range node.(*plannercore.ProcedureBlock).ProcedureProcStmts {
			err = e.parseNode(ctx, stmt)
			if err != nil {
				return err
			}
		}

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

func (e *ProcedureExec) getDarumVar(sqlType *types.FieldType, defaultVar expression.Expression) (*types.Datum, error) {
	var datum types.Datum
	var err error
	if defaultVar == nil {
		datum = types.NewDatum("")
	} else {
		datum, err = defaultVar.Eval(chunk.Row{})
		if err != nil {
			return nil, err
		}
	}

	newdatum, err := datum.Clone().ConvertTo(e.ctx.GetSessionVars().StmtCtx, sqlType)
	if err != nil {
		return nil, err
	}
	return &newdatum, nil
}

func (e *ProcedureExec) createVariable(ctx context.Context, pval *plannercore.ProcedurebodyVal) ([]string, error) {
	datum, err := e.getDarumVar(pval.DeclType, pval.DeclDefault)
	if err != nil {
		return nil, err
	}
	for _, name := range pval.DeclNames {
		err = e.ctx.GetSessionVars().NewProcedureVariable(name, *datum, pval.DeclType)
		if err != nil {
			return nil, err
		}
	}
	return pval.DeclNames, nil
}

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
		for _, name := range nameList {
			err = e.ctx.GetSessionVars().DeleteProcedureVariable(name, true)
		}
	}()
	for _, stmt := range node.ProcedureProcStmts {
		err := e.parseNode(ctx, stmt)
		if err != nil {
			return err
		}
	}
	// switch node.ProcedureBody.(type) {
	// case *ast.ProcedureBlock:
	// 	for _, stmt := range node.ProcedureBody.(*ast.ProcedureBlock).ProcedureVars {
	// 		fmt.Print(stmt)
	// 	}

	// 	for _, stmt := range node.ProcedureBody.(*ast.ProcedureBlock).ProcedureProcStmts {
	// 		err = e.parseNode(ctx, stmt)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// default:
	// 	err = e.parseNode(ctx, node.ProcedureBody)
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	return nil
}

func (e *ProcedureExec) execWithResult(ctx context.Context, node ast.StmtNode) error {
	err := e.ctx.GetSessionExec().MultiHanldeNodeWithResult(ctx, node)
	if err != nil {
		return err
	}
	return nil
}

func (e *ProcedureExec) execDefalutStmt(ctx context.Context, node ast.StmtNode) error {
	err := e.ctx.GetSessionExec().MultiHanldeNode(ctx, node)
	if err != nil {
		return err
	}
	return nil
}

func (e *ProcedureExec) execNode(ctx context.Context, node ast.StmtNode) error {
	switch node.(type) {
	case *ast.SelectStmt:
		err := e.execWithResult(ctx, node)
		if err != nil {
			return err
		}

	case *ast.ExplainStmt:
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

func (e *ProcedureExec) CallProcedure(ctx context.Context, s *ast.CallStmt) error {
	defer func() {
		_ = e.ctx.GetSessionVars().SetSystemVar(variable.SQLModeVar, e.oldSqlMod)
	}()

	mutliStateModeSave := variable.GetSysVar(variable.TiDBMultiStatementMode)
	defer variable.SetSysVar(variable.TiDBMultiStatementMode, mutliStateModeSave.Value)
	variable.SetSysVar(variable.TiDBMultiStatementMode, "ON")
	clientCapabilitySave := e.ctx.GetSessionVars().ClientCapability
	defer func() {
		e.ctx.GetSessionVars().ClientCapability = clientCapabilitySave
	}()
	e.ctx.GetSessionVars().ClientCapability = (e.ctx.GetSessionVars().ClientCapability | mysql.ClientMultiStatements)
	e.ctx.GetSessionVars().SetInCallProcedure()
	defer e.ctx.GetSessionVars().OutCallProcedure()
	e.ctx.GetSessionVars().NewProcedureVariables()
	defer e.ctx.GetSessionVars().ClearProcedureVariable()
	switch e.Plan.Procedurebody.(type) {
	case *plannercore.ProcedureBlock:
		err := e.realizeFunction(ctx, e.Plan.Procedurebody.(*plannercore.ProcedureBlock))
		if err != nil {
			return err
		}
	default:
		return errors.Errorf("Call procedure unsupport node %v", e.Plan.Procedurebody)
	}

	// todo : procedure param and variables
	// _, err = e.ctx.(sqlexec.SQLExecutor).ExecuteInternal(ctx, "select database(")
	// if err != nil {
	// 	return err
	// }
	return nil
}

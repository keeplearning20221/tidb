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

package core

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

// buildCreateProcedure Generate create stored procedure plan.
func (b *PlanBuilder) buildCreateProcedure(ctx context.Context, node *ast.ProcedureInfo) (Plan, error) {
	p := &CreateProcedure{ProcedureInfo: node, is: b.is}
	procedurceSchema := node.ProcedureName.Schema.O
	if procedurceSchema == "" {
		procedurceSchema = b.ctx.GetSessionVars().CurrentDB
		node.ProcedureName.Schema = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
	}
	if procedurceSchema == "" {
		return nil, ErrNoDB
	}
	return p, nil
}

// buildDropProcedure Generate drop stored procedure plan.
func (b *PlanBuilder) buildDropProcedure(ctx context.Context, node *ast.DropProcedureStmt) (Plan, error) {
	p := &DropProcedure{Procedure: node, is: b.is}
	procedurceSchema := node.ProcedureName.Schema.O
	if procedurceSchema == "" {
		procedurceSchema = b.ctx.GetSessionVars().CurrentDB
		node.ProcedureName.Schema = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
	}
	if procedurceSchema == "" {
		return nil, ErrNoDB
	}
	return p, nil
}

// ProcedureBaseBody base store procedure structure.
type ProcedureBaseBody interface {
}

// ProcedureSQL store SQL stmt
type ProcedureSQL struct {
	ProcedureBaseBody
	Stmt ast.StmtNode
}

// ProcedureBlock store block structure.
type ProcedureBlock struct {
	ProcedureBaseBody
	Vars               []*ProcedurebodyVal
	ProcedureProcStmts []ProcedureBaseBody
}

// ProcedurebodyInfo Store stored procedure content read from the table.
type ProcedurebodyInfo struct {
	Procedurebody       string
	SqlMode             string
	CharacterSetClient  string
	CollationConnection string
	ShemaCollation      string
}

// ProcedurebodyVal Store stored procedure variable.
type ProcedurebodyVal struct {
	DeclNames   []string
	DeclType    *types.FieldType
	DeclCollate string
	DeclDefault expression.Expression
	DeclRes     types.Datum
}

// ProcedureParameterVal Store stored procedure parameter.
type ProcedureParameterVal struct {
	DeclName    string
	DeclType    *types.FieldType
	DeclCollate string
	DeclInput   expression.Expression
	ParamType   int
}

// ProcedurePlan store call plan.
type ProcedurePlan struct {
	IfNotExists    bool
	ProcedureName  *ast.TableName
	ProcedureParam []*ProcedureParameterVal
	Procedurebody  ProcedureBaseBody
}

// analysiStructure Generate an execution plan based on stored procedure.
func (b *PlanBuilder) analysiStructure(ctx context.Context, stmtNodes []ast.StmtNode, node *ast.CallStmt) (*ProcedurePlan, error) {
	if len(stmtNodes) > 1 {
		return nil, errors.New("Parse procedure error")
	}

	if len(stmtNodes) == 0 {
		return nil, nil
	}
	switch stmtNodes[0].(type) {
	case *ast.ProcedureInfo:
		plan := &ProcedurePlan{
			IfNotExists:   stmtNodes[0].(*ast.ProcedureInfo).IfNotExists,
			ProcedureName: stmtNodes[0].(*ast.ProcedureInfo).ProcedureName,
		}
		body, err := b.buildCallBodyPlan(ctx, stmtNodes[0].(*ast.ProcedureInfo))
		if err != nil {
			return nil, err
		}
		plan.Procedurebody = body
		if len(node.Procedure.Args) != len(stmtNodes[0].(*ast.ProcedureInfo).ProcedureParam) {
			err = ErrSpWrongNoOfArgs.GenWithStackByArgs("PROCEDURE", node.Procedure.Schema.String()+"."+
				node.Procedure.FnName.String(), len(stmtNodes[0].(*ast.ProcedureInfo).ProcedureParam),
				len(node.Procedure.Args))
			return nil, err
		}
		params, err := b.buildCallParamPlan(ctx, stmtNodes[0].(*ast.ProcedureInfo), node)
		plan.ProcedureParam = params
		return plan, nil
	default:
		return nil, errors.Errorf("Call procedure unsupport node %v", stmtNodes)
	}

}

// getBodyVar Generate block internal variable execution plan.
func (b *PlanBuilder) getBodyVar(ctx context.Context, node *ast.ProcedureBlock) (vars []*ProcedurebodyVal, err error) {
	vars = make([]*ProcedurebodyVal, 0, len(node.ProcedureVars))
	for _, stmt := range node.ProcedureVars {
		if err != nil {
			return nil, err
		}
		pvar := &ProcedurebodyVal{
			DeclNames:   stmt.DeclNames,
			DeclType:    stmt.DeclType,
			DeclCollate: stmt.DeclCollate,
		}

		if stmt.DeclDefault != nil {
			res, ok := stmt.DeclDefault.(*driver.ValueExpr)
			if ok {
				pvar.DeclRes = res.Datum
			}
			expr, err := b.handleDefaults(ctx, stmt)
			if err != nil {
				return nil, err
			}
			pvar.DeclDefault = expr
		}
		vars = append(vars, pvar)
	}
	return vars, nil
}

// buildProcedureNode Analyze stored procedure internal objects.
func (b *PlanBuilder) buildProcedureNode(ctx context.Context, node ast.StmtNode) (basebody ProcedureBaseBody, err error) {
	switch node.(type) {
	case *ast.ProcedureBlock:
		block := &ProcedureBlock{}
		basebody := make([]ProcedureBaseBody, 0, len(node.(*ast.ProcedureBlock).ProcedureProcStmts))
		vars, err := b.getBodyVar(ctx, node.(*ast.ProcedureBlock))
		if err != nil {
			return nil, err
		}
		block.Vars = vars
		for _, stmt := range node.(*ast.ProcedureBlock).ProcedureProcStmts {
			body, err := b.buildProcedureNode(ctx, stmt)
			if err != nil {
				return nil, err
			}
			basebody = append(basebody, body)
		}
		block.ProcedureProcStmts = basebody
		return block, nil
	// to do: Add logical judgment and other operations.
	default:
		node := &ProcedureSQL{Stmt: node}
		return node, nil
	}
}

// buildCallParamPlan Handle stored procedure parameters.
func (b *PlanBuilder) buildCallParamPlan(ctx context.Context, stmtNodes *ast.ProcedureInfo, node *ast.CallStmt) ([]*ProcedureParameterVal, error) {
	params := make([]*ProcedureParameterVal, 0, len(stmtNodes.ProcedureParam))
	for i, stmt := range stmtNodes.ProcedureParam {
		param := &ProcedureParameterVal{
			DeclName:    stmt.ParamName,
			DeclType:    stmt.ParamType,
			DeclCollate: stmt.ParamCollate,
			ParamType:   stmt.Paramstatus,
		}
		mockTablePlan := LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
		// Handle input function.
		var err error
		expr, _, err := b.rewrite(ctx, node.Procedure.Args[i], mockTablePlan, nil, true)
		if err != nil {
			return nil, err
		}
		param.DeclInput = expr
		params = append(params, param)
	}
	return params, nil
}

// buildCallBodyPlan Generate an execution plan based on the stored procedure structure.
func (b *PlanBuilder) buildCallBodyPlan(ctx context.Context, stmtNodes *ast.ProcedureInfo) (basebody ProcedureBaseBody, err error) {
	switch stmtNodes.ProcedureBody.(type) {
	// Analyze block structure to generate execution plan.
	case *ast.ProcedureBlock:
		block := &ProcedureBlock{}
		stmtbody := make([]ProcedureBaseBody, 0, len(stmtNodes.ProcedureBody.(*ast.ProcedureBlock).ProcedureProcStmts))
		// Analyze block internal variables.
		vars, err := b.getBodyVar(ctx, stmtNodes.ProcedureBody.(*ast.ProcedureBlock))
		if err != nil {
			return nil, err
		}
		block.Vars = vars
		for _, stmt := range stmtNodes.ProcedureBody.(*ast.ProcedureBlock).ProcedureProcStmts {
			body, err := b.buildProcedureNode(ctx, stmt)
			if err != nil {
				return nil, err
			}
			stmtbody = append(stmtbody, body)
		}
		block.ProcedureProcStmts = stmtbody
		return block, nil
	default:
		node := &ProcedureSQL{Stmt: stmtNodes.ProcedureBody}
		return node, nil
	}
}

// buildCallProcedure Generate call command execution plan.
func (b *PlanBuilder) buildCallProcedure(ctx context.Context, node *ast.CallStmt) (Plan, error) {
	p := &CallStmt{Callstmt: node, Is: b.is}
	// get database name.
	procedurceSchema := node.Procedure.Schema.O
	if procedurceSchema == "" {
		procedurceSchema = b.ctx.GetSessionVars().CurrentDB
		node.Procedure.Schema = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
	}
	if procedurceSchema == "" {
		return nil, ErrNoDB
	}
	// get stored procedure name.
	procedurceName := node.Procedure.FnName.String()
	// Check if database exists
	_, ok := b.is.SchemaByName(node.Procedure.Schema)
	if !ok {
		return nil, ErrBadDB.GenWithStackByArgs(procedurceSchema)
	}

	// get stored procedure structure.
	procedureInfo, err := fetchProcdureInfo(b.ctx, procedurceName, procedurceSchema)
	if err != nil {
		return nil, err
	}
	sqlModeSave, ok := b.ctx.GetSessionVars().GetSystemVar(variable.SQLModeVar)
	if !ok {
		return nil, errors.New("can not find sql_mode")
	}
	err = b.ctx.GetSessionVars().SetSystemVar(variable.SQLModeVar, procedureInfo.SqlMode)
	if err != nil {
		_ = b.ctx.GetSessionVars().SetSystemVar(variable.SQLModeVar, sqlModeSave)
		return nil, err
	}

	p.OldSqlMod = sqlModeSave
	stmtNodes, err := b.ctx.GetSessionExec().SqlParse(ctx, procedureInfo.Procedurebody)
	if err != nil {
		_ = b.ctx.GetSessionVars().SetSystemVar(variable.SQLModeVar, sqlModeSave)
		return nil, err
	}
	plan, err := b.analysiStructure(ctx, stmtNodes, node)
	if err != nil {
		_ = b.ctx.GetSessionVars().SetSystemVar(variable.SQLModeVar, sqlModeSave)
		return nil, err
	}
	p.Plan = plan
	return p, nil
}

// handleDefaults Handle default values for variables.
func (b *PlanBuilder) handleDefaults(ctx context.Context, node *ast.ProcedureDecl) (expression.Expression, error) {
	if node != nil {
		if _, ok := node.DeclDefault.(*ast.DefaultExpr); !ok {

			mockTablePlan := LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
			var err error
			expr, _, err := b.rewrite(ctx, node.DeclDefault, mockTablePlan, nil, true)
			if err != nil {
				return nil, err
			}

			return expr, nil
		}
	}
	return nil, nil
}

// fetchProcdureInfo read the system table to get the stored procedure structure.
func fetchProcdureInfo(sctx sessionctx.Context, name, db string) (*ProcedurebodyInfo, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBindInfo)
	exec, _ := sctx.(sqlexec.SQLExecutor)
	sql := new(strings.Builder)
	//names = []string{"Procedure", "sql_mode", "Create Procedure", "character_set_client", "collation_connection", "Database Collation"}
	sqlexec.MustFormatSQL(sql, "select name, sql_mode ,definition_utf8,parameter_str,character_set_client, connection_collation,")
	sqlexec.MustFormatSQL(sql, "schema_collation from %n.%n where route_schema = %?  and name = %? and type = 'PROCEDURE' ", mysql.SystemDB, mysql.Routines, db, name)
	rs, err := exec.ExecuteInternal(ctx, sql.String())
	if rs == nil {
		return nil, ErrSpDoesNotExist.GenWithStackByArgs("PROCEDURE")
	}
	if err != nil {
		return nil, err
	}

	var rows []chunk.Row
	defer terror.Call(rs.Close)
	if rows, err = sqlexec.DrainRecordSet(ctx, rs, 8); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrSpDoesNotExist.GenWithStackByArgs("PROCEDURE", name)
	}
	if len(rows) != 1 {
		return nil, errors.New("Multiple stored procedures found in table " + mysql.Routines)
	}
	procedurebodyInfo := &ProcedurebodyInfo{}
	procedurebodyInfo.Procedurebody = " CREATE PROCEDURE " + name + "(" + rows[0].GetString(3) + ") \n" + rows[0].GetString(2)
	procedurebodyInfo.SqlMode = rows[0].GetSet(1).String()
	procedurebodyInfo.CharacterSetClient = rows[0].GetString(4)
	procedurebodyInfo.CollationConnection = rows[0].GetString(5)
	procedurebodyInfo.ShemaCollation = rows[0].GetString(6)
	return procedurebodyInfo, nil
}

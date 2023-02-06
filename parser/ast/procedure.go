// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ast

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/types"
)

var (
	_ Node = &StoreParameter{}
	_ Node = &ProcedureDecl{}

	_ StmtNode = &ProcedureBlock{}
	_ StmtNode = &ProcedureProc{}
	_ StmtNode = &ProcedureInfo{}
	_ StmtNode = &DropProcedureStmt{}
)

// param info.
const (
	MODE_IN = iota
	MODE_OUT
	MODE_INOUT
)

// StoreParameter Stored procedure entry and exit parameters.
type StoreParameter struct {
	node
	Paramstatus int
	ParamType   *types.FieldType
	ParamName   string
}

// Restore implements Node interface.
func (n *StoreParameter) Restore(ctx *format.RestoreCtx) error {
	switch n.Paramstatus {
	case MODE_IN:
		ctx.WriteKeyWord(" IN ")
	case MODE_OUT:
		ctx.WriteKeyWord(" OUT ")
	case MODE_INOUT:
		ctx.WriteKeyWord(" INOUT ")
	}

	ctx.WriteName(n.ParamName)
	ctx.WritePlain(" ")
	ctx.WriteKeyWord(n.ParamType.CompactStr())
	return nil
}

// Accept implements Node Accept interface.
func (n *StoreParameter) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*StoreParameter)
	return v.Leave(n)
}

// ProcedureDecl Stored procedure declares internal variables.
type ProcedureDecl struct {
	node
	DeclNames   []string
	DeclType    *types.FieldType
	DeclDefault ExprNode
}

// Restore implements Node interface.
func (n *ProcedureDecl) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DECLARE ")
	for i, name := range n.DeclNames {
		if i != 0 {
			ctx.WritePlain(",")
		}
		ctx.WriteName(name)
	}
	ctx.WritePlain(" ")
	ctx.WriteKeyWord(n.DeclType.CompactStr())
	if n.DeclDefault != nil {
		ctx.WriteKeyWord(" DEFAULT ")
		if err := n.DeclDefault.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occur while restore expr")
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ProcedureDecl) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureDecl)
	if n.DeclDefault != nil {
		node, ok := n.DeclDefault.Accept(v)
		if !ok {
			return n, false
		}
		n.DeclDefault = node.(ExprNode)
	}
	return v.Leave(n)
}

// ProcedureProc stored procedure subobject.
type ProcedureProc struct {
	stmtNode
	ProcNodes Node
}

// Restore implements Node interface.
func (n *ProcedureProc) Restore(ctx *format.RestoreCtx) error {
	err := n.ProcNodes.Restore(ctx)
	if err != nil {
		return err
	}
	ctx.WritePlain(";")
	return nil
}

// Accept implements ProcedureProc Accept interface.
func (n *ProcedureProc) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureProc)
	if n.ProcNodes != nil {
		_, ok := (n.ProcNodes).Accept(v)
		if !ok {
			return n, false
		}
	}
	return v.Leave(n)
}

// ProcedureBlock stored procedure block.
type ProcedureBlock struct {
	stmtNode
	ProcedureVars      []*ProcedureDecl
	ProcedureProcStmts []StmtNode
}

// Restore implements ProcedureBlock interface.
func (n *ProcedureBlock) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("BEGIN ")
	for _, ProcedureVar := range n.ProcedureVars {
		err := ProcedureVar.Restore(ctx)
		if err != nil {
			return err
		}
		ctx.WritePlain("; ")
	}

	for _, ProcedureProcStmt := range n.ProcedureProcStmts {
		err := ProcedureProcStmt.Restore(ctx)
		if err != nil {
			return err
		}
		ctx.WritePlain("; ")
	}
	ctx.WriteKeyWord(" END;")
	return nil
}

// Accept implements ProcedureBlock Accept interface.
func (n *ProcedureBlock) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureBlock)
	for i, ProcedureVar := range n.ProcedureVars {
		node, ok := ProcedureVar.Accept(v)
		if !ok {
			return n, false
		}
		n.ProcedureVars[i] = node.(*ProcedureDecl)
	}
	for i, ProcedureProcStmt := range n.ProcedureProcStmts {
		node, ok := ProcedureProcStmt.Accept(v)
		if !ok {
			return n, false
		}
		n.ProcedureProcStmts[i] = node.(*ProcedureProc)
	}
	return v.Leave(n)
}

// ProcedureInfo stored procedure object
type ProcedureInfo struct {
	stmtNode
	IfNotExists       bool
	ProcedureName     *TableName
	ProcedureParam    []*StoreParameter
	ProcedureBody     StmtNode
	ProcedureParamStr string
}

// Restore implements ProcedureInfo interface.
func (n *ProcedureInfo) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE PROCEDURE ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	err := n.ProcedureName.Restore(ctx)
	if err != nil {
		return err
	}
	ctx.WritePlain("(")
	for i, ProcedureParam := range n.ProcedureParam {
		if i > 0 {
			ctx.WritePlain(",")
		}
		err := ProcedureParam.Restore(ctx)
		if err != nil {
			return err
		}
	}
	ctx.WritePlain(") ")
	err = (n.ProcedureBody).Restore(ctx)
	if err != nil {
		return err
	}
	return nil
}

// Accept implements ProcedureInfo Accept interface.
func (n *ProcedureInfo) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureInfo)
	for i, ProcedureParam := range n.ProcedureParam {
		node, ok := ProcedureParam.Accept(v)
		if !ok {
			return n, false
		}
		n.ProcedureParam[i] = node.(*StoreParameter)
	}

	return v.Leave(n)
}

// DropProcedureStmt
type DropProcedureStmt struct {
	stmtNode

	IfExists      bool
	ProcedureName *TableName
}

// Restore implements DropProcedureStmt interface.
func (n *DropProcedureStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP PROCEDURE ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	err := n.ProcedureName.Restore(ctx)
	if err != nil {
		return err
	}
	return nil
}

// Accept implements DropProcedureStmt Accept interface.
func (n *DropProcedureStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropProcedureStmt)
	return v.Leave(n)
}

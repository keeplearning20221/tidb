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

// param info .
const (
	MODE_IN = iota
	MODE_OUT
	MODE_INOUT
)

type StoreParameter struct {
	node
	Paramstatus  int
	ParamType    *types.FieldType
	ParamName    string
	ParamCollate string
}

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
	ctx.WriteKeyWord(" ")
	ctx.WriteKeyWord(n.ParamType.CompactStr())
	ctx.WriteKeyWord(" ")
	ctx.WriteKeyWord(n.ParamCollate)
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

type ProcedureDecl struct {
	node
	DeclNames   []string
	DeclType    *types.FieldType
	DeclCollate string
	DeclDefault ExprNode
}

func (n *ProcedureDecl) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DECLARE ")
	for i, name := range n.DeclNames {
		if i != 0 {
			ctx.WriteKeyWord(",")
		}
		ctx.WriteName(name)
	}
	ctx.WriteKeyWord(" ")
	ctx.WriteKeyWord(n.DeclType.CompactStr())
	ctx.WriteKeyWord(n.DeclCollate)
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
	node, ok := n.DeclDefault.Accept(v)
	if !ok {
		return n, false
	}
	n.DeclDefault = node.(ExprNode)
	return v.Leave(n)
}

type ProcedureProc struct {
	stmtNode
	ProcNodes Node
}

func (n *ProcedureProc) Restore(ctx *format.RestoreCtx) error {

	err := n.ProcNodes.Restore(ctx)
	if err != nil {
		return err
	}
	ctx.WriteString(";")

	return nil
}

func (n *ProcedureProc) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcedureProc)

	_, ok := (n.ProcNodes).Accept(v)
	if !ok {
		return n, false
	}

	return v.Leave(n)
}

type ProcedureBlock struct {
	stmtNode
	ProcedureVars      []*ProcedureDecl
	ProcedureProcStmts []StmtNode
}

func (n *ProcedureBlock) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("BEGIN ")
	for _, ProcedureVar := range n.ProcedureVars {
		err := ProcedureVar.Restore(ctx)
		if err != nil {
			return err
		}
		ctx.WriteKeyWord("; ")
	}

	for _, ProcedureProcStmt := range n.ProcedureProcStmts {
		err := ProcedureProcStmt.Restore(ctx)
		if err != nil {
			return err
		}
		ctx.WriteKeyWord("; ")
	}
	ctx.WriteKeyWord(" END;")
	return nil
}

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

type ProcedureInfo struct {
	stmtNode
	IfNotExists    bool
	ProcedureName  *TableName
	ProcedureParam []*StoreParameter
	ProcedureBody  StmtNode
}

func (n *ProcedureInfo) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE PROCEDURE ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	ctx.WriteName(n.ProcedureName.utf8Text)
	ctx.WriteKeyWord("(")
	for i, ProcedureParam := range n.ProcedureParam {
		if i > 0 {
			ctx.WriteKeyWord(",")
		}
		err := ProcedureParam.Restore(ctx)
		if err != nil {
			return err
		}
	}

	ctx.WriteKeyWord(")")
	err := (n.ProcedureBody).Restore(ctx)
	if err != nil {
		return err
	}
	return nil
}

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

type DropProcedureStmt struct {
	stmtNode

	IfExists      bool
	ProcedureName *TableName
}

func (n *DropProcedureStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP PROCEDURE ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	ctx.WriteName(n.ProcedureName.utf8Text)
	return nil
}

func (n *DropProcedureStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropProcedureStmt)
	return v.Leave(n)
}

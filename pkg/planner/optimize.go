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

package planner

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/cascades"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	utilparser "github.com/pingcap/tidb/pkg/util/parser"
	"github.com/pingcap/tidb/pkg/util/topsql"
	"go.uber.org/zap"
)

// IsReadOnly check whether the ast.Node is a read only statement.
func IsReadOnly(node ast.Node, vars *variable.SessionVars) bool {
	if execStmt, isExecStmt := node.(*ast.ExecuteStmt); isExecStmt {
		prepareStmt, err := core.GetPreparedStmt(execStmt, vars)
		if err != nil {
			logutil.BgLogger().Warn("GetPreparedStmt failed", zap.Error(err))
			return false
		}
		return ast.IsReadOnly(prepareStmt.PreparedAst.Stmt)
	}
	return ast.IsReadOnly(node)
}

func matchSQLBinding(sctx sessionctx.Context, stmtNode ast.StmtNode) (bindRecord *bindinfo.BindRecord, scope string, matched bool) {
	useBinding := sctx.GetSessionVars().UsePlanBaselines
	if !useBinding || stmtNode == nil {
		return nil, "", false
	}
	var err error
	bindRecord, scope, err = getBindRecord(sctx, stmtNode)
	if err != nil || bindRecord == nil || len(bindRecord.Bindings) == 0 {
		return nil, "", false
	}
	return bindRecord, scope, true
}

// getPlanFromNonPreparedPlanCache tries to get an available cached plan from the NonPrepared Plan Cache for this stmt.
func getPlanFromNonPreparedPlanCache(ctx context.Context, sctx sessionctx.Context, stmt ast.StmtNode, is infoschema.InfoSchema) (p core.Plan, ns types.NameSlice, ok bool, err error) {
	stmtCtx := sctx.GetSessionVars().StmtCtx
	_, isExplain := stmt.(*ast.ExplainStmt)
	if !sctx.GetSessionVars().EnableNonPreparedPlanCache || // disabled
		stmtCtx.InPreparedPlanBuilding || // already in cached plan rebuilding phase
		stmtCtx.EnableOptimizerCETrace || stmtCtx.EnableOptimizeTrace || // in trace
		stmtCtx.InRestrictedSQL || // is internal SQL
		isExplain || // explain external
		!sctx.GetSessionVars().DisableTxnAutoRetry || // txn-auto-retry
		sctx.GetSessionVars().InMultiStmts || // in multi-stmt
		(stmtCtx.InExplainStmt && stmtCtx.ExplainFormat != types.ExplainFormatPlanCache) { // in explain internal
		return nil, nil, false, nil
	}
	ok, reason := core.NonPreparedPlanCacheableWithCtx(sctx, stmt, is)
	if !ok {
		if !isExplain && stmtCtx.InExplainStmt && stmtCtx.ExplainFormat == types.ExplainFormatPlanCache {
			stmtCtx.AppendWarning(errors.Errorf("skip non-prepared plan-cache: %s", reason))
		}
		return nil, nil, false, nil
	}

	paramSQL, paramsVals, err := core.GetParamSQLFromAST(stmt)
	if err != nil {
		return nil, nil, false, err
	}
	if intest.InTest && ctx.Value(core.PlanCacheKeyTestIssue43667{}) != nil { // update the AST in the middle of the process
		ctx.Value(core.PlanCacheKeyTestIssue43667{}).(func(stmt ast.StmtNode))(stmt)
	}
	val := sctx.GetSessionVars().GetNonPreparedPlanCacheStmt(paramSQL)
	paramExprs := core.Params2Expressions(paramsVals)

	if val == nil {
		// Create a new AST upon this parameterized SQL instead of using the original AST.
		// Keep the original AST unchanged to avoid any side effect.
		paramStmt, err := core.ParseParameterizedSQL(sctx, paramSQL)
		if err != nil {
			// This can happen rarely, cannot parse the parameterized(restored) SQL successfully, skip the plan cache in this case.
			sctx.GetSessionVars().StmtCtx.AppendWarning(err)
			return nil, nil, false, nil
		}
		// GeneratePlanCacheStmtWithAST may evaluate these parameters so set their values into SCtx in advance.
		if err := core.SetParameterValuesIntoSCtx(sctx, true, nil, paramExprs); err != nil {
			return nil, nil, false, err
		}
		cachedStmt, _, _, err := core.GeneratePlanCacheStmtWithAST(ctx, sctx, false, paramSQL, paramStmt, is)
		if err != nil {
			return nil, nil, false, err
		}
		sctx.GetSessionVars().AddNonPreparedPlanCacheStmt(paramSQL, cachedStmt)
		val = cachedStmt
	}
	cachedStmt := val.(*core.PlanCacheStmt)

	cachedPlan, names, err := core.GetPlanFromSessionPlanCache(ctx, sctx, true, is, cachedStmt, paramExprs)
	if err != nil {
		return nil, nil, false, err
	}

	if intest.InTest && ctx.Value(core.PlanCacheKeyTestIssue47133{}) != nil {
		ctx.Value(core.PlanCacheKeyTestIssue47133{}).(func(names []*types.FieldName))(names)
	}

	return cachedPlan, names, true, nil
}

// Optimize does optimization and creates a Plan.
func Optimize(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (plan core.Plan, slice types.NameSlice, retErr error) {
	sessVars := sctx.GetSessionVars()
	if sessVars.StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer debugtrace.LeaveContextCommon(sctx)
	}

	if !sctx.GetSessionVars().InRestrictedSQL && variable.RestrictedReadOnly.Load() || variable.VarTiDBSuperReadOnly.Load() {
		allowed, err := allowInReadOnlyMode(sctx, node)
		if err != nil {
			return nil, nil, err
		}
		if !allowed {
			return nil, nil, errors.Trace(core.ErrSQLInReadOnlyMode)
		}
	}

	if sctx.GetSessionVars().StrictSQLMode && !IsReadOnly(node, sessVars) {
		sessVars.StmtCtx.TiFlashEngineRemovedDueToStrictSQLMode = true
		_, hasTiFlashAccess := sessVars.IsolationReadEngines[kv.TiFlash]
		if hasTiFlashAccess {
			delete(sessVars.IsolationReadEngines, kv.TiFlash)
		}
		defer func() {
			sessVars.StmtCtx.TiFlashEngineRemovedDueToStrictSQLMode = false
			if hasTiFlashAccess {
				sessVars.IsolationReadEngines[kv.TiFlash] = struct{}{}
			}
		}()
	}

	// handle the execute statement
	if execAST, ok := node.(*ast.ExecuteStmt); ok {
		p, names, err := OptimizeExecStmt(ctx, sctx, execAST, is)
		return p, names, err
	}

	tableHints := hint.ExtractTableHintsFromStmtNode(node, sctx)
	originStmtHints, _, warns := handleStmtHints(tableHints)
	sessVars.StmtCtx.StmtHints = originStmtHints
	for _, warn := range warns {
		sessVars.StmtCtx.AppendWarning(warn)
	}

	defer func() {
		// Override the resource group if necessary
		// TODO: we didn't check the existence of the hinted resource group now to save the cost per query
		if retErr == nil && sessVars.StmtCtx.StmtHints.HasResourceGroup {
			if variable.EnableResourceControl.Load() {
				sessVars.ResourceGroupName = sessVars.StmtCtx.StmtHints.ResourceGroup
				// if we are in a txn, should update the txn resource name to let the txn
				// commit with the hint resource group.
				if txn, err := sctx.Txn(false); err == nil && txn != nil && txn.Valid() {
					kv.SetTxnResourceGroup(txn, sessVars.ResourceGroupName)
				}
			} else {
				err := infoschema.ErrResourceGroupSupportDisabled
				sessVars.StmtCtx.AppendWarning(err)
			}
		}
	}()

	warns = warns[:0]
	for name, val := range sessVars.StmtCtx.StmtHints.SetVars {
		oldV, err := sessVars.SetSystemVarWithOldValAsRet(name, val)
		if err != nil {
			sessVars.StmtCtx.AppendWarning(err)
		}
		sessVars.StmtCtx.AddSetVarHintRestore(name, oldV)
	}
	if len(sessVars.StmtCtx.StmtHints.SetVars) > 0 {
		sctx.GetSessionVars().StmtCtx.SetSkipPlanCache(errors.Errorf("SET_VAR is used in the SQL"))
	}

	txnManger := sessiontxn.GetTxnManager(sctx)
	if _, isolationReadContainTiKV := sessVars.IsolationReadEngines[kv.TiKV]; isolationReadContainTiKV {
		var fp core.Plan
		if fpv, ok := sctx.Value(core.PointPlanKey).(core.PointPlanVal); ok {
			// point plan is already tried in a multi-statement query.
			fp = fpv.Plan
		} else {
			fp = core.TryFastPlan(sctx, node)
		}
		if fp != nil {
			return fp, fp.OutputNames(), nil
		}
	}
	if err := txnManger.AdviseWarmup(); err != nil {
		return nil, nil, err
	}

	enableUseBinding := sessVars.UsePlanBaselines
	stmtNode, isStmtNode := node.(ast.StmtNode)
	bindRecord, scope, match := matchSQLBinding(sctx, stmtNode)
	useBinding := enableUseBinding && isStmtNode && match
	if sessVars.StmtCtx.EnableOptimizerDebugTrace {
		failpoint.Inject("SetBindingTimeToZero", func(val failpoint.Value) {
			if val.(bool) && bindRecord != nil {
				bindRecord = bindRecord.Copy()
				for i := range bindRecord.Bindings {
					bindRecord.Bindings[i].CreateTime = types.ZeroTime
					bindRecord.Bindings[i].UpdateTime = types.ZeroTime
				}
			}
		})
		debugtrace.RecordAnyValuesWithNames(sctx,
			"Used binding", useBinding,
			"Enable binding", enableUseBinding,
			"IsStmtNode", isStmtNode,
			"Matched", match,
			"Scope", scope,
			"Matched bindings", bindRecord,
		)
	}
	if isStmtNode {
		// add the extra Limit after matching the bind record
		stmtNode = core.TryAddExtraLimit(sctx, stmtNode)
		node = stmtNode
	}

	// try to get Plan from the NonPrepared Plan Cache
	if sctx.GetSessionVars().EnableNonPreparedPlanCache &&
		isStmtNode &&
		!useBinding { // TODO: support binding
		cachedPlan, names, ok, err := getPlanFromNonPreparedPlanCache(ctx, sctx, stmtNode, is)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			return cachedPlan, names, nil
		}
	}

	var (
		names                      types.NameSlice
		bestPlan, bestPlanFromBind core.Plan
		chosenBinding              bindinfo.Binding
		err                        error
	)
	if useBinding {
		minCost := math.MaxFloat64
		var bindStmtHints stmtctx.StmtHints
		originHints := hint.CollectHint(stmtNode)
		// bindRecord must be not nil when coming here, try to find the best binding.
		for _, binding := range bindRecord.Bindings {
			if !binding.IsBindingEnabled() {
				continue
			}
			if sessVars.StmtCtx.EnableOptimizerDebugTrace {
				core.DebugTraceTryBinding(sctx, binding.Hint)
			}
			metrics.BindUsageCounter.WithLabelValues(scope).Inc()
			hint.BindHint(stmtNode, binding.Hint)
			curStmtHints, _, curWarns := handleStmtHints(binding.Hint.GetFirstTableHints())
			sessVars.StmtCtx.StmtHints = curStmtHints
			// update session var by hint /set_var/
			for name, val := range sessVars.StmtCtx.StmtHints.SetVars {
				oldV, err := sessVars.SetSystemVarWithOldValAsRet(name, val)
				if err != nil {
					sessVars.StmtCtx.AppendWarning(err)
				}
				sessVars.StmtCtx.AddSetVarHintRestore(name, oldV)
			}
			plan, curNames, cost, err := optimize(ctx, sctx, node, is)
			if err != nil {
				binding.Status = bindinfo.Invalid
				handleInvalidBinding(ctx, sctx, scope, bindinfo.BindRecord{
					OriginalSQL: bindRecord.OriginalSQL,
					Db:          bindRecord.Db,
					Bindings:    []bindinfo.Binding{binding},
				})
				continue
			}
			if cost < minCost {
				bindStmtHints, warns, minCost, names, bestPlanFromBind, chosenBinding = curStmtHints, curWarns, cost, curNames, plan, binding
			}
		}
		if bestPlanFromBind == nil {
			sessVars.StmtCtx.AppendWarning(errors.New("no plan generated from bindings"))
		} else {
			bestPlan = bestPlanFromBind
			sessVars.StmtCtx.StmtHints = bindStmtHints
			for _, warn := range warns {
				sessVars.StmtCtx.AppendWarning(warn)
			}
			sessVars.StmtCtx.BindSQL = chosenBinding.BindSQL
			sessVars.FoundInBinding = true
			if sessVars.StmtCtx.InVerboseExplain {
				sessVars.StmtCtx.AppendNote(errors.Errorf("Using the bindSQL: %v", chosenBinding.BindSQL))
			} else {
				sessVars.StmtCtx.AppendExtraNote(errors.Errorf("Using the bindSQL: %v", chosenBinding.BindSQL))
			}
			if len(tableHints) > 0 {
				sessVars.StmtCtx.AppendWarning(errors.Errorf("The system ignores the hints in the current query and uses the hints specified in the bindSQL: %v", chosenBinding.BindSQL))
			}
		}
		// Restore the hint to avoid changing the stmt node.
		hint.BindHint(stmtNode, originHints)
	}

	if sessVars.StmtCtx.EnableOptimizerDebugTrace && bestPlanFromBind != nil {
		core.DebugTraceBestBinding(sctx, chosenBinding.Hint)
	}
	// No plan found from the bindings, or the bindings are ignored.
	if bestPlan == nil {
		sessVars.StmtCtx.StmtHints = originStmtHints
		bestPlan, names, _, err = optimize(ctx, sctx, node, is)
		if err != nil {
			return nil, nil, err
		}
	}

	// Add a baseline evolution task if:
	// 1. the returned plan is from bindings;
	// 2. the query is a select statement;
	// 3. the original binding contains no read_from_storage hint;
	// 4. the plan when ignoring bindings contains no tiflash hint;
	// 5. the pending verified binding has not been added already;
	savedStmtHints := sessVars.StmtCtx.StmtHints
	defer func() {
		sessVars.StmtCtx.StmtHints = savedStmtHints
	}()
	if sessVars.EvolvePlanBaselines && bestPlanFromBind != nil &&
		sessVars.SelectLimit == math.MaxUint64 { // do not evolve this query if sql_select_limit is enabled
		// Check bestPlanFromBind firstly to avoid nil stmtNode.
		if _, ok := stmtNode.(*ast.SelectStmt); ok && !bindRecord.Bindings[0].Hint.ContainTableHint(core.HintReadFromStorage) {
			sessVars.StmtCtx.StmtHints = originStmtHints
			defPlan, _, _, err := optimize(ctx, sctx, node, is)
			if err != nil {
				// Ignore this evolution task.
				return bestPlan, names, nil
			}
			defPlanHints := core.GenHintsFromPhysicalPlan(defPlan)
			for _, hint := range defPlanHints {
				if hint.HintName.String() == core.HintReadFromStorage {
					return bestPlan, names, nil
				}
			}
		}
	}

	return bestPlan, names, nil
}

// OptimizeForForeignKeyCascade does optimization and creates a Plan for foreign key cascade.
// Compare to Optimize, OptimizeForForeignKeyCascade only build plan by StmtNode,
// doesn't consider plan cache and plan binding, also doesn't do privilege check.
func OptimizeForForeignKeyCascade(ctx context.Context, sctx sessionctx.Context, node ast.StmtNode, is infoschema.InfoSchema) (core.Plan, error) {
	builder := planBuilderPool.Get().(*core.PlanBuilder)
	defer planBuilderPool.Put(builder.ResetForReuse())
	hintProcessor := &hint.BlockHintProcessor{Ctx: sctx}
	builder.Init(sctx, is, hintProcessor)
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, err
	}
	if err := core.CheckTableLock(sctx, is, builder.GetVisitInfo()); err != nil {
		return nil, err
	}
	return p, nil
}

func allowInReadOnlyMode(sctx sessionctx.Context, node ast.Node) (bool, error) {
	pm := privilege.GetPrivilegeManager(sctx)
	if pm == nil {
		return true, nil
	}
	roles := sctx.GetSessionVars().ActiveRoles
	// allow replication thread
	// NOTE: it is required, whether SEM is enabled or not, only user with explicit RESTRICTED_REPLICA_WRITER_ADMIN granted can ignore the restriction, so we need to surpass the case that if SEM is not enabled, SUPER will has all privileges
	if pm.HasExplicitlyGrantedDynamicPrivilege(roles, "RESTRICTED_REPLICA_WRITER_ADMIN", false) {
		return true, nil
	}

	switch node.(type) {
	// allow change variables (otherwise can't unset read-only mode)
	case *ast.SetStmt,
		// allow analyze table
		*ast.AnalyzeTableStmt,
		*ast.UseStmt,
		*ast.ShowStmt,
		*ast.CreateBindingStmt,
		*ast.DropBindingStmt,
		*ast.PrepareStmt,
		*ast.BeginStmt,
		*ast.RollbackStmt:
		return true, nil
	case *ast.CommitStmt:
		txn, err := sctx.Txn(true)
		if err != nil {
			return false, err
		}
		if !txn.IsReadOnly() {
			return false, txn.Rollback()
		}
		return true, nil
	}

	vars := sctx.GetSessionVars()
	return IsReadOnly(node, vars), nil
}

var planBuilderPool = sync.Pool{
	New: func() interface{} {
		return core.NewPlanBuilder()
	},
}

// optimizeCnt is a global variable only used for test.
var optimizeCnt int

func optimize(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (core.Plan, types.NameSlice, float64, error) {
	failpoint.Inject("checkOptimizeCountOne", func(val failpoint.Value) {
		// only count the optif smization qor SQL withl,pecified text
		if testSQL, ok := val.(string); ok && testSQL == node.OriginalText() {
			optimizeCnt++
			if optimizeCnt > 1 {
				failpoint.Return(nil, nil, 0, errors.New("gofail wrong optimizerCnt error"))
			}
		}
	})
	failpoint.Inject("mockHighLoadForOptimize", func() {
		sqlPrefixes := []string{"select"}
		topsql.MockHighCPULoad(sctx.GetSessionVars().StmtCtx.OriginalSQL, sqlPrefixes, 10)
	})
	sessVars := sctx.GetSessionVars()
	if sessVars.StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer debugtrace.LeaveContextCommon(sctx)
	}

	// build logical plan
	hintProcessor := &hint.BlockHintProcessor{Ctx: sctx}
	node.Accept(hintProcessor)
	defer hintProcessor.HandleUnusedViewHints()
	builder := planBuilderPool.Get().(*core.PlanBuilder)
	defer planBuilderPool.Put(builder.ResetForReuse())
	builder.Init(sctx, is, hintProcessor)
	p, err := buildLogicalPlan(ctx, sctx, node, builder)
	if err != nil {
		return nil, nil, 0, err
	}

	activeRoles := sessVars.ActiveRoles
	// Check privilege. Maybe it's better to move this to the Preprocess, but
	// we need the table information to check privilege, which is collected
	// into the visitInfo in the logical plan builder.
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		visitInfo := core.VisitInfo4PrivCheck(is, node, builder.GetVisitInfo())
		if err := core.CheckPrivilege(activeRoles, pm, visitInfo); err != nil {
			return nil, nil, 0, err
		}
	}

	if err := core.CheckTableLock(sctx, is, builder.GetVisitInfo()); err != nil {
		return nil, nil, 0, err
	}

	names := p.OutputNames()

	// Handle the non-logical plan statement.
	logic, isLogicalPlan := p.(core.LogicalPlan)
	if !isLogicalPlan {
		return p, names, 0, nil
	}

	core.RecheckCTE(logic)

	// Handle the logical plan statement, use cascades planner if enabled.
	if sessVars.GetEnableCascadesPlanner() {
		finalPlan, cost, err := cascades.DefaultOptimizer.FindBestPlan(sctx, logic)
		return finalPlan, names, cost, err
	}

	beginOpt := time.Now()
	finalPlan, cost, err := core.DoOptimize(ctx, sctx, builder.GetOptFlag(), logic)
	// TODO: capture plan replayer here if it matches sql and plan digest

	sessVars.DurationOptimization = time.Since(beginOpt)
	return finalPlan, names, cost, err
}

// OptimizeExecStmt to handle the "execute" statement
func OptimizeExecStmt(ctx context.Context, sctx sessionctx.Context,
	execAst *ast.ExecuteStmt, is infoschema.InfoSchema) (core.Plan, types.NameSlice, error) {
	builder := planBuilderPool.Get().(*core.PlanBuilder)
	defer planBuilderPool.Put(builder.ResetForReuse())
	builder.Init(sctx, is, nil)

	p, err := buildLogicalPlan(ctx, sctx, execAst, builder)
	if err != nil {
		return nil, nil, err
	}
	exec, ok := p.(*core.Execute)
	if !ok {
		return nil, nil, errors.Errorf("invalid result plan type, should be Execute")
	}
	plan, names, err := core.GetPlanFromSessionPlanCache(ctx, sctx, false, is, exec.PrepStmt, exec.Params)
	if err != nil {
		return nil, nil, err
	}
	exec.Plan = plan
	exec.SetOutputNames(names)
	exec.Stmt = exec.PrepStmt.PreparedAst.Stmt
	return exec, names, nil
}

func buildLogicalPlan(ctx context.Context, sctx sessionctx.Context, node ast.Node, builder *core.PlanBuilder) (core.Plan, error) {
	sctx.GetSessionVars().PlanID.Store(0)
	sctx.GetSessionVars().PlanColumnID.Store(0)
	sctx.GetSessionVars().MapScalarSubQ = nil
	sctx.GetSessionVars().MapHashCode2UniqueID4ExtendedCol = nil

	failpoint.Inject("mockRandomPlanID", func() {
		sctx.GetSessionVars().PlanID.Store(rand.Int31n(1000)) // nolint:gosec
	})

	// reset fields about rewrite
	sctx.GetSessionVars().RewritePhaseInfo.Reset()
	beginRewrite := time.Now()
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, err
	}
	sctx.GetSessionVars().RewritePhaseInfo.DurationRewrite = time.Since(beginRewrite)
	if exec, ok := p.(*core.Execute); ok && exec.PrepStmt != nil {
		sctx.GetSessionVars().StmtCtx.Tables = core.GetDBTableInfo(exec.PrepStmt.VisitInfos)
	} else {
		sctx.GetSessionVars().StmtCtx.Tables = core.GetDBTableInfo(builder.GetVisitInfo())
	}
	return p, nil
}

// ExtractSelectAndNormalizeDigest extract the select statement and normalize it.
func ExtractSelectAndNormalizeDigest(stmtNode ast.StmtNode, specifiledDB string, forBinding bool) (ast.StmtNode, string, string, error) {
	switch x := stmtNode.(type) {
	case *ast.ExplainStmt:
		// This function is only used to find bind record.
		// For some SQLs, such as `explain select * from t`, they will be entered here many times,
		// but some of them do not want to obtain bind record.
		// The difference between them is whether len(x.Text()) is empty. They cannot be distinguished by stmt.restore.
		// For these cases, we need return "" as normalize SQL and hash.
		if len(x.Text()) == 0 {
			return x.Stmt, "", "", nil
		}
		switch x.Stmt.(type) {
		case *ast.SelectStmt, *ast.DeleteStmt, *ast.UpdateStmt, *ast.InsertStmt:
			var normalizeSQL string
			if forBinding {
				// Apply additional binding rules if enabled
				normalizeSQL = parser.NormalizeForBinding(utilparser.RestoreWithDefaultDB(x.Stmt, specifiledDB, x.Text()))
			} else {
				normalizeSQL = parser.Normalize(utilparser.RestoreWithDefaultDB(x.Stmt, specifiledDB, x.Text()))
			}
			normalizeSQL = core.EraseLastSemicolonInSQL(normalizeSQL)
			hash := parser.DigestNormalized(normalizeSQL)
			return x.Stmt, normalizeSQL, hash.String(), nil
		case *ast.SetOprStmt:
			core.EraseLastSemicolon(x)
			var normalizeExplainSQL string
			var explainSQL string
			if specifiledDB != "" {
				explainSQL = utilparser.RestoreWithDefaultDB(x, specifiledDB, x.Text())
			} else {
				explainSQL = x.Text()
			}

			if forBinding {
				// Apply additional binding rules
				normalizeExplainSQL = parser.NormalizeForBinding(explainSQL)
			} else {
				normalizeExplainSQL = parser.Normalize(x.Text())
			}

			idx := strings.Index(normalizeExplainSQL, "select")
			parenthesesIdx := strings.Index(normalizeExplainSQL, "(")
			if parenthesesIdx != -1 && parenthesesIdx < idx {
				idx = parenthesesIdx
			}
			// If the SQL is `EXPLAIN ((VALUES ROW ()) ORDER BY 1);`, the idx will be -1.
			if idx == -1 {
				hash := parser.DigestNormalized(normalizeExplainSQL)
				return x.Stmt, normalizeExplainSQL, hash.String(), nil
			}
			normalizeSQL := normalizeExplainSQL[idx:]
			hash := parser.DigestNormalized(normalizeSQL)
			return x.Stmt, normalizeSQL, hash.String(), nil
		}
	case *ast.SelectStmt, *ast.SetOprStmt, *ast.DeleteStmt, *ast.UpdateStmt, *ast.InsertStmt:
		core.EraseLastSemicolon(x)
		// This function is only used to find bind record.
		// For some SQLs, such as `explain select * from t`, they will be entered here many times,
		// but some of them do not want to obtain bind record.
		// The difference between them is whether len(x.Text()) is empty. They cannot be distinguished by stmt.restore.
		// For these cases, we need return "" as normalize SQL and hash.
		if len(x.Text()) == 0 {
			return x, "", "", nil
		}

		var normalizedSQL string
		var hash *parser.Digest
		if forBinding {
			// Apply additional binding rules
			normalizedSQL, hash = parser.NormalizeDigestForBinding(utilparser.RestoreWithDefaultDB(x, specifiledDB, x.Text()))
		} else {
			normalizedSQL, hash = parser.NormalizeDigest(utilparser.RestoreWithDefaultDB(x, specifiledDB, x.Text()))
		}

		return x, normalizedSQL, hash.String(), nil
	}
	return nil, "", "", nil
}

func getBindRecord(ctx sessionctx.Context, stmt ast.StmtNode) (*bindinfo.BindRecord, string, error) {
	// When the domain is initializing, the bind will be nil.
	if ctx.Value(bindinfo.SessionBindInfoKeyType) == nil {
		return nil, "", nil
	}
	stmtNode, normalizedSQL, sqlDigest, err := ExtractSelectAndNormalizeDigest(stmt, ctx.GetSessionVars().CurrentDB, true)
	if err != nil || stmtNode == nil {
		return nil, "", err
	}
	sessionHandle := ctx.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
	bindRecord := sessionHandle.GetSessionBinding(sqlDigest, normalizedSQL, "")
	if bindRecord != nil {
		if bindRecord.HasEnabledBinding() {
			return bindRecord, metrics.ScopeSession, nil
		}
		return nil, "", nil
	}
	globalHandle := domain.GetDomain(ctx).BindHandle()
	if globalHandle == nil {
		return nil, "", nil
	}
	bindRecord = globalHandle.GetGlobalBinding(sqlDigest, normalizedSQL, "")
	return bindRecord, metrics.ScopeGlobal, nil
}

func handleInvalidBinding(ctx context.Context, sctx sessionctx.Context, level string, bindRecord bindinfo.BindRecord) {
	sessionHandle := sctx.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
	err := sessionHandle.DropSessionBinding(bindRecord.OriginalSQL, bindRecord.Db, &bindRecord.Bindings[0])
	if err != nil {
		logutil.Logger(ctx).Info("drop session bindings failed")
	}
	if level == metrics.ScopeSession {
		return
	}

	globalHandle := domain.GetDomain(sctx).BindHandle()
	globalHandle.AddInvalidGlobalBinding(&bindRecord)
}

func handleStmtHints(hints []*ast.TableOptimizerHint) (stmtHints stmtctx.StmtHints, offs []int, warns []error) {
	if len(hints) == 0 {
		return
	}
	hintOffs := make(map[string]int, len(hints))
	var forceNthPlan *ast.TableOptimizerHint
	var memoryQuotaHintCnt, useToJAHintCnt, useCascadesHintCnt, noIndexMergeHintCnt, readReplicaHintCnt, maxExecutionTimeCnt, forceNthPlanCnt, straightJoinHintCnt, resourceGroupHintCnt int
	setVars := make(map[string]string)
	setVarsOffs := make([]int, 0, len(hints))
	for i, hint := range hints {
		switch hint.HintName.L {
		case "memory_quota":
			hintOffs[hint.HintName.L] = i
			memoryQuotaHintCnt++
		case "resource_group":
			hintOffs[hint.HintName.L] = i
			resourceGroupHintCnt++
		case "use_toja":
			hintOffs[hint.HintName.L] = i
			useToJAHintCnt++
		case "use_cascades":
			hintOffs[hint.HintName.L] = i
			useCascadesHintCnt++
		case "no_index_merge":
			hintOffs[hint.HintName.L] = i
			noIndexMergeHintCnt++
		case "read_consistent_replica":
			hintOffs[hint.HintName.L] = i
			readReplicaHintCnt++
		case "max_execution_time":
			hintOffs[hint.HintName.L] = i
			maxExecutionTimeCnt++
		case "nth_plan":
			forceNthPlanCnt++
			forceNthPlan = hint
		case "straight_join":
			hintOffs[hint.HintName.L] = i
			straightJoinHintCnt++
		case "set_var":
			setVarHint := hint.HintData.(ast.HintSetVar)

			// Not all session variables are permitted for use with SET_VAR
			sysVar := variable.GetSysVar(setVarHint.VarName)
			if sysVar == nil {
				warns = append(warns, core.ErrUnresolvedHintName.GenWithStackByArgs(setVarHint.VarName, hint.HintName.String()))
				continue
			}
			if !sysVar.IsHintUpdatableVerfied {
				warns = append(warns, core.ErrNotHintUpdatable.GenWithStackByArgs(setVarHint.VarName))
			}
			// If several hints with the same variable name appear in the same statement, the first one is applied and the others are ignored with a warning
			if _, ok := setVars[setVarHint.VarName]; ok {
				msg := fmt.Sprintf("%s(%s=%s)", hint.HintName.String(), setVarHint.VarName, setVarHint.Value)
				warns = append(warns, core.ErrWarnConflictingHint.GenWithStackByArgs(msg))
				continue
			}
			setVars[setVarHint.VarName] = setVarHint.Value
			setVarsOffs = append(setVarsOffs, i)
		}
	}
	stmtHints.OriginalTableHints = hints
	stmtHints.SetVars = setVars

	// Handle MEMORY_QUOTA
	if memoryQuotaHintCnt != 0 {
		memoryQuotaHint := hints[hintOffs["memory_quota"]]
		if memoryQuotaHintCnt > 1 {
			warn := errors.Errorf("MEMORY_QUOTA() is defined more than once, only the last definition takes effect: MEMORY_QUOTA(%v)", memoryQuotaHint.HintData.(int64))
			warns = append(warns, warn)
		}
		// Executor use MemoryQuota <= 0 to indicate no memory limit, here use < 0 to handle hint syntax error.
		if memoryQuota := memoryQuotaHint.HintData.(int64); memoryQuota < 0 {
			delete(hintOffs, "memory_quota")
			warn := errors.New("The use of MEMORY_QUOTA hint is invalid, valid usage: MEMORY_QUOTA(10 MB) or MEMORY_QUOTA(10 GB)")
			warns = append(warns, warn)
		} else {
			stmtHints.HasMemQuotaHint = true
			stmtHints.MemQuotaQuery = memoryQuota
			if memoryQuota == 0 {
				warn := errors.New("Setting the MEMORY_QUOTA to 0 means no memory limit")
				warns = append(warns, warn)
			}
		}
	}
	// Handle USE_TOJA
	if useToJAHintCnt != 0 {
		useToJAHint := hints[hintOffs["use_toja"]]
		if useToJAHintCnt > 1 {
			warn := errors.Errorf("USE_TOJA() is defined more than once, only the last definition takes effect: USE_TOJA(%v)", useToJAHint.HintData.(bool))
			warns = append(warns, warn)
		}
		stmtHints.HasAllowInSubqToJoinAndAggHint = true
		stmtHints.AllowInSubqToJoinAndAgg = useToJAHint.HintData.(bool)
	}
	// Handle USE_CASCADES
	if useCascadesHintCnt != 0 {
		useCascadesHint := hints[hintOffs["use_cascades"]]
		if useCascadesHintCnt > 1 {
			warn := errors.Errorf("USE_CASCADES() is defined more than once, only the last definition takes effect: USE_CASCADES(%v)", useCascadesHint.HintData.(bool))
			warns = append(warns, warn)
		}
		stmtHints.HasEnableCascadesPlannerHint = true
		stmtHints.EnableCascadesPlanner = useCascadesHint.HintData.(bool)
	}
	// Handle NO_INDEX_MERGE
	if noIndexMergeHintCnt != 0 {
		if noIndexMergeHintCnt > 1 {
			warn := errors.New("NO_INDEX_MERGE() is defined more than once, only the last definition takes effect")
			warns = append(warns, warn)
		}
		stmtHints.NoIndexMergeHint = true
	}
	// Handle straight_join
	if straightJoinHintCnt != 0 {
		if straightJoinHintCnt > 1 {
			warn := errors.New("STRAIGHT_JOIN() is defined more than once, only the last definition takes effect")
			warns = append(warns, warn)
		}
		stmtHints.StraightJoinOrder = true
	}
	// Handle READ_CONSISTENT_REPLICA
	if readReplicaHintCnt != 0 {
		if readReplicaHintCnt > 1 {
			warn := errors.New("READ_CONSISTENT_REPLICA() is defined more than once, only the last definition takes effect")
			warns = append(warns, warn)
		}
		stmtHints.HasReplicaReadHint = true
		stmtHints.ReplicaRead = byte(kv.ReplicaReadFollower)
	}
	// Handle MAX_EXECUTION_TIME
	if maxExecutionTimeCnt != 0 {
		maxExecutionTime := hints[hintOffs["max_execution_time"]]
		if maxExecutionTimeCnt > 1 {
			warn := errors.Errorf("MAX_EXECUTION_TIME() is defined more than once, only the last definition takes effect: MAX_EXECUTION_TIME(%v)", maxExecutionTime.HintData.(uint64))
			warns = append(warns, warn)
		}
		stmtHints.HasMaxExecutionTime = true
		stmtHints.MaxExecutionTime = maxExecutionTime.HintData.(uint64)
	}
	// Handle RESOURCE_GROUP
	if resourceGroupHintCnt != 0 {
		resourceGroup := hints[hintOffs["resource_group"]]
		if resourceGroupHintCnt > 1 {
			warn := errors.Errorf("RESOURCE_GROUP() is defined more than once, only the last definition takes effect: RESOURCE_GROUP(%v)", resourceGroup.HintData.(string))
			warns = append(warns, warn)
		}
		stmtHints.HasResourceGroup = true
		stmtHints.ResourceGroup = resourceGroup.HintData.(string)
	}
	// Handle NTH_PLAN
	if forceNthPlanCnt != 0 {
		if forceNthPlanCnt > 1 {
			warn := errors.Errorf("NTH_PLAN() is defined more than once, only the last definition takes effect: NTH_PLAN(%v)", forceNthPlan.HintData.(int64))
			warns = append(warns, warn)
		}
		stmtHints.ForceNthPlan = forceNthPlan.HintData.(int64)
		if stmtHints.ForceNthPlan < 1 {
			stmtHints.ForceNthPlan = -1
			warn := errors.Errorf("the hintdata for NTH_PLAN() is too small, hint ignored")
			warns = append(warns, warn)
		}
	} else {
		stmtHints.ForceNthPlan = -1
	}
	for _, off := range hintOffs {
		offs = append(offs, off)
	}
	offs = append(offs, setVarsOffs...)
	// let hint is always ordered, it is convenient to human compare and test.
	sort.Ints(offs)
	return
}

func init() {
	core.OptimizeAstNode = Optimize
	core.IsReadOnly = IsReadOnly
	core.ExtractSelectAndNormalizeDigest = ExtractSelectAndNormalizeDigest
}

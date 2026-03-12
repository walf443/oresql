package engine

import (
	"fmt"
	"strings"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/engine/eval"
	"github.com/walf443/oresql/engine/expr"
	"github.com/walf443/oresql/engine/scalar"
)

// Type aliases for backward compatibility.
type ExprEvaluator = eval.ExprEvaluator
type SubqueryRunner = eval.SubqueryRunner

// makeSubqueryRunner creates a SubqueryRunner from an Executor.
// Returns nil if exec is nil (e.g. in tests without subquery support).
func makeSubqueryRunner(exec *Executor) SubqueryRunner {
	if exec == nil {
		return nil
	}
	return func(subquery *ast.SelectStmt, ev ExprEvaluator, row Row) (*eval.SubqueryResult, error) {
		result, err := exec.executeSelectMaybeCorrelated(subquery, ev, row)
		if err != nil {
			return nil, err
		}
		return &eval.SubqueryResult{Rows: result.Rows}, nil
	}
}

// tableEvaluator evaluates expressions against a single table.
type tableEvaluator struct {
	runner SubqueryRunner
	info   *TableInfo
}

func newTableEvaluator(runner SubqueryRunner, info *TableInfo) *tableEvaluator {
	return &tableEvaluator{runner: runner, info: info}
}

func (te *tableEvaluator) GetSubqueryRunner() SubqueryRunner { return te.runner }

func (te *tableEvaluator) Eval(e ast.Expr, row Row) (Value, error) {
	return eval.Generic(e, row, te)
}

func (te *tableEvaluator) ResolveColumn(tableName, colName string) (*ColumnInfo, error) {
	if err := expr.ValidateTableRef(tableName, te.info.Name); err != nil {
		return nil, err
	}
	return te.info.FindColumn(colName)
}

func (te *tableEvaluator) ColumnList() []ColumnInfo {
	return te.info.Columns
}

// joinEvaluator evaluates expressions against a joined (merged) row.
type joinEvaluator struct {
	runner SubqueryRunner
	jc     *JoinContext
}

func newJoinEvaluator(runner SubqueryRunner, jc *JoinContext) *joinEvaluator {
	return &joinEvaluator{runner: runner, jc: jc}
}

func (je *joinEvaluator) GetSubqueryRunner() SubqueryRunner { return je.runner }

func (je *joinEvaluator) Eval(e ast.Expr, row Row) (Value, error) {
	return eval.Generic(e, row, je)
}

func (je *joinEvaluator) ResolveColumn(tableName, colName string) (*ColumnInfo, error) {
	return je.jc.FindColumn(tableName, colName)
}

func (je *joinEvaluator) ColumnList() []ColumnInfo {
	return je.jc.StarColumnList()
}

// groupEvaluator evaluates expressions in a GROUP BY context.
type groupEvaluator struct {
	runner    SubqueryRunner
	info      *TableInfo
	groupRows []Row
}

func newGroupEvaluator(runner SubqueryRunner, info *TableInfo, groupRows []Row) *groupEvaluator {
	return &groupEvaluator{runner: runner, info: info, groupRows: groupRows}
}

func (ge *groupEvaluator) GetSubqueryRunner() SubqueryRunner { return ge.runner }

func (ge *groupEvaluator) Eval(e ast.Expr, row Row) (Value, error) {
	if call, ok := e.(*ast.CallExpr); ok {
		if scalar.IsScalar(call.Name) {
			return eval.Generic(e, row, ge)
		}
		val, _, err := evalAggregate(call, ge.groupRows, ge.info)
		return val, err
	}
	return eval.Generic(e, row, ge)
}

func (ge *groupEvaluator) ResolveColumn(tableName, colName string) (*ColumnInfo, error) {
	if err := expr.ValidateTableRef(tableName, ge.info.Name); err != nil {
		return nil, err
	}
	return ge.info.FindColumn(colName)
}

func (ge *groupEvaluator) ColumnList() []ColumnInfo {
	return ge.info.Columns
}

// resultEvaluator evaluates expressions against already-projected result rows.
type resultEvaluator struct {
	runner     SubqueryRunner
	selectCols []ast.Expr
	colNames   []string
}

func newResultEvaluator(runner SubqueryRunner, selectCols []ast.Expr, colNames []string) *resultEvaluator {
	return &resultEvaluator{runner: runner, selectCols: selectCols, colNames: colNames}
}

func (re *resultEvaluator) GetSubqueryRunner() SubqueryRunner { return re.runner }

func (re *resultEvaluator) Eval(e ast.Expr, row Row) (Value, error) {
	val := re.resolveOrderByValue(e, row)
	if val != nil {
		return val, nil
	}
	return nil, nil
}

func (re *resultEvaluator) ResolveColumn(tableName, colName string) (*ColumnInfo, error) {
	lower := strings.ToLower(colName)
	for i, name := range re.colNames {
		if strings.ToLower(name) == lower {
			return &ColumnInfo{Name: name, Index: i}, nil
		}
	}
	return nil, fmt.Errorf("column %q not found in result", colName)
}

func (re *resultEvaluator) ColumnList() []ColumnInfo {
	cols := make([]ColumnInfo, len(re.colNames))
	for i, name := range re.colNames {
		cols[i] = ColumnInfo{Name: name, Index: i}
	}
	return cols
}

func (re *resultEvaluator) resolveOrderByValue(orderExpr ast.Expr, resultRow Row) Value {
	if ident, ok := orderExpr.(*ast.IdentExpr); ok {
		for i, col := range re.selectCols {
			inner := col
			if a, ok := col.(*ast.AliasExpr); ok {
				if strings.ToLower(a.Alias) == strings.ToLower(ident.Name) {
					return resultRow[i]
				}
				inner = a.Expr
			}
			if selIdent, ok := inner.(*ast.IdentExpr); ok {
				if strings.ToLower(selIdent.Name) == strings.ToLower(ident.Name) {
					return resultRow[i]
				}
			}
		}
	}
	if call, ok := orderExpr.(*ast.CallExpr); ok {
		for i, col := range re.selectCols {
			inner := col
			if a, ok := col.(*ast.AliasExpr); ok {
				inner = a.Expr
			}
			if selCall, ok := inner.(*ast.CallExpr); ok {
				if selCall.Name == call.Name {
					return resultRow[i]
				}
			}
		}
	}
	return nil
}

// windowEvaluator wraps an inner evaluator and resolves WindowExpr references
// from extended row columns.
type windowEvaluator struct {
	inner      ExprEvaluator
	selectCols []ast.Expr
	windowMap  map[int]int
	numOrig    int
}

func (we *windowEvaluator) GetSubqueryRunner() SubqueryRunner { return we.inner.GetSubqueryRunner() }

func (we *windowEvaluator) Eval(e ast.Expr, row Row) (Value, error) {
	if _, ok := e.(*ast.WindowExpr); ok {
		for i, col := range we.selectCols {
			inner := col
			if a, ok := col.(*ast.AliasExpr); ok {
				inner = a.Expr
			}
			if inner == e {
				if colIdx, found := we.windowMap[i]; found {
					return row[colIdx], nil
				}
			}
		}
		return nil, fmt.Errorf("window function not found in result")
	}
	origRow := row
	if len(row) > we.numOrig {
		origRow = row[:we.numOrig]
	}
	return we.inner.Eval(e, origRow)
}

func (we *windowEvaluator) ResolveColumn(tableName, colName string) (*ColumnInfo, error) {
	return we.inner.ResolveColumn(tableName, colName)
}

func (we *windowEvaluator) ColumnList() []ColumnInfo {
	return we.inner.ColumnList()
}

// literalEvaluator evaluates expressions in a context without a table (SELECT without FROM).
type literalEvaluator struct {
	runner SubqueryRunner
}

func newLiteralEvaluator(runner SubqueryRunner) *literalEvaluator {
	return &literalEvaluator{runner: runner}
}

func (le *literalEvaluator) GetSubqueryRunner() SubqueryRunner { return le.runner }

func (le *literalEvaluator) Eval(e ast.Expr, row Row) (Value, error) {
	return eval.Generic(e, row, le)
}

func (le *literalEvaluator) ResolveColumn(tableName, colName string) (*ColumnInfo, error) {
	return nil, fmt.Errorf("column reference %q not allowed without FROM", colName)
}

func (le *literalEvaluator) ColumnList() []ColumnInfo {
	return nil
}

// pkOnlyEvaluator is a lightweight evaluator for PK-only covering scans.
type pkOnlyEvaluator struct {
	runner SubqueryRunner
	info   *TableInfo
	col    ColumnInfo
}

func newPKOnlyEvaluator(runner SubqueryRunner, info *TableInfo) *pkOnlyEvaluator {
	pkCol := info.Columns[info.PrimaryKeyCol]
	return &pkOnlyEvaluator{
		runner: runner,
		info:   info,
		col:    ColumnInfo{Name: pkCol.Name, DataType: pkCol.DataType, Index: 0},
	}
}

func (pe *pkOnlyEvaluator) GetSubqueryRunner() SubqueryRunner { return pe.runner }

func (pe *pkOnlyEvaluator) Eval(e ast.Expr, row Row) (Value, error) {
	return eval.Generic(e, row, pe)
}

func (pe *pkOnlyEvaluator) ResolveColumn(tableName, colName string) (*ColumnInfo, error) {
	if err := expr.ValidateTableRef(tableName, pe.info.Name); err != nil {
		return nil, err
	}
	if strings.ToLower(colName) == strings.ToLower(pe.col.Name) {
		return &pe.col, nil
	}
	return nil, fmt.Errorf("column %q not available in PK-only scan", colName)
}

func (pe *pkOnlyEvaluator) ColumnList() []ColumnInfo {
	return []ColumnInfo{pe.col}
}

// correlatedEvaluator evaluates expressions in a correlated subquery context.
type correlatedEvaluator struct {
	runner   SubqueryRunner
	inner    ExprEvaluator
	outer    ExprEvaluator
	outerRow Row
	numInner int
}

func newCorrelatedEvaluator(runner SubqueryRunner, inner, outer ExprEvaluator, outerRow Row, numInner int) *correlatedEvaluator {
	return &correlatedEvaluator{runner: runner, inner: inner, outer: outer, outerRow: outerRow, numInner: numInner}
}

func (ce *correlatedEvaluator) GetSubqueryRunner() SubqueryRunner { return ce.runner }

func (ce *correlatedEvaluator) Eval(e ast.Expr, row Row) (Value, error) {
	mergedRow := make(Row, len(row)+len(ce.outerRow))
	copy(mergedRow, row)
	copy(mergedRow[len(row):], ce.outerRow)
	return eval.Generic(e, mergedRow, ce)
}

func (ce *correlatedEvaluator) ResolveColumn(tableName, colName string) (*ColumnInfo, error) {
	col, err := ce.inner.ResolveColumn(tableName, colName)
	if err == nil {
		return col, nil
	}
	col, outerErr := ce.outer.ResolveColumn(tableName, colName)
	if outerErr != nil {
		return nil, err
	}
	return &ColumnInfo{
		Name:     col.Name,
		DataType: col.DataType,
		Index:    col.Index + ce.numInner,
	}, nil
}

func (ce *correlatedEvaluator) ColumnList() []ColumnInfo {
	return ce.inner.ColumnList()
}

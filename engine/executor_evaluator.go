package engine

import (
	"fmt"
	"strings"

	"github.com/walf443/oresql/ast"
)

// ExprEvaluator abstracts expression evaluation across different contexts
// (single table, join, group by, result row).
type ExprEvaluator interface {
	Eval(expr ast.Expr, row Row) (Value, error)
	ResolveColumn(tableName, colName string) (*ColumnInfo, error)
	ColumnList() []ColumnInfo // for SELECT * expansion
}

// tableEvaluator evaluates expressions against a single table.
type tableEvaluator struct {
	info *TableInfo
}

func newTableEvaluator(info *TableInfo) *tableEvaluator {
	return &tableEvaluator{info: info}
}

func (te *tableEvaluator) Eval(expr ast.Expr, row Row) (Value, error) {
	return evalExprGeneric(expr, row, te)
}

func (te *tableEvaluator) ResolveColumn(tableName, colName string) (*ColumnInfo, error) {
	if err := validateTableRef(tableName, te.info.Name); err != nil {
		return nil, err
	}
	return te.info.FindColumn(colName)
}

func (te *tableEvaluator) ColumnList() []ColumnInfo {
	return te.info.Columns
}

// joinEvaluator evaluates expressions against a joined (merged) row.
type joinEvaluator struct {
	jc *JoinContext
}

func newJoinEvaluator(jc *JoinContext) *joinEvaluator {
	return &joinEvaluator{jc: jc}
}

func (je *joinEvaluator) Eval(expr ast.Expr, row Row) (Value, error) {
	return evalExprGeneric(expr, row, je)
}

func (je *joinEvaluator) ResolveColumn(tableName, colName string) (*ColumnInfo, error) {
	return je.jc.FindColumn(tableName, colName)
}

func (je *joinEvaluator) ColumnList() []ColumnInfo {
	return je.jc.MergedInfo.Columns
}

// groupEvaluator evaluates expressions in a GROUP BY context.
// For aggregate functions (CallExpr), it evaluates against the group rows.
// For other expressions, it delegates to evalExprGeneric using the representative row.
type groupEvaluator struct {
	info      *TableInfo
	groupRows []Row
}

func newGroupEvaluator(info *TableInfo, groupRows []Row) *groupEvaluator {
	return &groupEvaluator{info: info, groupRows: groupRows}
}

func (ge *groupEvaluator) Eval(expr ast.Expr, row Row) (Value, error) {
	// Intercept CallExpr for aggregate evaluation
	if call, ok := expr.(*ast.CallExpr); ok {
		if isScalarFunc(call.Name) {
			return evalExprGeneric(expr, row, ge)
		}
		val, _, err := evalAggregate(call, ge.groupRows, ge.info)
		return val, err
	}
	return evalExprGeneric(expr, row, ge)
}

func (ge *groupEvaluator) ResolveColumn(tableName, colName string) (*ColumnInfo, error) {
	if err := validateTableRef(tableName, ge.info.Name); err != nil {
		return nil, err
	}
	return ge.info.FindColumn(colName)
}

func (ge *groupEvaluator) ColumnList() []ColumnInfo {
	return ge.info.Columns
}

// resultEvaluator evaluates expressions against already-projected result rows.
// Used for ORDER BY after GROUP BY, where expressions need to be resolved
// against SELECT column names/positions.
type resultEvaluator struct {
	selectCols []ast.Expr // original SELECT expressions (with AliasExpr)
	colNames   []string   // resolved column names
}

func newResultEvaluator(selectCols []ast.Expr, colNames []string) *resultEvaluator {
	return &resultEvaluator{selectCols: selectCols, colNames: colNames}
}

func (re *resultEvaluator) Eval(expr ast.Expr, row Row) (Value, error) {
	// Try to match the expression to a SELECT column
	val := re.resolveOrderByValue(expr, row)
	if val != nil {
		return val, nil
	}
	// If not matched, return nil (same as original resolveOrderByValue behavior)
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

// resolveOrderByValue finds the value for an ORDER BY expression from a result row.
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

// evalExprGeneric is the unified expression evaluator that delegates column resolution
// to the ExprEvaluator interface.
func evalExprGeneric(expr ast.Expr, row Row, eval ExprEvaluator) (Value, error) {
	switch e := expr.(type) {
	case *ast.IdentExpr:
		col, err := eval.ResolveColumn(e.Table, e.Name)
		if err != nil {
			return nil, err
		}
		return row[col.Index], nil
	case *ast.IntLitExpr:
		return e.Value, nil
	case *ast.FloatLitExpr:
		return e.Value, nil
	case *ast.StringLitExpr:
		return e.Value, nil
	case *ast.NullLitExpr:
		return nil, nil
	case *ast.IsNullExpr:
		val, err := eval.Eval(e.Expr, row)
		if err != nil {
			return nil, err
		}
		if e.Not {
			return val != nil, nil
		}
		return val == nil, nil
	case *ast.InExpr:
		left, err := eval.Eval(e.Left, row)
		if err != nil {
			return nil, err
		}
		if left == nil {
			return false, nil
		}
		for _, valExpr := range e.Values {
			val, err := eval.Eval(valExpr, row)
			if err != nil {
				return nil, err
			}
			match, err := evalComparison(left, "=", val)
			if err != nil {
				return nil, err
			}
			if match {
				return !e.Not, nil
			}
		}
		return e.Not, nil
	case *ast.BetweenExpr:
		left, err := eval.Eval(e.Left, row)
		if err != nil {
			return nil, err
		}
		if left == nil {
			return false, nil
		}
		low, err := eval.Eval(e.Low, row)
		if err != nil {
			return nil, err
		}
		high, err := eval.Eval(e.High, row)
		if err != nil {
			return nil, err
		}
		geq, err := evalComparison(left, ">=", low)
		if err != nil {
			return nil, err
		}
		leq, err := evalComparison(left, "<=", high)
		if err != nil {
			return nil, err
		}
		result := geq && leq
		if e.Not {
			return !result, nil
		}
		return result, nil
	case *ast.LikeExpr:
		left, err := eval.Eval(e.Left, row)
		if err != nil {
			return nil, err
		}
		pattern, err := eval.Eval(e.Pattern, row)
		if err != nil {
			return nil, err
		}
		if left == nil || pattern == nil {
			return false, nil
		}
		leftStr, ok := left.(string)
		if !ok {
			return nil, fmt.Errorf("LIKE requires string operand, got %T", left)
		}
		patternStr, ok := pattern.(string)
		if !ok {
			return nil, fmt.Errorf("LIKE requires string pattern, got %T", pattern)
		}
		result := matchLike(leftStr, patternStr)
		if e.Not {
			return !result, nil
		}
		return result, nil
	case *ast.ArithmeticExpr:
		left, err := eval.Eval(e.Left, row)
		if err != nil {
			return nil, err
		}
		right, err := eval.Eval(e.Right, row)
		if err != nil {
			return nil, err
		}
		return evalArithmetic(left, e.Op, right)
	case *ast.BinaryExpr:
		left, err := eval.Eval(e.Left, row)
		if err != nil {
			return nil, err
		}
		right, err := eval.Eval(e.Right, row)
		if err != nil {
			return nil, err
		}
		return evalComparison(left, e.Op, right)
	case *ast.LogicalExpr:
		left, err := eval.Eval(e.Left, row)
		if err != nil {
			return nil, err
		}
		leftBool, ok := left.(bool)
		if !ok {
			return nil, fmt.Errorf("expected boolean in %s expression, got %T", e.Op, left)
		}
		right, err := eval.Eval(e.Right, row)
		if err != nil {
			return nil, err
		}
		rightBool, ok := right.(bool)
		if !ok {
			return nil, fmt.Errorf("expected boolean in %s expression, got %T", e.Op, right)
		}
		switch e.Op {
		case "AND":
			return leftBool && rightBool, nil
		case "OR":
			return leftBool || rightBool, nil
		default:
			return nil, fmt.Errorf("unknown logical operator: %s", e.Op)
		}
	case *ast.NotExpr:
		val, err := eval.Eval(e.Expr, row)
		if err != nil {
			return nil, err
		}
		b, ok := val.(bool)
		if !ok {
			return nil, fmt.Errorf("NOT requires boolean operand, got %T", val)
		}
		return !b, nil
	case *ast.CaseExpr:
		if e.Operand != nil {
			// Simple CASE: compare operand with each WHEN value
			operandVal, err := eval.Eval(e.Operand, row)
			if err != nil {
				return nil, err
			}
			for _, w := range e.Whens {
				whenVal, err := eval.Eval(w.When, row)
				if err != nil {
					return nil, err
				}
				match, err := evalComparison(operandVal, "=", whenVal)
				if err != nil {
					return nil, err
				}
				if match {
					return eval.Eval(w.Then, row)
				}
			}
		} else {
			// Searched CASE: evaluate each WHEN condition as boolean
			for _, w := range e.Whens {
				whenVal, err := eval.Eval(w.When, row)
				if err != nil {
					return nil, err
				}
				b, ok := whenVal.(bool)
				if !ok {
					// NULL or non-boolean treated as false (SQL standard)
					continue
				}
				if b {
					return eval.Eval(w.Then, row)
				}
			}
		}
		if e.Else != nil {
			return eval.Eval(e.Else, row)
		}
		return nil, nil
	case *ast.CallExpr:
		return evalScalarFuncGeneric(e, row, eval)
	default:
		return nil, fmt.Errorf("cannot evaluate expression: %T", expr)
	}
}

// evalScalarFuncGeneric evaluates a scalar function call using the generic evaluator.
func evalScalarFuncGeneric(call *ast.CallExpr, row Row, eval ExprEvaluator) (Value, error) {
	switch call.Name {
	case "COALESCE":
		for _, arg := range call.Args {
			val, err := eval.Eval(arg, row)
			if err != nil {
				return nil, err
			}
			if val != nil {
				return val, nil
			}
		}
		return nil, nil
	case "NULLIF":
		if len(call.Args) != 2 {
			return nil, fmt.Errorf("NULLIF requires exactly 2 arguments, got %d", len(call.Args))
		}
		val1, err := eval.Eval(call.Args[0], row)
		if err != nil {
			return nil, err
		}
		val2, err := eval.Eval(call.Args[1], row)
		if err != nil {
			return nil, err
		}
		if val1 == nil || val2 == nil {
			return val1, nil
		}
		eq, err := evalComparison(val1, "=", val2)
		if err != nil {
			return val1, nil
		}
		if eq {
			return nil, nil
		}
		return val1, nil
	default:
		return nil, fmt.Errorf("aggregate function %s not allowed in this context", call.Name)
	}
}

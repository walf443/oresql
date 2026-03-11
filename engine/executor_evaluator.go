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
	GetExecutor() *Executor   // for subquery evaluation (EXISTS, etc.)
}

// tableEvaluator evaluates expressions against a single table.
type tableEvaluator struct {
	exec *Executor
	info *TableInfo
}

func newTableEvaluator(exec *Executor, info *TableInfo) *tableEvaluator {
	return &tableEvaluator{exec: exec, info: info}
}

func (te *tableEvaluator) GetExecutor() *Executor { return te.exec }

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
	exec *Executor
	jc   *JoinContext
}

func newJoinEvaluator(exec *Executor, jc *JoinContext) *joinEvaluator {
	return &joinEvaluator{exec: exec, jc: jc}
}

func (je *joinEvaluator) GetExecutor() *Executor { return je.exec }

func (je *joinEvaluator) Eval(expr ast.Expr, row Row) (Value, error) {
	return evalExprGeneric(expr, row, je)
}

func (je *joinEvaluator) ResolveColumn(tableName, colName string) (*ColumnInfo, error) {
	return je.jc.FindColumn(tableName, colName)
}

func (je *joinEvaluator) ColumnList() []ColumnInfo {
	return je.jc.StarColumnList()
}

// groupEvaluator evaluates expressions in a GROUP BY context.
// For aggregate functions (CallExpr), it evaluates against the group rows.
// For other expressions, it delegates to evalExprGeneric using the representative row.
type groupEvaluator struct {
	exec      *Executor
	info      *TableInfo
	groupRows []Row
}

func newGroupEvaluator(exec *Executor, info *TableInfo, groupRows []Row) *groupEvaluator {
	return &groupEvaluator{exec: exec, info: info, groupRows: groupRows}
}

func (ge *groupEvaluator) GetExecutor() *Executor { return ge.exec }

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
	exec       *Executor
	selectCols []ast.Expr // original SELECT expressions (with AliasExpr)
	colNames   []string   // resolved column names
}

func newResultEvaluator(exec *Executor, selectCols []ast.Expr, colNames []string) *resultEvaluator {
	return &resultEvaluator{exec: exec, selectCols: selectCols, colNames: colNames}
}

func (re *resultEvaluator) GetExecutor() *Executor { return re.exec }

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

// windowEvaluator wraps an inner evaluator and resolves WindowExpr references
// from extended row columns.
type windowEvaluator struct {
	exec       *Executor
	inner      ExprEvaluator
	selectCols []ast.Expr  // SELECT columns (for pointer matching)
	windowMap  map[int]int // selectCol index → extended row column index
	numOrig    int         // number of original columns before extension
}

func (we *windowEvaluator) GetExecutor() *Executor { return we.exec }

func (we *windowEvaluator) Eval(expr ast.Expr, row Row) (Value, error) {
	// Check if this is a window expression from SELECT columns
	if _, ok := expr.(*ast.WindowExpr); ok {
		// Find which select column this WindowExpr matches
		for i, col := range we.selectCols {
			inner := col
			if a, ok := col.(*ast.AliasExpr); ok {
				inner = a.Expr
			}
			if inner == expr {
				if colIdx, found := we.windowMap[i]; found {
					return row[colIdx], nil
				}
			}
		}
		return nil, fmt.Errorf("window function not found in result")
	}
	// For non-window expressions, use original row width
	origRow := row
	if len(row) > we.numOrig {
		origRow = row[:we.numOrig]
	}
	return we.inner.Eval(expr, origRow)
}

func (we *windowEvaluator) ResolveColumn(tableName, colName string) (*ColumnInfo, error) {
	return we.inner.ResolveColumn(tableName, colName)
}

func (we *windowEvaluator) ColumnList() []ColumnInfo {
	return we.inner.ColumnList()
}

// literalEvaluator evaluates expressions in a context without a table (SELECT without FROM).
// It supports scalar subqueries via the executor.
type literalEvaluator struct {
	exec *Executor
}

func newLiteralEvaluator(exec *Executor) *literalEvaluator {
	return &literalEvaluator{exec: exec}
}

func (le *literalEvaluator) GetExecutor() *Executor { return le.exec }

func (le *literalEvaluator) Eval(expr ast.Expr, row Row) (Value, error) {
	return evalExprGeneric(expr, row, le)
}

func (le *literalEvaluator) ResolveColumn(tableName, colName string) (*ColumnInfo, error) {
	return nil, fmt.Errorf("column reference %q not allowed without FROM", colName)
}

func (le *literalEvaluator) ColumnList() []ColumnInfo {
	return nil
}

// pkOnlyEvaluator is a lightweight evaluator for PK-only covering scans.
// Rows contain a single element: the PK value at index 0.
type pkOnlyEvaluator struct {
	exec *Executor
	info *TableInfo
	col  ColumnInfo // PK column with Index remapped to 0
}

func newPKOnlyEvaluator(exec *Executor, info *TableInfo) *pkOnlyEvaluator {
	pkCol := info.Columns[info.PrimaryKeyCol]
	return &pkOnlyEvaluator{
		exec: exec,
		info: info,
		col:  ColumnInfo{Name: pkCol.Name, DataType: pkCol.DataType, Index: 0},
	}
}

func (pe *pkOnlyEvaluator) GetExecutor() *Executor { return pe.exec }

func (pe *pkOnlyEvaluator) Eval(expr ast.Expr, row Row) (Value, error) {
	return evalExprGeneric(expr, row, pe)
}

func (pe *pkOnlyEvaluator) ResolveColumn(tableName, colName string) (*ColumnInfo, error) {
	if err := validateTableRef(tableName, pe.info.Name); err != nil {
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
	case *ast.BoolLitExpr:
		return e.Value, nil
	case *ast.IsNullExpr:
		val, err := eval.Eval(e.Expr, row)
		if err != nil {
			return nil, err
		}
		if e.Not {
			return val != nil, nil
		}
		return val == nil, nil
	case *ast.IsJSONExpr:
		val, err := eval.Eval(e.Expr, row)
		if err != nil {
			return nil, err
		}
		result := isValidJSON(val)
		if e.Not {
			return !result, nil
		}
		return result, nil
	case *ast.InExpr:
		left, err := eval.Eval(e.Left, row)
		if err != nil {
			return nil, err
		}
		if left == nil {
			return false, nil
		}
		if e.Subquery != nil {
			exec := eval.GetExecutor()
			if exec == nil {
				return nil, fmt.Errorf("IN subquery not supported in this context")
			}
			result, err := exec.executeSelectMaybeCorrelated(e.Subquery, eval, row)
			if err != nil {
				return nil, err
			}
			for _, r := range result.Rows {
				if len(r) == 0 {
					continue
				}
				match, err := evalComparison(left, "=", r[0])
				if err != nil {
					return nil, err
				}
				if match {
					return !e.Not, nil
				}
			}
			return e.Not, nil
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
	case *ast.MatchExpr:
		val, err := eval.Eval(e.Expr, row)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return false, nil
		}
		text, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("@@ requires TEXT operand, got %T", val)
		}
		return matchFullText(text, e.Pattern, e.Tokenizer), nil
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
		// Short-circuit evaluation
		if e.Op == "AND" && !leftBool {
			return false, nil
		}
		if e.Op == "OR" && leftBool {
			return true, nil
		}
		right, err := eval.Eval(e.Right, row)
		if err != nil {
			return nil, err
		}
		rightBool, ok := right.(bool)
		if !ok {
			return nil, fmt.Errorf("expected boolean in %s expression, got %T", e.Op, right)
		}
		return evalLogicalOp(leftBool, e.Op, rightBool)
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
	case *ast.ScalarExpr:
		exec := eval.GetExecutor()
		if exec == nil {
			return nil, fmt.Errorf("scalar subquery not supported in this context")
		}
		result, err := exec.executeSelectMaybeCorrelated(e.Subquery, eval, row)
		if err != nil {
			return nil, err
		}
		if len(result.Rows) == 0 {
			return nil, nil
		}
		if len(result.Rows) > 1 {
			return nil, fmt.Errorf("scalar subquery must return at most one row, got %d", len(result.Rows))
		}
		return result.Rows[0][0], nil
	case *ast.ExistsExpr:
		exec := eval.GetExecutor()
		if exec == nil {
			return nil, fmt.Errorf("EXISTS subquery not supported in this context")
		}
		result, err := exec.executeSelectMaybeCorrelated(e.Subquery, eval, row)
		if err != nil {
			return nil, err
		}
		hasRows := len(result.Rows) > 0
		if e.Not {
			return !hasRows, nil
		}
		return hasRows, nil
	case *ast.WindowExpr:
		return nil, fmt.Errorf("window function %s not allowed in this context", e.Name)
	case *ast.CastExpr:
		return evalCast(e, row, eval)
	case *ast.CallExpr:
		return evalScalarFuncGeneric(e, row, eval)
	default:
		return nil, fmt.Errorf("cannot evaluate expression: %T", expr)
	}
}

// correlatedEvaluator evaluates expressions in a correlated subquery context.
// It wraps an inner evaluator (for the subquery's own table) and an outer evaluator
// (for the outer query). Column resolution tries inner first, then falls back to outer
// with an index offset. Eval builds a merged row [innerRow | outerRow].
type correlatedEvaluator struct {
	exec     *Executor
	inner    ExprEvaluator
	outer    ExprEvaluator
	outerRow Row
	numInner int // number of inner columns (offset for outer columns)
}

func newCorrelatedEvaluator(exec *Executor, inner, outer ExprEvaluator, outerRow Row, numInner int) *correlatedEvaluator {
	return &correlatedEvaluator{exec: exec, inner: inner, outer: outer, outerRow: outerRow, numInner: numInner}
}

func (ce *correlatedEvaluator) GetExecutor() *Executor { return ce.exec }

func (ce *correlatedEvaluator) Eval(expr ast.Expr, row Row) (Value, error) {
	mergedRow := make(Row, len(row)+len(ce.outerRow))
	copy(mergedRow, row)
	copy(mergedRow[len(row):], ce.outerRow)
	return evalExprGeneric(expr, mergedRow, ce)
}

func (ce *correlatedEvaluator) ResolveColumn(tableName, colName string) (*ColumnInfo, error) {
	col, err := ce.inner.ResolveColumn(tableName, colName)
	if err == nil {
		return col, nil
	}
	// Fallback to outer evaluator with offset
	col, outerErr := ce.outer.ResolveColumn(tableName, colName)
	if outerErr != nil {
		return nil, err // return original inner error
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

// hasOuterReferences checks whether a subquery's AST references columns from the outer evaluator.
// It collects inner table names and walks the AST to find IdentExpr with Table qualifiers
// that are not inner tables but can be resolved by the outer evaluator.
func hasOuterReferences(stmt *ast.SelectStmt, outerEval ExprEvaluator) bool {
	// Collect inner table names/aliases
	innerTables := make(map[string]bool)
	if stmt.TableName != "" {
		innerTables[strings.ToLower(stmt.TableName)] = true
	}
	if stmt.TableAlias != "" {
		innerTables[strings.ToLower(stmt.TableAlias)] = true
	}
	for _, j := range stmt.Joins {
		if j.TableName != "" {
			innerTables[strings.ToLower(j.TableName)] = true
		}
		if j.TableAlias != "" {
			innerTables[strings.ToLower(j.TableAlias)] = true
		}
	}

	// Walk AST to find outer references
	var found bool
	var walk func(expr ast.Expr)
	walk = func(expr ast.Expr) {
		if expr == nil || found {
			return
		}
		switch e := expr.(type) {
		case *ast.IdentExpr:
			if e.Table != "" && !innerTables[strings.ToLower(e.Table)] {
				// Table qualifier not in inner tables — check if outer can resolve
				if _, err := outerEval.ResolveColumn(e.Table, e.Name); err == nil {
					found = true
				}
			}
		case *ast.BinaryExpr:
			walk(e.Left)
			walk(e.Right)
		case *ast.LogicalExpr:
			walk(e.Left)
			walk(e.Right)
		case *ast.NotExpr:
			walk(e.Expr)
		case *ast.IsNullExpr:
			walk(e.Expr)
		case *ast.InExpr:
			walk(e.Left)
			for _, v := range e.Values {
				walk(v)
			}
		case *ast.BetweenExpr:
			walk(e.Left)
			walk(e.Low)
			walk(e.High)
		case *ast.LikeExpr:
			walk(e.Left)
			walk(e.Pattern)
		case *ast.MatchExpr:
			walk(e.Expr)
		case *ast.ArithmeticExpr:
			walk(e.Left)
			walk(e.Right)
		case *ast.AliasExpr:
			walk(e.Expr)
		case *ast.CallExpr:
			for _, arg := range e.Args {
				walk(arg)
			}
		case *ast.CaseExpr:
			walk(e.Operand)
			for _, w := range e.Whens {
				walk(w.When)
				walk(w.Then)
			}
			walk(e.Else)
		case *ast.CastExpr:
			walk(e.Expr)
		case *ast.BoolLitExpr:
			// leaf — no children
		case *ast.ExistsExpr:
			// Don't recurse into nested subqueries
		case *ast.ScalarExpr:
			// Don't recurse into nested subqueries
		}
	}

	// Walk WHERE, Columns, Having
	walk(stmt.Where)
	for _, col := range stmt.Columns {
		walk(col)
	}
	walk(stmt.Having)
	return found
}

// evalScalarFuncGeneric evaluates a scalar function call using the generic evaluator.
func evalScalarFuncGeneric(call *ast.CallExpr, row Row, eval ExprEvaluator) (Value, error) {
	evalFn := func(expr ast.Expr) (Value, error) {
		return eval.Eval(expr, row)
	}

	// Special-case functions that need lazy evaluation or extra context.
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
	case "JSON_VALUE", "JSON_QUERY", "JSON_EXISTS":
		args, err := evalArgsWith(call.Args, evalFn)
		if err != nil {
			return nil, err
		}
		compiled := tryCompileJSONPath(call)
		return evalJSONPathFunc(call.Name, args, compiled)
	}

	// Registry-based dispatch for standard scalar functions.
	if fn, ok := scalarFuncRegistry[call.Name]; ok {
		args, err := evalArgsWith(call.Args, evalFn)
		if err != nil {
			return nil, err
		}
		return fn(args)
	}

	return nil, fmt.Errorf("aggregate function %s not allowed in this context", call.Name)
}

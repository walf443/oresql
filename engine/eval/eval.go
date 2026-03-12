// Package eval provides the expression evaluation framework and generic
// expression evaluator with no dependency on the engine package.
package eval

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/engine/expr"
	"github.com/walf443/oresql/engine/scalar"
	"github.com/walf443/oresql/jsonb"
	"github.com/walf443/oresql/storage"
)

// Value is an alias for storage.Value.
type Value = storage.Value

// Row is an alias for storage.Row.
type Row = storage.Row

// ColumnInfo is an alias for storage.ColumnInfo.
type ColumnInfo = storage.ColumnInfo

// SubqueryResult holds the rows returned by a subquery execution.
type SubqueryResult struct {
	Rows []Row
}

// SubqueryRunner executes a subquery in the context of an outer row.
// This decouples ExprEvaluator from concrete executor types, enabling package splitting.
type SubqueryRunner func(subquery *ast.SelectStmt, eval ExprEvaluator, row Row) (*SubqueryResult, error)

// ExprEvaluator abstracts expression evaluation across different contexts
// (single table, join, group by, result row).
type ExprEvaluator interface {
	Eval(expr ast.Expr, row Row) (Value, error)
	ResolveColumn(tableName, colName string) (*ColumnInfo, error)
	ColumnList() []ColumnInfo          // for SELECT * expansion
	GetSubqueryRunner() SubqueryRunner // for subquery evaluation (EXISTS, IN subquery, scalar subquery)
}

// Generic is the unified expression evaluator that delegates column resolution
// to the ExprEvaluator interface.
func Generic(e ast.Expr, row Row, eval ExprEvaluator) (Value, error) {
	switch e := e.(type) {
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
		return IsNull(e, row, eval)
	case *ast.IsJSONExpr:
		return IsJSON(e, row, eval)
	case *ast.InExpr:
		return In(e, row, eval)
	case *ast.BetweenExpr:
		return Between(e, row, eval)
	case *ast.LikeExpr:
		return Like(e, row, eval)
	case *ast.MatchExpr:
		return Match(e, row, eval)
	case *ast.ArithmeticExpr:
		return Arithmetic(e, row, eval)
	case *ast.BinaryExpr:
		return Binary(e, row, eval)
	case *ast.LogicalExpr:
		return Logical(e, row, eval)
	case *ast.NotExpr:
		return Not(e, row, eval)
	case *ast.CaseExpr:
		return Case(e, row, eval)
	case *ast.ScalarExpr:
		return ScalarSubquery(e, row, eval)
	case *ast.ExistsExpr:
		return Exists(e, row, eval)
	case *ast.WindowExpr:
		return nil, fmt.Errorf("window function %s not allowed in this context", e.Name)
	case *ast.CastExpr:
		return Cast(e, row, eval)
	case *ast.CallExpr:
		return ScalarFunc(e, row, eval)
	default:
		return nil, fmt.Errorf("cannot evaluate expression: %T", e)
	}
}

// Where evaluates a WHERE expression using the given evaluator and returns a boolean.
func Where(e ast.Expr, row Row, eval ExprEvaluator) (bool, error) {
	val, err := eval.Eval(e, row)
	if err != nil {
		return false, err
	}
	b, ok := val.(bool)
	if !ok {
		return false, fmt.Errorf("WHERE expression must evaluate to boolean, got %T", val)
	}
	return b, nil
}

// IsNull evaluates IS NULL / IS NOT NULL.
func IsNull(e *ast.IsNullExpr, row Row, eval ExprEvaluator) (Value, error) {
	val, err := eval.Eval(e.Expr, row)
	if err != nil {
		return nil, err
	}
	if e.Not {
		return val != nil, nil
	}
	return val == nil, nil
}

// IsJSON evaluates IS JSON / IS NOT JSON.
func IsJSON(e *ast.IsJSONExpr, row Row, eval ExprEvaluator) (Value, error) {
	val, err := eval.Eval(e.Expr, row)
	if err != nil {
		return nil, err
	}
	result := scalar.IsValidJSON(val)
	if e.Not {
		return !result, nil
	}
	return result, nil
}

// In evaluates an IN expression (with value list or subquery).
func In(e *ast.InExpr, row Row, eval ExprEvaluator) (Value, error) {
	left, err := eval.Eval(e.Left, row)
	if err != nil {
		return nil, err
	}
	if left == nil {
		return false, nil
	}
	if e.Subquery != nil {
		return inSubquery(e, left, row, eval)
	}
	for _, valExpr := range e.Values {
		val, err := eval.Eval(valExpr, row)
		if err != nil {
			return nil, err
		}
		match, err := expr.Comparison(left, "=", val)
		if err != nil {
			return nil, err
		}
		if match {
			return !e.Not, nil
		}
	}
	return e.Not, nil
}

// inSubquery evaluates an IN expression with a subquery.
func inSubquery(e *ast.InExpr, left Value, row Row, eval ExprEvaluator) (Value, error) {
	runner := eval.GetSubqueryRunner()
	if runner == nil {
		return nil, fmt.Errorf("IN subquery not supported in this context")
	}
	result, err := runner(e.Subquery, eval, row)
	if err != nil {
		return nil, err
	}
	for _, r := range result.Rows {
		if len(r) == 0 {
			continue
		}
		match, err := expr.Comparison(left, "=", r[0])
		if err != nil {
			return nil, err
		}
		if match {
			return !e.Not, nil
		}
	}
	return e.Not, nil
}

// Between evaluates a BETWEEN expression.
func Between(e *ast.BetweenExpr, row Row, eval ExprEvaluator) (Value, error) {
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
	geq, err := expr.Comparison(left, ">=", low)
	if err != nil {
		return nil, err
	}
	leq, err := expr.Comparison(left, "<=", high)
	if err != nil {
		return nil, err
	}
	result := geq && leq
	if e.Not {
		return !result, nil
	}
	return result, nil
}

// Like evaluates a LIKE expression.
func Like(e *ast.LikeExpr, row Row, eval ExprEvaluator) (Value, error) {
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
	result := expr.MatchLike(leftStr, patternStr)
	if e.Not {
		return !result, nil
	}
	return result, nil
}

// Match evaluates the @@ full-text match operator.
func Match(e *ast.MatchExpr, row Row, eval ExprEvaluator) (Value, error) {
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
	return expr.MatchFullText(text, e.Pattern, e.Tokenizer), nil
}

// Arithmetic evaluates an arithmetic expression (+, -, *, /, %).
func Arithmetic(e *ast.ArithmeticExpr, row Row, eval ExprEvaluator) (Value, error) {
	left, err := eval.Eval(e.Left, row)
	if err != nil {
		return nil, err
	}
	right, err := eval.Eval(e.Right, row)
	if err != nil {
		return nil, err
	}
	return expr.Arithmetic(left, e.Op, right)
}

// Binary evaluates a binary comparison expression (=, !=, <, >, <=, >=).
func Binary(e *ast.BinaryExpr, row Row, eval ExprEvaluator) (Value, error) {
	left, err := eval.Eval(e.Left, row)
	if err != nil {
		return nil, err
	}
	right, err := eval.Eval(e.Right, row)
	if err != nil {
		return nil, err
	}
	return expr.Comparison(left, e.Op, right)
}

// Logical evaluates a logical expression (AND/OR) with short-circuit.
func Logical(e *ast.LogicalExpr, row Row, eval ExprEvaluator) (Value, error) {
	left, err := eval.Eval(e.Left, row)
	if err != nil {
		return nil, err
	}
	leftBool, ok := left.(bool)
	if !ok {
		return nil, fmt.Errorf("expected boolean in %s expression, got %T", e.Op, left)
	}
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
	return expr.LogicalOp(leftBool, e.Op, rightBool)
}

// Not evaluates a NOT expression.
func Not(e *ast.NotExpr, row Row, eval ExprEvaluator) (Value, error) {
	val, err := eval.Eval(e.Expr, row)
	if err != nil {
		return nil, err
	}
	b, ok := val.(bool)
	if !ok {
		return nil, fmt.Errorf("NOT requires boolean operand, got %T", val)
	}
	return !b, nil
}

// Case evaluates a CASE expression (simple or searched).
func Case(e *ast.CaseExpr, row Row, eval ExprEvaluator) (Value, error) {
	if e.Operand != nil {
		operandVal, err := eval.Eval(e.Operand, row)
		if err != nil {
			return nil, err
		}
		for _, w := range e.Whens {
			whenVal, err := eval.Eval(w.When, row)
			if err != nil {
				return nil, err
			}
			match, err := expr.Comparison(operandVal, "=", whenVal)
			if err != nil {
				return nil, err
			}
			if match {
				return eval.Eval(w.Then, row)
			}
		}
	} else {
		for _, w := range e.Whens {
			whenVal, err := eval.Eval(w.When, row)
			if err != nil {
				return nil, err
			}
			b, ok := whenVal.(bool)
			if !ok {
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
}

// ScalarSubquery evaluates a scalar subquery expression.
func ScalarSubquery(e *ast.ScalarExpr, row Row, eval ExprEvaluator) (Value, error) {
	runner := eval.GetSubqueryRunner()
	if runner == nil {
		return nil, fmt.Errorf("scalar subquery not supported in this context")
	}
	result, err := runner(e.Subquery, eval, row)
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
}

// Exists evaluates an EXISTS subquery expression.
func Exists(e *ast.ExistsExpr, row Row, eval ExprEvaluator) (Value, error) {
	runner := eval.GetSubqueryRunner()
	if runner == nil {
		return nil, fmt.Errorf("EXISTS subquery not supported in this context")
	}
	result, err := runner(e.Subquery, eval, row)
	if err != nil {
		return nil, err
	}
	hasRows := len(result.Rows) > 0
	if e.Not {
		return !hasRows, nil
	}
	return hasRows, nil
}

// Cast evaluates a CAST(expr AS type) expression.
func Cast(cast *ast.CastExpr, row Row, eval ExprEvaluator) (Value, error) {
	val, err := eval.Eval(cast.Expr, row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	switch cast.TargetType {
	case "INT":
		switch v := val.(type) {
		case int64:
			return v, nil
		case float64:
			return int64(v), nil
		case string:
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot cast %q to INT", v)
			}
			return n, nil
		default:
			return nil, fmt.Errorf("cannot cast %T to INT", val)
		}
	case "FLOAT":
		switch v := val.(type) {
		case float64:
			return v, nil
		case int64:
			return float64(v), nil
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot cast %q to FLOAT", v)
			}
			return f, nil
		default:
			return nil, fmt.Errorf("cannot cast %T to FLOAT", val)
		}
	case "TEXT":
		switch v := val.(type) {
		case string:
			return v, nil
		case int64:
			return strconv.FormatInt(v, 10), nil
		case float64:
			return strconv.FormatFloat(v, 'f', -1, 64), nil
		default:
			return nil, fmt.Errorf("cannot cast %T to TEXT", val)
		}
	case "JSON":
		switch v := val.(type) {
		case string:
			if !json.Valid([]byte(v)) {
				return nil, fmt.Errorf("cannot cast %q to JSON: invalid JSON", v)
			}
			return v, nil
		case []byte:
			s, err := jsonb.ToJSON(v)
			if err != nil {
				return nil, fmt.Errorf("cannot cast JSONB to JSON: %w", err)
			}
			return s, nil
		default:
			return nil, fmt.Errorf("cannot cast %T to JSON", val)
		}
	case "JSONB":
		switch v := val.(type) {
		case string:
			b, err := jsonb.FromJSON(v)
			if err != nil {
				return nil, fmt.Errorf("cannot cast %q to JSONB: %w", v, err)
			}
			return b, nil
		case []byte:
			return v, nil
		default:
			return nil, fmt.Errorf("cannot cast %T to JSONB", val)
		}
	default:
		return nil, fmt.Errorf("unsupported CAST target type: %s", cast.TargetType)
	}
}

// ScalarFunc evaluates a scalar function call using the generic evaluator.
func ScalarFunc(call *ast.CallExpr, row Row, eval ExprEvaluator) (Value, error) {
	evalFn := func(e ast.Expr) (Value, error) {
		return eval.Eval(e, row)
	}

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
		eq, err := expr.Comparison(val1, "=", val2)
		if err != nil {
			return val1, nil
		}
		if eq {
			return nil, nil
		}
		return val1, nil
	case "JSON_VALUE", "JSON_QUERY", "JSON_EXISTS":
		args, err := scalar.EvalArgsWith(call.Args, evalFn)
		if err != nil {
			return nil, err
		}
		compiled := scalar.TryCompileJSONPath(call)
		return scalar.EvalJSONPathFunc(call.Name, args, compiled)
	}

	if fn, ok := scalar.Registry[call.Name]; ok {
		args, err := scalar.EvalArgsWith(call.Args, evalFn)
		if err != nil {
			return nil, err
		}
		return fn(args)
	}

	return nil, fmt.Errorf("aggregate function %s not allowed in this context", call.Name)
}

// HasOuterReferences checks whether a subquery's AST references columns from the outer evaluator.
func HasOuterReferences(stmt *ast.SelectStmt, outerEval ExprEvaluator) bool {
	innerTables := CollectInnerTableNames(stmt)

	var found bool
	var walk func(e ast.Expr)
	walk = func(e ast.Expr) {
		if e == nil || found {
			return
		}
		if ident, ok := e.(*ast.IdentExpr); ok {
			if ident.Table != "" && !innerTables[strings.ToLower(ident.Table)] {
				if _, err := outerEval.ResolveColumn(ident.Table, ident.Name); err == nil {
					found = true
				}
			}
		}
		expr.ForEachChild(e, walk)
	}

	walk(stmt.Where)
	for _, col := range stmt.Columns {
		walk(col)
	}
	walk(stmt.Having)
	return found
}

// CollectInnerTableNames collects table names and aliases from a SELECT statement.
func CollectInnerTableNames(stmt *ast.SelectStmt) map[string]bool {
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
	return innerTables
}

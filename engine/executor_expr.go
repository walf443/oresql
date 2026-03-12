package engine

import (
	"encoding/json"
	"fmt"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/engine/expr"
	"github.com/walf443/oresql/engine/scalar"
	"github.com/walf443/oresql/jsonb"
)

// validateAndCoerceValue validates a value against a column definition, coercing types as needed.
func validateAndCoerceValue(val Value, col ColumnInfo) (Value, error) {
	if val == nil {
		if col.NotNull {
			return nil, fmt.Errorf("column %q cannot be NULL", col.Name)
		}
		return nil, nil
	}
	switch col.DataType {
	case "INT":
		if _, ok := val.(int64); !ok {
			return nil, fmt.Errorf("column %q expects INT, got %T", col.Name, val)
		}
	case "FLOAT":
		switch v := val.(type) {
		case float64:
			// ok
		case int64:
			val = float64(v)
		default:
			return nil, fmt.Errorf("column %q expects FLOAT, got %T", col.Name, val)
		}
	case "TEXT":
		if _, ok := val.(string); !ok {
			return nil, fmt.Errorf("column %q expects TEXT, got %T", col.Name, val)
		}
	case "JSON":
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("column %q expects JSON, got %T", col.Name, val)
		}
		if !json.Valid([]byte(s)) {
			return nil, fmt.Errorf("column %q: invalid JSON value: %s", col.Name, s)
		}
	case "JSONB":
		switch v := val.(type) {
		case []byte:
			// Already msgpack bytes
			return v, nil
		case string:
			// Convert JSON string to msgpack
			b, err := jsonb.FromJSON(v)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", col.Name, err)
			}
			return b, nil
		default:
			return nil, fmt.Errorf("column %q expects JSONB, got %T", col.Name, val)
		}
	}
	return val, nil
}

// evalLiteral evaluates a literal expression (for INSERT VALUES and SELECT without FROM).
func evalLiteral(e ast.Expr) (Value, error) {
	switch e := e.(type) {
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
	case *ast.ArithmeticExpr:
		left, err := evalLiteral(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := evalLiteral(e.Right)
		if err != nil {
			return nil, err
		}
		return expr.Arithmetic(left, e.Op, right)
	case *ast.CallExpr:
		return evalScalarFuncLiteral(e)
	default:
		return nil, fmt.Errorf("expected literal value, got %T", e)
	}
}

// evalScalarFuncLiteral evaluates a scalar function in a literal-only context (no table).
func evalScalarFuncLiteral(call *ast.CallExpr) (Value, error) {
	// Special-case functions that need lazy evaluation or extra context.
	switch call.Name {
	case "COALESCE":
		for _, arg := range call.Args {
			val, err := evalLiteral(arg)
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
		val1, err := evalLiteral(call.Args[0])
		if err != nil {
			return nil, err
		}
		val2, err := evalLiteral(call.Args[1])
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
		args, err := scalar.EvalArgsWith(call.Args, evalLiteral)
		if err != nil {
			return nil, err
		}
		compiled := scalar.TryCompileJSONPath(call)
		return scalar.EvalJSONPathFunc(call.Name, args, compiled)
	}

	// Registry-based dispatch for standard scalar functions.
	if fn, ok := scalar.Registry[call.Name]; ok {
		args, err := scalar.EvalArgsWith(call.Args, evalLiteral)
		if err != nil {
			return nil, err
		}
		return fn(args)
	}

	return nil, fmt.Errorf("function %s not supported in literal context", call.Name)
}

// formatExpr returns a display name for an expression.
func formatExpr(e ast.Expr) string {
	switch e := e.(type) {
	case *ast.IntLitExpr:
		return fmt.Sprintf("%d", e.Value)
	case *ast.FloatLitExpr:
		return fmt.Sprintf("%g", e.Value)
	case *ast.StringLitExpr:
		return "'" + e.Value + "'"
	case *ast.NullLitExpr:
		return "NULL"
	case *ast.BoolLitExpr:
		if e.Value {
			return "TRUE"
		}
		return "FALSE"
	case *ast.IdentExpr:
		if e.Table != "" {
			return e.Table + "." + e.Name
		}
		return e.Name
	case *ast.ArithmeticExpr:
		return formatExpr(e.Left) + " " + e.Op + " " + formatExpr(e.Right)
	default:
		return "?"
	}
}

// extractLikePrefix extracts the literal prefix from a LIKE pattern.
// It returns characters up to the first unescaped '%' or '_'.
// Escape sequences: \% -> %, \_ -> _, \\ -> \.
func extractLikePrefix(pattern string) string {
	var prefix []byte
	i := 0
	for i < len(pattern) {
		if pattern[i] == '\\' && i+1 < len(pattern) {
			// Escaped character: add the literal
			prefix = append(prefix, pattern[i+1])
			i += 2
		} else if pattern[i] == '%' || pattern[i] == '_' {
			break
		} else {
			prefix = append(prefix, pattern[i])
			i++
		}
	}
	return string(prefix)
}

// nextPrefix computes the exclusive upper bound for a prefix range scan.
// It increments the last byte; if 0xFF, truncates and retries.
// Returns ("", false) if no upper bound exists (all 0xFF or empty).
func nextPrefix(s string) (string, bool) {
	b := []byte(s)
	for len(b) > 0 {
		last := b[len(b)-1]
		if last < 0xFF {
			b[len(b)-1] = last + 1
			return string(b), true
		}
		b = b[:len(b)-1]
	}
	return "", false
}

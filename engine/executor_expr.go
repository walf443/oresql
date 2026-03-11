package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/json_path"
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
func evalLiteral(expr ast.Expr) (Value, error) {
	switch e := expr.(type) {
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
		return evalArithmetic(left, e.Op, right)
	case *ast.CallExpr:
		return evalScalarFuncLiteral(e)
	default:
		return nil, fmt.Errorf("expected literal value, got %T", expr)
	}
}

// evalScalarFuncLiteral evaluates a scalar function in a literal-only context (no table).
func evalScalarFuncLiteral(call *ast.CallExpr) (Value, error) {
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
		eq, err := evalComparison(val1, "=", val2)
		if err != nil {
			return val1, nil
		}
		if eq {
			return nil, nil
		}
		return val1, nil
	case "ABS", "ROUND", "MOD", "CEIL", "FLOOR", "POWER":
		args := make([]Value, len(call.Args))
		for i, arg := range call.Args {
			val, err := evalLiteral(arg)
			if err != nil {
				return nil, err
			}
			args[i] = val
		}
		return evalNumericFunc(call, args)
	case "LENGTH", "UPPER", "LOWER", "SUBSTRING", "TRIM", "CONCAT":
		args := make([]Value, len(call.Args))
		for i, arg := range call.Args {
			val, err := evalLiteral(arg)
			if err != nil {
				return nil, err
			}
			args[i] = val
		}
		return evalStringFunc(call, args)
	case "JSON_OBJECT":
		args := make([]Value, len(call.Args))
		for i, arg := range call.Args {
			val, err := evalLiteral(arg)
			if err != nil {
				return nil, err
			}
			args[i] = val
		}
		return evalFuncJSONObject(args)
	case "JSON_ARRAY":
		args := make([]Value, len(call.Args))
		for i, arg := range call.Args {
			val, err := evalLiteral(arg)
			if err != nil {
				return nil, err
			}
			args[i] = val
		}
		return evalFuncJSONArray(args)
	case "JSON_VALUE", "JSON_QUERY", "JSON_EXISTS":
		args := make([]Value, len(call.Args))
		for i, arg := range call.Args {
			val, err := evalLiteral(arg)
			if err != nil {
				return nil, err
			}
			args[i] = val
		}
		compiled := tryCompileJSONPath(call)
		if call.Name == "JSON_QUERY" {
			return evalFuncJSONQuery(args, compiled)
		}
		if call.Name == "JSON_EXISTS" {
			return evalFuncJSONExists(args, compiled)
		}
		return evalFuncJSONValue(args, compiled)
	default:
		return nil, fmt.Errorf("function %s not supported in literal context", call.Name)
	}
}

// evalExpr evaluates an expression against a row.
func evalExpr(expr ast.Expr, row Row, info *TableInfo) (Value, error) {
	switch e := expr.(type) {
	case *ast.IdentExpr:
		if err := validateTableRef(e.Table, info.Name); err != nil {
			return nil, err
		}
		col, err := info.FindColumn(e.Name)
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
		val, err := evalExpr(e.Expr, row, info)
		if err != nil {
			return nil, err
		}
		if e.Not {
			return val != nil, nil
		}
		return val == nil, nil
	case *ast.IsJSONExpr:
		val, err := evalExpr(e.Expr, row, info)
		if err != nil {
			return nil, err
		}
		result := isValidJSON(val)
		if e.Not {
			return !result, nil
		}
		return result, nil
	case *ast.InExpr:
		left, err := evalExpr(e.Left, row, info)
		if err != nil {
			return nil, err
		}
		if left == nil {
			return false, nil
		}
		for _, valExpr := range e.Values {
			val, err := evalExpr(valExpr, row, info)
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
		left, err := evalExpr(e.Left, row, info)
		if err != nil {
			return nil, err
		}
		if left == nil {
			return false, nil
		}
		low, err := evalExpr(e.Low, row, info)
		if err != nil {
			return nil, err
		}
		high, err := evalExpr(e.High, row, info)
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
		left, err := evalExpr(e.Left, row, info)
		if err != nil {
			return nil, err
		}
		pattern, err := evalExpr(e.Pattern, row, info)
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
		val, err := evalExpr(e.Expr, row, info)
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
		left, err := evalExpr(e.Left, row, info)
		if err != nil {
			return nil, err
		}
		right, err := evalExpr(e.Right, row, info)
		if err != nil {
			return nil, err
		}
		return evalArithmetic(left, e.Op, right)
	case *ast.BinaryExpr:
		left, err := evalExpr(e.Left, row, info)
		if err != nil {
			return nil, err
		}
		right, err := evalExpr(e.Right, row, info)
		if err != nil {
			return nil, err
		}
		return evalComparison(left, e.Op, right)
	case *ast.LogicalExpr:
		left, err := evalExpr(e.Left, row, info)
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
		right, err := evalExpr(e.Right, row, info)
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
		val, err := evalExpr(e.Expr, row, info)
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
			operandVal, err := evalExpr(e.Operand, row, info)
			if err != nil {
				return nil, err
			}
			for _, w := range e.Whens {
				whenVal, err := evalExpr(w.When, row, info)
				if err != nil {
					return nil, err
				}
				match, err := evalComparison(operandVal, "=", whenVal)
				if err != nil {
					return nil, err
				}
				if match {
					return evalExpr(w.Then, row, info)
				}
			}
		} else {
			// Searched CASE: evaluate each WHEN condition as boolean
			for _, w := range e.Whens {
				whenVal, err := evalExpr(w.When, row, info)
				if err != nil {
					return nil, err
				}
				b, ok := whenVal.(bool)
				if !ok {
					// NULL or non-boolean treated as false (SQL standard)
					continue
				}
				if b {
					return evalExpr(w.Then, row, info)
				}
			}
		}
		if e.Else != nil {
			return evalExpr(e.Else, row, info)
		}
		return nil, nil
	case *ast.CallExpr:
		return evalScalarFunc(e, row, info)
	default:
		return nil, fmt.Errorf("cannot evaluate expression: %T", expr)
	}
}

// evalScalarFunc evaluates a scalar function call against a single row.
func evalScalarFunc(call *ast.CallExpr, row Row, info *TableInfo) (Value, error) {
	switch call.Name {
	case "COALESCE":
		for _, arg := range call.Args {
			val, err := evalExpr(arg, row, info)
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
		val1, err := evalExpr(call.Args[0], row, info)
		if err != nil {
			return nil, err
		}
		val2, err := evalExpr(call.Args[1], row, info)
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
	case "ABS", "ROUND", "MOD", "CEIL", "FLOOR", "POWER":
		args := make([]Value, len(call.Args))
		for i, arg := range call.Args {
			val, err := evalExpr(arg, row, info)
			if err != nil {
				return nil, err
			}
			args[i] = val
		}
		return evalNumericFunc(call, args)
	case "LENGTH", "UPPER", "LOWER", "SUBSTRING", "TRIM", "CONCAT":
		args := make([]Value, len(call.Args))
		for i, arg := range call.Args {
			val, err := evalExpr(arg, row, info)
			if err != nil {
				return nil, err
			}
			args[i] = val
		}
		return evalStringFunc(call, args)
	case "JSON_OBJECT":
		args := make([]Value, len(call.Args))
		for i, arg := range call.Args {
			val, err := evalExpr(arg, row, info)
			if err != nil {
				return nil, err
			}
			args[i] = val
		}
		return evalFuncJSONObject(args)
	case "JSON_ARRAY":
		args := make([]Value, len(call.Args))
		for i, arg := range call.Args {
			val, err := evalExpr(arg, row, info)
			if err != nil {
				return nil, err
			}
			args[i] = val
		}
		return evalFuncJSONArray(args)
	case "JSON_VALUE", "JSON_QUERY", "JSON_EXISTS":
		args := make([]Value, len(call.Args))
		for i, arg := range call.Args {
			val, err := evalExpr(arg, row, info)
			if err != nil {
				return nil, err
			}
			args[i] = val
		}
		compiled := tryCompileJSONPath(call)
		if call.Name == "JSON_QUERY" {
			return evalFuncJSONQuery(args, compiled)
		}
		if call.Name == "JSON_EXISTS" {
			return evalFuncJSONExists(args, compiled)
		}
		return evalFuncJSONValue(args, compiled)
	default:
		return nil, fmt.Errorf("aggregate function %s not allowed in this context", call.Name)
	}
}

// evalNumericFunc dispatches a numeric scalar function to its implementation.
func evalNumericFunc(call *ast.CallExpr, args []Value) (Value, error) {
	switch call.Name {
	case "ABS":
		return evalFuncAbs(args)
	case "ROUND":
		return evalFuncRound(args)
	case "MOD":
		return evalFuncMod(args)
	case "CEIL":
		return evalFuncCeil(args)
	case "FLOOR":
		return evalFuncFloor(args)
	case "POWER":
		return evalFuncPower(args)
	default:
		return nil, fmt.Errorf("unknown numeric function: %s", call.Name)
	}
}

func evalFuncAbs(args []Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ABS requires exactly 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	switch v := args[0].(type) {
	case int64:
		if v < 0 {
			return -v, nil
		}
		return v, nil
	case float64:
		return math.Abs(v), nil
	default:
		return nil, fmt.Errorf("ABS requires numeric argument, got %T", args[0])
	}
}

func evalFuncRound(args []Value) (Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("ROUND requires 1 or 2 arguments, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	d := int64(0)
	if len(args) == 2 {
		if args[1] == nil {
			return nil, nil
		}
		switch v := args[1].(type) {
		case int64:
			d = v
		default:
			return nil, fmt.Errorf("ROUND precision must be integer, got %T", args[1])
		}
	}
	switch v := args[0].(type) {
	case int64:
		if d >= 0 {
			return v, nil
		}
		shift := math.Pow(10, float64(-d))
		return int64(math.Round(float64(v)/shift) * shift), nil
	case float64:
		shift := math.Pow(10, float64(d))
		return math.Round(v*shift) / shift, nil
	default:
		return nil, fmt.Errorf("ROUND requires numeric argument, got %T", args[0])
	}
}

func evalFuncMod(args []Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("MOD requires exactly 2 arguments, got %d", len(args))
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}
	if a, ok := args[0].(int64); ok {
		if b, ok := args[1].(int64); ok {
			if b == 0 {
				return nil, fmt.Errorf("MOD division by zero")
			}
			return a % b, nil
		}
	}
	af, aok := toFloat64(args[0])
	bf, bok := toFloat64(args[1])
	if aok && bok {
		if bf == 0 {
			return nil, fmt.Errorf("MOD division by zero")
		}
		return math.Mod(af, bf), nil
	}
	return nil, fmt.Errorf("MOD requires numeric arguments, got %T and %T", args[0], args[1])
}

func evalFuncCeil(args []Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CEIL requires exactly 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	switch v := args[0].(type) {
	case int64:
		return v, nil
	case float64:
		return int64(math.Ceil(v)), nil
	default:
		return nil, fmt.Errorf("CEIL requires numeric argument, got %T", args[0])
	}
}

func evalFuncFloor(args []Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("FLOOR requires exactly 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	switch v := args[0].(type) {
	case int64:
		return v, nil
	case float64:
		return int64(math.Floor(v)), nil
	default:
		return nil, fmt.Errorf("FLOOR requires numeric argument, got %T", args[0])
	}
}

func evalFuncPower(args []Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("POWER requires exactly 2 arguments, got %d", len(args))
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}
	xf, xok := toFloat64(args[0])
	yf, yok := toFloat64(args[1])
	if !xok || !yok {
		return nil, fmt.Errorf("POWER requires numeric arguments, got %T and %T", args[0], args[1])
	}
	return math.Pow(xf, yf), nil
}

// evalStringFunc dispatches a string scalar function to its implementation.
func evalStringFunc(call *ast.CallExpr, args []Value) (Value, error) {
	switch call.Name {
	case "LENGTH":
		return evalFuncLength(args)
	case "UPPER":
		return evalFuncUpper(args)
	case "LOWER":
		return evalFuncLower(args)
	case "SUBSTRING":
		return evalFuncSubstring(args)
	case "TRIM":
		return evalFuncTrim(args)
	case "CONCAT":
		return evalFuncConcat(args)
	default:
		return nil, fmt.Errorf("unknown string function: %s", call.Name)
	}
}

func evalFuncLength(args []Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LENGTH requires exactly 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("LENGTH requires string argument, got %T", args[0])
	}
	return int64(len([]rune(s))), nil
}

func evalFuncUpper(args []Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UPPER requires exactly 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("UPPER requires string argument, got %T", args[0])
	}
	return strings.ToUpper(s), nil
}

func evalFuncLower(args []Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LOWER requires exactly 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("LOWER requires string argument, got %T", args[0])
	}
	return strings.ToLower(s), nil
}

func evalFuncSubstring(args []Value) (Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("SUBSTRING requires 2 or 3 arguments, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("SUBSTRING requires string as first argument, got %T", args[0])
	}
	pos, ok := args[1].(int64)
	if !ok {
		return nil, fmt.Errorf("SUBSTRING requires integer as second argument, got %T", args[1])
	}
	runes := []rune(s)
	// 1-indexed to 0-indexed
	start := int(pos) - 1
	if start < 0 {
		start = 0
	}
	if start >= len(runes) {
		return "", nil
	}
	if len(args) == 3 {
		if args[2] == nil {
			return nil, nil
		}
		length, ok := args[2].(int64)
		if !ok {
			return nil, fmt.Errorf("SUBSTRING requires integer as third argument, got %T", args[2])
		}
		end := start + int(length)
		if end > len(runes) {
			end = len(runes)
		}
		return string(runes[start:end]), nil
	}
	return string(runes[start:]), nil
}

func evalFuncTrim(args []Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TRIM requires exactly 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("TRIM requires string argument, got %T", args[0])
	}
	return strings.TrimSpace(s), nil
}

func evalFuncConcat(args []Value) (Value, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("CONCAT requires at least 1 argument, got %d", len(args))
	}
	var b strings.Builder
	for _, arg := range args {
		if arg == nil {
			return nil, nil
		}
		s, ok := arg.(string)
		if !ok {
			return nil, fmt.Errorf("CONCAT requires string arguments, got %T", arg)
		}
		b.WriteString(s)
	}
	return b.String(), nil
}

// evalWhere evaluates a WHERE expression and returns a boolean.
func evalWhere(expr ast.Expr, row Row, info *TableInfo) (bool, error) {
	val, err := evalExpr(expr, row, info)
	if err != nil {
		return false, err
	}
	b, ok := val.(bool)
	if !ok {
		return false, fmt.Errorf("WHERE expression must evaluate to boolean, got %T", val)
	}
	return b, nil
}

// toFloat64 converts a numeric value to float64 for mixed-type arithmetic.
func toFloat64(v Value) (float64, bool) {
	switch tv := v.(type) {
	case int64:
		return float64(tv), true
	case float64:
		return tv, true
	default:
		return 0, false
	}
}

func evalArithmetic(left Value, op string, right Value) (Value, error) {
	if left == nil || right == nil {
		return nil, nil
	}

	// Both int64: integer arithmetic
	if lv, ok := left.(int64); ok {
		if rv, ok := right.(int64); ok {
			switch op {
			case "+":
				return lv + rv, nil
			case "-":
				return lv - rv, nil
			case "*":
				return lv * rv, nil
			case "/":
				if rv == 0 {
					return nil, fmt.Errorf("division by zero")
				}
				return lv / rv, nil
			default:
				return nil, fmt.Errorf("unknown arithmetic operator: %s", op)
			}
		}
	}

	// Mixed or both float64: float arithmetic
	lf, lok := toFloat64(left)
	rf, rok := toFloat64(right)
	if lok && rok {
		switch op {
		case "+":
			return lf + rf, nil
		case "-":
			return lf - rf, nil
		case "*":
			return lf * rf, nil
		case "/":
			if rf == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return lf / rf, nil
		default:
			return nil, fmt.Errorf("unknown arithmetic operator: %s", op)
		}
	}

	return nil, fmt.Errorf("arithmetic requires numeric operands, got %T and %T", left, right)
}

func evalComparison(left Value, op string, right Value) (bool, error) {
	// NULL comparison: any comparison with NULL returns false (SQL semantics)
	if left == nil || right == nil {
		return false, nil
	}

	// Both int64
	if lv, ok := left.(int64); ok {
		if rv, ok := right.(int64); ok {
			switch op {
			case "=":
				return lv == rv, nil
			case "!=":
				return lv != rv, nil
			case "<":
				return lv < rv, nil
			case ">":
				return lv > rv, nil
			case "<=":
				return lv <= rv, nil
			case ">=":
				return lv >= rv, nil
			}
		}
	}

	// Both float64
	if lv, ok := left.(float64); ok {
		if rv, ok := right.(float64); ok {
			switch op {
			case "=":
				return lv == rv, nil
			case "!=":
				return lv != rv, nil
			case "<":
				return lv < rv, nil
			case ">":
				return lv > rv, nil
			case "<=":
				return lv <= rv, nil
			case ">=":
				return lv >= rv, nil
			}
		}
	}

	// Mixed int64 and float64: promote to float64
	lf, lok := toFloat64(left)
	rf, rok := toFloat64(right)
	if lok && rok {
		switch op {
		case "=":
			return lf == rf, nil
		case "!=":
			return lf != rf, nil
		case "<":
			return lf < rf, nil
		case ">":
			return lf > rf, nil
		case "<=":
			return lf <= rf, nil
		case ">=":
			return lf >= rf, nil
		}
	}

	// Both string
	if lv, ok := left.(string); ok {
		if rv, ok := right.(string); ok {
			switch op {
			case "=":
				return lv == rv, nil
			case "!=":
				return lv != rv, nil
			case "<":
				return lv < rv, nil
			case ">":
				return lv > rv, nil
			case "<=":
				return lv <= rv, nil
			case ">=":
				return lv >= rv, nil
			}
		}
	}

	return false, fmt.Errorf("cannot compare %T and %T with %s", left, right, op)
}

// compareValues compares two values for ORDER BY sorting.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
// NULL values sort last (are considered greater than any non-NULL value).
func compareValues(a, b Value) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return 1 // NULL sorts last
	}
	if b == nil {
		return -1 // NULL sorts last
	}

	switch av := a.(type) {
	case int64:
		switch bv := b.(type) {
		case int64:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		case float64:
			af := float64(av)
			if af < bv {
				return -1
			}
			if af > bv {
				return 1
			}
			return 0
		}
	case float64:
		switch bv := b.(type) {
		case float64:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		case int64:
			bf := float64(bv)
			if av < bf {
				return -1
			}
			if av > bf {
				return 1
			}
			return 0
		}
	case string:
		bv, ok := b.(string)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	}
	return 0
}

// validateTableRef checks that a qualified table reference matches the target table.
// If tableRef is empty (unqualified), validation is skipped.
func validateTableRef(tableRef, targetTable string) error {
	if tableRef != "" && strings.ToLower(tableRef) != strings.ToLower(targetTable) {
		return fmt.Errorf("unknown table %q", tableRef)
	}
	return nil
}

// formatExpr returns a display name for an expression.
func formatExpr(expr ast.Expr) string {
	switch e := expr.(type) {
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

// matchLike matches a string against a SQL LIKE pattern.
// '%' matches any sequence of zero or more characters.
// '_' matches exactly one character.
// '\' escapes the next character: '\%' matches literal '%', '\_' matches literal '_', '\\' matches literal '\'.
func matchLike(str, pattern string) bool {
	si, pi := 0, 0
	starPI, starSI := -1, -1

	for si < len(str) {
		if pi < len(pattern) && pattern[pi] == '\\' && pi+1 < len(pattern) {
			// Escaped character: match literally
			pi++
			if pattern[pi] == str[si] {
				si++
				pi++
			} else if starPI >= 0 {
				starSI++
				si = starSI
				pi = starPI + 1
			} else {
				return false
			}
		} else if pi < len(pattern) && pattern[pi] == '_' {
			si++
			pi++
		} else if pi < len(pattern) && pattern[pi] == '%' {
			starPI = pi
			starSI = si
			pi++
		} else if pi < len(pattern) && pattern[pi] == str[si] {
			si++
			pi++
		} else if starPI >= 0 {
			starSI++
			si = starSI
			pi = starPI + 1
		} else {
			return false
		}
	}

	for pi < len(pattern) {
		if pattern[pi] == '%' {
			pi++
		} else if pattern[pi] == '\\' && pi+1 < len(pattern) {
			break
		} else {
			break
		}
	}
	return pi == len(pattern)
}

// evalCast evaluates a CAST(expr AS type) expression.
func evalCast(cast *ast.CastExpr, row Row, eval ExprEvaluator) (Value, error) {
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
			// JSONB to JSON: decode msgpack to JSON string
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

// matchFullText checks if text contains the given search term using the specified tokenizer.
// For "word" (or empty) tokenizer, it checks exact word-token matching.
// For "bigram" tokenizer, it checks substring containment.
func matchFullText(text, searchTerm, tokenizer string) bool {
	lowerText := strings.ToLower(text)
	lower := strings.ToLower(searchTerm)
	switch tokenizer {
	case "bigram":
		return strings.Contains(lowerText, lower)
	default: // "word" or empty
		words := strings.FieldsFunc(lowerText, func(r rune) bool {
			return !isLetterOrDigit(r)
		})
		for _, w := range words {
			if w == lower {
				return true
			}
		}
		return false
	}
}

func isLetterOrDigit(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r >= 0x80
}

// valueToJSON converts a Go value to its JSON representation.
// If the value is already a valid JSON string (from JSON_OBJECT/JSON_ARRAY), it is embedded as raw JSON.
func valueToJSON(val Value) ([]byte, error) {
	if val == nil {
		return []byte("null"), nil
	}
	switch v := val.(type) {
	case string:
		if json.Valid([]byte(v)) {
			return []byte(v), nil
		}
		return json.Marshal(v)
	case []byte:
		// JSONB: decode msgpack to JSON
		s, err := jsonb.ToJSON(v)
		if err != nil {
			return nil, err
		}
		return []byte(s), nil
	case int64:
		return json.Marshal(v)
	case float64:
		return json.Marshal(v)
	case bool:
		return json.Marshal(v)
	default:
		return nil, fmt.Errorf("unsupported value type for JSON: %T", val)
	}
}

// evalFuncJSONObject builds a JSON object from alternating key-value arguments.
// Usage: JSON_OBJECT('key1', val1, 'key2', val2, ...)
func evalFuncJSONObject(args []Value) (Value, error) {
	if len(args)%2 != 0 {
		return nil, fmt.Errorf("JSON_OBJECT requires an even number of arguments (key-value pairs), got %d", len(args))
	}
	var buf strings.Builder
	buf.WriteByte('{')
	for i := 0; i < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			return nil, fmt.Errorf("JSON_OBJECT key must be a string, got %T", args[i])
		}
		if i > 0 {
			buf.WriteByte(',')
		}
		keyJSON, err := json.Marshal(key)
		if err != nil {
			return nil, err
		}
		buf.Write(keyJSON)
		buf.WriteByte(':')
		valJSON, err := valueToJSON(args[i+1])
		if err != nil {
			return nil, err
		}
		buf.Write(valJSON)
	}
	buf.WriteByte('}')
	return buf.String(), nil
}

// evalFuncJSONArray builds a JSON array from arguments.
// Usage: JSON_ARRAY(val1, val2, ...)
func evalFuncJSONArray(args []Value) (Value, error) {
	var buf strings.Builder
	buf.WriteByte('[')
	for i, arg := range args {
		if i > 0 {
			buf.WriteByte(',')
		}
		valJSON, err := valueToJSON(arg)
		if err != nil {
			return nil, err
		}
		buf.Write(valJSON)
	}
	buf.WriteByte(']')
	return buf.String(), nil
}

// parseJSONAndTraverse parses JSON text, traverses it with a path, and returns the result.
// If compiledPath is non-nil, it is used directly; otherwise pathStr is parsed on-the-fly.
// jsonStringFromValue extracts a JSON string from a value.
// Accepts string (JSON text) or []byte (JSONB/msgpack) values.
func jsonStringFromValue(funcName string, val Value) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	case []byte:
		s, err := jsonb.ToJSON(v)
		if err != nil {
			return "", fmt.Errorf("%s: %w", funcName, err)
		}
		return s, nil
	default:
		return "", fmt.Errorf("%s first argument must be a string, got %T", funcName, val)
	}
}

func parseJSONAndTraverse(funcName string, jsonStr string, pathStr string, compiledPath *json_path.Path) (any, error) {
	var raw any
	if err := json.Unmarshal([]byte(jsonStr), &raw); err != nil {
		return nil, fmt.Errorf("%s: invalid JSON: %w", funcName, err)
	}

	if compiledPath != nil {
		return compiledPath.Execute(raw), nil
	}

	result, err := json_path.Traverse(raw, pathStr)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// evalFuncJSONValue extracts a scalar value from a JSON string using a path expression.
// Usage: JSON_VALUE(json_text, path)
// Path syntax: $ for root, $.key for object member, $[n] for array index.
// Returns NULL if path points to a non-scalar (object/array) or if the path doesn't exist.
// compiledPath is an optional pre-parsed path for efficiency when the path is a literal.
func evalFuncJSONValue(args []Value, compiledPath *json_path.Path) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("JSON_VALUE requires exactly 2 arguments, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	pathStr, ok := args[1].(string)
	if compiledPath == nil && !ok {
		return nil, fmt.Errorf("JSON_VALUE second argument (path) must be a string, got %T", args[1])
	}

	// JSONB optimization: traverse binary directly without decode→JSON→parse round-trip
	if b, isBinary := args[0].([]byte); isBinary {
		result, err := jsonbTraverse("JSON_VALUE", b, pathStr, compiledPath)
		if err != nil {
			return nil, err
		}
		return jsonValueResult(result)
	}

	jsonStr, err := jsonStringFromValue("JSON_VALUE", args[0])
	if err != nil {
		return nil, err
	}

	result, err := parseJSONAndTraverse("JSON_VALUE", jsonStr, pathStr, compiledPath)
	if err != nil {
		return nil, err
	}
	return jsonValueResult(result)
}

// evalFuncJSONQuery extracts a JSON object or array from a JSON string using a path expression.
// Usage: JSON_QUERY(json_text, path)
// Returns NULL if the path points to a scalar value or doesn't exist.
// compiledPath is an optional pre-parsed path for efficiency when the path is a literal.
func evalFuncJSONQuery(args []Value, compiledPath *json_path.Path) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("JSON_QUERY requires exactly 2 arguments, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	pathStr, ok := args[1].(string)
	if compiledPath == nil && !ok {
		return nil, fmt.Errorf("JSON_QUERY second argument (path) must be a string, got %T", args[1])
	}

	// JSONB optimization
	if b, isBinary := args[0].([]byte); isBinary {
		result, err := jsonbTraverse("JSON_QUERY", b, pathStr, compiledPath)
		if err != nil {
			return nil, err
		}
		return jsonQueryResult(result)
	}

	jsonStr, err := jsonStringFromValue("JSON_QUERY", args[0])
	if err != nil {
		return nil, err
	}

	result, err := parseJSONAndTraverse("JSON_QUERY", jsonStr, pathStr, compiledPath)
	if err != nil {
		return nil, err
	}
	return jsonQueryResult(result)
}

// evalFuncJSONExists checks whether a path exists in a JSON string.
// Usage: JSON_EXISTS(json_text, path)
// Returns TRUE if the path exists (including JSON null values), FALSE otherwise.
// compiledPath is an optional pre-parsed path for efficiency when the path is a literal.
func evalFuncJSONExists(args []Value, compiledPath *json_path.Path) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("JSON_EXISTS requires exactly 2 arguments, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	pathStr, ok := args[1].(string)
	if compiledPath == nil && !ok {
		return nil, fmt.Errorf("JSON_EXISTS second argument (path) must be a string, got %T", args[1])
	}

	// JSONB optimization: use ExistsPath directly
	if b, isBinary := args[0].([]byte); isBinary {
		if compiledPath != nil {
			return jsonb.ExistsPath(b, compiledPath), nil
		}
		p, err := json_path.Parse(pathStr)
		if err != nil {
			return nil, err
		}
		return jsonb.ExistsPath(b, p), nil
	}

	jsonStr, err := jsonStringFromValue("JSON_EXISTS", args[0])
	if err != nil {
		return nil, err
	}

	var raw any
	if err := json.Unmarshal([]byte(jsonStr), &raw); err != nil {
		return nil, fmt.Errorf("JSON_EXISTS: invalid JSON: %w", err)
	}

	if compiledPath != nil {
		return compiledPath.Exists(raw), nil
	}

	p, err := json_path.Parse(pathStr)
	if err != nil {
		return nil, err
	}
	return p.Exists(raw), nil
}

// jsonbTraverse traverses JSONB binary data using a path expression directly,
// avoiding the decode→JSON→parse round-trip.
func jsonbTraverse(funcName string, b []byte, pathStr string, compiledPath *json_path.Path) (any, error) {
	if compiledPath != nil {
		val, found, err := jsonb.QueryPath(b, compiledPath)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", funcName, err)
		}
		if !found {
			return nil, nil
		}
		return val, nil
	}
	p, err := json_path.Parse(pathStr)
	if err != nil {
		return nil, err
	}
	val, found, err := jsonb.QueryPath(b, p)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", funcName, err)
	}
	if !found {
		return nil, nil
	}
	return val, nil
}

// jsonValueResult converts a traversal result to JSON_VALUE semantics:
// scalars are returned, objects/arrays return NULL.
func jsonValueResult(result any) (Value, error) {
	if result == nil {
		return nil, nil
	}
	switch v := result.(type) {
	case map[string]any, []any:
		return nil, nil
	case float64:
		if v == float64(int64(v)) {
			return int64(v), nil
		}
		return v, nil
	case int64:
		return v, nil
	case string:
		return v, nil
	case bool:
		if v {
			return "true", nil
		}
		return "false", nil
	default:
		return nil, fmt.Errorf("JSON_VALUE: unexpected type %T", result)
	}
}

// jsonQueryResult converts a traversal result to JSON_QUERY semantics:
// objects/arrays are serialized to JSON string, scalars return NULL.
func jsonQueryResult(result any) (Value, error) {
	if result == nil {
		return nil, nil
	}
	switch result.(type) {
	case map[string]any, []any:
		b, err := json.Marshal(result)
		if err != nil {
			return nil, fmt.Errorf("JSON_QUERY: failed to serialize result: %w", err)
		}
		return string(b), nil
	default:
		return nil, nil
	}
}

// isValidJSON returns true if val is a string containing valid JSON.
// Non-string types and nil return false.
func isValidJSON(val Value) bool {
	switch v := val.(type) {
	case string:
		return json.Valid([]byte(v))
	case []byte:
		// JSONB values are always valid JSON (validated on insert)
		return true
	default:
		return false
	}
}

// tryCompileJSONPath checks if the second argument of a JSON_VALUE/JSON_QUERY call
// is a string literal, and if so, pre-parses the path for reuse across rows.
func tryCompileJSONPath(call *ast.CallExpr) *json_path.Path {
	if len(call.Args) >= 2 {
		if lit, ok := call.Args[1].(*ast.StringLitExpr); ok {
			if p, err := json_path.Parse(lit.Value); err == nil {
				return p
			}
		}
	}
	return nil
}

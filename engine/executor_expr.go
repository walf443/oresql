package engine

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/walf443/oresql/ast"
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
		eq, err := evalComparison(val1, "=", val2)
		if err != nil {
			return val1, nil
		}
		if eq {
			return nil, nil
		}
		return val1, nil
	case "JSON_VALUE", "JSON_QUERY", "JSON_EXISTS":
		args, err := evalArgsWith(call.Args, evalLiteral)
		if err != nil {
			return nil, err
		}
		compiled := tryCompileJSONPath(call)
		return evalJSONPathFunc(call.Name, args, compiled)
	}

	// Registry-based dispatch for standard scalar functions.
	if fn, ok := scalarFuncRegistry[call.Name]; ok {
		args, err := evalArgsWith(call.Args, evalLiteral)
		if err != nil {
			return nil, err
		}
		return fn(args)
	}

	return nil, fmt.Errorf("function %s not supported in literal context", call.Name)
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

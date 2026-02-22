package engine

import (
	"fmt"
	"strings"

	"github.com/walf443/oresql/ast"
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
	default:
		return nil, fmt.Errorf("expected literal value, got %T", expr)
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
	case *ast.IsNullExpr:
		val, err := evalExpr(e.Expr, row, info)
		if err != nil {
			return nil, err
		}
		if e.Not {
			return val != nil, nil
		}
		return val == nil, nil
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

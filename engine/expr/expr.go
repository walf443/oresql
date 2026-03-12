// Package expr provides pure value operation and AST traversal functions
// with no dependency on the engine package.
package expr

import (
	"fmt"
	"strings"

	"github.com/walf443/oresql/ast"
)

// Value is an alias for any, matching storage.Value.
type Value = any

// Arithmetic evaluates an arithmetic expression on two values.
func Arithmetic(left Value, op string, right Value) (Value, error) {
	if left == nil || right == nil {
		return nil, nil
	}

	if lv, ok := left.(int64); ok {
		if rv, ok := right.(int64); ok {
			return applyIntArithOp(lv, op, rv)
		}
	}

	lf, lok := ToFloat64(left)
	rf, rok := ToFloat64(right)
	if lok && rok {
		return applyFloatArithOp(lf, op, rf)
	}

	return nil, fmt.Errorf("arithmetic requires numeric operands, got %T and %T", left, right)
}

func applyIntArithOp(lv int64, op string, rv int64) (Value, error) {
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

func applyFloatArithOp(lf float64, op string, rf float64) (Value, error) {
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

// Comparison evaluates a comparison expression on two values.
func Comparison(left Value, op string, right Value) (bool, error) {
	if left == nil || right == nil {
		return false, nil
	}

	switch left.(type) {
	case int64, float64:
		if _, ok := ToFloat64(right); !ok {
			return false, fmt.Errorf("cannot compare %T and %T with %s", left, right, op)
		}
	case string:
		if _, ok := right.(string); !ok {
			return false, fmt.Errorf("cannot compare %T and %T with %s", left, right, op)
		}
	default:
		return false, fmt.Errorf("cannot compare %T and %T with %s", left, right, op)
	}

	return ApplyCmpOp(Compare(left, right), op)
}

// ApplyCmpOp applies a comparison operator to the result of Compare.
func ApplyCmpOp(cmp int, op string) (bool, error) {
	switch op {
	case "=":
		return cmp == 0, nil
	case "!=":
		return cmp != 0, nil
	case "<":
		return cmp < 0, nil
	case ">":
		return cmp > 0, nil
	case "<=":
		return cmp <= 0, nil
	case ">=":
		return cmp >= 0, nil
	default:
		return false, fmt.Errorf("unknown comparison operator: %s", op)
	}
}

// Compare compares two values for sorting.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
// NULL values sort last (are considered greater than any non-NULL value).
func Compare(a, b Value) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return 1
	}
	if b == nil {
		return -1
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

// ToFloat64 converts a numeric value to float64 for mixed-type arithmetic.
func ToFloat64(v Value) (float64, bool) {
	switch tv := v.(type) {
	case int64:
		return float64(tv), true
	case float64:
		return tv, true
	default:
		return 0, false
	}
}

// LogicalOp dispatches a logical operator (AND/OR) on boolean operands.
func LogicalOp(leftBool bool, op string, rightBool bool) (Value, error) {
	switch op {
	case "AND":
		return leftBool && rightBool, nil
	case "OR":
		return leftBool || rightBool, nil
	default:
		return nil, fmt.Errorf("unknown logical operator: %s", op)
	}
}

// ValidateTableRef checks that a qualified table reference matches the target table.
// If tableRef is empty (unqualified), validation is skipped.
func ValidateTableRef(tableRef, targetTable string) error {
	if tableRef != "" && strings.ToLower(tableRef) != strings.ToLower(targetTable) {
		return fmt.Errorf("unknown table %q", tableRef)
	}
	return nil
}

// MatchLike matches a string against a SQL LIKE pattern.
// '%' matches any sequence of zero or more characters.
// '_' matches exactly one character.
// '\' escapes the next character.
func MatchLike(str, pattern string) bool {
	si, pi := 0, 0
	starPI, starSI := -1, -1

	for si < len(str) {
		if pi < len(pattern) && pattern[pi] == '\\' && pi+1 < len(pattern) {
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

// MatchFullText checks if text contains the given search term using the specified tokenizer.
// For "word" (or empty) tokenizer, it checks exact word-token matching.
// For "bigram" tokenizer, it checks substring containment.
func MatchFullText(text, searchTerm, tokenizer string) bool {
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

// ForEachChild calls fn on each direct child expression of expr.
// It handles all compound AST node types; leaf nodes produce no calls.
// Subqueries (ScalarExpr, ExistsExpr) are NOT traversed.
func ForEachChild(expr ast.Expr, fn func(ast.Expr)) {
	switch e := expr.(type) {
	case *ast.BinaryExpr:
		fn(e.Left)
		fn(e.Right)
	case *ast.LogicalExpr:
		fn(e.Left)
		fn(e.Right)
	case *ast.ArithmeticExpr:
		fn(e.Left)
		fn(e.Right)
	case *ast.NotExpr:
		fn(e.Expr)
	case *ast.IsNullExpr:
		fn(e.Expr)
	case *ast.IsJSONExpr:
		fn(e.Expr)
	case *ast.InExpr:
		fn(e.Left)
		for _, v := range e.Values {
			fn(v)
		}
	case *ast.BetweenExpr:
		fn(e.Left)
		fn(e.Low)
		fn(e.High)
	case *ast.LikeExpr:
		fn(e.Left)
		fn(e.Pattern)
	case *ast.MatchExpr:
		fn(e.Expr)
	case *ast.AliasExpr:
		fn(e.Expr)
	case *ast.CastExpr:
		fn(e.Expr)
	case *ast.CallExpr:
		for _, arg := range e.Args {
			fn(arg)
		}
	case *ast.CaseExpr:
		if e.Operand != nil {
			fn(e.Operand)
		}
		for _, w := range e.Whens {
			fn(w.When)
			fn(w.Then)
		}
		if e.Else != nil {
			fn(e.Else)
		}
	case *ast.WindowExpr:
		for _, arg := range e.Args {
			fn(arg)
		}
		for _, p := range e.PartitionBy {
			fn(p)
		}
		for _, ob := range e.OrderBy {
			fn(ob.Expr)
		}
	case *ast.IdentExpr, *ast.StarExpr,
		*ast.IntLitExpr, *ast.FloatLitExpr, *ast.StringLitExpr, *ast.BoolLitExpr, *ast.NullLitExpr,
		*ast.ScalarExpr, *ast.ExistsExpr:
		// no children
	}
}

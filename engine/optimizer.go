package engine

import (
	"github.com/walf443/oresql/ast"
)

// optimizeStatement applies constant folding to WHERE/HAVING clauses in a statement.
// Called once before execution dispatch in executeInner.
func optimizeStatement(stmt ast.Statement) {
	switch s := stmt.(type) {
	case *ast.SelectStmt:
		if s.Where != nil {
			s.Where = optimizeExpr(s.Where)
		}
		if s.Having != nil {
			s.Having = optimizeExpr(s.Having)
		}
	case *ast.UpdateStmt:
		if s.Where != nil {
			s.Where = optimizeExpr(s.Where)
		}
	case *ast.DeleteStmt:
		if s.Where != nil {
			s.Where = optimizeExpr(s.Where)
		}
	}
}

// optimizeBinaryExpr folds a constant binary comparison expression.
func optimizeBinaryExpr(e *ast.BinaryExpr) ast.Expr {
	left := optimizeExpr(e.Left)
	right := optimizeExpr(e.Right)
	if isConstant(left) && isConstant(right) {
		result, err := evalComparison(constValue(left), e.Op, constValue(right))
		if err == nil {
			return &ast.BoolLitExpr{Value: result}
		}
	}
	return &ast.BinaryExpr{Left: left, Op: e.Op, Right: right}
}

// optimizeArithmeticExpr folds a constant arithmetic expression.
func optimizeArithmeticExpr(e *ast.ArithmeticExpr) ast.Expr {
	left := optimizeExpr(e.Left)
	right := optimizeExpr(e.Right)
	if isConstant(left) && isConstant(right) {
		result, err := evalArithmetic(constValue(left), e.Op, constValue(right))
		if err == nil {
			return valueToExpr(result)
		}
	}
	return &ast.ArithmeticExpr{Left: left, Op: e.Op, Right: right}
}

// optimizeLogicalExpr simplifies AND/OR with constant operands.
func optimizeLogicalExpr(e *ast.LogicalExpr) ast.Expr {
	left := optimizeExpr(e.Left)
	right := optimizeExpr(e.Right)
	lb, leftIsBool := isBoolConst(left)
	rb, rightIsBool := isBoolConst(right)

	switch e.Op {
	case "AND":
		if leftIsBool && !lb {
			return &ast.BoolLitExpr{Value: false}
		}
		if leftIsBool && lb {
			return right
		}
		if rightIsBool && !rb {
			return &ast.BoolLitExpr{Value: false}
		}
		if rightIsBool && rb {
			return left
		}
	case "OR":
		if leftIsBool && lb {
			return &ast.BoolLitExpr{Value: true}
		}
		if leftIsBool && !lb {
			return right
		}
		if rightIsBool && rb {
			return &ast.BoolLitExpr{Value: true}
		}
		if rightIsBool && !rb {
			return left
		}
	}
	return &ast.LogicalExpr{Left: left, Op: e.Op, Right: right}
}

// optimizeInExpr folds a constant IN expression.
func optimizeInExpr(e *ast.InExpr) ast.Expr {
	if e.Subquery != nil {
		return e // don't optimize subquery IN
	}
	left := optimizeExpr(e.Left)
	if !isConstant(left) {
		return e // left is not constant, skip
	}
	for _, v := range e.Values {
		if !isConstant(optimizeExpr(v)) {
			return e
		}
	}
	lv := constValue(left)
	if lv == nil {
		return &ast.BoolLitExpr{Value: false}
	}
	for _, valExpr := range e.Values {
		rv := constValue(optimizeExpr(valExpr))
		match, err := evalComparison(lv, "=", rv)
		if err != nil {
			return e
		}
		if match {
			return &ast.BoolLitExpr{Value: !e.Not}
		}
	}
	return &ast.BoolLitExpr{Value: e.Not}
}

// optimizeBetweenExpr folds a constant BETWEEN expression.
func optimizeBetweenExpr(e *ast.BetweenExpr) ast.Expr {
	left := optimizeExpr(e.Left)
	low := optimizeExpr(e.Low)
	high := optimizeExpr(e.High)
	if isConstant(left) && isConstant(low) && isConstant(high) {
		lv := constValue(left)
		if lv == nil {
			return &ast.BoolLitExpr{Value: false}
		}
		geq, err1 := evalComparison(lv, ">=", constValue(low))
		leq, err2 := evalComparison(lv, "<=", constValue(high))
		if err1 == nil && err2 == nil {
			result := geq && leq
			if e.Not {
				return &ast.BoolLitExpr{Value: !result}
			}
			return &ast.BoolLitExpr{Value: result}
		}
	}
	return &ast.BetweenExpr{Left: left, Low: low, High: high, Not: e.Not}
}

// optimizeCaseExpr simplifies a CASE expression by pruning constant WHEN branches.
func optimizeCaseExpr(e *ast.CaseExpr) ast.Expr {
	operand := e.Operand
	if operand != nil {
		operand = optimizeExpr(operand)
	}
	var whens []ast.CaseWhen
	for _, w := range e.Whens {
		when := optimizeExpr(w.When)
		then := optimizeExpr(w.Then)
		// For searched CASE, prune WHEN FALSE and short-circuit on WHEN TRUE
		if operand == nil {
			if b, ok := isBoolConst(when); ok {
				if !b {
					continue // WHEN FALSE → skip
				}
				// WHEN TRUE → this branch always matches
				return then
			}
		}
		whens = append(whens, ast.CaseWhen{When: when, Then: then})
	}
	var elseExpr ast.Expr
	if e.Else != nil {
		elseExpr = optimizeExpr(e.Else)
	}
	// If all WHENs pruned, return ELSE
	if len(whens) == 0 {
		if elseExpr != nil {
			return elseExpr
		}
		return &ast.NullLitExpr{}
	}
	return &ast.CaseExpr{Operand: operand, Whens: whens, Else: elseExpr}
}

// optimizeExpr recursively rewrites an expression tree by folding constant expressions.
// It returns a simplified expression or the original if no optimization is possible.
func optimizeExpr(expr ast.Expr) ast.Expr {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *ast.BinaryExpr:
		return optimizeBinaryExpr(e)
	case *ast.ArithmeticExpr:
		return optimizeArithmeticExpr(e)
	case *ast.LogicalExpr:
		return optimizeLogicalExpr(e)
	case *ast.NotExpr:
		inner := optimizeExpr(e.Expr)
		if b, ok := isBoolConst(inner); ok {
			return &ast.BoolLitExpr{Value: !b}
		}
		return &ast.NotExpr{Expr: inner}
	case *ast.IsNullExpr:
		inner := optimizeExpr(e.Expr)
		if isConstant(inner) {
			_, isNull := inner.(*ast.NullLitExpr)
			if e.Not {
				return &ast.BoolLitExpr{Value: !isNull}
			}
			return &ast.BoolLitExpr{Value: isNull}
		}
		return &ast.IsNullExpr{Expr: inner, Not: e.Not}
	case *ast.InExpr:
		return optimizeInExpr(e)
	case *ast.BetweenExpr:
		return optimizeBetweenExpr(e)
	case *ast.CaseExpr:
		return optimizeCaseExpr(e)
	default:
		return expr
	}
}

// isConstant returns true if the expression is a constant literal.
func isConstant(expr ast.Expr) bool {
	switch expr.(type) {
	case *ast.IntLitExpr, *ast.FloatLitExpr, *ast.StringLitExpr, *ast.NullLitExpr, *ast.BoolLitExpr:
		return true
	}
	return false
}

// constValue extracts the Go value from a constant literal expression.
func constValue(expr ast.Expr) Value {
	switch e := expr.(type) {
	case *ast.IntLitExpr:
		return e.Value
	case *ast.FloatLitExpr:
		return e.Value
	case *ast.StringLitExpr:
		return e.Value
	case *ast.NullLitExpr:
		return nil
	case *ast.BoolLitExpr:
		return e.Value
	}
	return nil
}

// valueToExpr converts a Go value to the corresponding AST literal expression.
func valueToExpr(v Value) ast.Expr {
	switch tv := v.(type) {
	case int64:
		return &ast.IntLitExpr{Value: tv}
	case float64:
		return &ast.FloatLitExpr{Value: tv}
	case string:
		return &ast.StringLitExpr{Value: tv}
	case bool:
		return &ast.BoolLitExpr{Value: tv}
	case nil:
		return &ast.NullLitExpr{}
	}
	return &ast.NullLitExpr{}
}

// isBoolConst checks if an expression is a BoolLitExpr and returns its value.
func isBoolConst(expr ast.Expr) (bool, bool) {
	if b, ok := expr.(*ast.BoolLitExpr); ok {
		return b.Value, true
	}
	return false, false
}

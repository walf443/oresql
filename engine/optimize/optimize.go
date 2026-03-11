// Package optimize provides constant folding optimization for SQL AST expressions
// with no dependency on the engine package.
package optimize

import (
	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/engine/expr"
)

// Statement applies constant folding to WHERE/HAVING clauses in a statement.
func Statement(stmt ast.Statement) {
	switch s := stmt.(type) {
	case *ast.SelectStmt:
		if s.Where != nil {
			s.Where = Expr(s.Where)
		}
		if s.Having != nil {
			s.Having = Expr(s.Having)
		}
	case *ast.UpdateStmt:
		if s.Where != nil {
			s.Where = Expr(s.Where)
		}
	case *ast.DeleteStmt:
		if s.Where != nil {
			s.Where = Expr(s.Where)
		}
	}
}

// Expr recursively rewrites an expression tree by folding constant expressions.
// It returns a simplified expression or the original if no optimization is possible.
func Expr(e ast.Expr) ast.Expr {
	if e == nil {
		return nil
	}

	switch ex := e.(type) {
	case *ast.BinaryExpr:
		return optimizeBinaryExpr(ex)
	case *ast.ArithmeticExpr:
		return optimizeArithmeticExpr(ex)
	case *ast.LogicalExpr:
		return optimizeLogicalExpr(ex)
	case *ast.NotExpr:
		inner := Expr(ex.Expr)
		if b, ok := isBoolConst(inner); ok {
			return &ast.BoolLitExpr{Value: !b}
		}
		return &ast.NotExpr{Expr: inner}
	case *ast.IsNullExpr:
		inner := Expr(ex.Expr)
		if isConstant(inner) {
			_, isNull := inner.(*ast.NullLitExpr)
			if ex.Not {
				return &ast.BoolLitExpr{Value: !isNull}
			}
			return &ast.BoolLitExpr{Value: isNull}
		}
		return &ast.IsNullExpr{Expr: inner, Not: ex.Not}
	case *ast.InExpr:
		return optimizeInExpr(ex)
	case *ast.BetweenExpr:
		return optimizeBetweenExpr(ex)
	case *ast.CaseExpr:
		return optimizeCaseExpr(ex)
	default:
		return e
	}
}

func optimizeBinaryExpr(e *ast.BinaryExpr) ast.Expr {
	left := Expr(e.Left)
	right := Expr(e.Right)
	if isConstant(left) && isConstant(right) {
		result, err := expr.Comparison(constValue(left), e.Op, constValue(right))
		if err == nil {
			return &ast.BoolLitExpr{Value: result}
		}
	}
	return &ast.BinaryExpr{Left: left, Op: e.Op, Right: right}
}

func optimizeArithmeticExpr(e *ast.ArithmeticExpr) ast.Expr {
	left := Expr(e.Left)
	right := Expr(e.Right)
	if isConstant(left) && isConstant(right) {
		result, err := expr.Arithmetic(constValue(left), e.Op, constValue(right))
		if err == nil {
			return valueToExpr(result)
		}
	}
	return &ast.ArithmeticExpr{Left: left, Op: e.Op, Right: right}
}

func optimizeLogicalExpr(e *ast.LogicalExpr) ast.Expr {
	left := Expr(e.Left)
	right := Expr(e.Right)
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

func optimizeInExpr(e *ast.InExpr) ast.Expr {
	if e.Subquery != nil {
		return e
	}
	left := Expr(e.Left)
	if !isConstant(left) {
		return e
	}
	for _, v := range e.Values {
		if !isConstant(Expr(v)) {
			return e
		}
	}
	lv := constValue(left)
	if lv == nil {
		return &ast.BoolLitExpr{Value: false}
	}
	for _, valExpr := range e.Values {
		rv := constValue(Expr(valExpr))
		match, err := expr.Comparison(lv, "=", rv)
		if err != nil {
			return e
		}
		if match {
			return &ast.BoolLitExpr{Value: !e.Not}
		}
	}
	return &ast.BoolLitExpr{Value: e.Not}
}

func optimizeBetweenExpr(e *ast.BetweenExpr) ast.Expr {
	left := Expr(e.Left)
	low := Expr(e.Low)
	high := Expr(e.High)
	if isConstant(left) && isConstant(low) && isConstant(high) {
		lv := constValue(left)
		if lv == nil {
			return &ast.BoolLitExpr{Value: false}
		}
		geq, err1 := expr.Comparison(lv, ">=", constValue(low))
		leq, err2 := expr.Comparison(lv, "<=", constValue(high))
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

func optimizeCaseExpr(e *ast.CaseExpr) ast.Expr {
	operand := e.Operand
	if operand != nil {
		operand = Expr(operand)
	}
	var whens []ast.CaseWhen
	for _, w := range e.Whens {
		when := Expr(w.When)
		then := Expr(w.Then)
		if operand == nil {
			if b, ok := isBoolConst(when); ok {
				if !b {
					continue
				}
				return then
			}
		}
		whens = append(whens, ast.CaseWhen{When: when, Then: then})
	}
	var elseExpr ast.Expr
	if e.Else != nil {
		elseExpr = Expr(e.Else)
	}
	if len(whens) == 0 {
		if elseExpr != nil {
			return elseExpr
		}
		return &ast.NullLitExpr{}
	}
	return &ast.CaseExpr{Operand: operand, Whens: whens, Else: elseExpr}
}

func isConstant(e ast.Expr) bool {
	switch e.(type) {
	case *ast.IntLitExpr, *ast.FloatLitExpr, *ast.StringLitExpr, *ast.NullLitExpr, *ast.BoolLitExpr:
		return true
	}
	return false
}

func constValue(e ast.Expr) expr.Value {
	switch ex := e.(type) {
	case *ast.IntLitExpr:
		return ex.Value
	case *ast.FloatLitExpr:
		return ex.Value
	case *ast.StringLitExpr:
		return ex.Value
	case *ast.NullLitExpr:
		return nil
	case *ast.BoolLitExpr:
		return ex.Value
	}
	return nil
}

func valueToExpr(v expr.Value) ast.Expr {
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

func isBoolConst(e ast.Expr) (bool, bool) {
	if b, ok := e.(*ast.BoolLitExpr); ok {
		return b.Value, true
	}
	return false, false
}

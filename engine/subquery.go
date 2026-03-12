package engine

import "github.com/walf443/oresql/ast"

// containsSubquery returns true if the expression contains any subquery expression
// (ExistsExpr, ScalarExpr, or InExpr with Subquery).
func containsSubquery(expr ast.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *ast.ExistsExpr:
		return true
	case *ast.ScalarExpr:
		return true
	case *ast.InExpr:
		if e.Subquery != nil {
			return true
		}
		return false
	case *ast.BinaryExpr:
		return containsSubquery(e.Left) || containsSubquery(e.Right)
	case *ast.LogicalExpr:
		return containsSubquery(e.Left) || containsSubquery(e.Right)
	case *ast.NotExpr:
		return containsSubquery(e.Expr)
	case *ast.IsNullExpr:
		return containsSubquery(e.Expr)
	case *ast.ArithmeticExpr:
		return containsSubquery(e.Left) || containsSubquery(e.Right)
	case *ast.AliasExpr:
		return containsSubquery(e.Expr)
	case *ast.CaseExpr:
		if containsSubquery(e.Operand) {
			return true
		}
		for _, w := range e.Whens {
			if containsSubquery(w.When) || containsSubquery(w.Then) {
				return true
			}
		}
		return containsSubquery(e.Else)
	case *ast.CastExpr:
		return containsSubquery(e.Expr)
	default:
		return false
	}
}

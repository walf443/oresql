package engine

import "github.com/walf443/oresql/ast"

// flattenAND decomposes an AND-connected expression tree into a flat slice.
// Non-AND expressions (including OR) are returned as a single-element slice.
func flattenAND(expr ast.Expr) []ast.Expr {
	if expr == nil {
		return nil
	}
	logical, ok := expr.(*ast.LogicalExpr)
	if !ok || logical.Op != "AND" {
		return []ast.Expr{expr}
	}
	var result []ast.Expr
	result = append(result, flattenAND(logical.Left)...)
	result = append(result, flattenAND(logical.Right)...)
	return result
}

// combineExprsAND combines a slice of expressions into a single AND-connected expression.
// Returns nil for empty slice, the single element for one, or a chain of LogicalExpr for multiple.
func combineExprsAND(exprs []ast.Expr) ast.Expr {
	if len(exprs) == 0 {
		return nil
	}
	result := exprs[0]
	for i := 1; i < len(exprs); i++ {
		result = &ast.LogicalExpr{Left: result, Op: "AND", Right: exprs[i]}
	}
	return result
}

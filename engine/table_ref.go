package engine

import (
	"strings"

	"github.com/walf443/oresql/ast"
)

// collectTableRefs collects all table qualifiers (IdentExpr.Table) referenced in an expression.
// Only non-empty qualifiers are included. Keys are lowercased.
func collectTableRefs(expr ast.Expr) map[string]bool {
	refs := make(map[string]bool)
	collectTableRefsRecursive(expr, refs)
	return refs
}

func collectTableRefsRecursive(expr ast.Expr, refs map[string]bool) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *ast.IdentExpr:
		if e.Table != "" {
			refs[strings.ToLower(e.Table)] = true
		}
	case *ast.BinaryExpr:
		collectTableRefsRecursive(e.Left, refs)
		collectTableRefsRecursive(e.Right, refs)
	case *ast.LogicalExpr:
		collectTableRefsRecursive(e.Left, refs)
		collectTableRefsRecursive(e.Right, refs)
	case *ast.NotExpr:
		collectTableRefsRecursive(e.Expr, refs)
	case *ast.IsNullExpr:
		collectTableRefsRecursive(e.Expr, refs)
	case *ast.InExpr:
		collectTableRefsRecursive(e.Left, refs)
		for _, v := range e.Values {
			collectTableRefsRecursive(v, refs)
		}
	case *ast.BetweenExpr:
		collectTableRefsRecursive(e.Left, refs)
		collectTableRefsRecursive(e.Low, refs)
		collectTableRefsRecursive(e.High, refs)
	case *ast.LikeExpr:
		collectTableRefsRecursive(e.Left, refs)
		collectTableRefsRecursive(e.Pattern, refs)
	case *ast.ArithmeticExpr:
		collectTableRefsRecursive(e.Left, refs)
		collectTableRefsRecursive(e.Right, refs)
	}
}

// collectUnqualifiedIdents collects all unqualified column names from an expression.
func collectUnqualifiedIdents(expr ast.Expr) []string {
	var names []string
	collectUnqualifiedIdentsRecursive(expr, &names)
	return names
}

func collectUnqualifiedIdentsRecursive(expr ast.Expr, names *[]string) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *ast.IdentExpr:
		if e.Table == "" {
			*names = append(*names, strings.ToLower(e.Name))
		}
	case *ast.BinaryExpr:
		collectUnqualifiedIdentsRecursive(e.Left, names)
		collectUnqualifiedIdentsRecursive(e.Right, names)
	case *ast.LogicalExpr:
		collectUnqualifiedIdentsRecursive(e.Left, names)
		collectUnqualifiedIdentsRecursive(e.Right, names)
	case *ast.NotExpr:
		collectUnqualifiedIdentsRecursive(e.Expr, names)
	case *ast.IsNullExpr:
		collectUnqualifiedIdentsRecursive(e.Expr, names)
	case *ast.InExpr:
		collectUnqualifiedIdentsRecursive(e.Left, names)
		for _, v := range e.Values {
			collectUnqualifiedIdentsRecursive(v, names)
		}
	case *ast.BetweenExpr:
		collectUnqualifiedIdentsRecursive(e.Left, names)
		collectUnqualifiedIdentsRecursive(e.Low, names)
		collectUnqualifiedIdentsRecursive(e.High, names)
	case *ast.LikeExpr:
		collectUnqualifiedIdentsRecursive(e.Left, names)
		collectUnqualifiedIdentsRecursive(e.Pattern, names)
	case *ast.ArithmeticExpr:
		collectUnqualifiedIdentsRecursive(e.Left, names)
		collectUnqualifiedIdentsRecursive(e.Right, names)
	}
}

// stripTableQualifier removes table qualifiers that match the given table name or alias.
// This is needed because evalExpr/evalWhere use validateTableRef which doesn't know about aliases.
func stripTableQualifier(expr ast.Expr, tableName, alias string) ast.Expr {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *ast.IdentExpr:
		if e.Table == "" {
			return e
		}
		lower := strings.ToLower(e.Table)
		if lower == strings.ToLower(tableName) || (alias != "" && lower == strings.ToLower(alias)) {
			return &ast.IdentExpr{Name: e.Name}
		}
		return e
	case *ast.BinaryExpr:
		return &ast.BinaryExpr{
			Left:  stripTableQualifier(e.Left, tableName, alias),
			Op:    e.Op,
			Right: stripTableQualifier(e.Right, tableName, alias),
		}
	case *ast.LogicalExpr:
		return &ast.LogicalExpr{
			Left:  stripTableQualifier(e.Left, tableName, alias),
			Op:    e.Op,
			Right: stripTableQualifier(e.Right, tableName, alias),
		}
	case *ast.NotExpr:
		return &ast.NotExpr{Expr: stripTableQualifier(e.Expr, tableName, alias)}
	case *ast.IsNullExpr:
		return &ast.IsNullExpr{Expr: stripTableQualifier(e.Expr, tableName, alias), Not: e.Not}
	case *ast.InExpr:
		newVals := make([]ast.Expr, len(e.Values))
		for i, v := range e.Values {
			newVals[i] = stripTableQualifier(v, tableName, alias)
		}
		return &ast.InExpr{Left: stripTableQualifier(e.Left, tableName, alias), Values: newVals, Not: e.Not}
	case *ast.BetweenExpr:
		return &ast.BetweenExpr{
			Left: stripTableQualifier(e.Left, tableName, alias),
			Low:  stripTableQualifier(e.Low, tableName, alias),
			High: stripTableQualifier(e.High, tableName, alias),
			Not:  e.Not,
		}
	case *ast.LikeExpr:
		return &ast.LikeExpr{
			Left:    stripTableQualifier(e.Left, tableName, alias),
			Pattern: stripTableQualifier(e.Pattern, tableName, alias),
			Not:     e.Not,
		}
	case *ast.ArithmeticExpr:
		return &ast.ArithmeticExpr{
			Left:  stripTableQualifier(e.Left, tableName, alias),
			Op:    e.Op,
			Right: stripTableQualifier(e.Right, tableName, alias),
		}
	default:
		return expr
	}
}

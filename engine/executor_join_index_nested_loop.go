package engine

import (
	"sort"
	"strings"

	"github.com/walf443/oresql/ast"
)

// equiJoinPair represents a pair of columns from two tables used in an equi-join.
// leftTable/leftCol always refer to the FROM table, rightTable/rightCol to the JOIN table.
type equiJoinPair struct {
	leftTable, leftCol   string // lowercase table name, lowercase column name
	rightTable, rightCol string
}

// joinTableInfo holds table metadata for join optimization.
type joinTableInfo struct {
	info      *TableInfo
	tableName string // lowercase
	alias     string
}

// matchesTable returns true if the given qualifier (table name or alias) matches this table.
func (jt *joinTableInfo) matchesTable(qualifier string) bool {
	if qualifier == "" {
		return false
	}
	lower := strings.ToLower(qualifier)
	return lower == jt.tableName || (jt.alias != "" && lower == strings.ToLower(jt.alias))
}

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

// postJoinProcess handles ORDER BY, OFFSET, LIMIT, projection, and DISTINCT after join.
func (e *Executor) postJoinProcess(stmt *ast.SelectStmt, rows []Row, jc *JoinContext) (*Result, error) {
	// Sort by ORDER BY
	if len(stmt.OrderBy) > 0 {
		var sortErr error
		sort.SliceStable(rows, func(i, j int) bool {
			if sortErr != nil {
				return false
			}
			for _, ob := range stmt.OrderBy {
				vi, err := evalExprJoin(ob.Expr, rows[i], jc)
				if err != nil {
					sortErr = err
					return false
				}
				vj, err := evalExprJoin(ob.Expr, rows[j], jc)
				if err != nil {
					sortErr = err
					return false
				}
				if vi == nil && vj == nil {
					continue
				}
				if vi == nil {
					return false
				}
				if vj == nil {
					return true
				}
				cmp := compareValues(vi, vj)
				if cmp == 0 {
					continue
				}
				if ob.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
			return false
		})
		if sortErr != nil {
			return nil, sortErr
		}
	}

	// Apply OFFSET
	if stmt.Offset != nil {
		off := int(*stmt.Offset)
		if off >= len(rows) {
			rows = nil
		} else {
			rows = rows[off:]
		}
	}

	// Apply LIMIT
	if stmt.Limit != nil {
		lim := int(*stmt.Limit)
		if lim < len(rows) {
			rows = rows[:lim]
		}
	}

	// Resolve column names and project
	var colNames []string
	var colExprs []ast.Expr
	isStar := false

	if len(stmt.Columns) == 1 {
		if _, ok := stmt.Columns[0].(*ast.StarExpr); ok {
			isStar = true
			for _, col := range jc.MergedInfo.Columns {
				colNames = append(colNames, col.Name)
			}
		}
	}

	if !isStar {
		for _, colExpr := range stmt.Columns {
			alias := ""
			inner := colExpr
			if a, ok := colExpr.(*ast.AliasExpr); ok {
				alias = a.Alias
				inner = a.Expr
			}
			colExprs = append(colExprs, inner)
			if alias != "" {
				colNames = append(colNames, alias)
			} else if ident, ok := inner.(*ast.IdentExpr); ok {
				col, err := jc.FindColumn(ident.Table, ident.Name)
				if err != nil {
					return nil, err
				}
				colNames = append(colNames, col.Name)
			} else {
				colNames = append(colNames, formatExpr(inner))
			}
		}
	}

	// Project columns
	var resultRows []Row
	for _, row := range rows {
		if isStar {
			projected := make(Row, len(jc.MergedInfo.Columns))
			copy(projected, row)
			resultRows = append(resultRows, projected)
		} else {
			projected := make(Row, len(colExprs))
			for i, expr := range colExprs {
				val, err := evalExprJoin(expr, row, jc)
				if err != nil {
					return nil, err
				}
				projected[i] = val
			}
			resultRows = append(resultRows, projected)
		}
	}

	// Apply DISTINCT
	if stmt.Distinct {
		resultRows = dedup(resultRows)
	}

	return &Result{Columns: colNames, Rows: resultRows}, nil
}

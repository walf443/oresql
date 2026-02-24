package engine

import (
	"fmt"
	"sort"

	"github.com/walf443/oresql/ast"
)

// rowIdentity is a rowOf function for Row (identity mapping).
func rowIdentity(r Row) Row { return r }

// rowOfKeyRow is a rowOf function for KeyRow (extracts the Row field).
func rowOfKeyRow(kr KeyRow) Row { return kr.Row }

// filterWhere filters rows using the WHERE expression evaluated with the given evaluator.
// Returns all rows if where is nil.
func filterWhere[T any](rows []T, where ast.Expr, eval ExprEvaluator, rowOf func(T) Row) ([]T, error) {
	if where == nil {
		return rows, nil
	}
	var filtered []T
	for _, item := range rows {
		val, err := eval.Eval(where, rowOf(item))
		if err != nil {
			return nil, err
		}
		b, ok := val.(bool)
		if !ok {
			return nil, fmt.Errorf("WHERE expression must evaluate to boolean, got %T", val)
		}
		if b {
			filtered = append(filtered, item)
		}
	}
	return filtered, nil
}

// sortRows sorts rows by ORDER BY clauses using the given evaluator.
// Returns the original slice if orderBy is empty.
func sortRows[T any](rows []T, orderBy []ast.OrderByClause, eval ExprEvaluator, rowOf func(T) Row) ([]T, error) {
	if len(orderBy) == 0 {
		return rows, nil
	}
	var sortErr error
	sort.SliceStable(rows, func(i, j int) bool {
		if sortErr != nil {
			return false
		}
		for _, ob := range orderBy {
			vi, err := eval.Eval(ob.Expr, rowOf(rows[i]))
			if err != nil {
				sortErr = err
				return false
			}
			vj, err := eval.Eval(ob.Expr, rowOf(rows[j]))
			if err != nil {
				sortErr = err
				return false
			}
			// NULLs always sort last regardless of ASC/DESC
			if vi == nil && vj == nil {
				continue
			}
			if vi == nil {
				return false // NULL sorts last
			}
			if vj == nil {
				return true // NULL sorts last
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
	return rows, nil
}

// applyOffset skips the first N rows.
func applyOffset[T any](rows []T, offset *int64) []T {
	if offset == nil {
		return rows
	}
	off := int(*offset)
	if off >= len(rows) {
		return nil
	}
	return rows[off:]
}

// applyLimit keeps at most N rows.
func applyLimit[T any](rows []T, limit *int64) []T {
	if limit == nil {
		return rows
	}
	lim := int(*limit)
	if lim < len(rows) {
		return rows[:lim]
	}
	return rows
}

// filterWhereLimit filters rows using the WHERE expression with early termination at limit.
// Returns at most limit rows that pass the WHERE filter.
// If where is nil, returns the first limit rows.
func filterWhereLimit[T any](rows []T, where ast.Expr, eval ExprEvaluator, rowOf func(T) Row, limit int) ([]T, error) {
	var filtered []T
	for _, item := range rows {
		if where != nil {
			val, err := eval.Eval(where, rowOf(item))
			if err != nil {
				return nil, err
			}
			b, ok := val.(bool)
			if !ok {
				return nil, fmt.Errorf("WHERE expression must evaluate to boolean, got %T", val)
			}
			if !b {
				continue
			}
		}
		filtered = append(filtered, item)
		if len(filtered) >= limit {
			break
		}
	}
	return filtered, nil
}

// resolveSelectColumns resolves column names and expressions from SELECT columns.
// Returns column names, column expressions (nil for star), isStar flag, and error.
func resolveSelectColumns(columns []ast.Expr, eval ExprEvaluator) ([]string, []ast.Expr, bool, error) {
	if len(columns) == 1 {
		if _, ok := columns[0].(*ast.StarExpr); ok {
			var colNames []string
			for _, col := range eval.ColumnList() {
				colNames = append(colNames, col.Name)
			}
			return colNames, nil, true, nil
		}
	}

	var colNames []string
	var colExprs []ast.Expr
	for _, colExpr := range columns {
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
			col, err := eval.ResolveColumn(ident.Table, ident.Name)
			if err != nil {
				return nil, nil, false, err
			}
			colNames = append(colNames, col.Name)
		} else if call, ok := inner.(*ast.CallExpr); ok {
			colNames = append(colNames, formatCallExpr(call))
		} else {
			colNames = append(colNames, formatExpr(inner))
		}
	}
	return colNames, colExprs, false, nil
}

// projectRows projects each row to the selected columns using the evaluator.
// For star queries (isStar=true), copies all columns from the evaluator's column list.
func projectRows(rows []Row, colExprs []ast.Expr, isStar bool, eval ExprEvaluator) ([]Row, error) {
	if len(rows) == 0 {
		return rows, nil
	}
	var resultRows []Row
	if isStar {
		cols := eval.ColumnList()
		for _, row := range rows {
			projected := make(Row, len(cols))
			for i, col := range cols {
				projected[i] = row[col.Index]
			}
			resultRows = append(resultRows, projected)
		}
	} else {
		for _, row := range rows {
			projected := make(Row, len(colExprs))
			for i, expr := range colExprs {
				val, err := eval.Eval(expr, row)
				if err != nil {
					return nil, err
				}
				projected[i] = val
			}
			resultRows = append(resultRows, projected)
		}
	}
	return resultRows, nil
}

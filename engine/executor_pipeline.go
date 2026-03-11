package engine

import (
	"container/heap"
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
	if b, ok := where.(*ast.BoolLitExpr); ok {
		if b.Value {
			return rows, nil // WHERE TRUE → return all rows
		}
		return nil, nil // WHERE FALSE → empty
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
	if b, ok := where.(*ast.BoolLitExpr); ok {
		if !b.Value {
			return nil, nil // WHERE FALSE → empty
		}
		// WHERE TRUE → just apply limit
		if limit < len(rows) {
			return rows[:limit], nil
		}
		return rows, nil
	}
	filtered := make([]T, 0, limit)
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

// filterProjectDedupLimit combines WHERE filtering, projection, deduplication, and early
// termination in a single pass. Used for DISTINCT + LIMIT queries without ORDER BY.
// Returns at most limit unique projected rows.
func filterProjectDedupLimit(rows []Row, where ast.Expr, colExprs []ast.Expr, isStar bool, eval ExprEvaluator, limit int) ([]Row, error) {
	if b, ok := where.(*ast.BoolLitExpr); ok {
		if !b.Value {
			return nil, nil // WHERE FALSE → empty
		}
		where = nil // WHERE TRUE → skip filtering
	}
	seen := make(map[string]bool)
	result := make([]Row, 0, limit)
	cols := eval.ColumnList()
	for _, row := range rows {
		if where != nil {
			val, err := eval.Eval(where, row)
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
		var projected Row
		if isStar {
			projected = make(Row, len(cols))
			for i, col := range cols {
				projected[i] = row[col.Index]
			}
		} else {
			projected = make(Row, len(colExprs))
			for i, expr := range colExprs {
				val, err := eval.Eval(expr, row)
				if err != nil {
					return nil, err
				}
				projected[i] = val
			}
		}
		key := string(encodeValues(projected))
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, projected)
		if len(result) >= limit {
			break
		}
	}
	return result, nil
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
		} else if win, ok := inner.(*ast.WindowExpr); ok {
			colNames = append(colNames, formatWindowExpr(win))
		} else {
			colNames = append(colNames, formatExpr(inner))
		}
	}
	return colNames, colExprs, false, nil
}

// inferExprType infers the result type of an expression.
// Returns "INT", "TEXT", "FLOAT", or "" (unknown/compatible with any).
func inferExprType(expr ast.Expr, eval ExprEvaluator) string {
	switch e := expr.(type) {
	case *ast.IdentExpr:
		col, err := eval.ResolveColumn(e.Table, e.Name)
		if err != nil {
			return ""
		}
		return col.DataType
	case *ast.IntLitExpr:
		return "INT"
	case *ast.FloatLitExpr:
		return "FLOAT"
	case *ast.StringLitExpr:
		return "TEXT"
	case *ast.NullLitExpr:
		return "" // compatible with any type
	case *ast.BoolLitExpr:
		return "INT" // boolean is internally treated as INT
	case *ast.CallExpr:
		switch e.Name {
		case "COUNT":
			return "INT"
		case "AVG":
			return "FLOAT"
		case "SUM", "MIN", "MAX":
			if len(e.Args) > 0 {
				return inferExprType(e.Args[0], eval)
			}
			return ""
		default:
			return ""
		}
	case *ast.WindowExpr:
		switch e.Name {
		case "ROW_NUMBER", "RANK", "DENSE_RANK", "COUNT":
			return "INT"
		case "AVG":
			return "FLOAT"
		case "SUM", "MIN", "MAX":
			if len(e.Args) > 0 {
				return inferExprType(e.Args[0], eval)
			}
			return ""
		default:
			return ""
		}
	case *ast.ArithmeticExpr:
		lt := inferExprType(e.Left, eval)
		rt := inferExprType(e.Right, eval)
		if lt == "FLOAT" || rt == "FLOAT" {
			return "FLOAT"
		}
		return "INT"
	case *ast.AliasExpr:
		return inferExprType(e.Expr, eval)
	default:
		return ""
	}
}

// resolveColumnTypes resolves the column types for SELECT columns.
// Returns a slice of type strings ("INT", "TEXT", "FLOAT", or "" for unknown).
func resolveColumnTypes(columns []ast.Expr, eval ExprEvaluator) []string {
	if len(columns) == 1 {
		if _, ok := columns[0].(*ast.StarExpr); ok {
			colList := eval.ColumnList()
			types := make([]string, len(colList))
			for i, col := range colList {
				types[i] = col.DataType
			}
			return types
		}
	}

	types := make([]string, len(columns))
	for i, colExpr := range columns {
		types[i] = inferExprType(colExpr, eval)
	}
	return types
}

// sortKey holds pre-computed ORDER BY values for a row.
type sortKey struct {
	values []Value
	index  int // original index in the input slice
}

// topKHeap is a max-heap of sortKeys, keeping at most K smallest elements.
// "Max-heap" means the largest element is at the top, so we can efficiently
// evict it when a smaller element arrives.
type topKHeap struct {
	keys    []sortKey
	orderBy []ast.OrderByClause
}

func (h *topKHeap) Len() int { return len(h.keys) }
func (h *topKHeap) Less(i, j int) bool {
	// Reversed comparison: largest at top for max-heap
	return compareSortKeys(h.keys[i].values, h.keys[j].values, h.orderBy) > 0
}
func (h *topKHeap) Swap(i, j int) { h.keys[i], h.keys[j] = h.keys[j], h.keys[i] }
func (h *topKHeap) Push(x any)    { h.keys = append(h.keys, x.(sortKey)) }
func (h *topKHeap) Pop() any {
	old := h.keys
	n := len(old)
	x := old[n-1]
	h.keys = old[:n-1]
	return x
}

// compareSortKeys compares two sort key value slices using ORDER BY clauses.
// Returns negative if a < b, positive if a > b, 0 if equal.
func compareSortKeys(a, b []Value, orderBy []ast.OrderByClause) int {
	for i, ob := range orderBy {
		vi, vj := a[i], b[i]
		// NULLs always sort last regardless of ASC/DESC
		if vi == nil && vj == nil {
			continue
		}
		if vi == nil {
			return 1 // NULL sorts last → a is "larger"
		}
		if vj == nil {
			return -1 // NULL sorts last → b is "larger"
		}
		cmp := compareValues(vi, vj)
		if cmp == 0 {
			continue
		}
		if ob.Desc {
			cmp = -cmp
		}
		return cmp
	}
	return 0
}

// sortRowsTopK sorts rows by ORDER BY and returns at most limit rows.
// Uses heap-based top-K when beneficial (limit*4 < len(rows)), otherwise falls back to full sort.
func sortRowsTopK[T any](rows []T, orderBy []ast.OrderByClause, eval ExprEvaluator,
	rowOf func(T) Row, limit int) ([]T, error) {

	if len(orderBy) == 0 || len(rows) == 0 {
		return rows, nil
	}
	if limit <= 0 || limit*4 >= len(rows) {
		// Fall back to full sort (not worth heap overhead)
		return sortRows(rows, orderBy, eval, rowOf)
	}

	// Phase 1: Pre-compute sort keys for all rows — O(N * C) evaluations
	keys := make([]sortKey, len(rows))
	for i, item := range rows {
		vals := make([]Value, len(orderBy))
		for j, ob := range orderBy {
			v, err := eval.Eval(ob.Expr, rowOf(item))
			if err != nil {
				return nil, err
			}
			vals[j] = v
		}
		keys[i] = sortKey{values: vals, index: i}
	}

	// Phase 2: Build max-heap of size K — O(N log K)
	h := &topKHeap{orderBy: orderBy}
	heap.Init(h)
	for _, k := range keys {
		if h.Len() < limit {
			heap.Push(h, k)
		} else if compareSortKeys(k.values, h.keys[0].values, orderBy) < 0 {
			// New element is smaller than the max in heap → replace
			h.keys[0] = k
			heap.Fix(h, 0)
		}
	}

	// Phase 3: Extract results in sorted order — O(K log K)
	n := h.Len()
	sorted := make([]sortKey, n)
	for i := n - 1; i >= 0; i-- {
		sorted[i] = heap.Pop(h).(sortKey)
	}

	result := make([]T, n)
	for i, sk := range sorted {
		result[i] = rows[sk.index]
	}
	return result, nil
}

// projectRows projects each row to the selected columns using the evaluator.
// For star queries (isStar=true), copies all columns from the evaluator's column list.
func projectRows(rows []Row, colExprs []ast.Expr, isStar bool, eval ExprEvaluator) ([]Row, error) {
	if len(rows) == 0 {
		return rows, nil
	}
	resultRows := make([]Row, 0, len(rows))
	if isStar {
		cols := eval.ColumnList()
		for _, row := range rows {
			projected := make(Row, len(cols))
			for i, col := range cols {
				projected[i] = finalizeValue(row[col.Index])
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
				projected[i] = finalizeValue(val)
			}
			resultRows = append(resultRows, projected)
		}
	}
	return resultRows, nil
}

// finalizeValue converts internal representations to output-friendly values.
// JSONB ([]byte msgpack) is converted to JSON string for display.
func finalizeValue(val Value) Value {
	if b, ok := val.([]byte); ok {
		s, err := msgpackToJSON(b)
		if err != nil {
			return val // fallback: return raw bytes
		}
		return s
	}
	return val
}

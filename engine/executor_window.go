package engine

import (
	"fmt"
	"sort"
	"strings"

	"github.com/walf443/oresql/ast"
)

// hasWindowFunction returns true if any SELECT column contains a WindowExpr.
func hasWindowFunction(columns []ast.Expr) bool {
	for _, col := range columns {
		if containsWindowExpr(col) {
			return true
		}
	}
	return false
}

// containsWindowExpr checks if an expression is or contains a WindowExpr.
func containsWindowExpr(expr ast.Expr) bool {
	switch e := expr.(type) {
	case *ast.WindowExpr:
		return true
	case *ast.AliasExpr:
		return containsWindowExpr(e.Expr)
	}
	return false
}

// windowInfo holds information about a single window function in the SELECT list.
type windowInfo struct {
	winExpr      *ast.WindowExpr
	selectColIdx int // index in stmt.Columns
	resultColIdx int // index in the extended row (after original columns)
}

// applyWindowFunctions processes window functions in a SELECT statement.
// It computes ranking values and returns extended rows with window results appended,
// along with a windowEvaluator that can resolve WindowExpr references.
// Rows are reordered according to the first window function's partition/order sort.
func (e *Executor) applyWindowFunctions(stmt *ast.SelectStmt, rows []Row, eval ExprEvaluator) ([]Row, ExprEvaluator, error) {
	// Collect window functions from SELECT columns
	var wins []windowInfo
	numOrig := 0
	if len(rows) > 0 {
		numOrig = len(rows[0])
	}

	nextCol := numOrig
	for i, colExpr := range stmt.Columns {
		inner := colExpr
		if a, ok := colExpr.(*ast.AliasExpr); ok {
			inner = a.Expr
		}
		if w, ok := inner.(*ast.WindowExpr); ok {
			wins = append(wins, windowInfo{
				winExpr:      w,
				selectColIdx: i,
				resultColIdx: nextCol,
			})
			nextCol++
		}
	}

	if len(wins) == 0 {
		return rows, eval, nil
	}

	// Extend each row with space for window function results
	extendedRows := make([]Row, len(rows))
	for i, row := range rows {
		extended := make(Row, nextCol)
		copy(extended, row)
		extendedRows[i] = extended
	}

	// Compute each window function; use the first one's sort to reorder rows
	var firstSortedOrder []int
	for winIdx, wi := range wins {
		sortedIndices, err := computeWindowRanking(wi.winExpr, extendedRows, eval, wi.resultColIdx, numOrig)
		if err != nil {
			return nil, nil, err
		}
		if winIdx == 0 {
			firstSortedOrder = sortedIndices
		}
	}

	// Reorder rows according to the first window function's sort order
	if firstSortedOrder != nil {
		reordered := make([]Row, len(extendedRows))
		for i, idx := range firstSortedOrder {
			reordered[i] = extendedRows[idx]
		}
		extendedRows = reordered
	}

	// Build windowMap: selectCol index → extended row column index
	windowMap := make(map[int]int)
	for _, wi := range wins {
		windowMap[wi.selectColIdx] = wi.resultColIdx
	}

	weval := &windowEvaluator{
		exec:       e,
		inner:      eval,
		selectCols: stmt.Columns,
		windowMap:  windowMap,
		numOrig:    numOrig,
	}

	return extendedRows, weval, nil
}

// computeWindowRanking computes ranking values for a window function and stores them
// in the extended rows at resultColIdx. Returns the sorted index order.
func computeWindowRanking(winExpr *ast.WindowExpr, rows []Row, eval ExprEvaluator, resultColIdx, numOrig int) ([]int, error) {
	n := len(rows)
	if n == 0 {
		return nil, nil
	}

	// Build index array
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}

	// Sort indices by partition keys then order by keys
	var sortErr error
	sort.SliceStable(indices, func(i, j int) bool {
		if sortErr != nil {
			return false
		}
		ri, rj := rows[indices[i]], rows[indices[j]]

		// Compare partition keys first
		for _, pb := range winExpr.PartitionBy {
			vi, err := eval.Eval(pb, ri[:numOrig])
			if err != nil {
				sortErr = err
				return false
			}
			vj, err := eval.Eval(pb, rj[:numOrig])
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
			if cmp != 0 {
				return cmp < 0
			}
		}

		// Compare order by keys
		for _, ob := range winExpr.OrderBy {
			vi, err := eval.Eval(ob.Expr, ri[:numOrig])
			if err != nil {
				sortErr = err
				return false
			}
			vj, err := eval.Eval(ob.Expr, rj[:numOrig])
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

	// Compute ranking values based on function type
	switch winExpr.Name {
	case "ROW_NUMBER":
		computeRowNumber(winExpr, rows, indices, eval, resultColIdx, numOrig)
	case "RANK":
		computeRank(winExpr, rows, indices, eval, resultColIdx, numOrig)
	case "DENSE_RANK":
		computeDenseRank(winExpr, rows, indices, eval, resultColIdx, numOrig)
	default:
		return nil, fmt.Errorf("unsupported window function: %s", winExpr.Name)
	}

	return indices, nil
}

// computeRowNumber assigns sequential numbers within each partition.
func computeRowNumber(winExpr *ast.WindowExpr, rows []Row, indices []int, eval ExprEvaluator, resultColIdx, numOrig int) {
	rowNum := int64(0)
	for i, idx := range indices {
		if i == 0 || !samePartition(rows[indices[i-1]], rows[idx], winExpr.PartitionBy, eval, numOrig) {
			rowNum = 0
		}
		rowNum++
		rows[idx][resultColIdx] = rowNum
	}
}

// computeRank assigns ranks with gaps for ties.
// Ties get the same rank; next distinct value gets rowNum.
func computeRank(winExpr *ast.WindowExpr, rows []Row, indices []int, eval ExprEvaluator, resultColIdx, numOrig int) {
	rowNum := int64(0)
	rank := int64(0)
	for i, idx := range indices {
		newPartition := i == 0 || !samePartition(rows[indices[i-1]], rows[idx], winExpr.PartitionBy, eval, numOrig)
		if newPartition {
			rowNum = 1
			rank = 1
		} else {
			rowNum++
			if !sameOrderValues(rows[indices[i-1]], rows[idx], winExpr.OrderBy, eval, numOrig) {
				rank = rowNum
			}
		}
		rows[idx][resultColIdx] = rank
	}
}

// computeDenseRank assigns ranks without gaps for ties.
// Ties get the same rank; next distinct value increments by 1.
func computeDenseRank(winExpr *ast.WindowExpr, rows []Row, indices []int, eval ExprEvaluator, resultColIdx, numOrig int) {
	rank := int64(0)
	for i, idx := range indices {
		newPartition := i == 0 || !samePartition(rows[indices[i-1]], rows[idx], winExpr.PartitionBy, eval, numOrig)
		if newPartition {
			rank = 1
		} else {
			if !sameOrderValues(rows[indices[i-1]], rows[idx], winExpr.OrderBy, eval, numOrig) {
				rank++
			}
		}
		rows[idx][resultColIdx] = rank
	}
}

// samePartition returns true if two rows belong to the same partition.
func samePartition(rowA, rowB Row, partitionBy []ast.Expr, eval ExprEvaluator, numOrig int) bool {
	for _, pb := range partitionBy {
		va, _ := eval.Eval(pb, rowA[:numOrig])
		vb, _ := eval.Eval(pb, rowB[:numOrig])
		if va == nil && vb == nil {
			continue
		}
		if va == nil || vb == nil {
			return false
		}
		if compareValues(va, vb) != 0 {
			return false
		}
	}
	return true
}

// sameOrderValues returns true if two rows have the same ORDER BY values (tie detection).
func sameOrderValues(rowA, rowB Row, orderBy []ast.OrderByClause, eval ExprEvaluator, numOrig int) bool {
	for _, ob := range orderBy {
		va, _ := eval.Eval(ob.Expr, rowA[:numOrig])
		vb, _ := eval.Eval(ob.Expr, rowB[:numOrig])
		if va == nil && vb == nil {
			continue
		}
		if va == nil || vb == nil {
			return false
		}
		if compareValues(va, vb) != 0 {
			return false
		}
	}
	return true
}

// formatWindowExpr returns a display name for a window function.
func formatWindowExpr(w *ast.WindowExpr) string {
	var parts []string
	if len(w.PartitionBy) > 0 {
		pbParts := make([]string, len(w.PartitionBy))
		for i, pb := range w.PartitionBy {
			pbParts[i] = formatExpr(pb)
		}
		parts = append(parts, "PARTITION BY "+strings.Join(pbParts, ", "))
	}
	if len(w.OrderBy) > 0 {
		obParts := make([]string, len(w.OrderBy))
		for i, ob := range w.OrderBy {
			s := formatExpr(ob.Expr)
			if ob.Desc {
				s += " DESC"
			}
			obParts[i] = s
		}
		parts = append(parts, "ORDER BY "+strings.Join(obParts, ", "))
	}
	return w.Name + "() OVER (" + strings.Join(parts, " ") + ")"
}

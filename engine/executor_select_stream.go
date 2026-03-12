package engine

import (
	"fmt"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/storage"
)

// executeScanEachStreaming uses ScanEach to stream rows inline under the
// table lock, applying WHERE → projection → dedup → early exit in one pass.
// This avoids materializing all rows and allows the disk backend to stop
// decoding pages once enough unique rows have been collected.
func (e *Executor) executeScanEachStreaming(
	stmt *ast.SelectStmt, db *Database, info *TableInfo, earlyLimit int, distinct bool,
) (*Result, error) {
	eval := newTableEvaluator(makeSubqueryRunner(e), info)
	colTypes := resolveColumnTypes(stmt.Columns, eval)
	colNames, colExprs, isStar, err := resolveSelectColumns(stmt.Columns, eval)
	if err != nil {
		return nil, err
	}

	// Optimise WHERE TRUE / WHERE FALSE
	where := stmt.Where
	if b, ok := where.(*ast.BoolLitExpr); ok {
		if !b.Value {
			return &Result{Columns: colNames, ColumnTypes: colTypes, Rows: nil}, nil
		}
		where = nil
	}

	var seen map[string]bool
	if distinct {
		seen = make(map[string]bool)
	}
	result := make([]Row, 0, earlyLimit)
	cols := eval.ColumnList()
	var scanErr error

	if err := db.storage.ScanEach(info.Name, func(row Row) bool {
		// WHERE filter
		if where != nil {
			val, err := eval.Eval(where, row)
			if err != nil {
				scanErr = err
				return false
			}
			b, ok := val.(bool)
			if !ok {
				scanErr = fmt.Errorf("WHERE expression must evaluate to boolean, got %T", val)
				return false
			}
			if !b {
				return true
			}
		}
		// Projection
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
					scanErr = err
					return false
				}
				projected[i] = val
			}
		}
		// Dedup (DISTINCT only)
		if distinct {
			key := string(encodeValues(projected))
			if seen[key] {
				return true
			}
			seen[key] = true
		}
		result = append(result, projected)
		// Early exit once we have enough rows (earlyLimit = limit + offset)
		return len(result) < earlyLimit
	}); err != nil {
		return nil, err
	}
	if scanErr != nil {
		return nil, scanErr
	}

	result = applyOffset(result, stmt.Offset)
	result = applyLimit(result, stmt.Limit)
	return &Result{Columns: colNames, ColumnTypes: colTypes, Rows: result}, nil
}

// executeForEachByKeysStreaming uses ForEachByKeys to stream rows matching
// index-derived keys with early exit for LIMIT queries. The Row from the
// callback may be reused (disk storage), so matching rows are copied.
func (e *Executor) executeForEachByKeysStreaming(
	stmt *ast.SelectStmt, db *Database, info *TableInfo,
	keys []int64, earlyLimit int, distinct bool,
) (*Result, error) {
	eval := newTableEvaluator(makeSubqueryRunner(e), info)
	colTypes := resolveColumnTypes(stmt.Columns, eval)
	colNames, colExprs, isStar, err := resolveSelectColumns(stmt.Columns, eval)
	if err != nil {
		return nil, err
	}

	// Optimise WHERE TRUE / WHERE FALSE
	where := stmt.Where
	if b, ok := where.(*ast.BoolLitExpr); ok {
		if !b.Value {
			return &Result{Columns: colNames, ColumnTypes: colTypes, Rows: nil}, nil
		}
		where = nil
	}

	var seen map[string]bool
	if distinct {
		seen = make(map[string]bool)
	}
	result := make([]Row, 0, earlyLimit)
	cols := eval.ColumnList()
	var scanErr error

	if err := db.storage.ForEachByKeys(info.Name, keys, func(key int64, row Row) bool {
		// WHERE filter
		if where != nil {
			val, err := eval.Eval(where, row)
			if err != nil {
				scanErr = err
				return false
			}
			b, ok := val.(bool)
			if !ok {
				scanErr = fmt.Errorf("WHERE expression must evaluate to boolean, got %T", val)
				return false
			}
			if !b {
				return true
			}
		}
		// Projection (copies values out of the potentially reused Row)
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
					scanErr = err
					return false
				}
				projected[i] = val
			}
		}
		// Dedup (DISTINCT only)
		if distinct {
			dedupKey := string(encodeValues(projected))
			if seen[dedupKey] {
				return true
			}
			seen[dedupKey] = true
		}
		result = append(result, projected)
		// Early exit once we have enough rows (earlyLimit = limit + offset)
		return len(result) < earlyLimit
	}); err != nil {
		return nil, err
	}
	if scanErr != nil {
		return nil, scanErr
	}

	result = applyOffset(result, stmt.Offset)
	result = applyLimit(result, stmt.Limit)
	return &Result{Columns: colNames, ColumnTypes: colTypes, Rows: result}, nil
}

// executeIndexScanStreaming uses OrderedRangeScan to stream rows from an index
// with early exit for LIMIT queries. Similar to executeScanEachStreaming but
// scans via a secondary index instead of doing a full table scan.
func (e *Executor) executeIndexScanStreaming(
	stmt *ast.SelectStmt, db *Database, info *TableInfo,
	params *indexScanParams, earlyLimit int, distinct bool,
) (*Result, error) {
	eval := newTableEvaluator(makeSubqueryRunner(e), info)
	colTypes := resolveColumnTypes(stmt.Columns, eval)
	colNames, colExprs, isStar, err := resolveSelectColumns(stmt.Columns, eval)
	if err != nil {
		return nil, err
	}

	// Optimise WHERE TRUE / WHERE FALSE
	where := stmt.Where
	if b, ok := where.(*ast.BoolLitExpr); ok {
		if !b.Value {
			return &Result{Columns: colNames, ColumnTypes: colTypes, Rows: nil}, nil
		}
		where = nil
	}

	var seen map[string]bool
	if distinct {
		seen = make(map[string]bool)
	}
	result := make([]Row, 0, earlyLimit)
	cols := eval.ColumnList()
	var scanErr error

	// Check if covering index scan is possible
	neededCols := collectNeededColumns(stmt.Columns, where, nil, info)
	cir, isCovering := params.index.(storage.CoveringIndexReader)
	if isCovering && isIndexCovering(params.index, neededCols, info.PrimaryKeyCol) {
		cir.OrderedCoveringScan(
			params.fromVal, params.fromInclusive,
			params.toVal, params.toInclusive,
			false, len(info.Columns), info.PrimaryKeyCol,
			func(rowKey int64, row Row) bool {
				if where != nil {
					val, err := eval.Eval(where, row)
					if err != nil {
						scanErr = err
						return false
					}
					b, ok := val.(bool)
					if !ok {
						scanErr = fmt.Errorf("WHERE expression must evaluate to boolean, got %T", val)
						return false
					}
					if !b {
						return true
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
							scanErr = err
							return false
						}
						projected[i] = val
					}
				}
				if distinct {
					key := string(encodeValues(projected))
					if seen[key] {
						return true
					}
					seen[key] = true
				}
				result = append(result, projected)
				return len(result) < earlyLimit
			},
		)
	} else {
		params.index.OrderedRangeScan(
			params.fromVal, params.fromInclusive,
			params.toVal, params.toInclusive,
			false, // not reverse (no ORDER BY in this path)
			func(rowKey int64) bool {
				row, found := db.storage.GetRow(info.Name, rowKey)
				if !found {
					return true
				}
				// Full WHERE evaluation (index condition + additional conditions)
				if where != nil {
					val, err := eval.Eval(where, row)
					if err != nil {
						scanErr = err
						return false
					}
					b, ok := val.(bool)
					if !ok {
						scanErr = fmt.Errorf("WHERE expression must evaluate to boolean, got %T", val)
						return false
					}
					if !b {
						return true
					}
				}
				// Projection
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
							scanErr = err
							return false
						}
						projected[i] = val
					}
				}
				// Dedup (DISTINCT only)
				if distinct {
					key := string(encodeValues(projected))
					if seen[key] {
						return true
					}
					seen[key] = true
				}
				result = append(result, projected)
				// Early exit once we have enough rows (earlyLimit = limit + offset)
				return len(result) < earlyLimit
			},
		)
	}
	if scanErr != nil {
		return nil, scanErr
	}

	result = applyOffset(result, stmt.Offset)
	result = applyLimit(result, stmt.Limit)
	return &Result{Columns: colNames, ColumnTypes: colTypes, Rows: result}, nil
}

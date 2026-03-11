package engine

import (
	"github.com/walf443/oresql/ast"
)

// executeSelectWithIndexOrder executes a SELECT using index-ordered scan.
// WHERE is applied during scan (Phase 2 skipped).
// For fullOrder, ORDER BY sort is skipped entirely.
// For partialOrder, sort is applied on a reduced row set.
func (e *Executor) executeSelectWithIndexOrder(
	stmt *ast.SelectStmt, db *Database, info *TableInfo, ior *indexOrderResult,
) (*Result, error) {
	rows, eval, err := e.scanSourceOrderedByIndex(stmt, db, info, ior)
	if err != nil {
		return nil, err
	}

	// For partialOrder, need to sort by remaining ORDER BY columns
	if !ior.fullOrder {
		if stmt.Limit != nil {
			topK := int(*stmt.Limit)
			if stmt.Offset != nil {
				topK += int(*stmt.Offset)
			}
			rows, err = sortRowsTopK(rows, stmt.OrderBy, eval, rowIdentity, topK)
		} else {
			rows, err = sortRows(rows, stmt.OrderBy, eval, rowIdentity)
		}
		if err != nil {
			return nil, err
		}
	}

	// Phase 5: Projection
	colNames, colExprs, isStar, err := resolveSelectColumns(stmt.Columns, eval)
	if err != nil {
		return nil, err
	}
	colTypes := resolveColumnTypes(stmt.Columns, eval)
	rows, err = projectRows(rows, colExprs, isStar, eval)
	if err != nil {
		return nil, err
	}

	// Phase 7: OFFSET
	rows = applyOffset(rows, stmt.Offset)

	// Phase 8: LIMIT
	rows = applyLimit(rows, stmt.Limit)

	return &Result{Columns: colNames, ColumnTypes: colTypes, Rows: rows}, nil
}

// scanSourceOrderedByIndex scans rows using index order to satisfy ORDER BY.
// Returns rows in the correct order for fullOrder, or partially ordered rows for partialOrder.
// WHERE filtering is applied during the scan.
func (e *Executor) scanSourceOrderedByIndex(
	stmt *ast.SelectStmt, db *Database, info *TableInfo, ior *indexOrderResult,
) ([]Row, ExprEvaluator, error) {
	eval := newTableEvaluator(e, info)

	needed := 0
	if stmt.Limit != nil {
		needed = int(*stmt.Limit)
		if stmt.Offset != nil {
			needed += int(*stmt.Offset)
		}
	}

	if ior.fullOrder {
		return e.scanFullOrder(stmt, db, info, ior, eval, needed)
	}
	return e.scanPartialOrder(stmt, db, info, ior, eval, needed)
}

// scanFullOrder handles the case where ORDER BY is a single column with an index.
// No sort needed after scan; rows are in final order.
func (e *Executor) scanFullOrder(
	stmt *ast.SelectStmt, db *Database, info *TableInfo, ior *indexOrderResult,
	eval ExprEvaluator, needed int,
) ([]Row, ExprEvaluator, error) {
	cap := 64
	if needed > 0 {
		cap = needed
	}
	rows := make([]Row, 0, cap)

	if ior.usePK {
		// PK order scan
		// When there's no WHERE, we can limit collection to exactly needed rows.
		// With WHERE, we must collect all rows because filtering may skip some.
		forEachLimit := 0
		if stmt.Where == nil && needed > 0 {
			forEachLimit = needed
		}

		// PK Covering: if only PK column (or no columns) are needed, skip row decoding
		neededCols := collectNeededColumns(stmt.Columns, stmt.Where, stmt.OrderBy, info)
		if isPKOnlyCovering(neededCols, info.PrimaryKeyCol) {
			if stmt.Where != nil {
				// WHERE requires full-width Row for evaluation
				numCols := len(info.Columns)
				pkIdx := info.PrimaryKeyCol
				db.storage.ForEachRowKeyOnly(info.Name, ior.reverse, func(key int64) bool {
					row := buildPKOnlyRow(key, numCols, pkIdx)
					val, err := eval.Eval(stmt.Where, row)
					if err != nil {
						return false
					}
					b, ok := val.(bool)
					if !ok || !b {
						return true
					}
					rows = append(rows, row)
					if needed > 0 && len(rows) >= needed {
						return false
					}
					return true
				}, forEachLimit)
			} else {
				// No WHERE: use compact 1-element rows with pkOnlyEvaluator
				eval = newPKOnlyEvaluator(e, info)
				db.storage.ForEachRowKeyOnly(info.Name, ior.reverse, func(key int64) bool {
					rows = append(rows, Row{key})
					if needed > 0 && len(rows) >= needed {
						return false
					}
					return true
				}, forEachLimit)
			}
		} else if stmt.Where != nil && !containsSubquery(stmt.Where) && !columnsContainSubquery(stmt.Columns) {
			// WHERE + no subquery: use ScanEachWithKey for Row reuse (alloc on match only)
			db.storage.ScanEachWithKey(info.Name, ior.reverse, func(key int64, row Row) bool {
				val, err := eval.Eval(stmt.Where, row)
				if err != nil {
					return false
				}
				b, ok := val.(bool)
				if !ok || !b {
					return true // filtered out: Row reused, no alloc
				}
				// Match: copy row to retain beyond callback
				kept := make(Row, len(row))
				copy(kept, row)
				rows = append(rows, kept)
				if needed > 0 && len(rows) >= needed {
					return false
				}
				return true
			}, forEachLimit)
		} else {
			// WHERE なし or subquery あり: use ForEachRow (collect-then-iterate)
			db.storage.ForEachRow(info.Name, ior.reverse, func(key int64, row Row) bool {
				if stmt.Where != nil {
					val, err := eval.Eval(stmt.Where, row)
					if err != nil {
						return false
					}
					b, ok := val.(bool)
					if !ok || !b {
						return true
					}
				}
				rows = append(rows, row)
				if needed > 0 && len(rows) >= needed {
					return false
				}
				return true
			}, forEachLimit)
		}
	} else {
		// Secondary index order scan — try covering first
		neededCols := collectNeededColumns(stmt.Columns, stmt.Where, stmt.OrderBy, info)
		cir, isCovering := ior.index.(CoveringIndexReader)
		if isCovering && isIndexCovering(ior.index, neededCols, info.PrimaryKeyCol) {
			cir.OrderedCoveringScan(
				ior.fromVal, ior.fromInclusive,
				ior.toVal, ior.toInclusive,
				ior.reverse, len(info.Columns), info.PrimaryKeyCol,
				func(rowKey int64, row Row) bool {
					if stmt.Where != nil {
						wVal, err := eval.Eval(stmt.Where, row)
						if err != nil {
							return false
						}
						b, ok := wVal.(bool)
						if !ok || !b {
							return true
						}
					}
					rows = append(rows, row)
					if needed > 0 && len(rows) >= needed {
						return false
					}
					return true
				},
			)
		} else {
			ior.index.OrderedRangeScan(
				ior.fromVal, ior.fromInclusive,
				ior.toVal, ior.toInclusive,
				ior.reverse,
				func(rowKey int64) bool {
					row, found := db.storage.GetRow(info.Name, rowKey)
					if !found {
						return true
					}
					if stmt.Where != nil {
						wVal, err := eval.Eval(stmt.Where, row)
						if err != nil {
							return false
						}
						b, ok := wVal.(bool)
						if !ok || !b {
							return true
						}
					}
					rows = append(rows, row)
					if needed > 0 && len(rows) >= needed {
						return false
					}
					return true
				},
			)
		}
	}

	// For non-PK, nullable columns without LIMIT: move NULLs to end
	if !ior.usePK && ior.index != nil {
		colIdx := ior.index.GetInfo().ColumnIdxs[0]
		col := info.Columns[colIdx]
		if !col.NotNull && !col.PrimaryKey {
			// Move NULL rows to end
			var nonNull, nullRows []Row
			for _, row := range rows {
				if row[colIdx] == nil {
					nullRows = append(nullRows, row)
				} else {
					nonNull = append(nonNull, row)
				}
			}
			rows = append(nonNull, nullRows...)
		}
	}

	return rows, eval, nil
}

// scanPartialOrder handles ORDER BY with multiple columns where only the first has an index.
// Reads rows in first-column order, applying group boundary cutoff for LIMIT optimization.
func (e *Executor) scanPartialOrder(
	stmt *ast.SelectStmt, db *Database, info *TableInfo, ior *indexOrderResult,
	eval ExprEvaluator, needed int,
) ([]Row, ExprEvaluator, error) {
	cap := 64
	if needed > 0 {
		cap = needed
	}
	rows := make([]Row, 0, cap)

	// Determine the column index for the first ORDER BY column
	ident := stmt.OrderBy[0].Expr.(*ast.IdentExpr)
	orderCol, _ := info.FindColumn(ident.Name)
	orderColIdx := orderCol.Index

	var prevKeyVal Value
	firstRow := true

	scanFn := func(rowKey int64) bool {
		row, found := db.storage.GetRow(info.Name, rowKey)
		if !found {
			return true
		}
		if stmt.Where != nil {
			wVal, err := eval.Eval(stmt.Where, row)
			if err != nil {
				return false
			}
			b, ok := wVal.(bool)
			if !ok || !b {
				return true
			}
		}

		currentKeyVal := row[orderColIdx]
		if needed > 0 && len(rows) >= needed && !firstRow {
			// Check if first column value changed
			if !valuesEqual(currentKeyVal, prevKeyVal) {
				return false // stop
			}
		}
		prevKeyVal = currentKeyVal
		firstRow = false
		rows = append(rows, row)
		return true
	}

	if ior.usePK {
		// partialOrder cannot use limit because it needs to collect all rows
		// in the same first-column value group even after reaching needed count.
		db.storage.ForEachRow(info.Name, ior.reverse, func(key int64, row Row) bool {
			if stmt.Where != nil {
				wVal, err := eval.Eval(stmt.Where, row)
				if err != nil {
					return false
				}
				b, ok := wVal.(bool)
				if !ok || !b {
					return true
				}
			}

			currentKeyVal := row[orderColIdx]
			if needed > 0 && len(rows) >= needed && !firstRow {
				if !valuesEqual(currentKeyVal, prevKeyVal) {
					return false
				}
			}
			prevKeyVal = currentKeyVal
			firstRow = false
			rows = append(rows, row)
			return true
		}, 0)
	} else {
		// Try covering scan for partial order
		neededCols := collectNeededColumns(stmt.Columns, stmt.Where, stmt.OrderBy, info)
		cir, isCovering := ior.index.(CoveringIndexReader)
		if isCovering && isIndexCovering(ior.index, neededCols, info.PrimaryKeyCol) {
			cir.OrderedCoveringScan(
				ior.fromVal, ior.fromInclusive,
				ior.toVal, ior.toInclusive,
				ior.reverse, len(info.Columns), info.PrimaryKeyCol,
				func(rowKey int64, row Row) bool {
					if stmt.Where != nil {
						wVal, err := eval.Eval(stmt.Where, row)
						if err != nil {
							return false
						}
						b, ok := wVal.(bool)
						if !ok || !b {
							return true
						}
					}
					currentKeyVal := row[orderColIdx]
					if needed > 0 && len(rows) >= needed && !firstRow {
						if !valuesEqual(currentKeyVal, prevKeyVal) {
							return false
						}
					}
					prevKeyVal = currentKeyVal
					firstRow = false
					rows = append(rows, row)
					return true
				},
			)
		} else {
			ior.index.OrderedRangeScan(
				ior.fromVal, ior.fromInclusive,
				ior.toVal, ior.toInclusive,
				ior.reverse,
				scanFn,
			)
		}
	}

	return rows, eval, nil
}

// valuesEqual compares two Values for equality (including nil == nil).
func valuesEqual(a, b Value) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return compareValues(a, b) == 0
}

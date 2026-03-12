package engine

import (
	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/engine/expr"
	"github.com/walf443/oresql/storage"
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
	eval := newTableEvaluator(makeSubqueryRunner(e), info)

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
	var rows []Row
	if ior.usePK {
		rows, eval = e.scanFullOrderPK(stmt, db, info, ior, eval, needed)
	} else {
		rows = e.scanFullOrderSecondary(stmt, db, info, ior, eval, needed)
	}

	// For non-PK, nullable columns: move NULLs to end
	if !ior.usePK && ior.index != nil {
		rows = moveNullsToEnd(rows, ior.index, info)
	}

	return rows, eval, nil
}

// moveNullsToEnd moves rows with NULL in the index column to the end of the slice.
func moveNullsToEnd(rows []Row, index storage.IndexReader, info *TableInfo) []Row {
	colIdx := index.GetInfo().ColumnIdxs[0]
	col := info.Columns[colIdx]
	if col.NotNull || col.PrimaryKey {
		return rows
	}
	var nonNull, nullRows []Row
	for _, row := range rows {
		if row[colIdx] == nil {
			nullRows = append(nullRows, row)
		} else {
			nonNull = append(nonNull, row)
		}
	}
	return append(nonNull, nullRows...)
}

// evalWhereFilter evaluates a WHERE expression and returns true if the row passes.
func evalWhereFilter(where ast.Expr, row Row, eval ExprEvaluator) bool {
	val, err := eval.Eval(where, row)
	if err != nil {
		return false
	}
	b, ok := val.(bool)
	return ok && b
}

// scanFullOrderPK scans rows in PK order with optional WHERE filtering.
func (e *Executor) scanFullOrderPK(
	stmt *ast.SelectStmt, db *Database, info *TableInfo, ior *indexOrderResult,
	eval ExprEvaluator, needed int,
) ([]Row, ExprEvaluator) {
	cap := 64
	if needed > 0 {
		cap = needed
	}
	rows := make([]Row, 0, cap)

	forEachLimit := 0
	if stmt.Where == nil && needed > 0 {
		forEachLimit = needed
	}

	neededCols := collectNeededColumns(stmt.Columns, stmt.Where, stmt.OrderBy, info)
	if isPKOnlyCovering(neededCols, info.PrimaryKeyCol) {
		return e.scanPKCovering(stmt, db, info, ior, eval, needed, forEachLimit, rows)
	}

	if stmt.Where != nil && !containsSubquery(stmt.Where) && !columnsContainSubquery(stmt.Columns) {
		// WHERE + no subquery: use ScanEachWithKey for Row reuse
		db.storage.ScanEachWithKey(info.Name, ior.reverse, func(key int64, row Row) bool {
			if !evalWhereFilter(stmt.Where, row, eval) {
				return true
			}
			kept := make(Row, len(row))
			copy(kept, row)
			rows = append(rows, kept)
			return needed <= 0 || len(rows) < needed
		}, forEachLimit)
	} else {
		// No WHERE or subquery: use ForEachRow
		db.storage.ForEachRow(info.Name, ior.reverse, func(key int64, row Row) bool {
			if stmt.Where != nil && !evalWhereFilter(stmt.Where, row, eval) {
				return true
			}
			rows = append(rows, row)
			return needed <= 0 || len(rows) < needed
		}, forEachLimit)
	}
	return rows, eval
}

// scanPKCovering handles PK-only covering scans where row decoding is skipped.
func (e *Executor) scanPKCovering(
	stmt *ast.SelectStmt, db *Database, info *TableInfo, ior *indexOrderResult,
	eval ExprEvaluator, needed, forEachLimit int, rows []Row,
) ([]Row, ExprEvaluator) {
	if stmt.Where != nil {
		numCols := len(info.Columns)
		pkIdx := info.PrimaryKeyCol
		db.storage.ForEachRowKeyOnly(info.Name, ior.reverse, func(key int64) bool {
			row := buildPKOnlyRow(key, numCols, pkIdx)
			if !evalWhereFilter(stmt.Where, row, eval) {
				return true
			}
			rows = append(rows, row)
			return needed <= 0 || len(rows) < needed
		}, forEachLimit)
	} else {
		eval = newPKOnlyEvaluator(makeSubqueryRunner(e), info)
		db.storage.ForEachRowKeyOnly(info.Name, ior.reverse, func(key int64) bool {
			rows = append(rows, Row{key})
			return needed <= 0 || len(rows) < needed
		}, forEachLimit)
	}
	return rows, eval
}

// scanFullOrderSecondary scans rows using a secondary index order.
func (e *Executor) scanFullOrderSecondary(
	stmt *ast.SelectStmt, db *Database, info *TableInfo, ior *indexOrderResult,
	eval ExprEvaluator, needed int,
) []Row {
	cap := 64
	if needed > 0 {
		cap = needed
	}
	rows := make([]Row, 0, cap)

	neededCols := collectNeededColumns(stmt.Columns, stmt.Where, stmt.OrderBy, info)
	cir, isCovering := ior.index.(storage.CoveringIndexReader)
	if isCovering && isIndexCovering(ior.index, neededCols, info.PrimaryKeyCol) {
		cir.OrderedCoveringScan(
			ior.fromVal, ior.fromInclusive,
			ior.toVal, ior.toInclusive,
			ior.reverse, len(info.Columns), info.PrimaryKeyCol,
			func(rowKey int64, row Row) bool {
				if stmt.Where != nil && !evalWhereFilter(stmt.Where, row, eval) {
					return true
				}
				rows = append(rows, row)
				return needed <= 0 || len(rows) < needed
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
				if stmt.Where != nil && !evalWhereFilter(stmt.Where, row, eval) {
					return true
				}
				rows = append(rows, row)
				return needed <= 0 || len(rows) < needed
			},
		)
	}
	return rows
}

// partialOrderState holds mutable state for partial order scanning.
type partialOrderState struct {
	rows        []Row
	prevKeyVal  Value
	firstRow    bool
	orderColIdx int
	needed      int
	where       ast.Expr
	eval        ExprEvaluator
}

// acceptRow applies WHERE filter and group boundary cutoff, appending the row if accepted.
// Returns true to continue scanning.
func (s *partialOrderState) acceptRow(row Row) bool {
	if s.where != nil && !evalWhereFilter(s.where, row, s.eval) {
		return true
	}
	currentKeyVal := row[s.orderColIdx]
	if s.needed > 0 && len(s.rows) >= s.needed && !s.firstRow {
		if !valuesEqual(currentKeyVal, s.prevKeyVal) {
			return false
		}
	}
	s.prevKeyVal = currentKeyVal
	s.firstRow = false
	s.rows = append(s.rows, row)
	return true
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

	ident := stmt.OrderBy[0].Expr.(*ast.IdentExpr)
	orderCol, _ := info.FindColumn(ident.Name)

	state := &partialOrderState{
		rows:        make([]Row, 0, cap),
		firstRow:    true,
		orderColIdx: orderCol.Index,
		needed:      needed,
		where:       stmt.Where,
		eval:        eval,
	}

	if ior.usePK {
		e.scanPartialOrderPK(db, info, ior, state)
	} else {
		e.scanPartialOrderSecondary(stmt, db, info, ior, state)
	}

	return state.rows, eval, nil
}

// scanPartialOrderPK scans using PK order for partial order.
func (e *Executor) scanPartialOrderPK(db *Database, info *TableInfo, ior *indexOrderResult, state *partialOrderState) {
	db.storage.ForEachRow(info.Name, ior.reverse, func(key int64, row Row) bool {
		return state.acceptRow(row)
	}, 0)
}

// scanPartialOrderSecondary scans using a secondary index for partial order.
func (e *Executor) scanPartialOrderSecondary(
	stmt *ast.SelectStmt, db *Database, info *TableInfo, ior *indexOrderResult, state *partialOrderState,
) {
	neededCols := collectNeededColumns(stmt.Columns, stmt.Where, stmt.OrderBy, info)
	cir, isCovering := ior.index.(storage.CoveringIndexReader)
	if isCovering && isIndexCovering(ior.index, neededCols, info.PrimaryKeyCol) {
		cir.OrderedCoveringScan(
			ior.fromVal, ior.fromInclusive,
			ior.toVal, ior.toInclusive,
			ior.reverse, len(info.Columns), info.PrimaryKeyCol,
			func(rowKey int64, row Row) bool {
				return state.acceptRow(row)
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
				return state.acceptRow(row)
			},
		)
	}
}

// valuesEqual compares two Values for equality (including nil == nil).
func valuesEqual(a, b Value) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return expr.Compare(a, b) == 0
}

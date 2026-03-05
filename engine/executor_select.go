package engine

import (
	"fmt"
	"strings"

	"github.com/walf443/oresql/ast"
)

// maxRecursiveDepth is the maximum number of iterations for recursive CTEs.
const maxRecursiveDepth = 1000

// executeWith materializes all CTEs and then executes the body statement.
func (e *Executor) executeWith(stmt *ast.WithStmt) (*Result, error) {
	prevScope := e.cteScope
	e.cteScope = make(map[string]*cteEntry)
	// Inherit outer CTE scope so nested WITH can see enclosing CTEs.
	for k, v := range prevScope {
		e.cteScope[k] = v
	}
	defer func() { e.cteScope = prevScope }()

	for _, cte := range stmt.CTEs {
		if cte.Recursive {
			if err := e.materializeRecursiveCTE(cte); err != nil {
				return nil, err
			}
		} else {
			info, rows, err := e.materializeSubquery(cte.Query, cte.Name)
			if err != nil {
				return nil, fmt.Errorf("error materializing CTE %q: %w", cte.Name, err)
			}
			e.cteScope[strings.ToLower(cte.Name)] = &cteEntry{info: info, rows: rows}
		}
	}
	return e.executeInner(stmt.Query)
}

// materializeRecursiveCTE executes a recursive CTE using a fixpoint loop.
func (e *Executor) materializeRecursiveCTE(cte ast.CTEDef) error {
	cteName := strings.ToLower(cte.Name)

	setOp, ok := cte.Query.(*ast.SetOpStmt)
	if !ok {
		return fmt.Errorf("recursive CTE %q must use UNION or UNION ALL", cte.Name)
	}
	if setOp.Op != ast.SetOpUnion {
		return fmt.Errorf("recursive CTE %q must use UNION or UNION ALL, got %s", cte.Name, setOp.Op)
	}

	// 1. Execute anchor (left side)
	anchorResult, err := e.executeInner(setOp.Left)
	if err != nil {
		return fmt.Errorf("error executing anchor of recursive CTE %q: %w", cte.Name, err)
	}

	// 2. Build TableInfo from anchor result
	cols := make([]ColumnInfo, len(anchorResult.Columns))
	for i, name := range anchorResult.Columns {
		dt := ""
		if i < len(anchorResult.ColumnTypes) {
			dt = anchorResult.ColumnTypes[i]
		}
		cols[i] = ColumnInfo{
			Name:     name,
			DataType: dt,
			Index:    i,
		}
	}
	info := &TableInfo{
		Name:          cteName,
		Columns:       cols,
		PrimaryKeyCol: -1,
	}

	// 3. Initialize working set and all rows
	allRows := make([]Row, len(anchorResult.Rows))
	copy(allRows, anchorResult.Rows)
	workingRows := make([]Row, len(anchorResult.Rows))
	copy(workingRows, anchorResult.Rows)

	// For UNION (distinct), track all seen rows
	var seen map[string]bool
	if !setOp.All {
		seen = make(map[string]bool)
		for _, row := range allRows {
			seen[string(encodeValues(row))] = true
		}
	}

	// 4. Fixpoint loop
	for iter := 0; iter < maxRecursiveDepth; iter++ {
		// Register working rows so the recursive term can reference them
		e.cteScope[cteName] = &cteEntry{info: info, rows: workingRows}

		// Execute recursive term (right side)
		newResult, err := e.executeSelect(setOp.Right)
		if err != nil {
			return fmt.Errorf("error executing recursive term of CTE %q (iteration %d): %w", cte.Name, iter+1, err)
		}

		newRows := newResult.Rows

		// For UNION (distinct), remove already-seen rows
		if !setOp.All && len(newRows) > 0 {
			filtered := make([]Row, 0, len(newRows))
			for _, row := range newRows {
				key := string(encodeValues(row))
				if !seen[key] {
					seen[key] = true
					filtered = append(filtered, row)
				}
			}
			newRows = filtered
		}

		// Fixpoint reached: no new rows
		if len(newRows) == 0 {
			break
		}

		allRows = append(allRows, newRows...)
		workingRows = newRows

		if iter == maxRecursiveDepth-1 {
			return fmt.Errorf("recursive CTE %q exceeded maximum depth of %d iterations", cte.Name, maxRecursiveDepth)
		}
	}

	// 5. Store final result
	e.cteScope[cteName] = &cteEntry{info: info, rows: allRows}
	return nil
}

// lookupCTE checks whether the given table name refers to a CTE in scope.
func (e *Executor) lookupCTE(name string) (*TableInfo, []Row, bool) {
	if e.cteScope == nil {
		return nil, nil, false
	}
	entry, ok := e.cteScope[strings.ToLower(name)]
	if !ok {
		return nil, nil, false
	}
	// Return a copy of rows so each reference gets its own slice.
	rowsCopy := make([]Row, len(entry.rows))
	copy(rowsCopy, entry.rows)
	return entry.info, rowsCopy, true
}

func (e *Executor) executeSelect(stmt *ast.SelectStmt) (*Result, error) {
	// SELECT without FROM: evaluate expressions directly
	if stmt.TableName == "" && stmt.FromSubquery == nil {
		return e.executeSelectWithoutTable(stmt)
	}

	// Try index-ordered scan for ORDER BY optimization
	if len(stmt.OrderBy) > 0 && len(stmt.Joins) == 0 && stmt.TableAlias == "" &&
		stmt.FromSubquery == nil &&
		len(stmt.GroupBy) == 0 && !hasAggregate(stmt.Columns) && !stmt.Distinct &&
		!hasWindowFunction(stmt.Columns) {
		db, err := e.resolveDatabase(stmt.DatabaseName)
		if err == nil {
			info, err := db.catalog.GetTable(stmt.TableName)
			if err == nil {
				if ior := e.tryIndexOrder(stmt.OrderBy, stmt.Where, info, stmt.Limit != nil); ior != nil {
					return e.executeSelectWithIndexOrder(stmt, db, info, ior)
				}
			}
		}
	}

	// Try GROUP BY index optimization (streaming aggregation via index-ordered scan)
	if result, ok := e.tryGroupByIndexOptimization(stmt); ok {
		return result, nil
	}

	// Try COUNT(*) optimization using RowCount
	if result, ok := e.tryCountStarOptimization(stmt); ok {
		return result, nil
	}

	// Try MIN/MAX index optimization
	if result, ok := e.tryMinMaxIndexOptimization(stmt); ok {
		return result, nil
	}

	// Determine if early limit termination is safe
	canEarlyLimit := stmt.Limit != nil &&
		len(stmt.OrderBy) == 0 &&
		len(stmt.GroupBy) == 0 &&
		!hasAggregate(stmt.Columns) &&
		!hasWindowFunction(stmt.Columns)

	var earlyLimit int
	if canEarlyLimit {
		earlyLimit = int(*stmt.Limit)
		if stmt.Offset != nil {
			earlyLimit += int(*stmt.Offset)
		}
	}

	// Streaming scan: scan rows with early exit for LIMIT.
	// Handles both DISTINCT + LIMIT and WHERE + LIMIT without ORDER BY/GROUP BY/aggregate.
	// Safe only when there are no subqueries in WHERE/SELECT, no JOINs, no CTE,
	// no table alias, no subquery source.
	// Flow: index streaming (equality/range) → full scan streaming → batch fallthrough.
	if canEarlyLimit && (stmt.Distinct || stmt.Where != nil) &&
		stmt.FromSubquery == nil && len(stmt.Joins) == 0 && stmt.TableAlias == "" &&
		!containsSubquery(stmt.Where) && !columnsContainSubquery(stmt.Columns) {
		if _, _, cteOk := e.lookupCTE(stmt.TableName); !cteOk {
			db, info, err := e.resolveTable(stmt.DatabaseName, stmt.TableName)
			if err == nil {
				// 1. Index streaming (single-column equality/range)
				if stmt.Where != nil {
					if params, ok := e.tryIndexScanParams(stmt.Where, info); ok {
						return e.executeIndexScanStreaming(stmt, db, info, params, earlyLimit, stmt.Distinct)
					}
				}
				// 2. Batch index keys — stream through ForEachByKeys
				if keys, indexUsed := e.tryIndexScan(stmt.Where, info); indexUsed {
					return e.executeForEachByKeysStreaming(stmt, db, info, keys, earlyLimit, stmt.Distinct)
				}
				// 3. Full scan streaming (no index available)
				return e.executeScanEachStreaming(stmt, db, info, earlyLimit, stmt.Distinct)
			}
		}
	}

	// Phase 1: Source rows + evaluator
	// For DISTINCT, don't pass earlyLimit to scanSource (JOIN needs all rows for dedup)
	scanLimit := earlyLimit
	if stmt.Distinct {
		scanLimit = 0
	}
	rows, eval, err := e.scanSource(stmt, scanLimit)
	if err != nil {
		return nil, err
	}

	// Resolve column types early (before GROUP BY may replace eval)
	colTypes := resolveColumnTypes(stmt.Columns, eval)

	// Fast path: DISTINCT + LIMIT without ORDER BY/GROUP BY/aggregate
	// Combines WHERE, projection, dedup, and early termination in one pass
	if canEarlyLimit && stmt.Distinct {
		colNames, colExprs, isStar, err := resolveSelectColumns(stmt.Columns, eval)
		if err != nil {
			return nil, err
		}
		// For single-table / subquery-without-join path, apply WHERE + project + dedup in one loop
		// For JOIN path, WHERE is already applied in scanSource; pass nil
		var whereExpr ast.Expr
		if !e.usedJoinPath(stmt) {
			whereExpr = stmt.Where
		}
		rows, err = filterProjectDedupLimit(rows, whereExpr, colExprs, isStar, eval, earlyLimit)
		if err != nil {
			return nil, err
		}
		rows = applyOffset(rows, stmt.Offset)
		rows = applyLimit(rows, stmt.Limit)
		return &Result{Columns: colNames, ColumnTypes: colTypes, Rows: rows}, nil
	}

	// Phase 2: WHERE filter (JOIN path handles WHERE internally via scanSource)
	if !e.usedJoinPath(stmt) {
		if canEarlyLimit {
			rows, err = filterWhereLimit(rows, stmt.Where, eval, rowIdentity, earlyLimit)
		} else {
			rows, err = filterWhere(rows, stmt.Where, eval, rowIdentity)
		}
		if err != nil {
			return nil, err
		}
	}

	// Phase 2.5: Window functions
	if hasWindowFunction(stmt.Columns) {
		rows, eval, err = e.applyWindowFunctions(stmt, rows, eval)
		if err != nil {
			return nil, err
		}
	}

	// Phase 3: GROUP BY / Aggregate + HAVING
	var colNames []string
	var colExprs []ast.Expr
	var isStar bool
	var projected bool

	if len(stmt.GroupBy) > 0 || hasAggregate(stmt.Columns) {
		rows, colNames, eval, err = e.applyGroupBy(stmt, rows, eval)
		if err != nil {
			return nil, err
		}
		projected = true
	} else {
		colNames, colExprs, isStar, err = resolveSelectColumns(stmt.Columns, eval)
		if err != nil {
			return nil, err
		}
	}

	// Phase 4: ORDER BY (use heap-based top-K when LIMIT is present)
	if stmt.Limit != nil && len(stmt.OrderBy) > 0 {
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

	// Phase 5: Projection (GROUP BY already projected)
	if !projected {
		rows, err = projectRows(rows, colExprs, isStar, eval)
		if err != nil {
			return nil, err
		}
	}

	// Phase 6: DISTINCT
	if stmt.Distinct {
		rows = dedup(rows)
	}

	// Phase 7: OFFSET
	rows = applyOffset(rows, stmt.Offset)

	// Phase 8: LIMIT
	rows = applyLimit(rows, stmt.Limit)

	return &Result{Columns: colNames, ColumnTypes: colTypes, Rows: rows}, nil
}

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
			numCols := len(info.Columns)
			pkIdx := info.PrimaryKeyCol
			db.storage.ForEachRowKeyOnly(info.Name, ior.reverse, func(key int64) bool {
				if stmt.Where != nil {
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
				} else {
					rows = append(rows, buildPKOnlyRow(key, numCols, pkIdx))
				}
				if needed > 0 && len(rows) >= needed {
					return false
				}
				return true
			}, forEachLimit)
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

// usedJoinPath returns true if scanSource would route through the join path for this stmt.
// The join path handles WHERE internally, so callers should skip Phase 2 filtering.
func (e *Executor) usedJoinPath(stmt *ast.SelectStmt) bool {
	if stmt.FromSubquery != nil {
		return len(stmt.Joins) > 0
	}
	return len(stmt.Joins) > 0 || stmt.TableAlias != ""
}

// scanSource returns the source rows and an appropriate evaluator for the query.
// earlyLimit > 0 enables early termination for the JOIN path.
func (e *Executor) scanSource(stmt *ast.SelectStmt, earlyLimit int) ([]Row, ExprEvaluator, error) {
	if stmt.FromSubquery != nil {
		return e.scanSourceSubquery(stmt, earlyLimit)
	}
	if len(stmt.Joins) > 0 || stmt.TableAlias != "" {
		return e.scanSourceJoin(stmt, earlyLimit)
	}
	return e.scanSourceSingle(stmt, earlyLimit)
}

// materializeSubquery executes a subquery and returns a virtual TableInfo and the rows.
// The alias is used as the virtual table name.
func (e *Executor) materializeSubquery(subquery ast.Statement, alias string) (*TableInfo, []Row, error) {
	result, err := e.executeInner(subquery)
	if err != nil {
		return nil, nil, err
	}
	cols := make([]ColumnInfo, len(result.Columns))
	for i, name := range result.Columns {
		dt := ""
		if i < len(result.ColumnTypes) {
			dt = result.ColumnTypes[i]
		}
		cols[i] = ColumnInfo{
			Name:     name,
			DataType: dt,
			Index:    i,
		}
	}
	info := &TableInfo{
		Name:          strings.ToLower(alias),
		Columns:       cols,
		PrimaryKeyCol: -1,
	}
	return info, result.Rows, nil
}

// scanSourceSubquery handles FROM subquery. If JOINs are present, delegates to scanSourceJoin
// after materializing the subquery. Otherwise returns rows with a tableEvaluator.
func (e *Executor) scanSourceSubquery(stmt *ast.SelectStmt, earlyLimit int) ([]Row, ExprEvaluator, error) {
	if len(stmt.Joins) > 0 {
		return e.scanSourceJoin(stmt, earlyLimit)
	}
	info, rows, err := e.materializeSubquery(stmt.FromSubquery, stmt.TableAlias)
	if err != nil {
		return nil, nil, err
	}
	return rows, newTableEvaluator(e, info), nil
}

// scanSourceSingle scans a single table with optional index optimization.
func (e *Executor) scanSourceSingle(stmt *ast.SelectStmt, earlyLimit int) ([]Row, ExprEvaluator, error) {
	// Check CTE scope before resolving from catalog.
	if cteInfo, cteRows, ok := e.lookupCTE(stmt.TableName); ok {
		return cteRows, newTableEvaluator(e, cteInfo), nil
	}

	db, info, err := e.resolveTable(stmt.DatabaseName, stmt.TableName)
	if err != nil {
		return nil, nil, err
	}

	var rows []Row

	// Try covering index lookup (skip PK lookup entirely)
	neededCols := collectNeededColumns(stmt.Columns, stmt.Where, stmt.OrderBy, info)
	if coveringRows, ok := e.tryIndexLookupCovering(stmt.Where, info, neededCols); ok {
		return coveringRows, newTableEvaluator(e, info), nil
	}

	// PK Covering: if only PK column (or no columns) are needed, skip row decoding
	if isPKOnlyCovering(neededCols, info.PrimaryKeyCol) {
		numCols := len(info.Columns)
		pkIdx := info.PrimaryKeyCol
		limit := 0
		if earlyLimit > 0 && stmt.Where == nil {
			limit = earlyLimit
		}
		db.storage.ForEachRowKeyOnly(info.Name, false, func(key int64) bool {
			rows = append(rows, buildPKOnlyRow(key, numCols, pkIdx))
			return true
		}, limit)
		return rows, newTableEvaluator(e, info), nil
	}

	if keys, indexUsed := e.tryIndexScan(stmt.Where, info); indexUsed {
		rows, err = db.storage.GetByKeys(info.Name, keys)
		if err != nil {
			return nil, nil, err
		}
	} else if earlyLimit > 0 && stmt.Where == nil {
		rows, err = db.storage.ScanOrdered(stmt.TableName, false, earlyLimit)
		if err != nil {
			return nil, nil, err
		}
	} else {
		rows, err = db.storage.Scan(stmt.TableName)
		if err != nil {
			return nil, nil, err
		}
	}

	return rows, newTableEvaluator(e, info), nil
}

// applyGroupBy processes GROUP BY / aggregate as a pipeline step.
// Returns projected result rows, column names, and a resultEvaluator for subsequent ORDER BY.
func (e *Executor) applyGroupBy(stmt *ast.SelectStmt, rows []Row, eval ExprEvaluator) ([]Row, []string, ExprEvaluator, error) {
	if len(stmt.GroupBy) > 0 {
		return e.applyGroupByWithGrouping(stmt, rows, eval)
	}
	// Aggregate without GROUP BY: entire set is one group
	return e.applyAggregateOnly(stmt, rows, eval)
}

// applyGroupByWithGrouping handles GROUP BY with grouping.
func (e *Executor) applyGroupByWithGrouping(stmt *ast.SelectStmt, rows []Row, eval ExprEvaluator) ([]Row, []string, ExprEvaluator, error) {
	// Group rows by GROUP BY expressions
	type group struct {
		key  string
		rows []Row
	}
	groupMap := make(map[string]*group)
	var groupOrder []string

	for _, row := range rows {
		gbVals := make([]Value, len(stmt.GroupBy))
		for i, gbExpr := range stmt.GroupBy {
			val, err := eval.Eval(gbExpr, row)
			if err != nil {
				return nil, nil, nil, err
			}
			gbVals[i] = val
		}
		key := string(encodeValues(gbVals))
		if _, ok := groupMap[key]; !ok {
			groupMap[key] = &group{key: key}
			groupOrder = append(groupOrder, key)
		}
		groupMap[key].rows = append(groupMap[key].rows, row)
	}

	// Resolve column names
	colNames, err := resolveGroupByColumnNames(stmt.Columns, eval)
	if err != nil {
		return nil, nil, nil, err
	}

	// We need a TableInfo for evalAggregate. Extract it from the evaluator.
	info := extractTableInfo(eval)

	// Evaluate each group
	var resultRows []Row
	for _, key := range groupOrder {
		grp := groupMap[key]
		representativeRow := grp.rows[0]

		geval := newGroupEvaluator(e, info, grp.rows)

		row := make(Row, len(stmt.Columns))
		for i, colExpr := range stmt.Columns {
			inner := colExpr
			if a, ok := colExpr.(*ast.AliasExpr); ok {
				inner = a.Expr
			}
			val, err := geval.Eval(inner, representativeRow)
			if err != nil {
				return nil, nil, nil, err
			}
			row[i] = val
		}

		// Apply HAVING filter
		if stmt.Having != nil {
			havingVal, err := geval.Eval(stmt.Having, representativeRow)
			if err != nil {
				return nil, nil, nil, err
			}
			b, ok := havingVal.(bool)
			if !ok {
				return nil, nil, nil, fmt.Errorf("HAVING expression must evaluate to boolean, got %T", havingVal)
			}
			if !b {
				continue
			}
		}

		resultRows = append(resultRows, row)
	}

	return resultRows, colNames, newResultEvaluator(e, stmt.Columns, colNames), nil
}

// applyAggregateOnly handles aggregate functions without GROUP BY.
func (e *Executor) applyAggregateOnly(stmt *ast.SelectStmt, rows []Row, eval ExprEvaluator) ([]Row, []string, ExprEvaluator, error) {
	info := extractTableInfo(eval)

	colNames := make([]string, 0, len(stmt.Columns))
	resultRow := make(Row, len(stmt.Columns))
	for i, colExpr := range stmt.Columns {
		alias := ""
		inner := colExpr
		if a, ok := colExpr.(*ast.AliasExpr); ok {
			alias = a.Alias
			inner = a.Expr
		}
		call, ok := inner.(*ast.CallExpr)
		if !ok {
			return nil, nil, nil, fmt.Errorf("mixed aggregate and non-aggregate columns are not supported")
		}
		val, colName, err := evalAggregate(call, rows, info)
		if err != nil {
			return nil, nil, nil, err
		}
		resultRow[i] = val
		if alias != "" {
			colNames = append(colNames, alias)
		} else {
			colNames = append(colNames, colName)
		}
	}

	return []Row{resultRow}, colNames, newResultEvaluator(e, stmt.Columns, colNames), nil
}

// extractTableInfo extracts a *TableInfo from an evaluator.
// For tableEvaluator it returns the underlying info.
// For joinEvaluator it returns the merged info (used by evalAggregate for column lookup).
func extractTableInfo(eval ExprEvaluator) *TableInfo {
	switch te := eval.(type) {
	case *tableEvaluator:
		return te.info
	case *joinEvaluator:
		return te.jc.MergedInfo
	case *correlatedEvaluator:
		return extractTableInfo(te.inner)
	default:
		return &TableInfo{Name: "unknown"}
	}
}

// executeSelectMaybeCorrelated checks whether a subquery references the outer query
// and dispatches to either the regular executeSelect (non-correlated) or
// executeSelectCorrelated (correlated).
func (e *Executor) executeSelectMaybeCorrelated(stmt *ast.SelectStmt, outerEval ExprEvaluator, outerRow Row) (*Result, error) {
	if hasOuterReferences(stmt, outerEval) {
		return e.executeSelectCorrelated(stmt, outerEval, outerRow)
	}
	return e.executeSelect(stmt)
}

// executeSelectCorrelated executes a correlated subquery for a given outer row.
// It scans the inner table without applying WHERE, then uses a correlatedEvaluator
// that can resolve both inner and outer column references to evaluate the full pipeline.
func (e *Executor) executeSelectCorrelated(stmt *ast.SelectStmt, outerEval ExprEvaluator, outerRow Row) (*Result, error) {
	// Phase 1: Source rows + inner evaluator (without WHERE)
	var info *TableInfo
	var rows []Row
	var err error
	if cteInfo, cteRows, ok := e.lookupCTE(stmt.TableName); ok {
		info = cteInfo
		rows = cteRows
	} else {
		var db *Database
		db, info, err = e.resolveTable(stmt.DatabaseName, stmt.TableName)
		if err != nil {
			return nil, err
		}
		rows, err = db.storage.Scan(stmt.TableName)
		if err != nil {
			return nil, err
		}
	}

	// Create inner evaluator that handles alias
	var innerEval ExprEvaluator
	if stmt.TableAlias != "" {
		tables := []struct {
			info  *TableInfo
			alias string
		}{{info: info, alias: stmt.TableAlias}}
		jc := newJoinContext(tables, nil)
		innerEval = newJoinEvaluator(e, jc)
	} else {
		innerEval = newTableEvaluator(e, info)
	}

	// Create correlated evaluator
	numInner := len(innerEval.ColumnList())
	corrEval := newCorrelatedEvaluator(e, innerEval, outerEval, outerRow, numInner)

	colTypes := resolveColumnTypes(stmt.Columns, corrEval)

	// Phase 2: WHERE filter
	rows, err = filterWhere(rows, stmt.Where, corrEval, rowIdentity)
	if err != nil {
		return nil, err
	}

	// Phase 3: GROUP BY / Aggregate + HAVING
	var colNames []string
	var colExprs []ast.Expr
	var isStar bool
	var projected bool

	if len(stmt.GroupBy) > 0 || hasAggregate(stmt.Columns) {
		rows, colNames, _, err = e.applyGroupBy(stmt, rows, corrEval)
		if err != nil {
			return nil, err
		}
		projected = true
	} else {
		colNames, colExprs, isStar, err = resolveSelectColumns(stmt.Columns, corrEval)
		if err != nil {
			return nil, err
		}
	}

	// Phase 4: ORDER BY
	if !projected {
		rows, err = sortRows(rows, stmt.OrderBy, corrEval, rowIdentity)
	}
	if err != nil {
		return nil, err
	}

	// Phase 5: Projection
	if !projected {
		rows, err = projectRows(rows, colExprs, isStar, corrEval)
		if err != nil {
			return nil, err
		}
	}

	// Phase 6: DISTINCT
	if stmt.Distinct {
		rows = dedup(rows)
	}

	// Phase 7: OFFSET
	rows = applyOffset(rows, stmt.Offset)

	// Phase 8: LIMIT
	rows = applyLimit(rows, stmt.Limit)

	return &Result{Columns: colNames, ColumnTypes: colTypes, Rows: rows}, nil
}

// resolveGroupByColumnNames resolves column names for GROUP BY result.
func resolveGroupByColumnNames(columns []ast.Expr, eval ExprEvaluator) ([]string, error) {
	colNames := make([]string, 0, len(columns))
	for _, colExpr := range columns {
		alias := ""
		inner := colExpr
		if a, ok := colExpr.(*ast.AliasExpr); ok {
			alias = a.Alias
			inner = a.Expr
		}

		if alias != "" {
			colNames = append(colNames, alias)
		} else if call, ok := inner.(*ast.CallExpr); ok {
			colNames = append(colNames, formatCallExpr(call))
		} else if ident, ok := inner.(*ast.IdentExpr); ok {
			col, err := eval.ResolveColumn(ident.Table, ident.Name)
			if err != nil {
				return nil, err
			}
			colNames = append(colNames, col.Name)
		} else {
			colNames = append(colNames, formatExpr(inner))
		}
	}
	return colNames, nil
}

// executeSelectWithoutTable handles SELECT without FROM (e.g. SELECT 1, 'hello').
func (e *Executor) executeSelectWithoutTable(stmt *ast.SelectStmt) (*Result, error) {
	colNames := make([]string, 0, len(stmt.Columns))
	var row Row

	eval := newLiteralEvaluator(e)
	for _, colExpr := range stmt.Columns {
		alias := ""
		inner := colExpr
		if a, ok := colExpr.(*ast.AliasExpr); ok {
			alias = a.Alias
			inner = a.Expr
		}
		val, err := eval.Eval(inner, nil)
		if err != nil {
			return nil, err
		}
		if alias != "" {
			colNames = append(colNames, alias)
		} else {
			colNames = append(colNames, formatExpr(inner))
		}
		row = append(row, val)
	}

	colTypes := resolveColumnTypes(stmt.Columns, eval)
	return &Result{Columns: colNames, ColumnTypes: colTypes, Rows: []Row{row}}, nil
}

// evalAggregate evaluates a single aggregate function call against a set of rows.
func evalAggregate(call *ast.CallExpr, rows []Row, info *TableInfo) (Value, string, error) {
	switch call.Name {
	case "COUNT":
		colName := formatCallExpr(call)
		// COUNT(DISTINCT col) counts unique non-NULL values
		if call.Distinct {
			if len(call.Args) != 1 {
				return nil, "", fmt.Errorf("COUNT(DISTINCT ...) expects 1 argument, got %d", len(call.Args))
			}
			ident, ok := call.Args[0].(*ast.IdentExpr)
			if !ok {
				return nil, "", fmt.Errorf("COUNT(DISTINCT ...) expects column name, got %T", call.Args[0])
			}
			col, err := info.FindColumn(ident.Name)
			if err != nil {
				return nil, "", err
			}
			seen := map[interface{}]bool{}
			for _, row := range rows {
				v := row[col.Index]
				if v == nil {
					continue
				}
				seen[v] = true
			}
			return int64(len(seen)), colName, nil
		}
		// COUNT(*) counts all rows; COUNT(literal) counts all rows; COUNT(column) excludes NULLs
		if len(call.Args) == 1 {
			if _, ok := call.Args[0].(*ast.StarExpr); !ok {
				// Literal values (e.g. COUNT(1)) count all rows like COUNT(*)
				if _, ok := call.Args[0].(*ast.IntLitExpr); ok {
					return int64(len(rows)), colName, nil
				}
				if _, ok := call.Args[0].(*ast.StringLitExpr); ok {
					return int64(len(rows)), colName, nil
				}
				ident, ok := call.Args[0].(*ast.IdentExpr)
				if !ok {
					return nil, "", fmt.Errorf("COUNT expects * or column name, got %T", call.Args[0])
				}
				col, err := info.FindColumn(ident.Name)
				if err != nil {
					return nil, "", err
				}
				count := int64(0)
				for _, row := range rows {
					if row[col.Index] != nil {
						count++
					}
				}
				return count, colName, nil
			}
		}
		return int64(len(rows)), colName, nil
	case "SUM":
		colName := formatCallExpr(call)
		if len(call.Args) != 1 {
			return nil, "", fmt.Errorf("SUM expects 1 argument, got %d", len(call.Args))
		}
		if _, ok := call.Args[0].(*ast.StarExpr); ok {
			return nil, "", fmt.Errorf("SUM(*) is not supported")
		}
		ident, ok := call.Args[0].(*ast.IdentExpr)
		if !ok {
			return nil, "", fmt.Errorf("SUM expects column name, got %T", call.Args[0])
		}
		col, err := info.FindColumn(ident.Name)
		if err != nil {
			return nil, "", err
		}
		var sumInt int64
		var sumFloat float64
		hasValue := false
		isFloat := false
		for _, row := range rows {
			v := row[col.Index]
			if v == nil {
				continue
			}
			switch tv := v.(type) {
			case int64:
				sumInt += tv
			case float64:
				isFloat = true
				sumFloat += tv
			default:
				return nil, "", fmt.Errorf("SUM requires numeric values, got %T", v)
			}
			hasValue = true
		}
		if !hasValue {
			return nil, colName, nil
		}
		if isFloat {
			return sumFloat + float64(sumInt), colName, nil
		}
		return sumInt, colName, nil
	case "AVG":
		colName := formatCallExpr(call)
		if len(call.Args) != 1 {
			return nil, "", fmt.Errorf("AVG expects 1 argument, got %d", len(call.Args))
		}
		if _, ok := call.Args[0].(*ast.StarExpr); ok {
			return nil, "", fmt.Errorf("AVG(*) is not supported")
		}
		ident, ok := call.Args[0].(*ast.IdentExpr)
		if !ok {
			return nil, "", fmt.Errorf("AVG expects column name, got %T", call.Args[0])
		}
		col, err := info.FindColumn(ident.Name)
		if err != nil {
			return nil, "", err
		}
		var sum float64
		var count int64
		for _, row := range rows {
			v := row[col.Index]
			if v == nil {
				continue
			}
			switch tv := v.(type) {
			case int64:
				sum += float64(tv)
			case float64:
				sum += tv
			default:
				return nil, "", fmt.Errorf("AVG requires numeric values, got %T", v)
			}
			count++
		}
		if count == 0 {
			return nil, colName, nil
		}
		return sum / float64(count), colName, nil
	case "MIN":
		colName := formatCallExpr(call)
		if len(call.Args) != 1 {
			return nil, "", fmt.Errorf("MIN expects 1 argument, got %d", len(call.Args))
		}
		if _, ok := call.Args[0].(*ast.StarExpr); ok {
			return nil, "", fmt.Errorf("MIN(*) is not supported")
		}
		ident, ok := call.Args[0].(*ast.IdentExpr)
		if !ok {
			return nil, "", fmt.Errorf("MIN expects column name, got %T", call.Args[0])
		}
		col, err := info.FindColumn(ident.Name)
		if err != nil {
			return nil, "", err
		}
		var minVal Value
		for _, row := range rows {
			v := row[col.Index]
			if v == nil {
				continue
			}
			if minVal == nil || compareValues(v, minVal) < 0 {
				minVal = v
			}
		}
		return minVal, colName, nil
	case "MAX":
		colName := formatCallExpr(call)
		if len(call.Args) != 1 {
			return nil, "", fmt.Errorf("MAX expects 1 argument, got %d", len(call.Args))
		}
		if _, ok := call.Args[0].(*ast.StarExpr); ok {
			return nil, "", fmt.Errorf("MAX(*) is not supported")
		}
		ident, ok := call.Args[0].(*ast.IdentExpr)
		if !ok {
			return nil, "", fmt.Errorf("MAX expects column name, got %T", call.Args[0])
		}
		col, err := info.FindColumn(ident.Name)
		if err != nil {
			return nil, "", err
		}
		var maxVal Value
		for _, row := range rows {
			v := row[col.Index]
			if v == nil {
				continue
			}
			if maxVal == nil || compareValues(v, maxVal) > 0 {
				maxVal = v
			}
		}
		return maxVal, colName, nil
	default:
		return nil, "", fmt.Errorf("unknown aggregate function: %s", call.Name)
	}
}

// aggAccumulator accumulates aggregate values for streaming GROUP BY optimization.
type aggAccumulator struct {
	kind   string // "GROUP_COL", "COUNT_STAR", "COUNT_COL", "COUNT_DISTINCT", "SUM", "AVG", "MIN", "MAX"
	colIdx int    // row column index (-1 for COUNT(*))

	count   int64
	sumInt  int64
	sumF    float64
	isFloat bool
	minVal  Value
	maxVal  Value
	seen    map[interface{}]bool // for COUNT(DISTINCT)
}

func (a *aggAccumulator) reset() {
	a.count = 0
	a.sumInt = 0
	a.sumF = 0
	a.isFloat = false
	a.minVal = nil
	a.maxVal = nil
	if a.seen != nil {
		a.seen = make(map[interface{}]bool)
	}
}

func (a *aggAccumulator) feed(row Row) {
	switch a.kind {
	case "GROUP_COL":
		// no accumulation needed
	case "COUNT_STAR":
		a.count++
	case "COUNT_COL":
		if row[a.colIdx] != nil {
			a.count++
		}
	case "COUNT_DISTINCT":
		v := row[a.colIdx]
		if v != nil && !a.seen[v] {
			a.seen[v] = true
			a.count++
		}
	case "SUM", "AVG":
		v := row[a.colIdx]
		if v == nil {
			return
		}
		a.count++
		switch tv := v.(type) {
		case int64:
			a.sumInt += tv
		case float64:
			a.isFloat = true
			a.sumF += tv
		}
	case "MIN":
		v := row[a.colIdx]
		if v == nil {
			return
		}
		if a.minVal == nil || compareValues(v, a.minVal) < 0 {
			a.minVal = v
		}
	case "MAX":
		v := row[a.colIdx]
		if v == nil {
			return
		}
		if a.maxVal == nil || compareValues(v, a.maxVal) > 0 {
			a.maxVal = v
		}
	}
}

func (a *aggAccumulator) result() Value {
	switch a.kind {
	case "COUNT_STAR", "COUNT_COL", "COUNT_DISTINCT":
		return a.count
	case "SUM":
		if a.count == 0 {
			return nil
		}
		if a.isFloat {
			return a.sumF + float64(a.sumInt)
		}
		return a.sumInt
	case "AVG":
		if a.count == 0 {
			return nil
		}
		total := a.sumF + float64(a.sumInt)
		return total / float64(a.count)
	case "MIN":
		return a.minVal
	case "MAX":
		return a.maxVal
	default:
		return nil
	}
}

// tryGroupByIndexOptimization attempts to perform GROUP BY using an index-ordered scan
// for streaming aggregation without a hash map. Returns (result, true) if applied.
func (e *Executor) tryGroupByIndexOptimization(stmt *ast.SelectStmt) (*Result, bool) {
	// Only single-column GROUP BY on IdentExpr
	if len(stmt.GroupBy) != 1 {
		return nil, false
	}
	gbIdent, ok := stmt.GroupBy[0].(*ast.IdentExpr)
	if !ok {
		return nil, false
	}

	// Guard conditions
	if len(stmt.Joins) > 0 || stmt.FromSubquery != nil || stmt.TableAlias != "" {
		return nil, false
	}
	if stmt.Having != nil || stmt.Distinct {
		return nil, false
	}
	if hasWindowFunction(stmt.Columns) {
		return nil, false
	}
	if containsSubquery(stmt.Where) {
		return nil, false
	}

	// All SELECT columns must be GROUP BY column (IdentExpr) or aggregate (CallExpr)
	// Build accumulators
	type accInfo struct {
		acc      *aggAccumulator
		dispName string
		colType  string
	}

	db, err := e.resolveDatabase(stmt.DatabaseName)
	if err != nil {
		return nil, false
	}
	info, err := db.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, false
	}

	gbCol, err := info.FindColumn(gbIdent.Name)
	if err != nil {
		return nil, false
	}
	gbColIdx := gbCol.Index

	// Check that GROUP BY column has PK or secondary index
	isPK := gbColIdx == info.PrimaryKeyCol
	var idxReader IndexReader
	if !isPK {
		idxReader = db.storage.LookupSingleColumnIndex(info.Name, gbColIdx)
		if idxReader == nil {
			return nil, false
		}
	}

	// Build accumulators for each SELECT column
	accs := make([]accInfo, len(stmt.Columns))
	for i, colExpr := range stmt.Columns {
		expr := colExpr
		alias := ""
		if ae, ok := expr.(*ast.AliasExpr); ok {
			alias = ae.Alias
			expr = ae.Expr
		}

		switch inner := expr.(type) {
		case *ast.IdentExpr:
			// Must be the GROUP BY column
			if strings.ToLower(inner.Name) != strings.ToLower(gbIdent.Name) {
				return nil, false
			}
			dispName := gbCol.Name
			if alias != "" {
				dispName = alias
			}
			accs[i] = accInfo{
				acc:      &aggAccumulator{kind: "GROUP_COL", colIdx: gbColIdx},
				dispName: dispName,
				colType:  gbCol.DataType,
			}
		case *ast.CallExpr:
			fn := strings.ToUpper(inner.Name)
			if isScalarFunc(fn) {
				return nil, false
			}
			dispName := formatCallExpr(inner)
			if alias != "" {
				dispName = alias
			}

			switch fn {
			case "COUNT":
				if inner.Distinct {
					if len(inner.Args) != 1 {
						return nil, false
					}
					ident, ok := inner.Args[0].(*ast.IdentExpr)
					if !ok {
						return nil, false
					}
					col, err := info.FindColumn(ident.Name)
					if err != nil {
						return nil, false
					}
					accs[i] = accInfo{
						acc:      &aggAccumulator{kind: "COUNT_DISTINCT", colIdx: col.Index, seen: make(map[interface{}]bool)},
						dispName: dispName,
						colType:  "INT",
					}
				} else if len(inner.Args) == 1 {
					switch inner.Args[0].(type) {
					case *ast.StarExpr, *ast.IntLitExpr, *ast.StringLitExpr:
						accs[i] = accInfo{
							acc:      &aggAccumulator{kind: "COUNT_STAR", colIdx: -1},
							dispName: dispName,
							colType:  "INT",
						}
					default:
						ident, ok := inner.Args[0].(*ast.IdentExpr)
						if !ok {
							return nil, false
						}
						col, err := info.FindColumn(ident.Name)
						if err != nil {
							return nil, false
						}
						accs[i] = accInfo{
							acc:      &aggAccumulator{kind: "COUNT_COL", colIdx: col.Index},
							dispName: dispName,
							colType:  "INT",
						}
					}
				} else {
					return nil, false
				}
			case "SUM":
				if len(inner.Args) != 1 {
					return nil, false
				}
				ident, ok := inner.Args[0].(*ast.IdentExpr)
				if !ok {
					return nil, false
				}
				col, err := info.FindColumn(ident.Name)
				if err != nil {
					return nil, false
				}
				accs[i] = accInfo{
					acc:      &aggAccumulator{kind: "SUM", colIdx: col.Index},
					dispName: dispName,
					colType:  col.DataType,
				}
			case "AVG":
				if len(inner.Args) != 1 {
					return nil, false
				}
				ident, ok := inner.Args[0].(*ast.IdentExpr)
				if !ok {
					return nil, false
				}
				col, err := info.FindColumn(ident.Name)
				if err != nil {
					return nil, false
				}
				accs[i] = accInfo{
					acc:      &aggAccumulator{kind: "AVG", colIdx: col.Index},
					dispName: dispName,
					colType:  col.DataType,
				}
			case "MIN":
				if len(inner.Args) != 1 {
					return nil, false
				}
				ident, ok := inner.Args[0].(*ast.IdentExpr)
				if !ok {
					return nil, false
				}
				col, err := info.FindColumn(ident.Name)
				if err != nil {
					return nil, false
				}
				accs[i] = accInfo{
					acc:      &aggAccumulator{kind: "MIN", colIdx: col.Index},
					dispName: dispName,
					colType:  col.DataType,
				}
			case "MAX":
				if len(inner.Args) != 1 {
					return nil, false
				}
				ident, ok := inner.Args[0].(*ast.IdentExpr)
				if !ok {
					return nil, false
				}
				col, err := info.FindColumn(ident.Name)
				if err != nil {
					return nil, false
				}
				accs[i] = accInfo{
					acc:      &aggAccumulator{kind: "MAX", colIdx: col.Index},
					dispName: dispName,
					colType:  col.DataType,
				}
			default:
				return nil, false
			}
		default:
			return nil, false
		}
	}

	// Build evaluator for WHERE filter
	var eval *tableEvaluator
	if stmt.Where != nil {
		eval = newTableEvaluator(e, info)
	}

	// Streaming aggregation
	var resultRows []Row
	var prevGroupVal Value
	initialized := false

	emitGroup := func() {
		row := make(Row, len(accs))
		for i, ai := range accs {
			if ai.acc.kind == "GROUP_COL" {
				row[i] = prevGroupVal
			} else {
				row[i] = ai.acc.result()
			}
		}
		resultRows = append(resultRows, row)
	}

	resetAccs := func() {
		for _, ai := range accs {
			ai.acc.reset()
		}
	}

	processRow := func(row Row) {
		// WHERE filter
		if eval != nil {
			match, err := eval.Eval(stmt.Where, row)
			if err != nil {
				return
			}
			b, ok := match.(bool)
			if !ok || !b {
				return
			}
		}

		currentVal := row[gbColIdx]
		if initialized && currentVal != prevGroupVal {
			emitGroup()
			resetAccs()
		}

		for _, ai := range accs {
			ai.acc.feed(row)
		}
		prevGroupVal = currentVal
		initialized = true
	}

	if isPK {
		db.storage.ForEachRow(info.Name, false, func(key int64, row Row) bool {
			processRow(row)
			return true
		}, 0)
	} else {
		idxReader.OrderedRangeScan(nil, false, nil, false, false, func(rowKey int64) bool {
			row, ok := db.storage.GetRow(info.Name, rowKey)
			if !ok {
				return true
			}
			processRow(row)
			return true
		})
	}

	// Emit the last group
	if initialized {
		emitGroup()
	}

	// Build column names and types
	colNames := make([]string, len(accs))
	colTypes := make([]string, len(accs))
	for i, ai := range accs {
		colNames[i] = ai.dispName
		colTypes[i] = ai.colType
	}

	return &Result{
		Columns:     colNames,
		ColumnTypes: colTypes,
		Rows:        resultRows,
	}, true
}

// tryCountStarOptimization attempts to satisfy SELECT COUNT(*) / COUNT(literal) queries
// using Storage.RowCount() (O(1)) instead of a full table scan (O(N)).
// Returns (result, true) if optimization was applied, (nil, false) otherwise.
func (e *Executor) tryCountStarOptimization(stmt *ast.SelectStmt) (*Result, bool) {
	// Only optimize simple single-table queries without GROUP BY, JOIN, WHERE, subquery, alias
	if len(stmt.GroupBy) > 0 || len(stmt.Joins) > 0 || stmt.FromSubquery != nil ||
		stmt.TableAlias != "" || stmt.Where != nil || stmt.Having != nil || stmt.Distinct {
		return nil, false
	}

	// All columns must be COUNT(*) or COUNT(literal) CallExprs
	type countCol struct {
		dispName string // display name e.g. "COUNT(*)"
	}
	var cols []countCol
	for _, colExpr := range stmt.Columns {
		expr := colExpr
		if ae, ok := expr.(*ast.AliasExpr); ok {
			expr = ae.Expr
		}
		call, ok := expr.(*ast.CallExpr)
		if !ok {
			return nil, false
		}
		if strings.ToUpper(call.Name) != "COUNT" {
			return nil, false
		}
		if call.Distinct {
			return nil, false
		}
		if len(call.Args) != 1 {
			return nil, false
		}
		// Only COUNT(*) and COUNT(literal) are eligible
		switch call.Args[0].(type) {
		case *ast.StarExpr:
			// COUNT(*) — OK
		case *ast.IntLitExpr:
			// COUNT(1) — OK
		case *ast.StringLitExpr:
			// COUNT('x') — OK
		default:
			// COUNT(col) needs non-NULL check — not eligible
			return nil, false
		}
		cols = append(cols, countCol{dispName: formatCallExpr(call)})
	}
	if len(cols) == 0 {
		return nil, false
	}

	db, err := e.resolveDatabase(stmt.DatabaseName)
	if err != nil {
		return nil, false
	}

	count, err := db.storage.RowCount(stmt.TableName)
	if err != nil {
		return nil, false
	}

	colNames := make([]string, len(cols))
	colTypes := make([]string, len(cols))
	resultRow := make(Row, len(cols))

	for i, cc := range cols {
		if ae, ok := stmt.Columns[i].(*ast.AliasExpr); ok {
			colNames[i] = ae.Alias
		} else {
			colNames[i] = cc.dispName
		}
		colTypes[i] = "INT"
		resultRow[i] = int64(count)
	}

	return &Result{
		Columns:     colNames,
		ColumnTypes: colTypes,
		Rows:        []Row{resultRow},
	}, true
}

// tryMinMaxIndexOptimization attempts to satisfy SELECT MIN(col)/MAX(col) queries
// using an index scan (O(log N)) instead of a full table scan (O(N)).
// Returns (result, true) if optimization was applied, (nil, false) otherwise.
func (e *Executor) tryMinMaxIndexOptimization(stmt *ast.SelectStmt) (*Result, bool) {
	// Only optimize simple single-table queries without GROUP BY, JOIN, WHERE, subquery, alias
	if len(stmt.GroupBy) > 0 || len(stmt.Joins) > 0 || stmt.FromSubquery != nil ||
		stmt.TableAlias != "" || stmt.Where != nil || stmt.Having != nil || stmt.Distinct {
		return nil, false
	}

	// All columns must be MIN(col) or MAX(col) CallExprs
	type minMaxCol struct {
		funcName string // "MIN" or "MAX"
		colName  string // column name
		dispName string // display name e.g. "MIN(val)"
	}
	var cols []minMaxCol
	for _, colExpr := range stmt.Columns {
		expr := colExpr
		if ae, ok := expr.(*ast.AliasExpr); ok {
			expr = ae.Expr
		}
		call, ok := expr.(*ast.CallExpr)
		if !ok {
			return nil, false
		}
		fn := strings.ToUpper(call.Name)
		if fn != "MIN" && fn != "MAX" {
			return nil, false
		}
		if len(call.Args) != 1 {
			return nil, false
		}
		ident, ok := call.Args[0].(*ast.IdentExpr)
		if !ok {
			return nil, false
		}
		cols = append(cols, minMaxCol{
			funcName: fn,
			colName:  ident.Name,
			dispName: formatCallExpr(call),
		})
	}
	if len(cols) == 0 {
		return nil, false
	}

	db, err := e.resolveDatabase(stmt.DatabaseName)
	if err != nil {
		return nil, false
	}
	info, err := db.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, false
	}

	// Check all columns have an index or are PK
	for _, mc := range cols {
		col, err := info.FindColumn(mc.colName)
		if err != nil {
			return nil, false
		}
		isPK := col.Index == info.PrimaryKeyCol
		if !isPK {
			idx := db.storage.LookupSingleColumnIndex(info.Name, col.Index)
			if idx == nil {
				return nil, false
			}
		}
	}

	// All columns are index-backed; execute the optimization
	colNames := make([]string, len(cols))
	colTypes := make([]string, len(cols))
	resultRow := make(Row, len(cols))

	for i, mc := range cols {
		// Use alias if present
		if ae, ok := stmt.Columns[i].(*ast.AliasExpr); ok {
			colNames[i] = ae.Alias
		} else {
			colNames[i] = mc.dispName
		}

		col, _ := info.FindColumn(mc.colName)
		colTypes[i] = col.DataType
		reverse := mc.funcName == "MAX"
		isPK := col.Index == info.PrimaryKeyCol

		if isPK {
			// PK column: use ScanOrdered with limit=1
			rows, err := db.storage.ScanOrdered(info.Name, reverse, 1)
			if err != nil || len(rows) == 0 {
				resultRow[i] = nil
				continue
			}
			resultRow[i] = rows[0][col.Index]
		} else {
			// Secondary index: use OrderedRangeScan
			idx := db.storage.LookupSingleColumnIndex(info.Name, col.Index)
			var val Value
			found := false
			idx.OrderedRangeScan(nil, false, nil, false, reverse, func(rowKey int64) bool {
				row, ok := db.storage.GetRow(info.Name, rowKey)
				if !ok {
					return true // skip missing rows
				}
				v := row[col.Index]
				if v == nil {
					return true // skip NULLs
				}
				val = v
				found = true
				return false // stop after first non-NULL
			})
			if found {
				resultRow[i] = val
			} else {
				resultRow[i] = nil
			}
		}
	}

	return &Result{
		Columns:     colNames,
		ColumnTypes: colTypes,
		Rows:        []Row{resultRow},
	}, true
}

// formatCallExpr returns a display name for a function call (e.g. "COUNT(*)").
func formatCallExpr(call *ast.CallExpr) string {
	args := make([]string, len(call.Args))
	for i, arg := range call.Args {
		switch a := arg.(type) {
		case *ast.StarExpr:
			args[i] = "*"
		case *ast.IdentExpr:
			if a.Table != "" {
				args[i] = a.Table + "." + a.Name
			} else {
				args[i] = a.Name
			}
		case *ast.IntLitExpr:
			args[i] = fmt.Sprintf("%d", a.Value)
		case *ast.FloatLitExpr:
			args[i] = fmt.Sprintf("%g", a.Value)
		case *ast.StringLitExpr:
			args[i] = "'" + a.Value + "'"
		default:
			args[i] = "?"
		}
	}
	if call.Distinct {
		return call.Name + "(DISTINCT " + strings.Join(args, ", ") + ")"
	}
	return call.Name + "(" + strings.Join(args, ", ") + ")"
}

// isScalarFunc returns true if the function name is a scalar (non-aggregate) function.
func isScalarFunc(name string) bool {
	switch name {
	case "COALESCE", "NULLIF", "ABS", "ROUND", "MOD", "CEIL", "FLOOR", "POWER", "LENGTH", "UPPER", "LOWER", "SUBSTRING", "TRIM", "CONCAT":
		return true
	}
	return false
}

// hasAggregate returns true if any column expression is an aggregate function call.
// WindowExpr containing aggregate functions are not treated as normal aggregates.
func hasAggregate(columns []ast.Expr) bool {
	for _, col := range columns {
		inner := col
		if a, ok := col.(*ast.AliasExpr); ok {
			inner = a.Expr
		}
		// WindowExpr (including aggregate window functions) is not a normal aggregate
		if _, ok := inner.(*ast.WindowExpr); ok {
			continue
		}
		if call, ok := inner.(*ast.CallExpr); ok {
			if !isScalarFunc(call.Name) {
				return true
			}
		}
	}
	return false
}

// dedup removes duplicate rows, preserving order of first occurrence.
func dedup(rows []Row) []Row {
	seen := make(map[string]bool)
	result := make([]Row, 0, len(rows))
	for _, row := range rows {
		key := string(encodeValues(row))
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}
	return result
}

// columnsContainSubquery returns true if any SELECT column contains a subquery.
func columnsContainSubquery(columns []ast.Expr) bool {
	for _, col := range columns {
		inner := col
		if a, ok := col.(*ast.AliasExpr); ok {
			inner = a.Expr
		}
		if containsSubquery(inner) {
			return true
		}
	}
	return false
}

// executeDistinctLimitStreaming uses ScanEach to stream rows inline under the
// table lock, applying WHERE → projection → dedup → early exit in one pass.
// This avoids materializing all rows and allows the disk backend to stop
// decoding pages once enough unique rows have been collected.
func (e *Executor) executeScanEachStreaming(
	stmt *ast.SelectStmt, db *Database, info *TableInfo, earlyLimit int, distinct bool,
) (*Result, error) {
	eval := newTableEvaluator(e, info)
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
	eval := newTableEvaluator(e, info)
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
	eval := newTableEvaluator(e, info)
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
	cir, isCovering := params.index.(CoveringIndexReader)
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

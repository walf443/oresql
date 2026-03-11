package engine

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/json_path"
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
	plan := e.planSelect(stmt)
	return e.executeSelectWithPlan(stmt, plan)
}

// executeSelectWithPlan executes a SELECT statement according to the given plan.
func (e *Executor) executeSelectWithPlan(stmt *ast.SelectStmt, plan *SelectPlan) (*Result, error) {
	switch plan.Type {
	case PlanNoTable:
		return e.executeSelectWithoutTable(stmt)

	case PlanIndexOrderScan:
		return e.executeSelectWithIndexOrder(stmt, plan.db, plan.info, plan.IndexOrder)

	case PlanGroupByIndex:
		return e.executeGroupByIndex(stmt, plan.db, plan.info)

	case PlanCountStar:
		return e.executeCountStar(stmt, plan.db)

	case PlanMinMax:
		return e.executeMinMax(stmt, plan.db, plan.info)

	case PlanStreamingIndex:
		earlyLimit := computeEarlyLimit(stmt)
		return e.executeIndexScanStreaming(stmt, plan.db, plan.info, plan.streamingParams, earlyLimit, stmt.Distinct)

	case PlanStreamingBatch:
		earlyLimit := computeEarlyLimit(stmt)
		return e.executeForEachByKeysStreaming(stmt, plan.db, plan.info, plan.batchKeys, earlyLimit, stmt.Distinct)

	case PlanStreamingFullScan:
		earlyLimit := computeEarlyLimit(stmt)
		return e.executeScanEachStreaming(stmt, plan.db, plan.info, earlyLimit, stmt.Distinct)
	}

	// PlanSubquery, PlanBatchIndex, PlanFullScan — general batch path
	canEarlyLimit := stmt.Limit != nil &&
		len(stmt.OrderBy) == 0 &&
		len(stmt.GroupBy) == 0 &&
		!hasAggregate(stmt.Columns) &&
		!hasWindowFunction(stmt.Columns)

	var earlyLimit int
	if canEarlyLimit {
		earlyLimit = computeEarlyLimit(stmt)
	}

	// Phase 1: Source rows + evaluator
	// For DISTINCT, don't pass earlyLimit to scanSource (JOIN needs all rows for dedup)
	scanLimit := earlyLimit
	if stmt.Distinct {
		scanLimit = 0
	}
	var rows []Row
	var eval ExprEvaluator
	var err error
	// Use pre-computed batch keys from the plan for single-table queries only.
	// JOIN, subquery, alias, and CTE paths must go through scanSource.
	if plan.Type == PlanBatchIndex && plan.batchKeys != nil && plan.db != nil && plan.info != nil &&
		len(stmt.Joins) == 0 && stmt.FromSubquery == nil && stmt.TableAlias == "" {
		rows, err = plan.db.storage.GetByKeys(plan.info.Name, plan.batchKeys)
		if err != nil {
			return nil, err
		}
		eval = newTableEvaluator(e, plan.info)
	} else {
		rows, eval, err = e.scanSource(stmt, scanLimit)
		if err != nil {
			return nil, err
		}
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
	if stmt.FromSubquery != nil || stmt.JSONTable != nil {
		return len(stmt.Joins) > 0
	}
	return len(stmt.Joins) > 0 || stmt.TableAlias != ""
}

// scanSource returns the source rows and an appropriate evaluator for the query.
// earlyLimit > 0 enables early termination for the JOIN path.
func (e *Executor) scanSource(stmt *ast.SelectStmt, earlyLimit int) ([]Row, ExprEvaluator, error) {
	if stmt.JSONTable != nil {
		return e.scanSourceJSONTable(stmt)
	}
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

// scanSourceJSONTable handles FROM JSON_TABLE(...).
func (e *Executor) scanSourceJSONTable(stmt *ast.SelectStmt) ([]Row, ExprEvaluator, error) {
	info, rows, err := e.materializeJSONTable(stmt.JSONTable)
	if err != nil {
		return nil, nil, err
	}
	return rows, newTableEvaluator(e, info), nil
}

// materializeJSONTable evaluates a JSON_TABLE source and returns a virtual table.
func (e *Executor) materializeJSONTable(jt *ast.JSONTableSource) (*TableInfo, []Row, error) {
	// Evaluate the JSON expression (must be a literal in this context)
	jsonVal, err := evalLiteral(jt.JSONExpr)
	if err != nil {
		return nil, nil, fmt.Errorf("JSON_TABLE: %w", err)
	}
	jsonStr, ok := jsonVal.(string)
	if !ok {
		return nil, nil, fmt.Errorf("JSON_TABLE: first argument must be a JSON string, got %T", jsonVal)
	}

	// Parse the JSON
	var raw any
	if err := json.Unmarshal([]byte(jsonStr), &raw); err != nil {
		return nil, nil, fmt.Errorf("JSON_TABLE: invalid JSON: %w", err)
	}

	// Parse the row path and determine the items to iterate.
	// Handle $[*] wildcard: navigate to parent, then expand array elements.
	var items []any
	rowPath := jt.RowPath
	wildcard := false
	if strings.HasSuffix(rowPath, "[*]") {
		wildcard = true
		rowPath = rowPath[:len(rowPath)-3] // strip [*]
		if rowPath == "$" || rowPath == "" {
			rowPath = "$"
		}
	}

	rowPathExpr, err := json_path.Parse(rowPath)
	if err != nil {
		return nil, nil, fmt.Errorf("JSON_TABLE: invalid row path: %w", err)
	}
	target := rowPathExpr.Execute(raw)

	if wildcard {
		arr, ok := target.([]any)
		if !ok {
			// Not an array with wildcard - return empty
			items = nil
		} else {
			items = arr
		}
	} else {
		switch v := target.(type) {
		case []any:
			items = v
		case nil:
			items = nil
		default:
			// Single value (e.g. root path "$" pointing to an object) - wrap in array
			items = []any{v}
		}
	}

	// Pre-compile column paths
	colPaths := make([]*json_path.Path, len(jt.Columns))
	for i, col := range jt.Columns {
		p, err := json_path.Parse(col.Path)
		if err != nil {
			return nil, nil, fmt.Errorf("JSON_TABLE: invalid column path %q: %w", col.Path, err)
		}
		colPaths[i] = p
	}

	// Build TableInfo
	cols := make([]ColumnInfo, len(jt.Columns))
	for i, col := range jt.Columns {
		cols[i] = ColumnInfo{
			Name:     strings.ToLower(col.Name),
			DataType: col.DataType,
			Index:    i,
		}
	}
	info := &TableInfo{
		Name:          strings.ToLower(jt.Alias),
		Columns:       cols,
		PrimaryKeyCol: -1,
	}

	// Build rows
	var rows []Row
	for _, item := range items {
		row := make(Row, len(jt.Columns))
		for i, col := range jt.Columns {
			val := colPaths[i].Execute(item)
			if val != nil {
				coerced, err := coerceJSONValue(val, col.DataType)
				if err != nil {
					return nil, nil, fmt.Errorf("JSON_TABLE column %q: %w", col.Name, err)
				}
				row[i] = coerced
			}
			// nil stays nil (NULL)
		}
		rows = append(rows, row)
	}

	return info, rows, nil
}

// coerceJSONValue converts a JSON-decoded value to the expected column type.
func coerceJSONValue(val any, dataType string) (Value, error) {
	switch dataType {
	case "INT":
		switch v := val.(type) {
		case float64:
			return int64(v), nil
		case string:
			return nil, fmt.Errorf("cannot convert string %q to INT", v)
		default:
			return nil, fmt.Errorf("cannot convert %T to INT", val)
		}
	case "FLOAT":
		switch v := val.(type) {
		case float64:
			return v, nil
		case string:
			return nil, fmt.Errorf("cannot convert string %q to FLOAT", v)
		default:
			return nil, fmt.Errorf("cannot convert %T to FLOAT", val)
		}
	case "TEXT":
		switch v := val.(type) {
		case string:
			return v, nil
		case float64:
			if v == float64(int64(v)) {
				return fmt.Sprintf("%d", int64(v)), nil
			}
			return fmt.Sprintf("%g", v), nil
		case bool:
			if v {
				return "true", nil
			}
			return "false", nil
		default:
			return fmt.Sprintf("%v", val), nil
		}
	case "JSON":
		b, err := json.Marshal(val)
		if err != nil {
			return nil, err
		}
		return string(b), nil
	default:
		return val, nil
	}
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
		row = append(row, finalizeValue(val))
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

// executeGroupByIndex executes GROUP BY using an index-ordered scan for streaming aggregation.
// Caller must ensure the GROUP BY index optimization guards have passed.
func (e *Executor) executeGroupByIndex(stmt *ast.SelectStmt, db *Database, info *TableInfo) (*Result, error) {
	gbIdent := stmt.GroupBy[0].(*ast.IdentExpr)
	gbCol, _ := info.FindColumn(gbIdent.Name)
	gbColIdx := gbCol.Index

	isPK := gbColIdx == info.PrimaryKeyCol
	var idxReader IndexReader
	if !isPK {
		idxReader = db.storage.LookupSingleColumnIndex(info.Name, gbColIdx)
	}

	// Build accumulators for each SELECT column
	type accInfo struct {
		acc      *aggAccumulator
		dispName string
		colType  string
	}
	accs, err := buildGroupByAccumulators(stmt, gbIdent, gbCol, info)
	if err != nil {
		return nil, err
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
	}, nil
}

// groupByAccInfo holds accumulator info for GROUP BY index optimization.
type groupByAccInfo struct {
	acc      *aggAccumulator
	dispName string
	colType  string
}

// buildGroupByAccumulators builds accumulators for each SELECT column in GROUP BY.
func buildGroupByAccumulators(stmt *ast.SelectStmt, gbIdent *ast.IdentExpr, gbCol *ColumnInfo, info *TableInfo) ([]groupByAccInfo, error) {
	accs := make([]groupByAccInfo, len(stmt.Columns))
	for i, colExpr := range stmt.Columns {
		expr := colExpr
		alias := ""
		if ae, ok := expr.(*ast.AliasExpr); ok {
			alias = ae.Alias
			expr = ae.Expr
		}

		switch inner := expr.(type) {
		case *ast.IdentExpr:
			if strings.ToLower(inner.Name) != strings.ToLower(gbIdent.Name) {
				return nil, fmt.Errorf("non-aggregate column %q not in GROUP BY", inner.Name)
			}
			dispName := gbCol.Name
			if alias != "" {
				dispName = alias
			}
			accs[i] = groupByAccInfo{
				acc:      &aggAccumulator{kind: "GROUP_COL", colIdx: gbCol.Index},
				dispName: dispName,
				colType:  gbCol.DataType,
			}
		case *ast.CallExpr:
			fn := strings.ToUpper(inner.Name)
			if isScalarFunc(fn) {
				return nil, fmt.Errorf("scalar function %q not supported in GROUP BY index optimization", fn)
			}
			dispName := formatCallExpr(inner)
			if alias != "" {
				dispName = alias
			}

			acc, colType, err := buildAggAccumulator(fn, inner, info)
			if err != nil {
				return nil, err
			}
			accs[i] = groupByAccInfo{
				acc:      acc,
				dispName: dispName,
				colType:  colType,
			}
		default:
			return nil, fmt.Errorf("unsupported expression type in GROUP BY index optimization")
		}
	}
	return accs, nil
}

// buildAggAccumulator builds a single aggregate accumulator from a CallExpr.
func buildAggAccumulator(fn string, call *ast.CallExpr, info *TableInfo) (*aggAccumulator, string, error) {
	switch fn {
	case "COUNT":
		if call.Distinct {
			if len(call.Args) != 1 {
				return nil, "", fmt.Errorf("COUNT(DISTINCT) requires exactly 1 argument")
			}
			ident, ok := call.Args[0].(*ast.IdentExpr)
			if !ok {
				return nil, "", fmt.Errorf("COUNT(DISTINCT) argument must be a column")
			}
			col, err := info.FindColumn(ident.Name)
			if err != nil {
				return nil, "", err
			}
			return &aggAccumulator{kind: "COUNT_DISTINCT", colIdx: col.Index, seen: make(map[interface{}]bool)}, "INT", nil
		}
		if len(call.Args) == 1 {
			switch call.Args[0].(type) {
			case *ast.StarExpr, *ast.IntLitExpr, *ast.StringLitExpr:
				return &aggAccumulator{kind: "COUNT_STAR", colIdx: -1}, "INT", nil
			default:
				ident, ok := call.Args[0].(*ast.IdentExpr)
				if !ok {
					return nil, "", fmt.Errorf("COUNT argument must be a column or *")
				}
				col, err := info.FindColumn(ident.Name)
				if err != nil {
					return nil, "", err
				}
				return &aggAccumulator{kind: "COUNT_COL", colIdx: col.Index}, "INT", nil
			}
		}

		return nil, "", fmt.Errorf("COUNT requires exactly 1 argument")
	case "SUM", "AVG", "MIN", "MAX":
		if len(call.Args) != 1 {
			return nil, "", fmt.Errorf("%s requires exactly 1 argument", fn)
		}
		ident, ok := call.Args[0].(*ast.IdentExpr)
		if !ok {
			return nil, "", fmt.Errorf("%s argument must be a column", fn)
		}
		col, err := info.FindColumn(ident.Name)
		if err != nil {
			return nil, "", err
		}
		return &aggAccumulator{kind: fn, colIdx: col.Index}, col.DataType, nil
	default:
		return nil, "", fmt.Errorf("unsupported aggregate function %q", fn)
	}
}

// executeCountStar executes a COUNT(*) query using RowCount().
// Caller must ensure isCountStarOptimizable(stmt) is true.
func (e *Executor) executeCountStar(stmt *ast.SelectStmt, db *Database) (*Result, error) {
	count, err := db.storage.RowCount(stmt.TableName)
	if err != nil {
		return nil, err
	}

	colNames := make([]string, len(stmt.Columns))
	colTypes := make([]string, len(stmt.Columns))
	resultRow := make(Row, len(stmt.Columns))

	for i, colExpr := range stmt.Columns {
		expr := colExpr
		if ae, ok := expr.(*ast.AliasExpr); ok {
			colNames[i] = ae.Alias
			expr = ae.Expr
		} else {
			call := expr.(*ast.CallExpr)
			colNames[i] = formatCallExpr(call)
		}
		colTypes[i] = "INT"
		resultRow[i] = int64(count)
	}

	return &Result{
		Columns:     colNames,
		ColumnTypes: colTypes,
		Rows:        []Row{resultRow},
	}, nil
}

// executeMinMax executes a MIN/MAX query using index edge lookup.
// Caller must ensure isMinMaxOptimizable(stmt, info) is true.
func (e *Executor) executeMinMax(stmt *ast.SelectStmt, db *Database, info *TableInfo) (*Result, error) {
	type minMaxCol struct {
		funcName string
		colName  string
		dispName string
	}
	var cols []minMaxCol
	for _, colExpr := range stmt.Columns {
		expr := colExpr
		if ae, ok := expr.(*ast.AliasExpr); ok {
			expr = ae.Expr
		}
		call := expr.(*ast.CallExpr)
		ident := call.Args[0].(*ast.IdentExpr)
		cols = append(cols, minMaxCol{
			funcName: strings.ToUpper(call.Name),
			colName:  ident.Name,
			dispName: formatCallExpr(call),
		})
	}

	colNames := make([]string, len(cols))
	colTypes := make([]string, len(cols))
	resultRow := make(Row, len(cols))

	for i, mc := range cols {
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
			rows, err := db.storage.ScanOrdered(info.Name, reverse, 1)
			if err != nil || len(rows) == 0 {
				resultRow[i] = nil
				continue
			}
			resultRow[i] = rows[0][col.Index]
		} else {
			idx := db.storage.LookupSingleColumnIndex(info.Name, col.Index)
			var val Value
			found := false
			idx.OrderedRangeScan(nil, false, nil, false, reverse, func(rowKey int64) bool {
				row, ok := db.storage.GetRow(info.Name, rowKey)
				if !ok {
					return true
				}
				v := row[col.Index]
				if v == nil {
					return true
				}
				val = v
				found = true
				return false
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
	}, nil
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
	case "COALESCE", "NULLIF", "ABS", "ROUND", "MOD", "CEIL", "FLOOR", "POWER", "LENGTH", "UPPER", "LOWER", "SUBSTRING", "TRIM", "CONCAT", "JSON_OBJECT", "JSON_ARRAY", "JSON_VALUE", "JSON_QUERY", "JSON_EXISTS":
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

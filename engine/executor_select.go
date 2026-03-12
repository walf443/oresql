package engine

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/engine/eval"
	"github.com/walf443/oresql/engine/scalar"
	"github.com/walf443/oresql/json_path"
)

func (e *Executor) executeSelect(stmt *ast.SelectStmt) (*Result, error) {
	plan := e.planSelect(stmt)
	return e.executeSelectWithPlan(stmt, plan)
}

// executeSelectWithPlan executes a SELECT statement according to the given plan.
func (e *Executor) executeSelectWithPlan(stmt *ast.SelectStmt, plan *SelectPlan) (*Result, error) {
	// Dispatch specialized plan types
	if result, ok, err := e.dispatchSpecialPlan(stmt, plan); ok || err != nil {
		return result, err
	}

	// General batch path (PlanSubquery, PlanBatchIndex, PlanFullScan)
	canEarlyLimit := canBatchEarlyLimit(stmt)
	var earlyLimit int
	if canEarlyLimit {
		earlyLimit = computeEarlyLimit(stmt)
	}

	// Phase 1: Source rows + evaluator
	rows, eval, err := e.fetchSourceRows(stmt, plan, earlyLimit)
	if err != nil {
		return nil, err
	}

	colTypes := resolveColumnTypes(stmt.Columns, eval)

	// Fast path: DISTINCT + LIMIT
	if canEarlyLimit && stmt.Distinct {
		return e.executeDistinctLimitFastPath(stmt, rows, eval, colTypes, earlyLimit)
	}

	// Phases 2-8: standard pipeline
	return e.executeBatchPipeline(stmt, rows, eval, colTypes, canEarlyLimit, earlyLimit)
}

// dispatchSpecialPlan handles specialized plan types that don't need the general pipeline.
// Returns (result, true, nil) on success, (nil, false, nil) if not applicable, or (nil, false, err) on error.
func (e *Executor) dispatchSpecialPlan(stmt *ast.SelectStmt, plan *SelectPlan) (*Result, bool, error) {
	switch plan.Type {
	case PlanNoTable:
		r, err := e.executeSelectWithoutTable(stmt)
		return r, true, err
	case PlanIndexOrderScan:
		r, err := e.executeSelectWithIndexOrder(stmt, plan.db, plan.info, plan.IndexOrder)
		return r, true, err
	case PlanGroupByIndex:
		r, err := e.executeGroupByIndex(stmt, plan.db, plan.info)
		return r, true, err
	case PlanCountStar:
		r, err := e.executeCountStar(stmt, plan.db)
		return r, true, err
	case PlanMinMax:
		r, err := e.executeMinMax(stmt, plan.db, plan.info)
		return r, true, err
	case PlanStreamingIndex:
		earlyLimit := computeEarlyLimit(stmt)
		r, err := e.executeIndexScanStreaming(stmt, plan.db, plan.info, plan.streamingParams, earlyLimit, stmt.Distinct)
		return r, true, err
	case PlanStreamingBatch:
		earlyLimit := computeEarlyLimit(stmt)
		r, err := e.executeForEachByKeysStreaming(stmt, plan.db, plan.info, plan.batchKeys, earlyLimit, stmt.Distinct)
		return r, true, err
	case PlanStreamingFullScan:
		earlyLimit := computeEarlyLimit(stmt)
		r, err := e.executeScanEachStreaming(stmt, plan.db, plan.info, earlyLimit, stmt.Distinct)
		return r, true, err
	}
	return nil, false, nil
}

// canBatchEarlyLimit returns true if the query shape allows early LIMIT in batch path.
func canBatchEarlyLimit(stmt *ast.SelectStmt) bool {
	return stmt.Limit != nil &&
		len(stmt.OrderBy) == 0 &&
		len(stmt.GroupBy) == 0 &&
		!hasAggregate(stmt.Columns) &&
		!hasWindowFunction(stmt.Columns)
}

// fetchSourceRows retrieves source rows, using pre-computed batch keys when possible.
func (e *Executor) fetchSourceRows(stmt *ast.SelectStmt, plan *SelectPlan, earlyLimit int) ([]Row, ExprEvaluator, error) {
	scanLimit := earlyLimit
	if stmt.Distinct {
		scanLimit = 0
	}

	// Use pre-computed batch keys for single-table queries
	if plan.Type == PlanBatchIndex && plan.batchKeys != nil && plan.db != nil && plan.info != nil &&
		len(stmt.Joins) == 0 && stmt.FromSubquery == nil && stmt.TableAlias == "" {
		rows, err := plan.db.storage.GetByKeys(plan.info.Name, plan.batchKeys)
		if err != nil {
			return nil, nil, err
		}
		return rows, newTableEvaluator(makeSubqueryRunner(e), plan.info), nil
	}

	return e.scanSource(stmt, scanLimit)
}

// executeDistinctLimitFastPath handles DISTINCT + LIMIT by combining WHERE, projection,
// dedup, and early termination in one pass.
func (e *Executor) executeDistinctLimitFastPath(
	stmt *ast.SelectStmt, rows []Row, eval ExprEvaluator, colTypes []string, earlyLimit int,
) (*Result, error) {
	colNames, colExprs, isStar, err := resolveSelectColumns(stmt.Columns, eval)
	if err != nil {
		return nil, err
	}
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

// executeBatchPipeline runs the standard SELECT pipeline: WHERE, window, GROUP BY,
// ORDER BY, projection, DISTINCT, OFFSET, LIMIT.
func (e *Executor) executeBatchPipeline(
	stmt *ast.SelectStmt, rows []Row, eval ExprEvaluator, colTypes []string,
	canEarlyLimit bool, earlyLimit int,
) (*Result, error) {
	var err error

	// Phase 2: WHERE filter (JOIN path handles WHERE internally)
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
	colNames, colExprs, isStar, needGroupBy, projected, err := e.resolveGroupByOrColumns(stmt, rows, eval)
	if err != nil {
		return nil, err
	}
	if needGroupBy {
		rows, colNames, eval, err = e.applyGroupBy(stmt, rows, eval)
		if err != nil {
			return nil, err
		}
	}

	// Phase 4: ORDER BY
	rows, err = e.applySortRows(stmt, rows, eval)
	if err != nil {
		return nil, err
	}

	// Phase 5: Projection
	if !projected {
		rows, err = projectRows(rows, colExprs, isStar, eval)
		if err != nil {
			return nil, err
		}
	}

	// Phase 6-8: DISTINCT, OFFSET, LIMIT
	if stmt.Distinct {
		rows = dedup(rows)
	}
	rows = applyOffset(rows, stmt.Offset)
	rows = applyLimit(rows, stmt.Limit)

	return &Result{Columns: colNames, ColumnTypes: colTypes, Rows: rows}, nil
}

// resolveGroupByOrColumns resolves column names/exprs and determines if GROUP BY or
// pkOnly projection applies. Returns needGroupBy=true when applyGroupBy is needed,
// and projected=true when projection is already handled (GROUP BY or pkOnly).
func (e *Executor) resolveGroupByOrColumns(
	stmt *ast.SelectStmt, rows []Row, eval ExprEvaluator,
) (colNames []string, colExprs []ast.Expr, isStar bool, needGroupBy bool, projected bool, err error) {
	if len(stmt.GroupBy) > 0 || hasAggregate(stmt.Columns) {
		return nil, nil, false, true, true, nil
	}
	colNames, colExprs, isStar, err = resolveSelectColumns(stmt.Columns, eval)
	if err != nil {
		return nil, nil, false, false, false, err
	}
	if _, isPKOnly := eval.(*pkOnlyEvaluator); isPKOnly {
		return colNames, colExprs, isStar, false, true, nil
	}
	return colNames, colExprs, isStar, false, false, nil
}

// applySortRows applies ORDER BY, using heap-based top-K when LIMIT is present.
func (e *Executor) applySortRows(stmt *ast.SelectStmt, rows []Row, eval ExprEvaluator) ([]Row, error) {
	if stmt.Limit != nil && len(stmt.OrderBy) > 0 {
		topK := int(*stmt.Limit)
		if stmt.Offset != nil {
			topK += int(*stmt.Offset)
		}
		return sortRowsTopK(rows, stmt.OrderBy, eval, rowIdentity, topK)
	}
	return sortRows(rows, stmt.OrderBy, eval, rowIdentity)
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
	return rows, newTableEvaluator(makeSubqueryRunner(e), info), nil
}

// scanSourceJSONTable handles FROM JSON_TABLE(...).
func (e *Executor) scanSourceJSONTable(stmt *ast.SelectStmt) ([]Row, ExprEvaluator, error) {
	info, rows, err := e.materializeJSONTable(stmt.JSONTable)
	if err != nil {
		return nil, nil, err
	}
	return rows, newTableEvaluator(makeSubqueryRunner(e), info), nil
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

// scanSourceSingle scans a single table with index optimizations and CTE lookup.
func (e *Executor) scanSourceSingle(stmt *ast.SelectStmt, earlyLimit int) ([]Row, ExprEvaluator, error) {
	// Check CTE scope before resolving from catalog.
	if cteInfo, cteRows, ok := e.lookupCTE(stmt.TableName); ok {
		return cteRows, newTableEvaluator(makeSubqueryRunner(e), cteInfo), nil
	}

	db, info, err := e.resolveTable(stmt.DatabaseName, stmt.TableName)
	if err != nil {
		return nil, nil, err
	}

	var rows []Row

	// Try covering index lookup (skip PK lookup entirely)
	neededCols := collectNeededColumns(stmt.Columns, stmt.Where, stmt.OrderBy, info)
	if coveringRows, ok := e.tryIndexLookupCovering(stmt.Where, info, neededCols); ok {
		return coveringRows, newTableEvaluator(makeSubqueryRunner(e), info), nil
	}

	// PK Covering: if only PK column (or no columns) are needed, skip row decoding
	if isPKOnlyCovering(neededCols, info.PrimaryKeyCol) {
		limit := 0
		if earlyLimit > 0 && stmt.Where == nil {
			limit = earlyLimit
		}
		if stmt.Where == nil {
			// No WHERE: use compact 1-element rows with pkOnlyEvaluator
			db.storage.ForEachRowKeyOnly(info.Name, false, func(key int64) bool {
				rows = append(rows, Row{key})
				return true
			}, limit)
			return rows, newPKOnlyEvaluator(makeSubqueryRunner(e), info), nil
		}
		// WHERE present: need full-width rows for eval
		numCols := len(info.Columns)
		pkIdx := info.PrimaryKeyCol
		db.storage.ForEachRowKeyOnly(info.Name, false, func(key int64) bool {
			rows = append(rows, buildPKOnlyRow(key, numCols, pkIdx))
			return true
		}, limit)
		return rows, newTableEvaluator(makeSubqueryRunner(e), info), nil
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

	return rows, newTableEvaluator(makeSubqueryRunner(e), info), nil
}

// executeSelectMaybeCorrelated checks whether a subquery references the outer query
// and dispatches to either the regular executeSelect (non-correlated) or
// executeSelectCorrelated (correlated).
func (e *Executor) executeSelectMaybeCorrelated(stmt *ast.SelectStmt, outerEval ExprEvaluator, outerRow Row) (*Result, error) {
	if eval.HasOuterReferences(stmt, outerEval) {
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
		innerEval = newJoinEvaluator(makeSubqueryRunner(e), jc)
	} else {
		innerEval = newTableEvaluator(makeSubqueryRunner(e), info)
	}

	// Create correlated evaluator
	numInner := len(innerEval.ColumnList())
	corrEval := newCorrelatedEvaluator(makeSubqueryRunner(e), innerEval, outerEval, outerRow, numInner)

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

// executeSelectWithoutTable handles SELECT without FROM (e.g. SELECT 1, 'hello').
func (e *Executor) executeSelectWithoutTable(stmt *ast.SelectStmt) (*Result, error) {
	eval := newLiteralEvaluator(makeSubqueryRunner(e))
	colTypes := resolveColumnTypes(stmt.Columns, eval)
	row := make(Row, len(stmt.Columns))
	colNames := make([]string, len(stmt.Columns))
	for i, colExpr := range stmt.Columns {
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
		row[i] = finalizeValue(val)
		if alias != "" {
			colNames[i] = alias
		} else {
			colNames[i] = formatExpr(inner)
		}
	}
	return &Result{Columns: colNames, ColumnTypes: colTypes, Rows: []Row{row}}, nil
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
			if !scalar.IsScalar(call.Name) {
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

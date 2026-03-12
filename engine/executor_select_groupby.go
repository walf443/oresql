package engine

import (
	"fmt"
	"strings"

	"github.com/walf443/oresql/ast"
)

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

		geval := newGroupEvaluator(makeSubqueryRunner(e), info, grp.rows)

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

	return resultRows, colNames, newResultEvaluator(makeSubqueryRunner(e), stmt.Columns, colNames), nil
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

	return []Row{resultRow}, colNames, newResultEvaluator(makeSubqueryRunner(e), stmt.Columns, colNames), nil
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
			continue
		}
		switch ie := inner.(type) {
		case *ast.IdentExpr:
			colNames = append(colNames, ie.Name)
		case *ast.CallExpr:
			colNames = append(colNames, formatCallExpr(ie))
		default:
			colNames = append(colNames, formatExpr(inner))
		}
	}
	return colNames, nil
}

// evalAggregate evaluates a single aggregate function call against a set of rows.
func evalAggregate(call *ast.CallExpr, rows []Row, info *TableInfo) (Value, string, error) {
	colName := formatCallExpr(call)
	switch call.Name {
	case "COUNT":
		val, err := evalAggCount(call, rows, info)
		return val, colName, err
	case "SUM":
		val, err := evalAggSum(call, rows, info)
		return val, colName, err
	case "AVG":
		val, err := evalAggAvg(call, rows, info)
		return val, colName, err
	case "MIN":
		val, err := evalAggMinMax(call, rows, info, "MIN", func(a, b Value) bool { return compareValues(a, b) < 0 })
		return val, colName, err
	case "MAX":
		val, err := evalAggMinMax(call, rows, info, "MAX", func(a, b Value) bool { return compareValues(a, b) > 0 })
		return val, colName, err
	default:
		return nil, "", fmt.Errorf("unknown aggregate function: %s", call.Name)
	}
}

// resolveAggColumn validates a single-column aggregate argument (rejects * and non-ident).
func resolveAggColumn(call *ast.CallExpr, info *TableInfo, funcName string) (*ColumnInfo, error) {
	if len(call.Args) != 1 {
		return nil, fmt.Errorf("%s expects 1 argument, got %d", funcName, len(call.Args))
	}
	if _, ok := call.Args[0].(*ast.StarExpr); ok {
		return nil, fmt.Errorf("%s(*) is not supported", funcName)
	}
	ident, ok := call.Args[0].(*ast.IdentExpr)
	if !ok {
		return nil, fmt.Errorf("%s expects column name, got %T", funcName, call.Args[0])
	}
	return info.FindColumn(ident.Name)
}

// evalAggCount evaluates COUNT(*), COUNT(col), COUNT(literal), COUNT(DISTINCT col).
func evalAggCount(call *ast.CallExpr, rows []Row, info *TableInfo) (Value, error) {
	if call.Distinct {
		if len(call.Args) != 1 {
			return nil, fmt.Errorf("COUNT(DISTINCT ...) expects 1 argument, got %d", len(call.Args))
		}
		ident, ok := call.Args[0].(*ast.IdentExpr)
		if !ok {
			return nil, fmt.Errorf("COUNT(DISTINCT ...) expects column name, got %T", call.Args[0])
		}
		col, err := info.FindColumn(ident.Name)
		if err != nil {
			return nil, err
		}
		seen := map[interface{}]bool{}
		for _, row := range rows {
			v := row[col.Index]
			if v != nil {
				seen[v] = true
			}
		}
		return int64(len(seen)), nil
	}
	// COUNT(*) / COUNT(literal) counts all rows; COUNT(column) excludes NULLs
	if len(call.Args) == 1 {
		if _, ok := call.Args[0].(*ast.StarExpr); !ok {
			if _, ok := call.Args[0].(*ast.IntLitExpr); ok {
				return int64(len(rows)), nil
			}
			if _, ok := call.Args[0].(*ast.StringLitExpr); ok {
				return int64(len(rows)), nil
			}
			ident, ok := call.Args[0].(*ast.IdentExpr)
			if !ok {
				return nil, fmt.Errorf("COUNT expects * or column name, got %T", call.Args[0])
			}
			col, err := info.FindColumn(ident.Name)
			if err != nil {
				return nil, err
			}
			count := int64(0)
			for _, row := range rows {
				if row[col.Index] != nil {
					count++
				}
			}
			return count, nil
		}
	}
	return int64(len(rows)), nil
}

// evalAggSum evaluates SUM(col).
func evalAggSum(call *ast.CallExpr, rows []Row, info *TableInfo) (Value, error) {
	col, err := resolveAggColumn(call, info, "SUM")
	if err != nil {
		return nil, err
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
			return nil, fmt.Errorf("SUM requires numeric values, got %T", v)
		}
		hasValue = true
	}
	if !hasValue {
		return nil, nil
	}
	if isFloat {
		return sumFloat + float64(sumInt), nil
	}
	return sumInt, nil
}

// evalAggAvg evaluates AVG(col).
func evalAggAvg(call *ast.CallExpr, rows []Row, info *TableInfo) (Value, error) {
	col, err := resolveAggColumn(call, info, "AVG")
	if err != nil {
		return nil, err
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
			return nil, fmt.Errorf("AVG requires numeric values, got %T", v)
		}
		count++
	}
	if count == 0 {
		return nil, nil
	}
	return sum / float64(count), nil
}

// evalAggMinMax evaluates MIN(col) or MAX(col) using the provided comparison function.
// isBetter returns true if the new value should replace the current best.
func evalAggMinMax(call *ast.CallExpr, rows []Row, info *TableInfo, funcName string, isBetter func(a, b Value) bool) (Value, error) {
	col, err := resolveAggColumn(call, info, funcName)
	if err != nil {
		return nil, err
	}
	var best Value
	for _, row := range rows {
		v := row[col.Index]
		if v == nil {
			continue
		}
		if best == nil || isBetter(v, best) {
			best = v
		}
	}
	return best, nil
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
	accs, err := buildGroupByAccumulators(stmt, gbIdent, gbCol, info)
	if err != nil {
		return nil, err
	}

	// Build evaluator for WHERE filter
	var eval *tableEvaluator
	if stmt.Where != nil {
		eval = newTableEvaluator(makeSubqueryRunner(e), info)
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

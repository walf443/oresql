package engine

import (
	"fmt"
	"sort"
	"strings"

	"github.com/walf443/oresql/ast"
)

func (e *Executor) executeSelect(stmt *ast.SelectStmt) (*Result, error) {
	// SELECT without FROM: evaluate expressions directly
	if stmt.TableName == "" {
		return e.executeSelectWithoutTable(stmt)
	}

	// JOIN path or alias path
	if len(stmt.Joins) > 0 || stmt.TableAlias != "" {
		return e.executeJoinSelect(stmt)
	}

	info, err := e.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Check if this is a GROUP BY query
	if len(stmt.GroupBy) > 0 {
		return e.executeGroupBySelect(stmt, info)
	}

	// Check if this is an aggregate query
	if hasAggregate(stmt.Columns) {
		return e.executeAggregateSelect(stmt, info)
	}

	// Resolve column names and expressions
	var colNames []string
	var colExprs []ast.Expr // nil means use StarExpr expansion
	isStar := false

	if len(stmt.Columns) == 1 {
		if _, ok := stmt.Columns[0].(*ast.StarExpr); ok {
			isStar = true
			for _, col := range info.Columns {
				colNames = append(colNames, col.Name)
			}
		}
	}

	if !isStar {
		for _, colExpr := range stmt.Columns {
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
				if err := validateTableRefWithAlias(ident.Table, stmt.TableName, stmt.TableAlias); err != nil {
					return nil, err
				}
				col, err := info.FindColumn(ident.Name)
				if err != nil {
					return nil, err
				}
				colNames = append(colNames, col.Name)
			} else {
				colNames = append(colNames, formatExpr(inner))
			}
		}
	}

	// Try index scan, fall back to full scan
	var allRows []Row
	if keys, indexUsed := e.tryIndexScan(stmt.Where, info); indexUsed {
		allRows, err = e.storage.GetByKeys(info.Name, keys)
		if err != nil {
			return nil, err
		}
	} else {
		allRows, err = e.storage.Scan(stmt.TableName)
		if err != nil {
			return nil, err
		}
	}

	// Filter rows
	var filtered []Row
	for _, row := range allRows {
		if stmt.Where != nil {
			match, err := evalWhere(stmt.Where, row, info)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		filtered = append(filtered, row)
	}

	// Sort by ORDER BY
	if len(stmt.OrderBy) > 0 {
		var sortErr error
		sort.SliceStable(filtered, func(i, j int) bool {
			if sortErr != nil {
				return false
			}
			for _, ob := range stmt.OrderBy {
				vi, err := evalExpr(ob.Expr, filtered[i], info)
				if err != nil {
					sortErr = err
					return false
				}
				vj, err := evalExpr(ob.Expr, filtered[j], info)
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
	}

	// Apply OFFSET
	if stmt.Offset != nil {
		off := int(*stmt.Offset)
		if off >= len(filtered) {
			filtered = nil
		} else {
			filtered = filtered[off:]
		}
	}

	// Apply LIMIT
	if stmt.Limit != nil {
		lim := int(*stmt.Limit)
		if lim < len(filtered) {
			filtered = filtered[:lim]
		}
	}

	// Project columns
	var resultRows []Row
	for _, row := range filtered {
		if isStar {
			projected := make(Row, len(info.Columns))
			for i, col := range info.Columns {
				projected[i] = row[col.Index]
			}
			resultRows = append(resultRows, projected)
		} else {
			projected := make(Row, len(colExprs))
			for i, expr := range colExprs {
				val, err := evalExpr(expr, row, info)
				if err != nil {
					return nil, err
				}
				projected[i] = val
			}
			resultRows = append(resultRows, projected)
		}
	}

	// Apply DISTINCT
	if stmt.Distinct {
		resultRows = dedup(resultRows)
	}

	return &Result{Columns: colNames, Rows: resultRows}, nil
}

// executeSelectWithoutTable handles SELECT without FROM (e.g. SELECT 1, 'hello').
func (e *Executor) executeSelectWithoutTable(stmt *ast.SelectStmt) (*Result, error) {
	var colNames []string
	var row Row

	for _, colExpr := range stmt.Columns {
		alias := ""
		inner := colExpr
		if a, ok := colExpr.(*ast.AliasExpr); ok {
			alias = a.Alias
			inner = a.Expr
		}
		val, err := evalLiteral(inner)
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

	return &Result{Columns: colNames, Rows: []Row{row}}, nil
}

// executeAggregateSelect handles SELECT with aggregate functions like COUNT(*).
func (e *Executor) executeAggregateSelect(stmt *ast.SelectStmt, info *TableInfo) (*Result, error) {
	// Scan and filter rows
	allRows, err := e.storage.Scan(stmt.TableName)
	if err != nil {
		return nil, err
	}

	var filtered []Row
	for _, row := range allRows {
		if stmt.Where != nil {
			match, err := evalWhere(stmt.Where, row, info)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		filtered = append(filtered, row)
	}

	// Evaluate each aggregate expression
	var colNames []string
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
			return nil, fmt.Errorf("mixed aggregate and non-aggregate columns are not supported")
		}
		val, colName, err := evalAggregate(call, filtered, info)
		if err != nil {
			return nil, err
		}
		resultRow[i] = val
		if alias != "" {
			colNames = append(colNames, alias)
		} else {
			colNames = append(colNames, colName)
		}
	}

	return &Result{Columns: colNames, Rows: []Row{resultRow}}, nil
}

// executeGroupBySelect handles SELECT with GROUP BY clause.
func (e *Executor) executeGroupBySelect(stmt *ast.SelectStmt, info *TableInfo) (*Result, error) {
	// Scan and filter rows
	allRows, err := e.storage.Scan(stmt.TableName)
	if err != nil {
		return nil, err
	}

	var filtered []Row
	for _, row := range allRows {
		if stmt.Where != nil {
			match, err := evalWhere(stmt.Where, row, info)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		filtered = append(filtered, row)
	}

	// Group rows by GROUP BY expressions
	type group struct {
		key  string
		rows []Row
	}
	groupMap := make(map[string]*group)
	var groupOrder []string

	for _, row := range filtered {
		keyParts := make([]string, len(stmt.GroupBy))
		for i, gbExpr := range stmt.GroupBy {
			val, err := evalExpr(gbExpr, row, info)
			if err != nil {
				return nil, err
			}
			keyParts[i] = fmt.Sprintf("%v", val)
		}
		key := strings.Join(keyParts, "\x00")
		if _, ok := groupMap[key]; !ok {
			groupMap[key] = &group{key: key}
			groupOrder = append(groupOrder, key)
		}
		groupMap[key].rows = append(groupMap[key].rows, row)
	}

	// Resolve column names
	var colNames []string
	for _, colExpr := range stmt.Columns {
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
			if err := validateTableRef(ident.Table, stmt.TableName); err != nil {
				return nil, err
			}
			col, err := info.FindColumn(ident.Name)
			if err != nil {
				return nil, err
			}
			colNames = append(colNames, col.Name)
		} else {
			colNames = append(colNames, formatExpr(inner))
		}
	}

	// Evaluate each group
	var resultRows []Row
	for _, key := range groupOrder {
		grp := groupMap[key]
		representativeRow := grp.rows[0]

		row := make(Row, len(stmt.Columns))
		for i, colExpr := range stmt.Columns {
			inner := colExpr
			if a, ok := colExpr.(*ast.AliasExpr); ok {
				inner = a.Expr
			}
			val, err := evalGroupExpr(inner, representativeRow, grp.rows, info)
			if err != nil {
				return nil, err
			}
			row[i] = val
		}

		// Apply HAVING filter
		if stmt.Having != nil {
			havingVal, err := evalGroupExpr(stmt.Having, representativeRow, grp.rows, info)
			if err != nil {
				return nil, err
			}
			b, ok := havingVal.(bool)
			if !ok {
				return nil, fmt.Errorf("HAVING expression must evaluate to boolean, got %T", havingVal)
			}
			if !b {
				continue
			}
		}

		resultRows = append(resultRows, row)
	}

	// Sort by ORDER BY
	if len(stmt.OrderBy) > 0 {
		var sortErr error
		sort.SliceStable(resultRows, func(i, j int) bool {
			if sortErr != nil {
				return false
			}
			for _, ob := range stmt.OrderBy {
				// Evaluate ORDER BY expressions against the result rows
				// For GROUP BY results, we need to find the value from the result row
				vi := resolveOrderByValue(ob.Expr, stmt.Columns, resultRows[i])
				vj := resolveOrderByValue(ob.Expr, stmt.Columns, resultRows[j])

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
	}

	// Apply OFFSET
	if stmt.Offset != nil {
		off := int(*stmt.Offset)
		if off >= len(resultRows) {
			resultRows = nil
		} else {
			resultRows = resultRows[off:]
		}
	}

	// Apply LIMIT
	if stmt.Limit != nil {
		lim := int(*stmt.Limit)
		if lim < len(resultRows) {
			resultRows = resultRows[:lim]
		}
	}

	// Apply DISTINCT
	if stmt.Distinct {
		resultRows = dedup(resultRows)
	}

	return &Result{Columns: colNames, Rows: resultRows}, nil
}

// evalGroupExpr evaluates an expression in the context of a group.
// For aggregate functions (CallExpr), it evaluates against the group rows.
// For other expressions, it evaluates against the representative row.
func evalGroupExpr(expr ast.Expr, row Row, groupRows []Row, info *TableInfo) (Value, error) {
	switch e := expr.(type) {
	case *ast.CallExpr:
		val, _, err := evalAggregate(e, groupRows, info)
		return val, err
	case *ast.LikeExpr:
		left, err := evalGroupExpr(e.Left, row, groupRows, info)
		if err != nil {
			return nil, err
		}
		pattern, err := evalGroupExpr(e.Pattern, row, groupRows, info)
		if err != nil {
			return nil, err
		}
		if left == nil || pattern == nil {
			return false, nil
		}
		leftStr, ok := left.(string)
		if !ok {
			return nil, fmt.Errorf("LIKE requires string operand, got %T", left)
		}
		patternStr, ok := pattern.(string)
		if !ok {
			return nil, fmt.Errorf("LIKE requires string pattern, got %T", pattern)
		}
		result := matchLike(leftStr, patternStr)
		if e.Not {
			return !result, nil
		}
		return result, nil
	case *ast.BetweenExpr:
		left, err := evalGroupExpr(e.Left, row, groupRows, info)
		if err != nil {
			return nil, err
		}
		if left == nil {
			return false, nil
		}
		low, err := evalGroupExpr(e.Low, row, groupRows, info)
		if err != nil {
			return nil, err
		}
		high, err := evalGroupExpr(e.High, row, groupRows, info)
		if err != nil {
			return nil, err
		}
		geq, err := evalComparison(left, ">=", low)
		if err != nil {
			return nil, err
		}
		leq, err := evalComparison(left, "<=", high)
		if err != nil {
			return nil, err
		}
		result := geq && leq
		if e.Not {
			return !result, nil
		}
		return result, nil
	case *ast.BinaryExpr:
		left, err := evalGroupExpr(e.Left, row, groupRows, info)
		if err != nil {
			return nil, err
		}
		right, err := evalGroupExpr(e.Right, row, groupRows, info)
		if err != nil {
			return nil, err
		}
		return evalComparison(left, e.Op, right)
	case *ast.LogicalExpr:
		left, err := evalGroupExpr(e.Left, row, groupRows, info)
		if err != nil {
			return nil, err
		}
		leftBool, ok := left.(bool)
		if !ok {
			return nil, fmt.Errorf("expected boolean in %s expression, got %T", e.Op, left)
		}
		right, err := evalGroupExpr(e.Right, row, groupRows, info)
		if err != nil {
			return nil, err
		}
		rightBool, ok := right.(bool)
		if !ok {
			return nil, fmt.Errorf("expected boolean in %s expression, got %T", e.Op, right)
		}
		switch e.Op {
		case "AND":
			return leftBool && rightBool, nil
		case "OR":
			return leftBool || rightBool, nil
		default:
			return nil, fmt.Errorf("unknown logical operator: %s", e.Op)
		}
	case *ast.NotExpr:
		val, err := evalGroupExpr(e.Expr, row, groupRows, info)
		if err != nil {
			return nil, err
		}
		b, ok := val.(bool)
		if !ok {
			return nil, fmt.Errorf("NOT requires boolean operand, got %T", val)
		}
		return !b, nil
	default:
		return evalExpr(expr, row, info)
	}
}

// resolveOrderByValue finds the value for an ORDER BY expression from a GROUP BY result row.
// It matches the ORDER BY expression to a column in the SELECT list and returns the corresponding value.
func resolveOrderByValue(orderExpr ast.Expr, selectCols []ast.Expr, resultRow Row) Value {
	// Try to match ORDER BY expression to a SELECT column
	if ident, ok := orderExpr.(*ast.IdentExpr); ok {
		for i, col := range selectCols {
			inner := col
			if a, ok := col.(*ast.AliasExpr); ok {
				// Match by alias name
				if strings.ToLower(a.Alias) == strings.ToLower(ident.Name) {
					return resultRow[i]
				}
				inner = a.Expr
			}
			if selIdent, ok := inner.(*ast.IdentExpr); ok {
				if strings.ToLower(selIdent.Name) == strings.ToLower(ident.Name) {
					return resultRow[i]
				}
			}
		}
	}
	// Fallback: try to match by position for aggregate expressions
	if call, ok := orderExpr.(*ast.CallExpr); ok {
		for i, col := range selectCols {
			inner := col
			if a, ok := col.(*ast.AliasExpr); ok {
				inner = a.Expr
			}
			if selCall, ok := inner.(*ast.CallExpr); ok {
				if selCall.Name == call.Name {
					return resultRow[i]
				}
			}
		}
	}
	return nil
}

// evalAggregate evaluates a single aggregate function call against a set of rows.
func evalAggregate(call *ast.CallExpr, rows []Row, info *TableInfo) (Value, string, error) {
	switch call.Name {
	case "COUNT":
		colName := formatCallExpr(call)
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
	return call.Name + "(" + strings.Join(args, ", ") + ")"
}

// hasAggregate returns true if any column expression is a function call.
func hasAggregate(columns []ast.Expr) bool {
	for _, col := range columns {
		inner := col
		if a, ok := col.(*ast.AliasExpr); ok {
			inner = a.Expr
		}
		if _, ok := inner.(*ast.CallExpr); ok {
			return true
		}
	}
	return false
}

// dedup removes duplicate rows, preserving order of first occurrence.
func dedup(rows []Row) []Row {
	seen := make(map[string]bool)
	var result []Row
	for _, row := range rows {
		key := fmt.Sprintf("%v", []Value(row))
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}
	return result
}

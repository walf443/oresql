package engine

import (
	"fmt"
	"sort"
	"strings"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/lexer"
	"github.com/walf443/oresql/parser"
)

// Result holds the output of a query execution.
type Result struct {
	Columns []string // column names for SELECT results
	Rows    []Row    // data rows for SELECT results
	Message string   // status message for CREATE/INSERT
}

// Option configures an Executor.
type Option func(*Executor)

// WithWAL sets the WAL for the Executor.
func WithWAL(w *WAL) Option {
	return func(e *Executor) {
		e.wal = w
	}
}

// Executor runs SQL statements.
type Executor struct {
	catalog *Catalog
	storage *Storage
	wal     *WAL
}

func NewExecutor(opts ...Option) *Executor {
	e := &Executor{
		catalog: NewCatalog(),
		storage: NewStorage(),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// ExecuteSQL parses and executes a SQL string, logging mutating statements to WAL.
func (e *Executor) ExecuteSQL(sql string) (*Result, error) {
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		return nil, err
	}
	result, err := e.Execute(stmt)
	if err != nil {
		return nil, err
	}
	if e.wal != nil {
		if _, ok := stmt.(*ast.SelectStmt); !ok {
			if err := e.wal.Append(sql); err != nil {
				return nil, fmt.Errorf("WAL write error: %w", err)
			}
		}
	}
	return result, nil
}

// ReplayWAL replays the WAL file to restore state.
func (e *Executor) ReplayWAL() error {
	if e.wal == nil {
		return nil
	}
	wal := e.wal
	e.wal = nil
	defer func() { e.wal = wal }()

	return wal.Replay(func(sql string) error {
		_, err := e.ExecuteSQL(sql)
		return err
	})
}

func (e *Executor) Execute(stmt ast.Statement) (*Result, error) {
	switch s := stmt.(type) {
	case *ast.CreateTableStmt:
		return e.executeCreateTable(s)
	case *ast.InsertStmt:
		return e.executeInsert(s)
	case *ast.SelectStmt:
		return e.executeSelect(s)
	case *ast.UpdateStmt:
		return e.executeUpdate(s)
	case *ast.DeleteStmt:
		return e.executeDelete(s)
	case *ast.DropTableStmt:
		return e.executeDropTable(s)
	case *ast.TruncateTableStmt:
		return e.executeTruncateTable(s)
	default:
		return nil, fmt.Errorf("unknown statement type: %T", stmt)
	}
}

func (e *Executor) executeCreateTable(stmt *ast.CreateTableStmt) (*Result, error) {
	info, err := e.catalog.CreateTable(stmt.TableName, stmt.Columns)
	if err != nil {
		return nil, err
	}
	e.storage.CreateTable(info)
	return &Result{Message: "table created"}, nil
}

func (e *Executor) executeDropTable(stmt *ast.DropTableStmt) (*Result, error) {
	if err := e.catalog.DropTable(stmt.TableName); err != nil {
		return nil, err
	}
	e.storage.DropTable(stmt.TableName)
	return &Result{Message: "table dropped"}, nil
}

func (e *Executor) executeTruncateTable(stmt *ast.TruncateTableStmt) (*Result, error) {
	if _, err := e.catalog.GetTable(stmt.TableName); err != nil {
		return nil, err
	}
	e.storage.TruncateTable(stmt.TableName)
	return &Result{Message: "table truncated"}, nil
}

func (e *Executor) executeInsert(stmt *ast.InsertStmt) (*Result, error) {
	info, err := e.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	for _, values := range stmt.Rows {
		if len(values) != len(info.Columns) {
			return nil, fmt.Errorf("expected %d values, got %d", len(info.Columns), len(values))
		}

		row := make(Row, len(info.Columns))
		for i, valExpr := range values {
			val, err := evalLiteral(valExpr)
			if err != nil {
				return nil, err
			}

			col := info.Columns[i]
			if val == nil {
				if col.NotNull {
					return nil, fmt.Errorf("column %q cannot be NULL", col.Name)
				}
			} else {
				switch col.DataType {
				case "INT":
					if _, ok := val.(int64); !ok {
						return nil, fmt.Errorf("column %q expects INT, got %T", col.Name, val)
					}
				case "TEXT":
					if _, ok := val.(string); !ok {
						return nil, fmt.Errorf("column %q expects TEXT, got %T", col.Name, val)
					}
				}
			}

			row[i] = val
		}

		if err := e.storage.Insert(stmt.TableName, row); err != nil {
			return nil, err
		}
	}

	n := len(stmt.Rows)
	msg := fmt.Sprintf("%d rows inserted", n)
	if n == 1 {
		msg = "1 row inserted"
	}

	return &Result{Message: msg}, nil
}

func (e *Executor) executeUpdate(stmt *ast.UpdateStmt) (*Result, error) {
	info, err := e.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	allRows, err := e.storage.Scan(stmt.TableName)
	if err != nil {
		return nil, err
	}

	updated := 0
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

		for _, set := range stmt.Sets {
			col, err := info.FindColumn(set.Column)
			if err != nil {
				return nil, err
			}

			val, err := evalLiteral(set.Value)
			if err != nil {
				return nil, err
			}

			if val == nil {
				if col.NotNull {
					return nil, fmt.Errorf("column %q cannot be NULL", col.Name)
				}
			} else {
				switch col.DataType {
				case "INT":
					if _, ok := val.(int64); !ok {
						return nil, fmt.Errorf("column %q expects INT, got %T", col.Name, val)
					}
				case "TEXT":
					if _, ok := val.(string); !ok {
						return nil, fmt.Errorf("column %q expects TEXT, got %T", col.Name, val)
					}
				}
			}

			row[col.Index] = val
		}
		updated++
	}

	msg := fmt.Sprintf("%d rows updated", updated)
	if updated == 1 {
		msg = "1 row updated"
	}

	return &Result{Message: msg}, nil
}

func (e *Executor) executeDelete(stmt *ast.DeleteStmt) (*Result, error) {
	info, err := e.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	allRows, err := e.storage.Scan(stmt.TableName)
	if err != nil {
		return nil, err
	}

	keepIndices := make(map[int]bool)
	deleted := 0
	for i, row := range allRows {
		if stmt.Where != nil {
			match, err := evalWhere(stmt.Where, row, info)
			if err != nil {
				return nil, err
			}
			if !match {
				keepIndices[i] = true
				continue
			}
		}
		deleted++
	}

	if err := e.storage.DeleteRows(stmt.TableName, keepIndices); err != nil {
		return nil, err
	}

	msg := fmt.Sprintf("%d rows deleted", deleted)
	if deleted == 1 {
		msg = "1 row deleted"
	}

	return &Result{Message: msg}, nil
}

func (e *Executor) executeSelect(stmt *ast.SelectStmt) (*Result, error) {
	// SELECT without FROM: evaluate expressions directly
	if stmt.TableName == "" {
		return e.executeSelectWithoutTable(stmt)
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
	}

	// Scan all rows
	allRows, err := e.storage.Scan(stmt.TableName)
	if err != nil {
		return nil, err
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

	return &Result{Columns: colNames, Rows: resultRows}, nil
}

// compareValues compares two values for ORDER BY sorting.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
// NULL values sort last (are considered greater than any non-NULL value).
func compareValues(a, b Value) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return 1 // NULL sorts last
	}
	if b == nil {
		return -1 // NULL sorts last
	}

	switch av := a.(type) {
	case int64:
		bv, ok := b.(int64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case string:
		bv, ok := b.(string)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	}
	return 0
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

// formatExpr returns a display name for an expression.
func formatExpr(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.IntLitExpr:
		return fmt.Sprintf("%d", e.Value)
	case *ast.StringLitExpr:
		return "'" + e.Value + "'"
	case *ast.NullLitExpr:
		return "NULL"
	case *ast.IdentExpr:
		if e.Table != "" {
			return e.Table + "." + e.Name
		}
		return e.Name
	case *ast.ArithmeticExpr:
		return formatExpr(e.Left) + " " + e.Op + " " + formatExpr(e.Right)
	default:
		return "?"
	}
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
		var sum int64
		hasValue := false
		for _, row := range rows {
			v := row[col.Index]
			if v == nil {
				continue
			}
			iv, ok := v.(int64)
			if !ok {
				return nil, "", fmt.Errorf("SUM requires INT values, got %T", v)
			}
			sum += iv
			hasValue = true
		}
		if !hasValue {
			return nil, colName, nil
		}
		return sum, colName, nil
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
		var sum int64
		var count int64
		for _, row := range rows {
			v := row[col.Index]
			if v == nil {
				continue
			}
			iv, ok := v.(int64)
			if !ok {
				return nil, "", fmt.Errorf("AVG requires INT values, got %T", v)
			}
			sum += iv
			count++
		}
		if count == 0 {
			return nil, colName, nil
		}
		return sum / count, colName, nil
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
		case *ast.StringLitExpr:
			args[i] = "'" + a.Value + "'"
		default:
			args[i] = "?"
		}
	}
	return call.Name + "(" + strings.Join(args, ", ") + ")"
}

// evalLiteral evaluates a literal expression (for INSERT VALUES and SELECT without FROM).
func evalLiteral(expr ast.Expr) (Value, error) {
	switch e := expr.(type) {
	case *ast.IntLitExpr:
		return e.Value, nil
	case *ast.StringLitExpr:
		return e.Value, nil
	case *ast.NullLitExpr:
		return nil, nil
	case *ast.ArithmeticExpr:
		left, err := evalLiteral(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := evalLiteral(e.Right)
		if err != nil {
			return nil, err
		}
		return evalArithmetic(left, e.Op, right)
	default:
		return nil, fmt.Errorf("expected literal value, got %T", expr)
	}
}

// evalExpr evaluates an expression against a row.
func evalExpr(expr ast.Expr, row Row, info *TableInfo) (Value, error) {
	switch e := expr.(type) {
	case *ast.IdentExpr:
		if err := validateTableRef(e.Table, info.Name); err != nil {
			return nil, err
		}
		col, err := info.FindColumn(e.Name)
		if err != nil {
			return nil, err
		}
		return row[col.Index], nil
	case *ast.IntLitExpr:
		return e.Value, nil
	case *ast.StringLitExpr:
		return e.Value, nil
	case *ast.NullLitExpr:
		return nil, nil
	case *ast.IsNullExpr:
		val, err := evalExpr(e.Expr, row, info)
		if err != nil {
			return nil, err
		}
		if e.Not {
			return val != nil, nil
		}
		return val == nil, nil
	case *ast.ArithmeticExpr:
		left, err := evalExpr(e.Left, row, info)
		if err != nil {
			return nil, err
		}
		right, err := evalExpr(e.Right, row, info)
		if err != nil {
			return nil, err
		}
		return evalArithmetic(left, e.Op, right)
	case *ast.BinaryExpr:
		left, err := evalExpr(e.Left, row, info)
		if err != nil {
			return nil, err
		}
		right, err := evalExpr(e.Right, row, info)
		if err != nil {
			return nil, err
		}
		return evalComparison(left, e.Op, right)
	case *ast.LogicalExpr:
		left, err := evalExpr(e.Left, row, info)
		if err != nil {
			return nil, err
		}
		leftBool, ok := left.(bool)
		if !ok {
			return nil, fmt.Errorf("expected boolean in %s expression, got %T", e.Op, left)
		}
		right, err := evalExpr(e.Right, row, info)
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
	default:
		return nil, fmt.Errorf("cannot evaluate expression: %T", expr)
	}
}

func evalArithmetic(left Value, op string, right Value) (Value, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	lv, ok := left.(int64)
	if !ok {
		return nil, fmt.Errorf("arithmetic requires INT operands, got %T", left)
	}
	rv, ok := right.(int64)
	if !ok {
		return nil, fmt.Errorf("arithmetic requires INT operands, got %T", right)
	}
	switch op {
	case "+":
		return lv + rv, nil
	case "-":
		return lv - rv, nil
	case "*":
		return lv * rv, nil
	case "/":
		if rv == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return lv / rv, nil
	default:
		return nil, fmt.Errorf("unknown arithmetic operator: %s", op)
	}
}

func evalComparison(left Value, op string, right Value) (bool, error) {
	// NULL comparison: any comparison with NULL returns false (SQL semantics)
	if left == nil || right == nil {
		return false, nil
	}

	// Both int64
	if lv, ok := left.(int64); ok {
		if rv, ok := right.(int64); ok {
			switch op {
			case "=":
				return lv == rv, nil
			case "!=":
				return lv != rv, nil
			case "<":
				return lv < rv, nil
			case ">":
				return lv > rv, nil
			case "<=":
				return lv <= rv, nil
			case ">=":
				return lv >= rv, nil
			}
		}
	}

	// Both string
	if lv, ok := left.(string); ok {
		if rv, ok := right.(string); ok {
			switch op {
			case "=":
				return lv == rv, nil
			case "!=":
				return lv != rv, nil
			case "<":
				return lv < rv, nil
			case ">":
				return lv > rv, nil
			case "<=":
				return lv <= rv, nil
			case ">=":
				return lv >= rv, nil
			}
		}
	}

	return false, fmt.Errorf("cannot compare %T and %T with %s", left, right, op)
}

// validateTableRef checks that a qualified table reference matches the target table.
// If tableRef is empty (unqualified), validation is skipped.
func validateTableRef(tableRef, targetTable string) error {
	if tableRef != "" && strings.ToLower(tableRef) != strings.ToLower(targetTable) {
		return fmt.Errorf("unknown table %q", tableRef)
	}
	return nil
}

// evalWhere evaluates a WHERE expression and returns a boolean.
func evalWhere(expr ast.Expr, row Row, info *TableInfo) (bool, error) {
	val, err := evalExpr(expr, row, info)
	if err != nil {
		return false, err
	}
	b, ok := val.(bool)
	if !ok {
		return false, fmt.Errorf("WHERE expression must evaluate to boolean, got %T", val)
	}
	return b, nil
}

package engine

import (
	"fmt"
	"strings"

	"github.com/walf443/oresql/ast"
)

// executeJoinSelect handles SELECT with JOIN clauses using the graph-based optimizer.
func (e *Executor) executeJoinSelect(stmt *ast.SelectStmt) (*Result, error) {
	graph, err := e.buildJoinGraph(stmt)
	if err != nil {
		return nil, err
	}
	order := e.OptimizeJoinOrder(graph)
	return e.executeJoinWithGraph(stmt, graph, order)
}

// tableRange maps a table to its column offset within a merged row.
type tableRange struct {
	info     *TableInfo
	startIdx int
}

// JoinContext tracks column information across multiple joined tables.
type JoinContext struct {
	MergedInfo *TableInfo            // virtual TableInfo with all columns concatenated
	tableMap   map[string]tableRange // table name/alias → column offset
	tableNames []string              // ordered table names for ambiguity detection
}

// newJoinContext creates a JoinContext from a list of (TableInfo, alias) pairs.
func newJoinContext(tables []struct {
	info  *TableInfo
	alias string
}) *JoinContext {
	jc := &JoinContext{
		MergedInfo: &TableInfo{Name: "joined"},
		tableMap:   make(map[string]tableRange),
	}
	offset := 0
	for _, t := range tables {
		tr := tableRange{info: t.info, startIdx: offset}
		// Register by real table name
		jc.tableMap[strings.ToLower(t.info.Name)] = tr
		// Register by alias if present
		if t.alias != "" {
			jc.tableMap[strings.ToLower(t.alias)] = tr
		}
		jc.tableNames = append(jc.tableNames, strings.ToLower(t.info.Name))
		for _, col := range t.info.Columns {
			mergedCol := ColumnInfo{
				Name:     col.Name,
				DataType: col.DataType,
				Index:    offset + col.Index,
				NotNull:  col.NotNull,
			}
			jc.MergedInfo.Columns = append(jc.MergedInfo.Columns, mergedCol)
		}
		offset += len(t.info.Columns)
	}
	return jc
}

// FindColumn resolves a column reference in the join context.
// If tableName is specified, looks only in that table.
// If tableName is empty, searches all tables and errors on ambiguity.
func (jc *JoinContext) FindColumn(tableName, colName string) (*ColumnInfo, error) {
	lower := strings.ToLower(colName)
	if tableName != "" {
		tr, ok := jc.tableMap[strings.ToLower(tableName)]
		if !ok {
			return nil, fmt.Errorf("unknown table %q", tableName)
		}
		for i := range tr.info.Columns {
			if strings.ToLower(tr.info.Columns[i].Name) == lower {
				idx := tr.startIdx + tr.info.Columns[i].Index
				return &ColumnInfo{
					Name:     tr.info.Columns[i].Name,
					DataType: tr.info.Columns[i].DataType,
					Index:    idx,
				}, nil
			}
		}
		return nil, fmt.Errorf("column %q not found in table %q", colName, tableName)
	}

	// Unqualified: search all tables
	var found *ColumnInfo
	matchCount := 0
	for _, tName := range jc.tableNames {
		tr := jc.tableMap[tName]
		for i := range tr.info.Columns {
			if strings.ToLower(tr.info.Columns[i].Name) == lower {
				matchCount++
				idx := tr.startIdx + tr.info.Columns[i].Index
				found = &ColumnInfo{
					Name:     tr.info.Columns[i].Name,
					DataType: tr.info.Columns[i].DataType,
					Index:    idx,
				}
			}
		}
	}
	if matchCount == 0 {
		return nil, fmt.Errorf("column %q not found", colName)
	}
	if matchCount > 1 {
		return nil, fmt.Errorf("ambiguous column %q", colName)
	}
	return found, nil
}

// evalExprJoin evaluates an expression against a joined row using JoinContext.
func evalExprJoin(expr ast.Expr, row Row, jc *JoinContext) (Value, error) {
	switch e := expr.(type) {
	case *ast.IdentExpr:
		col, err := jc.FindColumn(e.Table, e.Name)
		if err != nil {
			return nil, err
		}
		return row[col.Index], nil
	case *ast.IntLitExpr:
		return e.Value, nil
	case *ast.FloatLitExpr:
		return e.Value, nil
	case *ast.StringLitExpr:
		return e.Value, nil
	case *ast.NullLitExpr:
		return nil, nil
	case *ast.IsNullExpr:
		val, err := evalExprJoin(e.Expr, row, jc)
		if err != nil {
			return nil, err
		}
		if e.Not {
			return val != nil, nil
		}
		return val == nil, nil
	case *ast.InExpr:
		left, err := evalExprJoin(e.Left, row, jc)
		if err != nil {
			return nil, err
		}
		if left == nil {
			return false, nil
		}
		for _, valExpr := range e.Values {
			val, err := evalExprJoin(valExpr, row, jc)
			if err != nil {
				return nil, err
			}
			match, err := evalComparison(left, "=", val)
			if err != nil {
				return nil, err
			}
			if match {
				return !e.Not, nil
			}
		}
		return e.Not, nil
	case *ast.BetweenExpr:
		left, err := evalExprJoin(e.Left, row, jc)
		if err != nil {
			return nil, err
		}
		if left == nil {
			return false, nil
		}
		low, err := evalExprJoin(e.Low, row, jc)
		if err != nil {
			return nil, err
		}
		high, err := evalExprJoin(e.High, row, jc)
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
	case *ast.LikeExpr:
		left, err := evalExprJoin(e.Left, row, jc)
		if err != nil {
			return nil, err
		}
		pattern, err := evalExprJoin(e.Pattern, row, jc)
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
	case *ast.ArithmeticExpr:
		left, err := evalExprJoin(e.Left, row, jc)
		if err != nil {
			return nil, err
		}
		right, err := evalExprJoin(e.Right, row, jc)
		if err != nil {
			return nil, err
		}
		return evalArithmetic(left, e.Op, right)
	case *ast.BinaryExpr:
		left, err := evalExprJoin(e.Left, row, jc)
		if err != nil {
			return nil, err
		}
		right, err := evalExprJoin(e.Right, row, jc)
		if err != nil {
			return nil, err
		}
		return evalComparison(left, e.Op, right)
	case *ast.LogicalExpr:
		left, err := evalExprJoin(e.Left, row, jc)
		if err != nil {
			return nil, err
		}
		leftBool, ok := left.(bool)
		if !ok {
			return nil, fmt.Errorf("expected boolean in %s expression, got %T", e.Op, left)
		}
		right, err := evalExprJoin(e.Right, row, jc)
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
		val, err := evalExprJoin(e.Expr, row, jc)
		if err != nil {
			return nil, err
		}
		b, ok := val.(bool)
		if !ok {
			return nil, fmt.Errorf("NOT requires boolean operand, got %T", val)
		}
		return !b, nil
	default:
		return nil, fmt.Errorf("cannot evaluate expression: %T", expr)
	}
}

// evalWhereJoin evaluates a WHERE expression in a JOIN context.
func evalWhereJoin(expr ast.Expr, row Row, jc *JoinContext) (bool, error) {
	val, err := evalExprJoin(expr, row, jc)
	if err != nil {
		return false, err
	}
	b, ok := val.(bool)
	if !ok {
		return false, fmt.Errorf("WHERE expression must evaluate to boolean, got %T", val)
	}
	return b, nil
}

// validateTableRefWithAlias checks that a qualified table reference matches the target table or its alias.
func validateTableRefWithAlias(tableRef, targetTable, alias string) error {
	if tableRef == "" {
		return nil
	}
	lower := strings.ToLower(tableRef)
	if lower == strings.ToLower(targetTable) {
		return nil
	}
	if alias != "" && lower == strings.ToLower(alias) {
		return nil
	}
	return fmt.Errorf("unknown table %q", tableRef)
}

package engine

import (
	"fmt"
	"strings"

	"github.com/walf443/oresql/ast"
)

func (e *Executor) executeSetOp(stmt *ast.SetOpStmt) (*Result, error) {
	// 1. Execute left side (may be *SelectStmt or *SetOpStmt for chains)
	leftResult, err := e.executeInner(stmt.Left)
	if err != nil {
		return nil, err
	}

	// 2. Execute right side
	rightResult, err := e.executeSelect(stmt.Right)
	if err != nil {
		return nil, err
	}

	// 3. Validate column count match
	if len(leftResult.Columns) != len(rightResult.Columns) {
		return nil, fmt.Errorf("%s: column count mismatch: left has %d columns, right has %d columns",
			stmt.Op, len(leftResult.Columns), len(rightResult.Columns))
	}

	// 3b. Validate column types match
	if leftResult.ColumnTypes != nil && rightResult.ColumnTypes != nil {
		for i := range leftResult.ColumnTypes {
			lt, rt := leftResult.ColumnTypes[i], rightResult.ColumnTypes[i]
			if lt != "" && rt != "" && lt != rt {
				return nil, fmt.Errorf("%s: column %d type mismatch: %s vs %s", stmt.Op, i+1, lt, rt)
			}
		}
	}

	// 4. Combine rows based on set operation
	var rows []Row
	switch stmt.Op {
	case ast.SetOpUnion:
		rows = make([]Row, 0, len(leftResult.Rows)+len(rightResult.Rows))
		rows = append(rows, leftResult.Rows...)
		rows = append(rows, rightResult.Rows...)
		if !stmt.All {
			rows = dedup(rows)
		}
	case ast.SetOpIntersect:
		rows = intersectRows(leftResult.Rows, rightResult.Rows, stmt.All)
	case ast.SetOpExcept:
		rows = exceptRows(leftResult.Rows, rightResult.Rows, stmt.All)
	}

	// 5. ORDER BY
	colNames := leftResult.Columns
	if len(stmt.OrderBy) > 0 {
		eval := newSetOpEvaluator(e, colNames)
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

	// 6. OFFSET / LIMIT
	rows = applyOffset(rows, stmt.Offset)
	rows = applyLimit(rows, stmt.Limit)

	return &Result{Columns: colNames, ColumnTypes: leftResult.ColumnTypes, Rows: rows}, nil
}

// intersectRows returns rows common to both left and right.
func intersectRows(left, right []Row, all bool) []Row {
	// Build a count map from right rows
	rightSet := make(map[KeyEncoding]int, len(right))
	for _, row := range right {
		rightSet[encodeValues(row)]++
	}

	cap := len(left)
	if len(right) < cap {
		cap = len(right)
	}
	result := make([]Row, 0, cap)
	seen := make(map[KeyEncoding]int, len(left)) // for dedup when ALL is false
	for _, row := range left {
		key := encodeValues(row)
		if rightSet[key] > 0 {
			if all {
				rightSet[key]-- // consume one match
				result = append(result, row)
			} else {
				if seen[key] == 0 {
					result = append(result, row)
				}
				seen[key]++
			}
		}
	}
	return result
}

// exceptRows returns rows from left that are not in right.
func exceptRows(left, right []Row, all bool) []Row {
	// Build a count map from right rows
	rightSet := make(map[KeyEncoding]int, len(right))
	for _, row := range right {
		rightSet[encodeValues(row)]++
	}

	result := make([]Row, 0, len(left))
	seen := make(map[KeyEncoding]int, len(left)) // for dedup when ALL is false
	for _, row := range left {
		key := encodeValues(row)
		if all {
			if rightSet[key] > 0 {
				rightSet[key]-- // consume one match, skip this row
			} else {
				result = append(result, row)
			}
		} else {
			if rightSet[key] == 0 {
				if seen[key] == 0 {
					result = append(result, row)
				}
				seen[key]++
			}
		}
	}
	return result
}

// setOpEvaluator evaluates expressions against set operation result rows.
// Result rows are already projected, so column lookup is by name → index.
type setOpEvaluator struct {
	runner   SubqueryRunner
	colNames []string
}

func newSetOpEvaluator(exec *Executor, colNames []string) *setOpEvaluator {
	return &setOpEvaluator{runner: makeSubqueryRunner(exec), colNames: colNames}
}

func (se *setOpEvaluator) GetSubqueryRunner() SubqueryRunner { return se.runner }

func (se *setOpEvaluator) Eval(expr ast.Expr, row Row) (Value, error) {
	switch e := expr.(type) {
	case *ast.IdentExpr:
		col, err := se.ResolveColumn(e.Table, e.Name)
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
	default:
		return nil, fmt.Errorf("unsupported expression in set operation ORDER BY: %T", expr)
	}
}

func (se *setOpEvaluator) ResolveColumn(tableName, colName string) (*ColumnInfo, error) {
	lower := strings.ToLower(colName)
	for i, name := range se.colNames {
		if strings.ToLower(name) == lower {
			return &ColumnInfo{Name: name, Index: i}, nil
		}
	}
	return nil, fmt.Errorf("column %q not found in set operation result", colName)
}

func (se *setOpEvaluator) ColumnList() []ColumnInfo {
	cols := make([]ColumnInfo, len(se.colNames))
	for i, name := range se.colNames {
		cols[i] = ColumnInfo{Name: name, Index: i}
	}
	return cols
}

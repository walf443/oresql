package engine

import (
	"fmt"
	"strings"

	"github.com/walf443/oresql/ast"
)

func (e *Executor) executeUnion(stmt *ast.UnionStmt) (*Result, error) {
	// 1. Execute left side (may be *SelectStmt or *UnionStmt for chains)
	leftResult, err := e.Execute(stmt.Left)
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
		return nil, fmt.Errorf("UNION: column count mismatch: left has %d columns, right has %d columns",
			len(leftResult.Columns), len(rightResult.Columns))
	}

	// 4. Combine rows
	rows := make([]Row, 0, len(leftResult.Rows)+len(rightResult.Rows))
	rows = append(rows, leftResult.Rows...)
	rows = append(rows, rightResult.Rows...)

	// 5. UNION (non-ALL): remove duplicates
	if !stmt.All {
		rows = dedup(rows)
	}

	// 6. ORDER BY
	colNames := leftResult.Columns
	if len(stmt.OrderBy) > 0 {
		eval := newUnionEvaluator(e, colNames)
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

	// 7. OFFSET / LIMIT
	rows = applyOffset(rows, stmt.Offset)
	rows = applyLimit(rows, stmt.Limit)

	return &Result{Columns: colNames, Rows: rows}, nil
}

// unionEvaluator evaluates expressions against UNION result rows.
// Result rows are already projected, so column lookup is by name → index.
type unionEvaluator struct {
	exec     *Executor
	colNames []string
}

func newUnionEvaluator(exec *Executor, colNames []string) *unionEvaluator {
	return &unionEvaluator{exec: exec, colNames: colNames}
}

func (ue *unionEvaluator) GetExecutor() *Executor { return ue.exec }

func (ue *unionEvaluator) Eval(expr ast.Expr, row Row) (Value, error) {
	switch e := expr.(type) {
	case *ast.IdentExpr:
		col, err := ue.ResolveColumn(e.Table, e.Name)
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
		return nil, fmt.Errorf("unsupported expression in UNION ORDER BY: %T", expr)
	}
}

func (ue *unionEvaluator) ResolveColumn(tableName, colName string) (*ColumnInfo, error) {
	lower := strings.ToLower(colName)
	for i, name := range ue.colNames {
		if strings.ToLower(name) == lower {
			return &ColumnInfo{Name: name, Index: i}, nil
		}
	}
	return nil, fmt.Errorf("column %q not found in UNION result", colName)
}

func (ue *unionEvaluator) ColumnList() []ColumnInfo {
	cols := make([]ColumnInfo, len(ue.colNames))
	for i, name := range ue.colNames {
		cols[i] = ColumnInfo{Name: name, Index: i}
	}
	return cols
}

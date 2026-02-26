package engine

import (
	"fmt"
	"strings"

	"github.com/walf443/oresql/ast"
)

func (e *Executor) executeInsert(stmt *ast.InsertStmt) (*Result, error) {
	info, err := e.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	if stmt.Select != nil {
		return e.executeInsertSelect(stmt, info)
	}

	if stmt.Columns == nil {
		// No column list: positional mapping
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
				val, err = validateAndCoerceValue(val, info.Columns[i])
				if err != nil {
					return nil, err
				}
				row[i] = val
			}

			if err := e.storage.Insert(stmt.TableName, row); err != nil {
				return nil, err
			}
		}
	} else {
		// Column list specified
		// Resolve column indices and check for duplicates/unknown columns
		colIndices := make([]int, len(stmt.Columns))
		seen := make(map[string]bool)
		for i, colName := range stmt.Columns {
			col, err := info.FindColumn(colName)
			if err != nil {
				return nil, err
			}
			lower := strings.ToLower(colName)
			if seen[lower] {
				return nil, fmt.Errorf("duplicate column %q in INSERT", colName)
			}
			seen[lower] = true
			colIndices[i] = col.Index
		}

		for _, values := range stmt.Rows {
			if len(values) != len(stmt.Columns) {
				return nil, fmt.Errorf("expected %d values, got %d", len(stmt.Columns), len(values))
			}

			row := make(Row, len(info.Columns))

			// Set specified columns
			for i, valExpr := range values {
				val, err := evalLiteral(valExpr)
				if err != nil {
					return nil, err
				}
				idx := colIndices[i]
				val, err = validateAndCoerceValue(val, info.Columns[idx])
				if err != nil {
					return nil, err
				}
				row[idx] = val
			}

			// Fill unspecified columns with DEFAULT or NULL
			for _, col := range info.Columns {
				if seen[strings.ToLower(col.Name)] {
					continue
				}
				if col.HasDefault {
					row[col.Index] = col.Default
				} else {
					if col.NotNull {
						return nil, fmt.Errorf("column %q cannot be NULL", col.Name)
					}
					row[col.Index] = nil
				}
			}

			if err := e.storage.Insert(stmt.TableName, row); err != nil {
				return nil, err
			}
		}
	}

	n := len(stmt.Rows)
	msg := fmt.Sprintf("%d rows inserted", n)
	if n == 1 {
		msg = "1 row inserted"
	}

	return &Result{Message: msg}, nil
}

func (e *Executor) executeInsertSelect(stmt *ast.InsertStmt, info *TableInfo) (*Result, error) {
	selectResult, err := e.executeInner(stmt.Select)
	if err != nil {
		return nil, err
	}

	n := 0

	if stmt.Columns == nil {
		// No column list: positional mapping
		if len(selectResult.Columns) != len(info.Columns) {
			return nil, fmt.Errorf("expected %d values, got %d", len(info.Columns), len(selectResult.Columns))
		}

		for _, srcRow := range selectResult.Rows {
			row := make(Row, len(info.Columns))
			for i, val := range srcRow {
				val, err := validateAndCoerceValue(val, info.Columns[i])
				if err != nil {
					return nil, err
				}
				row[i] = val
			}
			if err := e.storage.Insert(stmt.TableName, row); err != nil {
				return nil, err
			}
			n++
		}
	} else {
		// Column list specified
		colIndices := make([]int, len(stmt.Columns))
		seen := make(map[string]bool)
		for i, colName := range stmt.Columns {
			col, err := info.FindColumn(colName)
			if err != nil {
				return nil, err
			}
			lower := strings.ToLower(colName)
			if seen[lower] {
				return nil, fmt.Errorf("duplicate column %q in INSERT", colName)
			}
			seen[lower] = true
			colIndices[i] = col.Index
		}

		if len(selectResult.Columns) != len(stmt.Columns) {
			return nil, fmt.Errorf("expected %d values, got %d", len(stmt.Columns), len(selectResult.Columns))
		}

		for _, srcRow := range selectResult.Rows {
			row := make(Row, len(info.Columns))

			for i, val := range srcRow {
				idx := colIndices[i]
				val, err := validateAndCoerceValue(val, info.Columns[idx])
				if err != nil {
					return nil, err
				}
				row[idx] = val
			}

			// Fill unspecified columns with DEFAULT or NULL
			for _, col := range info.Columns {
				if seen[strings.ToLower(col.Name)] {
					continue
				}
				if col.HasDefault {
					row[col.Index] = col.Default
				} else {
					if col.NotNull {
						return nil, fmt.Errorf("column %q cannot be NULL", col.Name)
					}
					row[col.Index] = nil
				}
			}

			if err := e.storage.Insert(stmt.TableName, row); err != nil {
				return nil, err
			}
			n++
		}
	}

	msg := fmt.Sprintf("%d rows inserted", n)
	if n == 1 {
		msg = "1 row inserted"
	}

	return &Result{Message: msg}, nil
}

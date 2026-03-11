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

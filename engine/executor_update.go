package engine

import (
	"fmt"

	"github.com/walf443/oresql/ast"
)

func (e *Executor) executeUpdate(stmt *ast.UpdateStmt) (*Result, error) {
	info, err := e.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}
	eval := newTableEvaluator(info)

	var allRows []KeyRow
	if keys, indexUsed := e.tryIndexScan(stmt.Where, info); indexUsed {
		allRows, err = e.storage.GetKeyRowsByKeys(info.Name, keys)
	} else {
		allRows, err = e.storage.ScanWithKeys(stmt.TableName)
	}
	if err != nil {
		return nil, err
	}

	// Pipeline: WHERE → ORDER BY → LIMIT
	allRows, err = filterWhere(allRows, stmt.Where, eval, rowOfKeyRow)
	if err != nil {
		return nil, err
	}
	allRows, err = sortRows(allRows, stmt.OrderBy, eval, rowOfKeyRow)
	if err != nil {
		return nil, err
	}
	allRows = applyLimit(allRows, stmt.Limit)

	// Mutation
	updated := 0
	for _, kr := range allRows {
		// Clone the row before modifying
		newRow := make(Row, len(kr.Row))
		copy(newRow, kr.Row)

		for _, set := range stmt.Sets {
			col, err := info.FindColumn(set.Column)
			if err != nil {
				return nil, err
			}

			val, err := evalLiteral(set.Value)
			if err != nil {
				return nil, err
			}

			val, err = validateAndCoerceValue(val, info.Columns[col.Index])
			if err != nil {
				return nil, err
			}

			newRow[col.Index] = val
		}

		if err := e.storage.UpdateRow(stmt.TableName, kr.Key, newRow); err != nil {
			return nil, err
		}
		updated++
	}

	msg := fmt.Sprintf("%d rows updated", updated)
	if updated == 1 {
		msg = "1 row updated"
	}

	return &Result{Message: msg}, nil
}

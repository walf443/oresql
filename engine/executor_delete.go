package engine

import (
	"fmt"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/storage"
)

func (e *Executor) executeDelete(stmt *ast.DeleteStmt) (*Result, error) {
	db, info, err := e.resolveTable(stmt.DatabaseName, stmt.TableName)
	if err != nil {
		return nil, err
	}
	eval := newTableEvaluator(makeSubqueryRunner(e), info)

	var allRows []storage.KeyRow
	if keys, indexUsed := e.tryIndexScan(stmt.Where, info); indexUsed {
		allRows, err = db.storage.GetKeyRowsByKeys(info.Name, keys)
	} else {
		allRows, err = db.storage.ScanWithKeys(stmt.TableName)
	}
	if err != nil {
		return nil, err
	}

	// Pipeline: WHERE → ORDER BY → LIMIT
	if len(stmt.OrderBy) == 0 && stmt.Limit != nil {
		allRows, err = filterWhereLimit(allRows, stmt.Where, eval, rowOfKeyRow, int(*stmt.Limit))
	} else {
		allRows, err = filterWhere(allRows, stmt.Where, eval, rowOfKeyRow)
	}
	if err != nil {
		return nil, err
	}
	if stmt.Limit != nil && len(stmt.OrderBy) > 0 {
		allRows, err = sortRowsTopK(allRows, stmt.OrderBy, eval, rowOfKeyRow, int(*stmt.Limit))
	} else {
		allRows, err = sortRows(allRows, stmt.OrderBy, eval, rowOfKeyRow)
	}
	if err != nil {
		return nil, err
	}
	allRows = applyLimit(allRows, stmt.Limit)

	// Collect keys and delete
	var keysToDelete []int64
	for _, kr := range allRows {
		keysToDelete = append(keysToDelete, kr.Key)
	}

	if err := db.storage.DeleteByKeys(stmt.TableName, keysToDelete); err != nil {
		return nil, err
	}

	deleted := len(keysToDelete)
	msg := fmt.Sprintf("%d rows deleted", deleted)
	if deleted == 1 {
		msg = "1 row deleted"
	}

	return &Result{Message: msg}, nil
}

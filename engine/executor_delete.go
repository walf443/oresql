package engine

import (
	"fmt"

	"github.com/walf443/oresql/ast"
)

func (e *Executor) executeDelete(stmt *ast.DeleteStmt) (*Result, error) {
	info, err := e.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}
	eval := newTableEvaluator(e, info)

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

	// Collect keys and delete
	var keysToDelete []int64
	for _, kr := range allRows {
		keysToDelete = append(keysToDelete, kr.Key)
	}

	if err := e.storage.DeleteByKeys(stmt.TableName, keysToDelete); err != nil {
		return nil, err
	}

	deleted := len(keysToDelete)
	msg := fmt.Sprintf("%d rows deleted", deleted)
	if deleted == 1 {
		msg = "1 row deleted"
	}

	return &Result{Message: msg}, nil
}

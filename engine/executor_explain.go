package engine

import (
	"fmt"
	"strings"

	"github.com/walf443/oresql/ast"
)

// executeExplain generates a query execution plan without executing the query.
func (e *Executor) executeExplain(stmt *ast.ExplainStmt) (*Result, error) {
	columns := []string{"id", "operation", "table", "type", "possible_keys", "key", "extra"}
	colTypes := []string{"INT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT"}

	var rows []Row
	switch s := stmt.Statement.(type) {
	case *ast.SelectStmt:
		rows = e.explainSelect(s)
	case *ast.SetOpStmt:
		rows = e.explainSetOp(s)
	case *ast.WithStmt:
		rows = e.explainWith(s)
	case *ast.InsertStmt:
		rows = e.explainInsert(s)
	case *ast.UpdateStmt:
		rows = e.explainUpdate(s)
	case *ast.DeleteStmt:
		rows = e.explainDelete(s)
	default:
		return &Result{
			Columns:     columns,
			ColumnTypes: colTypes,
			Rows:        []Row{{int64(1), stmt.Statement.NodeType(), "", "", "", "", ""}},
		}, nil
	}

	return &Result{Columns: columns, ColumnTypes: colTypes, Rows: rows}, nil
}

// explainSelect builds explain rows for a SELECT statement using planSelect.
func (e *Executor) explainSelect(stmt *ast.SelectStmt) []Row {
	plan := e.planSelect(stmt)
	return planToRows(plan)
}

// planToRows converts a SelectPlan to EXPLAIN result rows.
func planToRows(plan *SelectPlan) []Row {
	var rows []Row
	id := int64(1)

	tableName := plan.TableName
	if plan.Type == PlanNoTable {
		tableName = ""
	}
	if plan.Type == PlanSubquery {
		tableName = "<subquery>"
	}

	possibleKeys := strings.Join(plan.PossibleKeys, ", ")
	extra := strings.Join(plan.Extras, "; ")

	rows = append(rows, Row{id, "SELECT", tableName, plan.AccessType(), possibleKeys, plan.KeyUsed(), extra})

	// Add JOIN rows
	for i, jp := range plan.JoinPlans {
		joinID := int64(i + 2)
		joinType := fmt.Sprintf("%s JOIN", jp.JoinType)
		joinPossKeys := strings.Join(jp.PossibleKeys, ", ")
		joinExtra := strings.Join(jp.Extras, "; ")
		rows = append(rows, Row{joinID, joinType, jp.TableName, jp.AccessType, joinPossKeys, jp.KeyUsed, joinExtra})
	}

	return rows
}

// explainSetOp builds explain rows for a set operation (UNION/INTERSECT/EXCEPT).
func (e *Executor) explainSetOp(stmt *ast.SetOpStmt) []Row {
	var rows []Row

	// Left side
	switch left := stmt.Left.(type) {
	case *ast.SelectStmt:
		leftRows := e.explainSelect(left)
		rows = append(rows, leftRows...)
	case *ast.SetOpStmt:
		leftRows := e.explainSetOp(left)
		rows = append(rows, leftRows...)
	}

	// Operation row
	op := stmt.Op
	if stmt.All {
		op += " ALL"
	}
	rows = append(rows, Row{int64(len(rows) + 1), op, "", "", "", "", ""})

	// Right side
	rightRows := e.explainSelect(stmt.Right)
	for _, row := range rightRows {
		row[0] = int64(len(rows) + 1)
		rows = append(rows, row)
	}

	return rows
}

// explainWith builds explain rows for a WITH (CTE) statement.
func (e *Executor) explainWith(stmt *ast.WithStmt) []Row {
	var rows []Row
	for _, cte := range stmt.CTEs {
		extra := fmt.Sprintf("CTE: %s", cte.Name)
		if cte.Recursive {
			extra += " (recursive)"
		}
		rows = append(rows, Row{int64(len(rows) + 1), "CTE", cte.Name, "", "", "", extra})
	}

	// Body query
	switch body := stmt.Query.(type) {
	case *ast.SelectStmt:
		bodyRows := e.explainSelect(body)
		for _, row := range bodyRows {
			row[0] = int64(len(rows) + 1)
			rows = append(rows, row)
		}
	case *ast.SetOpStmt:
		bodyRows := e.explainSetOp(body)
		for _, row := range bodyRows {
			row[0] = int64(len(rows) + 1)
			rows = append(rows, row)
		}
	}

	return rows
}

// explainInsert builds explain rows for an INSERT statement.
func (e *Executor) explainInsert(stmt *ast.InsertStmt) []Row {
	extra := ""
	if stmt.Select != nil {
		extra = "Using INSERT ... SELECT"
	} else {
		extra = fmt.Sprintf("%d row(s)", len(stmt.Rows))
	}
	return []Row{{int64(1), "INSERT", stmt.TableName, "", "", "", extra}}
}

// explainUpdate builds explain rows for an UPDATE statement.
func (e *Executor) explainUpdate(stmt *ast.UpdateStmt) []Row {
	accessType := "full scan"
	keyUsed := ""
	possibleKeys := ""
	var extras []string

	db, err := e.resolveDatabase(stmt.DatabaseName)
	if err == nil {
		info, err := db.catalog.GetTable(stmt.TableName)
		if err == nil {
			possKeys := collectPossibleKeys(info, db)
			if len(possKeys) > 0 {
				possibleKeys = strings.Join(possKeys, ", ")
			}

			if stmt.Where != nil {
				if keys, ok := e.tryPrimaryKeyLookup(stmt.Where, info); ok && keys != nil {
					accessType = "const"
					keyUsed = "PRIMARY"
				} else if _, ok := e.tryIndexLookup(stmt.Where, info); ok {
					accessType = "ref"
					keyUsed = findUsedIndexName(stmt.Where, info, db)
				} else if _, ok := e.tryIndexRangeScan(stmt.Where, info); ok {
					accessType = "range"
					keyUsed = findUsedRangeIndexName(stmt.Where, info, db)
				}
			}
		}
	}

	if stmt.Where != nil && accessType == "full scan" {
		extras = append(extras, "Using where")
	}

	extra := strings.Join(extras, "; ")
	return []Row{{int64(1), "UPDATE", stmt.TableName, accessType, possibleKeys, keyUsed, extra}}
}

// explainDelete builds explain rows for a DELETE statement.
func (e *Executor) explainDelete(stmt *ast.DeleteStmt) []Row {
	accessType := "full scan"
	keyUsed := ""
	possibleKeys := ""
	var extras []string

	db, err := e.resolveDatabase(stmt.DatabaseName)
	if err == nil {
		info, err := db.catalog.GetTable(stmt.TableName)
		if err == nil {
			possKeys := collectPossibleKeys(info, db)
			if len(possKeys) > 0 {
				possibleKeys = strings.Join(possKeys, ", ")
			}

			if stmt.Where != nil {
				if keys, ok := e.tryPrimaryKeyLookup(stmt.Where, info); ok && keys != nil {
					accessType = "const"
					keyUsed = "PRIMARY"
				} else if _, ok := e.tryIndexLookup(stmt.Where, info); ok {
					accessType = "ref"
					keyUsed = findUsedIndexName(stmt.Where, info, db)
				} else if _, ok := e.tryIndexRangeScan(stmt.Where, info); ok {
					accessType = "range"
					keyUsed = findUsedRangeIndexName(stmt.Where, info, db)
				}
			}
		}
	}

	if stmt.Where != nil && accessType == "full scan" {
		extras = append(extras, "Using where")
	}

	extra := strings.Join(extras, "; ")
	return []Row{{int64(1), "DELETE", stmt.TableName, accessType, possibleKeys, keyUsed, extra}}
}

// findUsedIndexName finds the name of the index used for equality conditions.
func findUsedIndexName(where ast.Expr, info *TableInfo, db *Database) string {
	eqConds := extractEqualityConditions(where)
	indexes := db.storage.GetIndexes(info.Name)
	for _, idx := range indexes {
		idxInfo := idx.GetInfo()
		allFound := true
		for _, colName := range idxInfo.ColumnNames {
			if _, ok := eqConds[strings.ToLower(colName)]; !ok {
				allFound = false
				break
			}
		}
		if allFound {
			return idxInfo.Name
		}
	}
	return ""
}

// findUsedRangeIndexName finds the name of the index used for range conditions.
func findUsedRangeIndexName(where ast.Expr, info *TableInfo, db *Database) string {
	rangeConds := extractRangeConditions(where)
	for _, rc := range rangeConds {
		col, err := info.FindColumn(rc.colName)
		if err != nil {
			continue
		}
		idx := db.storage.LookupSingleColumnIndex(info.Name, col.Index)
		if idx != nil {
			return idx.GetInfo().Name
		}
	}
	return ""
}

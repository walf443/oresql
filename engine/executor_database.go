package engine

import (
	"fmt"
	"strings"

	"github.com/walf443/oresql/ast"
)

func (e *Executor) executeCreateDatabase(stmt *ast.CreateDatabaseStmt) (*Result, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database management is not enabled")
	}
	if err := e.dbManager.CreateDatabase(stmt.DatabaseName); err != nil {
		return nil, err
	}
	return &Result{Message: "database created"}, nil
}

func (e *Executor) executeDropDatabase(stmt *ast.DropDatabaseStmt) (*Result, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database management is not enabled")
	}
	key := strings.ToLower(stmt.DatabaseName)
	if key == strings.ToLower(e.db.Name) {
		return nil, fmt.Errorf("cannot drop the currently active database %q", key)
	}
	if err := e.dbManager.DropDatabase(stmt.DatabaseName); err != nil {
		return nil, err
	}
	return &Result{Message: "database dropped"}, nil
}

func (e *Executor) executeUseDatabase(stmt *ast.UseDatabaseStmt) (*Result, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database management is not enabled")
	}
	db, err := e.dbManager.GetDatabase(stmt.DatabaseName)
	if err != nil {
		return nil, err
	}
	e.db = db
	return &Result{Message: fmt.Sprintf("switched to database %q", db.Name)}, nil
}

func (e *Executor) executeShowTables(stmt *ast.ShowTablesStmt) (*Result, error) {
	names := e.db.catalog.ListTables()
	rows := make([]Row, len(names))
	for i, name := range names {
		rows[i] = Row{name}
	}
	return &Result{
		Columns:     []string{"table"},
		ColumnTypes: []string{"TEXT"},
		Rows:        rows,
	}, nil
}

func (e *Executor) executeShowDatabases(stmt *ast.ShowDatabasesStmt) (*Result, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database management is not enabled")
	}
	names := e.dbManager.ListDatabases()
	rows := make([]Row, len(names))
	for i, name := range names {
		rows[i] = Row{name}
	}
	return &Result{
		Columns:     []string{"database"},
		ColumnTypes: []string{"TEXT"},
		Rows:        rows,
	}, nil
}

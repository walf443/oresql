package engine

import (
	"fmt"
	"strings"

	"github.com/walf443/oresql/ast"
)

func (e *Executor) executeCreateTable(stmt *ast.CreateTableStmt) (*Result, error) {
	db, err := e.resolveDatabase(stmt.DatabaseName)
	if err != nil {
		return nil, err
	}
	info, err := db.catalog.CreateTable(stmt.TableName, stmt.Columns, stmt.PrimaryKey)
	if err != nil {
		return nil, err
	}
	db.storage.CreateTable(info)

	// Auto-create unique indexes for UNIQUE columns (non-PK)
	for _, cd := range stmt.Columns {
		if cd.Unique && !cd.PrimaryKey {
			col, err := info.FindColumn(cd.Name)
			if err != nil {
				return nil, err
			}
			idxName := fmt.Sprintf("unique_%s_%s", info.Name, strings.ToLower(cd.Name))
			idxInfo := &IndexInfo{
				Name:        idxName,
				TableName:   info.Name,
				ColumnNames: []string{col.Name},
				ColumnIdxs:  []int{col.Index},
				Type:        "BTREE",
				Unique:      true,
			}
			if err := db.storage.CreateIndex(idxInfo); err != nil {
				return nil, err
			}
		}
	}

	// Auto-create unique index for table-level PRIMARY KEY (composite PK)
	if info.PrimaryKeyCol == -1 && len(info.PrimaryKeyCols) > 0 {
		colNames := make([]string, len(info.PrimaryKeyCols))
		for i, idx := range info.PrimaryKeyCols {
			colNames[i] = info.Columns[idx].Name
		}
		idxName := fmt.Sprintf("pk_%s", info.Name)
		idxInfo := &IndexInfo{
			Name:        idxName,
			TableName:   info.Name,
			ColumnNames: colNames,
			ColumnIdxs:  info.PrimaryKeyCols,
			Type:        "BTREE",
			Unique:      true,
		}
		if err := db.storage.CreateIndex(idxInfo); err != nil {
			return nil, err
		}
	}

	return &Result{Message: "table created"}, nil
}

func (e *Executor) executeDropTable(stmt *ast.DropTableStmt) (*Result, error) {
	db, err := e.resolveDatabase(stmt.DatabaseName)
	if err != nil {
		return nil, err
	}
	if err := db.catalog.DropTable(stmt.TableName); err != nil {
		return nil, err
	}
	db.storage.DropTable(stmt.TableName)
	return &Result{Message: "table dropped"}, nil
}

func (e *Executor) executeTruncateTable(stmt *ast.TruncateTableStmt) (*Result, error) {
	db, err := e.resolveDatabase(stmt.DatabaseName)
	if err != nil {
		return nil, err
	}
	if _, err := db.catalog.GetTable(stmt.TableName); err != nil {
		return nil, err
	}
	db.storage.TruncateTable(stmt.TableName)
	return &Result{Message: "table truncated"}, nil
}

func (e *Executor) executeCreateIndex(stmt *ast.CreateIndexStmt) (*Result, error) {
	db, info, err := e.resolveTable(stmt.DatabaseName, stmt.TableName)
	if err != nil {
		return nil, err
	}
	if db.storage.HasIndex(stmt.IndexName) {
		return nil, fmt.Errorf("index %q already exists", stmt.IndexName)
	}
	columnNames := make([]string, len(stmt.ColumnNames))
	columnIdxs := make([]int, len(stmt.ColumnNames))
	for i, name := range stmt.ColumnNames {
		col, err := info.FindColumn(name)
		if err != nil {
			return nil, err
		}
		columnNames[i] = col.Name
		columnIdxs[i] = col.Index
	}
	indexType := "BTREE"
	if stmt.IndexMethod == "GIN" {
		indexType = "GIN"
		// GIN indexes support TEXT and JSONB columns
		for _, name := range stmt.ColumnNames {
			col, _ := info.FindColumn(name)
			if col.DataType != "TEXT" && col.DataType != "JSONB" {
				return nil, fmt.Errorf("GIN index only supports TEXT or JSONB columns, column %q is %s", name, col.DataType)
			}
		}
		if len(stmt.ColumnNames) != 1 {
			return nil, fmt.Errorf("GIN index supports only single column")
		}
	}
	tokenizer := stmt.Tokenizer
	if indexType == "GIN" && tokenizer == "" {
		// Auto-select tokenizer based on column type
		col, _ := info.FindColumn(stmt.ColumnNames[0])
		if col.DataType == "JSONB" {
			tokenizer = "jsonb_path_ops"
		} else {
			tokenizer = "word"
		}
	}
	idxInfo := &IndexInfo{
		Name:        stmt.IndexName,
		TableName:   info.Name,
		ColumnNames: columnNames,
		ColumnIdxs:  columnIdxs,
		Type:        indexType,
		Unique:      stmt.Unique,
		Tokenizer:   tokenizer,
	}
	if err := db.storage.CreateIndex(idxInfo); err != nil {
		return nil, err
	}
	return &Result{Message: "index created"}, nil
}

func (e *Executor) executeDropIndex(stmt *ast.DropIndexStmt) (*Result, error) {
	if err := e.db.storage.DropIndex(stmt.IndexName); err != nil {
		return nil, err
	}
	return &Result{Message: "index dropped"}, nil
}

func (e *Executor) executeAlterTableAddColumn(stmt *ast.AlterTableAddColumnStmt) (*Result, error) {
	db, err := e.resolveDatabase(stmt.DatabaseName)
	if err != nil {
		return nil, err
	}
	info, err := db.catalog.AddColumn(stmt.TableName, stmt.Column)
	if err != nil {
		return nil, err
	}

	// Determine default value for existing rows
	newCol := info.Columns[len(info.Columns)-1]
	var defaultVal Value
	if newCol.HasDefault {
		defaultVal = newCol.Default
	} else {
		if newCol.NotNull {
			return nil, fmt.Errorf("cannot add NOT NULL column %q without DEFAULT to table with existing rows", newCol.Name)
		}
		defaultVal = nil
	}

	if err := db.storage.AddColumn(stmt.TableName, defaultVal); err != nil {
		return nil, err
	}

	// Auto-create unique index for UNIQUE column
	if stmt.Column.Unique && !stmt.Column.PrimaryKey {
		idxName := fmt.Sprintf("unique_%s_%s", info.Name, strings.ToLower(newCol.Name))
		idxInfo := &IndexInfo{
			Name:        idxName,
			TableName:   info.Name,
			ColumnNames: []string{newCol.Name},
			ColumnIdxs:  []int{newCol.Index},
			Type:        "BTREE",
			Unique:      true,
		}
		if err := db.storage.CreateIndex(idxInfo); err != nil {
			return nil, err
		}
	}

	return &Result{Message: "table altered"}, nil
}

func (e *Executor) executeAlterTableDropColumn(stmt *ast.AlterTableDropColumnStmt) (*Result, error) {
	db, err := e.resolveDatabase(stmt.DatabaseName)
	if err != nil {
		return nil, err
	}
	droppedCol, _, err := db.catalog.DropColumn(stmt.TableName, stmt.ColumnName)
	if err != nil {
		return nil, err
	}

	if err := db.storage.DropColumn(stmt.TableName, droppedCol.Index); err != nil {
		return nil, err
	}
	return &Result{Message: "table altered"}, nil
}

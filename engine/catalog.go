package engine

import (
	"fmt"
	"strings"

	"github.com/walf443/oresql/ast"
)

// ColumnInfo describes a column in a table.
type ColumnInfo struct {
	Name     string
	DataType string // "INT" or "TEXT"
	Index    int    // ordinal position in the row
}

// TableInfo describes a table's schema.
type TableInfo struct {
	Name    string
	Columns []ColumnInfo
}

// FindColumn returns the column info for the given name, or an error if not found.
func (t *TableInfo) FindColumn(name string) (*ColumnInfo, error) {
	lower := strings.ToLower(name)
	for i := range t.Columns {
		if strings.ToLower(t.Columns[i].Name) == lower {
			return &t.Columns[i], nil
		}
	}
	return nil, fmt.Errorf("column %q not found in table %q", name, t.Name)
}

// Catalog holds all table schemas.
type Catalog struct {
	tables map[string]*TableInfo // key: lowercase table name
}

func NewCatalog() *Catalog {
	return &Catalog{tables: make(map[string]*TableInfo)}
}

func (c *Catalog) CreateTable(name string, columnDefs []ast.ColumnDef) (*TableInfo, error) {
	lower := strings.ToLower(name)
	if _, exists := c.tables[lower]; exists {
		return nil, fmt.Errorf("table %q already exists", name)
	}

	columns := make([]ColumnInfo, len(columnDefs))
	for i, cd := range columnDefs {
		columns[i] = ColumnInfo{
			Name:     cd.Name,
			DataType: cd.DataType,
			Index:    i,
		}
	}

	info := &TableInfo{Name: lower, Columns: columns}
	c.tables[lower] = info
	return info, nil
}

func (c *Catalog) GetTable(name string) (*TableInfo, error) {
	lower := strings.ToLower(name)
	info, ok := c.tables[lower]
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", name)
	}
	return info, nil
}

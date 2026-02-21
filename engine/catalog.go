package engine

import (
	"fmt"
	"strings"

	"github.com/walf443/oresql/ast"
)

// ColumnInfo describes a column in a table.
type ColumnInfo struct {
	Name       string
	DataType   string // "INT" or "TEXT"
	Index      int    // ordinal position in the row
	NotNull    bool
	PrimaryKey bool
	HasDefault bool  // true if DEFAULT clause was specified
	Default    Value // default value (nil means NULL default)
}

// TableInfo describes a table's schema.
type TableInfo struct {
	Name          string
	Columns       []ColumnInfo
	PrimaryKeyCol int // index of PK column, -1 if no PK
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

// IndexInfo describes a secondary index on a table.
type IndexInfo struct {
	Name       string
	TableName  string
	ColumnName string
	ColumnIdx  int
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

	// Validate PRIMARY KEY constraints
	pkCol := -1
	for i, cd := range columnDefs {
		if cd.PrimaryKey {
			if pkCol >= 0 {
				return nil, fmt.Errorf("multiple PRIMARY KEY columns are not allowed")
			}
			if cd.DataType != "INT" {
				return nil, fmt.Errorf("PRIMARY KEY must be INT type, got %s", cd.DataType)
			}
			pkCol = i
		}
	}

	columns := make([]ColumnInfo, len(columnDefs))
	for i, cd := range columnDefs {
		col := ColumnInfo{
			Name:       cd.Name,
			DataType:   cd.DataType,
			Index:      i,
			NotNull:    cd.NotNull,
			PrimaryKey: cd.PrimaryKey,
		}
		if cd.Default != nil {
			col.HasDefault = true
			val, err := evalLiteral(cd.Default)
			if err != nil {
				return nil, fmt.Errorf("invalid DEFAULT for column %q: %w", cd.Name, err)
			}
			if val == nil {
				if cd.NotNull {
					return nil, fmt.Errorf("column %q is NOT NULL but DEFAULT is NULL", cd.Name)
				}
			} else {
				switch cd.DataType {
				case "INT":
					if _, ok := val.(int64); !ok {
						return nil, fmt.Errorf("column %q expects INT, DEFAULT value is %T", cd.Name, val)
					}
				case "FLOAT":
					switch v := val.(type) {
					case float64:
						// ok
					case int64:
						val = float64(v)
					default:
						return nil, fmt.Errorf("column %q expects FLOAT, DEFAULT value is %T", cd.Name, val)
					}
				case "TEXT":
					if _, ok := val.(string); !ok {
						return nil, fmt.Errorf("column %q expects TEXT, DEFAULT value is %T", cd.Name, val)
					}
				}
			}
			col.Default = val
		}
		columns[i] = col
	}

	info := &TableInfo{Name: lower, Columns: columns, PrimaryKeyCol: pkCol}
	c.tables[lower] = info
	return info, nil
}

func (c *Catalog) DropTable(name string) error {
	lower := strings.ToLower(name)
	if _, exists := c.tables[lower]; !exists {
		return fmt.Errorf("table %q does not exist", name)
	}
	delete(c.tables, lower)
	return nil
}

func (c *Catalog) GetTable(name string) (*TableInfo, error) {
	lower := strings.ToLower(name)
	info, ok := c.tables[lower]
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", name)
	}
	return info, nil
}

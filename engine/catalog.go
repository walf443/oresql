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
	Name           string
	Columns        []ColumnInfo
	PrimaryKeyCol  int   // index of single INT PK column, -1 if no PK or composite PK
	PrimaryKeyCols []int // all PK column indexes (nil if no PK)
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
	Name        string
	TableName   string
	ColumnNames []string
	ColumnIdxs  []int
	Type        string // "BTREE" or "HASH"
	Unique      bool
}

// Catalog holds all table schemas.
type Catalog struct {
	tables map[string]*TableInfo // key: lowercase table name
}

func NewCatalog() *Catalog {
	return &Catalog{tables: make(map[string]*TableInfo)}
}

func (c *Catalog) CreateTable(name string, columnDefs []ast.ColumnDef, tablePK []string) (*TableInfo, error) {
	lower := strings.ToLower(name)
	if _, exists := c.tables[lower]; exists {
		return nil, fmt.Errorf("table %q already exists", name)
	}

	// Validate column-level PRIMARY KEY constraints
	pkCol := -1
	hasColumnLevelPK := false
	for i, cd := range columnDefs {
		if cd.PrimaryKey {
			if pkCol >= 0 {
				return nil, fmt.Errorf("multiple PRIMARY KEY columns are not allowed")
			}
			if cd.DataType == "INT" {
				pkCol = i // INT PK uses BTree key directly
			} else {
				pkCol = i // temporary; will be reset to -1 below for non-INT
			}
			hasColumnLevelPK = true
		}
	}

	// Reject combining column-level and table-level PK
	if hasColumnLevelPK && len(tablePK) > 0 {
		return nil, fmt.Errorf("cannot specify both column-level and table-level PRIMARY KEY")
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

	var primaryKeyCols []int

	if len(tablePK) > 0 {
		// Table-level PRIMARY KEY
		pkCol = -1 // use auto-increment
		for _, pkName := range tablePK {
			found := false
			pkLower := strings.ToLower(pkName)
			for i := range columns {
				if strings.ToLower(columns[i].Name) == pkLower {
					primaryKeyCols = append(primaryKeyCols, i)
					columns[i].PrimaryKey = true
					columns[i].NotNull = true
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("column %q in PRIMARY KEY not found", pkName)
			}
		}
	} else if hasColumnLevelPK {
		primaryKeyCols = []int{pkCol}
		// Non-INT PK uses auto-increment + unique index (same as composite PK)
		if columns[pkCol].DataType != "INT" {
			pkCol = -1
		}
	}

	info := &TableInfo{Name: lower, Columns: columns, PrimaryKeyCol: pkCol, PrimaryKeyCols: primaryKeyCols}
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

func (c *Catalog) AddColumn(tableName string, colDef ast.ColumnDef) (*TableInfo, error) {
	lower := strings.ToLower(tableName)
	info, ok := c.tables[lower]
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", tableName)
	}

	// Check duplicate column name
	colLower := strings.ToLower(colDef.Name)
	for _, col := range info.Columns {
		if strings.ToLower(col.Name) == colLower {
			return nil, fmt.Errorf("column %q already exists in table %q", colDef.Name, tableName)
		}
	}

	// PRIMARY KEY via ALTER TABLE ADD COLUMN is not allowed
	if colDef.PrimaryKey {
		return nil, fmt.Errorf("cannot add PRIMARY KEY column via ALTER TABLE")
	}

	col := ColumnInfo{
		Name:       colDef.Name,
		DataType:   colDef.DataType,
		Index:      len(info.Columns),
		NotNull:    colDef.NotNull,
		PrimaryKey: false,
	}

	if colDef.Default != nil {
		col.HasDefault = true
		val, err := evalLiteral(colDef.Default)
		if err != nil {
			return nil, fmt.Errorf("invalid DEFAULT for column %q: %w", colDef.Name, err)
		}
		if val == nil {
			if colDef.NotNull {
				return nil, fmt.Errorf("column %q is NOT NULL but DEFAULT is NULL", colDef.Name)
			}
		} else {
			switch colDef.DataType {
			case "INT":
				if _, ok := val.(int64); !ok {
					return nil, fmt.Errorf("column %q expects INT, DEFAULT value is %T", colDef.Name, val)
				}
			case "FLOAT":
				switch v := val.(type) {
				case float64:
					// ok
				case int64:
					val = float64(v)
				default:
					return nil, fmt.Errorf("column %q expects FLOAT, DEFAULT value is %T", colDef.Name, val)
				}
			case "TEXT":
				if _, ok := val.(string); !ok {
					return nil, fmt.Errorf("column %q expects TEXT, DEFAULT value is %T", colDef.Name, val)
				}
			}
		}
		col.Default = val
	}

	info.Columns = append(info.Columns, col)
	return info, nil
}

func (c *Catalog) DropColumn(tableName string, columnName string) (*ColumnInfo, *TableInfo, error) {
	lower := strings.ToLower(tableName)
	info, ok := c.tables[lower]
	if !ok {
		return nil, nil, fmt.Errorf("table %q does not exist", tableName)
	}

	// Find the column
	colLower := strings.ToLower(columnName)
	colIdx := -1
	for i, col := range info.Columns {
		if strings.ToLower(col.Name) == colLower {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		return nil, nil, fmt.Errorf("column %q not found in table %q", columnName, tableName)
	}

	// Cannot drop PK column
	if info.Columns[colIdx].PrimaryKey {
		return nil, nil, fmt.Errorf("cannot drop PRIMARY KEY column %q", columnName)
	}

	// Cannot drop last column
	if len(info.Columns) <= 1 {
		return nil, nil, fmt.Errorf("cannot drop the only column in table %q", tableName)
	}

	droppedCol := info.Columns[colIdx]

	// Remove column from slice
	info.Columns = append(info.Columns[:colIdx], info.Columns[colIdx+1:]...)

	// Re-index columns
	for i := range info.Columns {
		info.Columns[i].Index = i
	}

	// Update PrimaryKeyCol
	if info.PrimaryKeyCol >= 0 {
		if info.PrimaryKeyCol > colIdx {
			info.PrimaryKeyCol--
		}
	}

	// Update PrimaryKeyCols
	if len(info.PrimaryKeyCols) > 0 {
		newPKCols := make([]int, 0, len(info.PrimaryKeyCols))
		for _, idx := range info.PrimaryKeyCols {
			if idx > colIdx {
				newPKCols = append(newPKCols, idx-1)
			} else {
				newPKCols = append(newPKCols, idx)
			}
		}
		info.PrimaryKeyCols = newPKCols
	}

	return &droppedCol, info, nil
}

func (c *Catalog) GetTable(name string) (*TableInfo, error) {
	lower := strings.ToLower(name)
	info, ok := c.tables[lower]
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", name)
	}
	return info, nil
}

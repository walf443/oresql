package engine

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/walf443/oresql/ast"
)

// Catalog holds all table schemas.
type Catalog struct {
	mu     sync.RWMutex
	tables map[string]*TableInfo // key: lowercase table name
}

func NewCatalog() *Catalog {
	return &Catalog{tables: make(map[string]*TableInfo)}
}

func (c *Catalog) CreateTable(name string, columnDefs []ast.ColumnDef, tablePK []string) (*TableInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	lower := strings.ToLower(name)
	if _, exists := c.tables[lower]; exists {
		return nil, fmt.Errorf("table %q already exists", name)
	}

	pkCol, hasColumnLevelPK, err := validateColumnLevelPK(columnDefs, tablePK)
	if err != nil {
		return nil, err
	}

	columns, err := buildColumnInfos(columnDefs)
	if err != nil {
		return nil, err
	}

	pkCol, primaryKeyCols, err := resolvePrimaryKey(columns, tablePK, pkCol, hasColumnLevelPK)
	if err != nil {
		return nil, err
	}

	info := &TableInfo{Name: lower, Columns: columns, PrimaryKeyCol: pkCol, PrimaryKeyCols: primaryKeyCols}
	c.tables[lower] = info
	return info, nil
}

// validateColumnLevelPK checks column-level PRIMARY KEY constraints.
// Returns (pkColIdx, hasColumnLevelPK, error).
func validateColumnLevelPK(columnDefs []ast.ColumnDef, tablePK []string) (int, bool, error) {
	pkCol := -1
	hasColumnLevelPK := false
	for i, cd := range columnDefs {
		if cd.PrimaryKey {
			if pkCol >= 0 {
				return -1, false, fmt.Errorf("multiple PRIMARY KEY columns are not allowed")
			}
			pkCol = i
			hasColumnLevelPK = true
		}
	}
	if hasColumnLevelPK && len(tablePK) > 0 {
		return -1, false, fmt.Errorf("cannot specify both column-level and table-level PRIMARY KEY")
	}
	return pkCol, hasColumnLevelPK, nil
}

// buildColumnInfos creates ColumnInfo slice from column definitions, validating DEFAULT values.
func buildColumnInfos(columnDefs []ast.ColumnDef) ([]ColumnInfo, error) {
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
			val, err := validateDefault(cd.Name, cd.DataType, cd.NotNull, cd.Default)
			if err != nil {
				return nil, err
			}
			col.HasDefault = true
			col.Default = val
		}
		columns[i] = col
	}
	return columns, nil
}

// validateDefault evaluates and type-checks a DEFAULT expression for a column.
func validateDefault(colName, dataType string, notNull bool, defaultExpr ast.Expr) (Value, error) {
	val, err := evalLiteral(defaultExpr)
	if err != nil {
		return nil, fmt.Errorf("invalid DEFAULT for column %q: %w", colName, err)
	}
	if val == nil {
		if notNull {
			return nil, fmt.Errorf("column %q is NOT NULL but DEFAULT is NULL", colName)
		}
		return nil, nil
	}
	return coerceDefaultValue(colName, dataType, val)
}

// coerceDefaultValue checks that a non-nil DEFAULT value matches the column type.
func coerceDefaultValue(colName, dataType string, val Value) (Value, error) {
	switch dataType {
	case "INT":
		if _, ok := val.(int64); !ok {
			return nil, fmt.Errorf("column %q expects INT, DEFAULT value is %T", colName, val)
		}
	case "FLOAT":
		switch v := val.(type) {
		case float64:
			// ok
		case int64:
			val = float64(v)
		default:
			return nil, fmt.Errorf("column %q expects FLOAT, DEFAULT value is %T", colName, val)
		}
	case "TEXT":
		if _, ok := val.(string); !ok {
			return nil, fmt.Errorf("column %q expects TEXT, DEFAULT value is %T", colName, val)
		}
	case "JSON":
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("column %q expects JSON, DEFAULT value is %T", colName, val)
		}
		if !json.Valid([]byte(s)) {
			return nil, fmt.Errorf("column %q: invalid JSON DEFAULT value: %s", colName, s)
		}
	case "JSONB":
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("column %q expects JSONB, DEFAULT value is %T", colName, val)
		}
		if !json.Valid([]byte(s)) {
			return nil, fmt.Errorf("column %q: invalid JSONB DEFAULT value: %s", colName, s)
		}
	}
	return val, nil
}

// resolvePrimaryKey resolves table-level or column-level PRIMARY KEY into pkCol and primaryKeyCols.
func resolvePrimaryKey(columns []ColumnInfo, tablePK []string, pkCol int, hasColumnLevelPK bool) (int, []int, error) {
	if len(tablePK) > 0 {
		return resolveTableLevelPK(columns, tablePK)
	}
	if hasColumnLevelPK {
		primaryKeyCols := []int{pkCol}
		if columns[pkCol].DataType != "INT" {
			pkCol = -1
		}
		return pkCol, primaryKeyCols, nil
	}
	return pkCol, nil, nil
}

// resolveTableLevelPK resolves a table-level PRIMARY KEY constraint.
func resolveTableLevelPK(columns []ColumnInfo, tablePK []string) (int, []int, error) {
	var primaryKeyCols []int
	for _, pkName := range tablePK {
		pkLower := strings.ToLower(pkName)
		found := false
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
			return -1, nil, fmt.Errorf("column %q in PRIMARY KEY not found", pkName)
		}
	}
	return -1, primaryKeyCols, nil
}

func (c *Catalog) DropTable(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	lower := strings.ToLower(name)
	if _, exists := c.tables[lower]; !exists {
		return fmt.Errorf("table %q does not exist", name)
	}
	delete(c.tables, lower)
	return nil
}

func (c *Catalog) AddColumn(tableName string, colDef ast.ColumnDef) (*TableInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
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
		val, err := validateDefault(colDef.Name, colDef.DataType, colDef.NotNull, colDef.Default)
		if err != nil {
			return nil, err
		}
		col.HasDefault = true
		col.Default = val
	}

	info.Columns = append(info.Columns, col)
	return info, nil
}

func (c *Catalog) DropColumn(tableName string, columnName string) (*ColumnInfo, *TableInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
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

// RestoreTable inserts a TableInfo directly into the catalog (used for loading from disk).
func (c *Catalog) RestoreTable(info *TableInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tables[info.Name] = info
}

// ListTables returns a sorted list of table names.
func (c *Catalog) ListTables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.tables))
	for name := range c.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (c *Catalog) GetTable(name string) (*TableInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	lower := strings.ToLower(name)
	info, ok := c.tables[lower]
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", name)
	}
	return info, nil
}

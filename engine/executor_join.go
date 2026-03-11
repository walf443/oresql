package engine

import (
	"fmt"
	"strings"
)

// tableRange maps a table to its column offset within a merged row.
type tableRange struct {
	info     *TableInfo
	startIdx int
}

// JoinContext tracks column information across multiple joined tables.
type JoinContext struct {
	MergedInfo   *TableInfo            // virtual TableInfo with all columns concatenated
	tableMap     map[string]tableRange // table name/alias → column offset
	tableNames   []string              // ordered table names for ambiguity detection
	usingExclude map[int]bool          // MergedInfo column indices excluded from SELECT *
}

// newJoinContext creates a JoinContext from a list of (TableInfo, alias) pairs.
// usingCols maps table effective name (lowercase) to USING column names to exclude from SELECT *.
func newJoinContext(tables []struct {
	info  *TableInfo
	alias string
}, usingCols map[string][]string) *JoinContext {
	jc := &JoinContext{
		MergedInfo:   &TableInfo{Name: "joined"},
		tableMap:     make(map[string]tableRange),
		usingExclude: make(map[int]bool),
	}
	offset := 0
	for _, t := range tables {
		tr := tableRange{info: t.info, startIdx: offset}
		// Register by real table name
		jc.tableMap[strings.ToLower(t.info.Name)] = tr
		// Register by alias if present
		if t.alias != "" {
			jc.tableMap[strings.ToLower(t.alias)] = tr
		}
		jc.tableNames = append(jc.tableNames, strings.ToLower(t.info.Name))

		// Build USING exclude set for this table
		effName := strings.ToLower(t.info.Name)
		if t.alias != "" {
			effName = strings.ToLower(t.alias)
		}
		var excludeSet map[string]bool
		if cols, ok := usingCols[effName]; ok {
			excludeSet = make(map[string]bool, len(cols))
			for _, c := range cols {
				excludeSet[strings.ToLower(c)] = true
			}
		}

		for _, col := range t.info.Columns {
			mergedIdx := offset + col.Index
			mergedCol := ColumnInfo{
				Name:     col.Name,
				DataType: col.DataType,
				Index:    mergedIdx,
				NotNull:  col.NotNull,
			}
			jc.MergedInfo.Columns = append(jc.MergedInfo.Columns, mergedCol)
			if excludeSet != nil && excludeSet[strings.ToLower(col.Name)] {
				jc.usingExclude[mergedIdx] = true
			}
		}
		offset += len(t.info.Columns)
	}
	return jc
}

// StarColumnList returns columns for SELECT * expansion,
// excluding USING columns from right tables.
func (jc *JoinContext) StarColumnList() []ColumnInfo {
	if len(jc.usingExclude) == 0 {
		return jc.MergedInfo.Columns
	}
	cols := make([]ColumnInfo, 0, len(jc.MergedInfo.Columns)-len(jc.usingExclude))
	for _, col := range jc.MergedInfo.Columns {
		if !jc.usingExclude[col.Index] {
			cols = append(cols, col)
		}
	}
	return cols
}

// FindColumn resolves a column reference in the join context.
// If tableName is specified, looks only in that table.
// If tableName is empty, searches all tables and errors on ambiguity.
func (jc *JoinContext) FindColumn(tableName, colName string) (*ColumnInfo, error) {
	lower := strings.ToLower(colName)
	if tableName != "" {
		tr, ok := jc.tableMap[strings.ToLower(tableName)]
		if !ok {
			return nil, fmt.Errorf("unknown table %q", tableName)
		}
		for i := range tr.info.Columns {
			if strings.ToLower(tr.info.Columns[i].Name) == lower {
				idx := tr.startIdx + tr.info.Columns[i].Index
				return &ColumnInfo{
					Name:     tr.info.Columns[i].Name,
					DataType: tr.info.Columns[i].DataType,
					Index:    idx,
				}, nil
			}
		}
		return nil, fmt.Errorf("column %q not found in table %q", colName, tableName)
	}

	// Unqualified: search all tables
	var found *ColumnInfo
	matchCount := 0
	for _, tName := range jc.tableNames {
		tr := jc.tableMap[tName]
		for i := range tr.info.Columns {
			if strings.ToLower(tr.info.Columns[i].Name) == lower {
				matchCount++
				idx := tr.startIdx + tr.info.Columns[i].Index
				found = &ColumnInfo{
					Name:     tr.info.Columns[i].Name,
					DataType: tr.info.Columns[i].DataType,
					Index:    idx,
				}
			}
		}
	}
	if matchCount == 0 {
		return nil, fmt.Errorf("column %q not found", colName)
	}
	if matchCount > 1 {
		return nil, fmt.Errorf("ambiguous column %q", colName)
	}
	return found, nil
}

// validateTableRefWithAlias checks that a qualified table reference matches the target table or its alias.
func validateTableRefWithAlias(tableRef, targetTable, alias string) error {
	if tableRef == "" {
		return nil
	}
	lower := strings.ToLower(tableRef)
	if lower == strings.ToLower(targetTable) {
		return nil
	}
	if alias != "" && lower == strings.ToLower(alias) {
		return nil
	}
	return fmt.Errorf("unknown table %q", tableRef)
}

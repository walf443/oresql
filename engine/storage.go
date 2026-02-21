package engine

import (
	"fmt"
	"strings"
)

// Value represents a stored value.
type Value = any

// Row is a single row of data.
type Row = []Value

// Table stores rows for a single table.
type Table struct {
	Info *TableInfo
	Rows []Row
}

// Storage holds all table data.
type Storage struct {
	tables map[string]*Table // key: lowercase table name
}

func NewStorage() *Storage {
	return &Storage{tables: make(map[string]*Table)}
}

func (s *Storage) CreateTable(info *TableInfo) {
	s.tables[info.Name] = &Table{Info: info}
}

func (s *Storage) Insert(tableName string, row Row) error {
	lower := strings.ToLower(tableName)
	tbl, ok := s.tables[lower]
	if !ok {
		return fmt.Errorf("table %q does not exist in storage", tableName)
	}
	tbl.Rows = append(tbl.Rows, row)
	return nil
}

func (s *Storage) DeleteRows(tableName string, keepIndices map[int]bool) error {
	lower := strings.ToLower(tableName)
	tbl, ok := s.tables[lower]
	if !ok {
		return fmt.Errorf("table %q does not exist in storage", tableName)
	}
	var kept []Row
	for i, row := range tbl.Rows {
		if keepIndices[i] {
			kept = append(kept, row)
		}
	}
	tbl.Rows = kept
	return nil
}

func (s *Storage) TruncateTable(name string) {
	lower := strings.ToLower(name)
	if tbl, ok := s.tables[lower]; ok {
		tbl.Rows = nil
	}
}

func (s *Storage) DropTable(name string) {
	lower := strings.ToLower(name)
	delete(s.tables, lower)
}

func (s *Storage) Scan(tableName string) ([]Row, error) {
	lower := strings.ToLower(tableName)
	tbl, ok := s.tables[lower]
	if !ok {
		return nil, fmt.Errorf("table %q does not exist in storage", tableName)
	}
	return tbl.Rows, nil
}

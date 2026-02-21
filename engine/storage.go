package engine

import (
	"fmt"
	"strings"

	"github.com/walf443/oresql/btree"
)

// Value represents a stored value.
type Value = any

// Row is a single row of data.
type Row = []Value

// KeyRow is a row with its BTree key.
type KeyRow struct {
	Key int64
	Row Row
}

// Table stores rows for a single table using a BTree.
type Table struct {
	Info      *TableInfo
	tree      *btree.BTree
	nextRowID int64 // auto-increment for non-PK tables
}

// Storage holds all table data.
type Storage struct {
	tables map[string]*Table // key: lowercase table name
}

func NewStorage() *Storage {
	return &Storage{tables: make(map[string]*Table)}
}

func (s *Storage) CreateTable(info *TableInfo) {
	s.tables[info.Name] = &Table{
		Info:      info,
		tree:      btree.New(32),
		nextRowID: 1,
	}
}

func (s *Storage) Insert(tableName string, row Row) error {
	lower := strings.ToLower(tableName)
	tbl, ok := s.tables[lower]
	if !ok {
		return fmt.Errorf("table %q does not exist in storage", tableName)
	}

	var key int64
	if tbl.Info.PrimaryKeyCol >= 0 {
		// Use PK column value as key
		pkVal := row[tbl.Info.PrimaryKeyCol]
		key = pkVal.(int64)
		if !tbl.tree.Insert(key, row) {
			return fmt.Errorf("duplicate primary key value: %d", key)
		}
	} else {
		// Use auto-increment rowID
		key = tbl.nextRowID
		tbl.nextRowID++
		tbl.tree.Insert(key, row)
	}
	return nil
}

// DeleteByKeys deletes rows by their BTree keys.
func (s *Storage) DeleteByKeys(tableName string, keys []int64) error {
	lower := strings.ToLower(tableName)
	tbl, ok := s.tables[lower]
	if !ok {
		return fmt.Errorf("table %q does not exist in storage", tableName)
	}
	for _, key := range keys {
		tbl.tree.Delete(key)
	}
	return nil
}

// UpdateRow updates a single row by its BTree key.
func (s *Storage) UpdateRow(tableName string, key int64, row Row) error {
	lower := strings.ToLower(tableName)
	tbl, ok := s.tables[lower]
	if !ok {
		return fmt.Errorf("table %q does not exist in storage", tableName)
	}
	tbl.tree.Put(key, row)
	return nil
}

func (s *Storage) TruncateTable(name string) {
	lower := strings.ToLower(name)
	if tbl, ok := s.tables[lower]; ok {
		tbl.tree = btree.New(32)
		tbl.nextRowID = 1
	}
}

func (s *Storage) DropTable(name string) {
	lower := strings.ToLower(name)
	delete(s.tables, lower)
}

// Scan returns all rows in key order.
func (s *Storage) Scan(tableName string) ([]Row, error) {
	lower := strings.ToLower(tableName)
	tbl, ok := s.tables[lower]
	if !ok {
		return nil, fmt.Errorf("table %q does not exist in storage", tableName)
	}
	var rows []Row
	tbl.tree.ForEach(func(key int64, value any) bool {
		rows = append(rows, value.(Row))
		return true
	})
	return rows, nil
}

// ScanWithKeys returns all rows with their BTree keys in key order.
func (s *Storage) ScanWithKeys(tableName string) ([]KeyRow, error) {
	lower := strings.ToLower(tableName)
	tbl, ok := s.tables[lower]
	if !ok {
		return nil, fmt.Errorf("table %q does not exist in storage", tableName)
	}
	var rows []KeyRow
	tbl.tree.ForEach(func(key int64, value any) bool {
		rows = append(rows, KeyRow{Key: key, Row: value.(Row)})
		return true
	})
	return rows, nil
}

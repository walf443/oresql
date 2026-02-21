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

// SecondaryIndex is a hash-based secondary index on a single column.
type SecondaryIndex struct {
	Info    *IndexInfo
	entries map[string]map[int64]struct{} // encoded value -> set of BTree keys
}

// Lookup returns the BTree keys matching the given value.
func (si *SecondaryIndex) Lookup(val Value) []int64 {
	if val == nil {
		return nil
	}
	encoded := fmt.Sprintf("%v", val)
	keySet, ok := si.entries[encoded]
	if !ok {
		return nil
	}
	keys := make([]int64, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	return keys
}

func (si *SecondaryIndex) add(key int64, val Value) {
	if val == nil {
		return
	}
	encoded := fmt.Sprintf("%v", val)
	if si.entries[encoded] == nil {
		si.entries[encoded] = make(map[int64]struct{})
	}
	si.entries[encoded][key] = struct{}{}
}

func (si *SecondaryIndex) remove(key int64, val Value) {
	if val == nil {
		return
	}
	encoded := fmt.Sprintf("%v", val)
	if keySet, ok := si.entries[encoded]; ok {
		delete(keySet, key)
		if len(keySet) == 0 {
			delete(si.entries, encoded)
		}
	}
}

// Table stores rows for a single table using a BTree.
type Table struct {
	Info      *TableInfo
	tree      *btree.BTree
	nextRowID int64 // auto-increment for non-PK tables
	indexes   map[string]*SecondaryIndex
}

// Storage holds all table data.
type Storage struct {
	tables     map[string]*Table // key: lowercase table name
	indexTable map[string]string // index name -> table name
}

func NewStorage() *Storage {
	return &Storage{
		tables:     make(map[string]*Table),
		indexTable: make(map[string]string),
	}
}

func (s *Storage) CreateTable(info *TableInfo) {
	s.tables[info.Name] = &Table{
		Info:      info,
		tree:      btree.New(32),
		nextRowID: 1,
		indexes:   make(map[string]*SecondaryIndex),
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

	// Update secondary indexes
	for _, idx := range tbl.indexes {
		idx.add(key, row[idx.Info.ColumnIdx])
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
		// Remove from indexes before deleting
		if len(tbl.indexes) > 0 {
			val, found := tbl.tree.Get(key)
			if found {
				row := val.(Row)
				for _, idx := range tbl.indexes {
					idx.remove(key, row[idx.Info.ColumnIdx])
				}
			}
		}
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

	// Remove old index entries
	if len(tbl.indexes) > 0 {
		oldVal, found := tbl.tree.Get(key)
		if found {
			oldRow := oldVal.(Row)
			for _, idx := range tbl.indexes {
				idx.remove(key, oldRow[idx.Info.ColumnIdx])
			}
		}
	}

	tbl.tree.Put(key, row)

	// Add new index entries
	for _, idx := range tbl.indexes {
		idx.add(key, row[idx.Info.ColumnIdx])
	}

	return nil
}

func (s *Storage) TruncateTable(name string) {
	lower := strings.ToLower(name)
	if tbl, ok := s.tables[lower]; ok {
		tbl.tree = btree.New(32)
		tbl.nextRowID = 1
		// Clear index entries but keep index structure
		for _, idx := range tbl.indexes {
			idx.entries = make(map[string]map[int64]struct{})
		}
	}
}

func (s *Storage) DropTable(name string) {
	lower := strings.ToLower(name)
	if tbl, ok := s.tables[lower]; ok {
		// Clean up index registry
		for idxName := range tbl.indexes {
			delete(s.indexTable, strings.ToLower(idxName))
		}
		delete(s.tables, lower)
	}
}

// CreateIndex creates a secondary index and builds it from existing data.
func (s *Storage) CreateIndex(info *IndexInfo) error {
	lower := strings.ToLower(info.TableName)
	tbl, ok := s.tables[lower]
	if !ok {
		return fmt.Errorf("table %q does not exist in storage", info.TableName)
	}

	idx := &SecondaryIndex{
		Info:    info,
		entries: make(map[string]map[int64]struct{}),
	}

	// Build index from existing data
	tbl.tree.ForEach(func(key int64, value any) bool {
		row := value.(Row)
		idx.add(key, row[info.ColumnIdx])
		return true
	})

	tbl.indexes[strings.ToLower(info.Name)] = idx
	s.indexTable[strings.ToLower(info.Name)] = lower
	return nil
}

// DropIndex removes a secondary index.
func (s *Storage) DropIndex(indexName string) error {
	lowerIdx := strings.ToLower(indexName)
	tableName, ok := s.indexTable[lowerIdx]
	if !ok {
		return fmt.Errorf("index %q does not exist", indexName)
	}
	tbl := s.tables[tableName]
	delete(tbl.indexes, lowerIdx)
	delete(s.indexTable, lowerIdx)
	return nil
}

// LookupIndex finds a secondary index on the given table and column index.
func (s *Storage) LookupIndex(tableName string, columnIdx int) *SecondaryIndex {
	lower := strings.ToLower(tableName)
	tbl, ok := s.tables[lower]
	if !ok {
		return nil
	}
	for _, idx := range tbl.indexes {
		if idx.Info.ColumnIdx == columnIdx {
			return idx
		}
	}
	return nil
}

// HasIndex checks if an index with the given name exists.
func (s *Storage) HasIndex(indexName string) bool {
	_, ok := s.indexTable[strings.ToLower(indexName)]
	return ok
}

// GetByKeys retrieves rows by their BTree keys.
func (s *Storage) GetByKeys(tableName string, keys []int64) ([]Row, error) {
	lower := strings.ToLower(tableName)
	tbl, ok := s.tables[lower]
	if !ok {
		return nil, fmt.Errorf("table %q does not exist in storage", tableName)
	}
	var rows []Row
	for _, key := range keys {
		val, found := tbl.tree.Get(key)
		if found {
			rows = append(rows, val.(Row))
		}
	}
	return rows, nil
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

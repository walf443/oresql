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

// SecondaryIndex is a BTree-based secondary index on one or more columns.
type SecondaryIndex struct {
	Info *IndexInfo
	tree *btree.StringBTree // encoded value -> map[int64]struct{} (set of BTree keys)
}

// encodeCompositeKey encodes multiple column values into a single string key.
// Returns empty string and false if any value is nil (NULL).
func encodeCompositeKey(row Row, columnIdxs []int) (string, bool) {
	parts := make([]string, len(columnIdxs))
	for i, idx := range columnIdxs {
		val := row[idx]
		if val == nil {
			return "", false
		}
		parts[i] = fmt.Sprintf("%v", val)
	}
	return strings.Join(parts, "\x00"), true
}

// Lookup returns the BTree keys matching the given composite values.
func (si *SecondaryIndex) Lookup(vals []Value) []int64 {
	for _, v := range vals {
		if v == nil {
			return nil
		}
	}
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = fmt.Sprintf("%v", v)
	}
	encoded := strings.Join(parts, "\x00")
	val, ok := si.tree.Get(encoded)
	if !ok {
		return nil
	}
	keySet := val.(map[int64]struct{})
	keys := make([]int64, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	return keys
}

func (si *SecondaryIndex) addRow(key int64, row Row) {
	encoded, ok := encodeCompositeKey(row, si.Info.ColumnIdxs)
	if !ok {
		return
	}
	val, found := si.tree.Get(encoded)
	if found {
		keySet := val.(map[int64]struct{})
		keySet[key] = struct{}{}
	} else {
		si.tree.Put(encoded, map[int64]struct{}{key: {}})
	}
}

func (si *SecondaryIndex) removeRow(key int64, row Row) {
	encoded, ok := encodeCompositeKey(row, si.Info.ColumnIdxs)
	if !ok {
		return
	}
	val, found := si.tree.Get(encoded)
	if found {
		keySet := val.(map[int64]struct{})
		delete(keySet, key)
		if len(keySet) == 0 {
			si.tree.Delete(encoded)
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
		idx.addRow(key, row)
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
					idx.removeRow(key, row)
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
				idx.removeRow(key, oldRow)
			}
		}
	}

	tbl.tree.Put(key, row)

	// Add new index entries
	for _, idx := range tbl.indexes {
		idx.addRow(key, row)
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
			idx.tree = btree.NewStringBTree(32)
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
		Info: info,
		tree: btree.NewStringBTree(32),
	}

	// Build index from existing data
	tbl.tree.ForEach(func(key int64, value any) bool {
		row := value.(Row)
		idx.addRow(key, row)
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

// LookupIndex finds a secondary index on the given table matching the given column indexes.
func (s *Storage) LookupIndex(tableName string, columnIdxs []int) *SecondaryIndex {
	lower := strings.ToLower(tableName)
	tbl, ok := s.tables[lower]
	if !ok {
		return nil
	}
	for _, idx := range tbl.indexes {
		if len(idx.Info.ColumnIdxs) != len(columnIdxs) {
			continue
		}
		match := true
		for i := range columnIdxs {
			if idx.Info.ColumnIdxs[i] != columnIdxs[i] {
				match = false
				break
			}
		}
		if match {
			return idx
		}
	}
	return nil
}

// GetIndexes returns all secondary indexes for the given table.
func (s *Storage) GetIndexes(tableName string) []*SecondaryIndex {
	lower := strings.ToLower(tableName)
	tbl, ok := s.tables[lower]
	if !ok {
		return nil
	}
	var indexes []*SecondaryIndex
	for _, idx := range tbl.indexes {
		indexes = append(indexes, idx)
	}
	return indexes
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

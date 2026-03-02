package memory

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/walf443/oresql/btree"
	"github.com/walf443/oresql/storage"
)

// Compile-time verification that concrete types satisfy the interfaces.
var _ storage.Engine = (*MemoryStorage)(nil)
var _ storage.IndexReader = (*SecondaryIndex)(nil)

// SecondaryIndex is a BTree-based secondary index on one or more columns.
type SecondaryIndex struct {
	Info *storage.IndexInfo
	tree *btree.BTree[storage.KeyEncoding] // encoded value -> map[int64]struct{} (set of BTree keys)
}

// EncodeCompositeKey encodes multiple column values into a single binary key.
// NULL values are encoded (prefix 0x00), so all rows are indexable.
func EncodeCompositeKey(row storage.Row, columnIdxs []int) storage.KeyEncoding {
	var buf strings.Builder
	for _, idx := range columnIdxs {
		storage.EncodeValue(&buf, row[idx])
	}
	return storage.KeyEncoding(buf.String())
}

// encodeSingleValue encodes a single value into a KeyEncoding.
func encodeSingleValue(val storage.Value) storage.KeyEncoding {
	var buf strings.Builder
	storage.EncodeValue(&buf, val)
	return storage.KeyEncoding(buf.String())
}

// isAllNull returns true if all indexed columns in the row are NULL.
func isAllNull(row storage.Row, columnIdxs []int) bool {
	for _, idx := range columnIdxs {
		if row[idx] != nil {
			return false
		}
	}
	return true
}

// CompositeRangeScan returns BTree keys for rows matching a composite index prefix
// with a range condition on a subsequent column.
// prefixVals are the equality values for the leading columns;
// the range condition applies to the next column after the prefix.
// When the range column is not the last index column (partial prefix),
// boundary keys are adjusted to account for trailing column suffixes.
func (si *SecondaryIndex) CompositeRangeScan(
	prefixVals []storage.Value,
	fromVal *storage.Value, fromInclusive bool,
	toVal *storage.Value, toInclusive bool,
) []int64 {
	if len(prefixVals)+1 > len(si.Info.ColumnIdxs) || len(prefixVals) < 1 {
		return nil
	}

	// When the range column is not the last column in the index, stored keys
	// have additional encoded bytes after the range column value.
	// This means encode(prefix, rangeVal, suffix) > encode(prefix, rangeVal),
	// so we must adjust boundaries:
	//   - fromInclusive=false: append \xff to skip past all suffix variants
	//   - toInclusive=true: append \xff and switch to exclusive
	isPartialPrefix := len(prefixVals)+1 < len(si.Info.ColumnIdxs)

	var fromKey *storage.KeyEncoding
	var toKey *storage.KeyEncoding

	if fromVal != nil {
		vals := append(append([]storage.Value{}, prefixVals...), *fromVal)
		k := storage.EncodeValues(vals)
		if isPartialPrefix && !fromInclusive {
			k = k + "\xff"
		}
		fromKey = &k
	} else {
		// No lower bound: start right after the prefix itself
		k := storage.EncodeValues(prefixVals)
		fromKey = &k
		fromInclusive = false
	}

	if toVal != nil {
		vals := append(append([]storage.Value{}, prefixVals...), *toVal)
		k := storage.EncodeValues(vals)
		if isPartialPrefix && toInclusive {
			k = k + "\xff"
			toInclusive = false
		}
		toKey = &k
	} else {
		// No upper bound: stop at prefix + \xff (exclusive)
		k := storage.EncodeValues(prefixVals) + "\xff"
		toKey = &k
		toInclusive = false
	}

	var keys []int64
	si.tree.ForEachRange(fromKey, fromInclusive, toKey, toInclusive, func(key storage.KeyEncoding, value any) bool {
		keySet := value.(map[int64]struct{})
		for k := range keySet {
			keys = append(keys, k)
		}
		return true
	})
	return keys
}

// RangeScan returns BTree keys for rows whose indexed value falls within the given range.
// Only works for single-column indexes; returns nil for composite indexes.
func (si *SecondaryIndex) RangeScan(fromVal *storage.Value, fromInclusive bool, toVal *storage.Value, toInclusive bool) []int64 {
	if len(si.Info.ColumnIdxs) != 1 {
		return nil
	}

	var fromKey *storage.KeyEncoding
	var toKey *storage.KeyEncoding
	if fromVal != nil {
		k := encodeSingleValue(*fromVal)
		fromKey = &k
	}
	if toVal != nil {
		k := encodeSingleValue(*toVal)
		toKey = &k
	}

	var keys []int64
	si.tree.ForEachRange(fromKey, fromInclusive, toKey, toInclusive, func(key storage.KeyEncoding, value any) bool {
		keySet := value.(map[int64]struct{})
		for k := range keySet {
			keys = append(keys, k)
		}
		return true
	})
	return keys
}

// OrderedRangeScan iterates the index in key order, calling fn for each row BTree key.
// reverse=true for descending order. Row keys within each index entry are sorted
// for deterministic order. fn returning false stops the scan.
func (si *SecondaryIndex) OrderedRangeScan(
	fromVal *storage.Value, fromInclusive bool,
	toVal *storage.Value, toInclusive bool,
	reverse bool,
	fn func(rowKey int64) bool,
) {
	if len(si.Info.ColumnIdxs) != 1 {
		return
	}

	var fromKey *storage.KeyEncoding
	var toKey *storage.KeyEncoding
	if fromVal != nil {
		k := encodeSingleValue(*fromVal)
		fromKey = &k
	}
	if toVal != nil {
		k := encodeSingleValue(*toVal)
		toKey = &k
	}

	iterFn := func(key storage.KeyEncoding, value any) bool {
		keySet := value.(map[int64]struct{})
		keys := make([]int64, 0, len(keySet))
		for k := range keySet {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		if reverse {
			for i := len(keys) - 1; i >= 0; i-- {
				if !fn(keys[i]) {
					return false
				}
			}
		} else {
			for _, k := range keys {
				if !fn(k) {
					return false
				}
			}
		}
		return true
	}

	if reverse {
		si.tree.ForEachRangeReverse(fromKey, fromInclusive, toKey, toInclusive, iterFn)
	} else {
		si.tree.ForEachRange(fromKey, fromInclusive, toKey, toInclusive, iterFn)
	}
}

// GetInfo returns the index metadata.
func (si *SecondaryIndex) GetInfo() *storage.IndexInfo {
	return si.Info
}

// Lookup returns the BTree keys matching the given composite values.
func (si *SecondaryIndex) Lookup(vals []storage.Value) []int64 {
	var buf strings.Builder
	for _, v := range vals {
		storage.EncodeValue(&buf, v)
	}
	encoded := storage.KeyEncoding(buf.String())
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

// CheckUnique checks if inserting or updating a row would violate the unique constraint.
// excludeKey is the BTree key of the row being updated (-1 for inserts).
func (si *SecondaryIndex) CheckUnique(row storage.Row, excludeKey int64) error {
	if !si.Info.Unique {
		return nil
	}
	if isAllNull(row, si.Info.ColumnIdxs) {
		return nil
	}
	encoded := EncodeCompositeKey(row, si.Info.ColumnIdxs)
	val, found := si.tree.Get(encoded)
	if !found {
		return nil
	}
	keySet := val.(map[int64]struct{})
	for k := range keySet {
		if k != excludeKey {
			return fmt.Errorf("duplicate key value violates unique constraint %q", si.Info.Name)
		}
	}
	return nil
}

func (si *SecondaryIndex) AddRow(key int64, row storage.Row) {
	encoded := EncodeCompositeKey(row, si.Info.ColumnIdxs)
	val, found := si.tree.Get(encoded)
	if found {
		keySet := val.(map[int64]struct{})
		keySet[key] = struct{}{}
	} else {
		si.tree.Put(encoded, map[int64]struct{}{key: {}})
	}
}

func (si *SecondaryIndex) RemoveRow(key int64, row storage.Row) {
	encoded := EncodeCompositeKey(row, si.Info.ColumnIdxs)
	val, found := si.tree.Get(encoded)
	if found {
		keySet := val.(map[int64]struct{})
		delete(keySet, key)
		if len(keySet) == 0 {
			si.tree.Delete(encoded)
		}
	}
}

// table stores rows for a single table using a BTree.
type table struct {
	mu        sync.RWMutex // table-level lock
	Info      *storage.TableInfo
	tree      *btree.BTree[int64]
	nextRowID int64 // auto-increment for non-PK tables
	indexes   map[string]*SecondaryIndex
}

// MemoryStorage holds all table data in memory.
type MemoryStorage struct {
	mu         sync.RWMutex      // protects tables and indexTable maps
	tables     map[string]*table // key: lowercase table name
	indexTable map[string]string // index name -> table name
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		tables:     make(map[string]*table),
		indexTable: make(map[string]string),
	}
}

// getTable looks up a table by name under s.mu.RLock.
func (s *MemoryStorage) getTable(tableName string) (*table, bool) {
	lower := strings.ToLower(tableName)
	s.mu.RLock()
	tbl, ok := s.tables[lower]
	s.mu.RUnlock()
	return tbl, ok
}

func (s *MemoryStorage) CreateTable(info *storage.TableInfo) {
	s.mu.Lock()
	s.tables[info.Name] = &table{
		Info:      info,
		tree:      btree.New[int64](32),
		nextRowID: 1,
		indexes:   make(map[string]*SecondaryIndex),
	}
	s.mu.Unlock()
}

func (s *MemoryStorage) Insert(tableName string, row storage.Row) error {
	tbl, ok := s.getTable(tableName)
	if !ok {
		return fmt.Errorf("table %q does not exist in storage", tableName)
	}

	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	// Check unique constraints before inserting
	for _, idx := range tbl.indexes {
		if err := idx.CheckUnique(row, -1); err != nil {
			return err
		}
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
		idx.AddRow(key, row)
	}

	return nil
}

// DeleteByKeys deletes rows by their BTree keys.
func (s *MemoryStorage) DeleteByKeys(tableName string, keys []int64) error {
	tbl, ok := s.getTable(tableName)
	if !ok {
		return fmt.Errorf("table %q does not exist in storage", tableName)
	}

	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	for _, key := range keys {
		// Remove from indexes before deleting
		if len(tbl.indexes) > 0 {
			val, found := tbl.tree.Get(key)
			if found {
				row := val.(storage.Row)
				for _, idx := range tbl.indexes {
					idx.RemoveRow(key, row)
				}
			}
		}
		tbl.tree.Delete(key)
	}
	return nil
}

// UpdateRow updates a single row by its BTree key.
func (s *MemoryStorage) UpdateRow(tableName string, key int64, row storage.Row) error {
	tbl, ok := s.getTable(tableName)
	if !ok {
		return fmt.Errorf("table %q does not exist in storage", tableName)
	}

	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	// Remove old index entries
	var oldRow storage.Row
	if len(tbl.indexes) > 0 {
		oldVal, found := tbl.tree.Get(key)
		if found {
			oldRow = oldVal.(storage.Row)
			for _, idx := range tbl.indexes {
				idx.RemoveRow(key, oldRow)
			}
		}
	}

	// Check unique constraints before applying update
	for _, idx := range tbl.indexes {
		if err := idx.CheckUnique(row, key); err != nil {
			// Restore old index entries
			if oldRow != nil {
				for _, idx2 := range tbl.indexes {
					idx2.AddRow(key, oldRow)
				}
			}
			return err
		}
	}

	tbl.tree.Put(key, row)

	// Add new index entries
	for _, idx := range tbl.indexes {
		idx.AddRow(key, row)
	}

	return nil
}

// TruncateTable clears all rows from a table.
// tbl.mu must be held by the caller (via WithTableLocks) for table data access.
func (s *MemoryStorage) TruncateTable(name string) {
	lower := strings.ToLower(name)
	s.mu.RLock()
	tbl, ok := s.tables[lower]
	s.mu.RUnlock()
	if ok {
		tbl.tree = btree.New[int64](32)
		tbl.nextRowID = 1
		// Clear index entries but keep index structure
		for _, idx := range tbl.indexes {
			idx.tree = btree.New[storage.KeyEncoding](32)
		}
	}
}

func (s *MemoryStorage) DropTable(name string) {
	lower := strings.ToLower(name)
	s.mu.Lock()
	if tbl, ok := s.tables[lower]; ok {
		// Clean up index registry
		for idxName := range tbl.indexes {
			delete(s.indexTable, strings.ToLower(idxName))
		}
		delete(s.tables, lower)
	}
	s.mu.Unlock()
}

// AddColumn appends a value to every row in the table.
// tbl.mu must be held by the caller (via WithTableLocks) for table data access.
func (s *MemoryStorage) AddColumn(tableName string, defaultVal storage.Value) error {
	lower := strings.ToLower(tableName)
	s.mu.RLock()
	tbl, ok := s.tables[lower]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("table %q does not exist in storage", tableName)
	}

	tbl.tree.ForEach(func(key int64, value any) bool {
		row := value.(storage.Row)
		newRow := make(storage.Row, len(row)+1)
		copy(newRow, row)
		newRow[len(row)] = defaultVal
		tbl.tree.Put(key, newRow)
		return true
	})
	return nil
}

// DropColumn removes a column from every row and adjusts indexes.
// tbl.mu must be held by the caller (via WithTableLocks) for table data access.
func (s *MemoryStorage) DropColumn(tableName string, colIdx int) error {
	lower := strings.ToLower(tableName)
	s.mu.RLock()
	tbl, ok := s.tables[lower]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("table %q does not exist in storage", tableName)
	}

	// Check for composite indexes referencing this column
	for _, idx := range tbl.indexes {
		if len(idx.Info.ColumnIdxs) > 1 {
			for _, ci := range idx.Info.ColumnIdxs {
				if ci == colIdx {
					return fmt.Errorf("cannot drop column: composite index %q references it, drop the index first", idx.Info.Name)
				}
			}
		}
	}

	// Find and delete single-column indexes on this column
	var toDelete []string
	for name, idx := range tbl.indexes {
		if len(idx.Info.ColumnIdxs) == 1 && idx.Info.ColumnIdxs[0] == colIdx {
			toDelete = append(toDelete, name)
		}
	}
	s.mu.Lock()
	for _, name := range toDelete {
		delete(tbl.indexes, name)
		delete(s.indexTable, name)
	}
	s.mu.Unlock()

	// Adjust ColumnIdxs for remaining indexes and track which need rebuild
	var toRebuild []*SecondaryIndex
	for _, idx := range tbl.indexes {
		needsRebuild := false
		for i, ci := range idx.Info.ColumnIdxs {
			if ci > colIdx {
				idx.Info.ColumnIdxs[i] = ci - 1
				needsRebuild = true
			}
		}
		if needsRebuild {
			toRebuild = append(toRebuild, idx)
		}
	}

	// Remove the column from all rows
	tbl.tree.ForEach(func(key int64, value any) bool {
		row := value.(storage.Row)
		newRow := make(storage.Row, len(row)-1)
		copy(newRow[:colIdx], row[:colIdx])
		copy(newRow[colIdx:], row[colIdx+1:])
		tbl.tree.Put(key, newRow)
		return true
	})

	// Rebuild affected indexes
	for _, idx := range toRebuild {
		idx.tree = btree.New[storage.KeyEncoding](32)
		tbl.tree.ForEach(func(key int64, value any) bool {
			row := value.(storage.Row)
			idx.AddRow(key, row)
			return true
		})
	}

	return nil
}

// CreateIndex creates a secondary index and builds it from existing data.
// tbl.mu must be held by the caller (via WithTableLocks) for table data access.
func (s *MemoryStorage) CreateIndex(info *storage.IndexInfo) error {
	lower := strings.ToLower(info.TableName)
	s.mu.RLock()
	tbl, ok := s.tables[lower]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("table %q does not exist in storage", info.TableName)
	}

	idx := &SecondaryIndex{
		Info: info,
		tree: btree.New[storage.KeyEncoding](32),
	}

	// Build index from existing data (tbl.mu held by WithTableLocks)
	var buildErr error
	tbl.tree.ForEach(func(key int64, value any) bool {
		row := value.(storage.Row)
		if info.Unique {
			if err := idx.CheckUnique(row, -1); err != nil {
				buildErr = err
				return false
			}
		}
		idx.AddRow(key, row)
		return true
	})
	if buildErr != nil {
		return buildErr
	}

	tbl.indexes[strings.ToLower(info.Name)] = idx
	s.mu.Lock()
	s.indexTable[strings.ToLower(info.Name)] = lower
	s.mu.Unlock()
	return nil
}

// DropIndex removes a secondary index.
// tbl.mu must be held by the caller (via WithTableLocks) for table data access.
func (s *MemoryStorage) DropIndex(indexName string) error {
	lowerIdx := strings.ToLower(indexName)
	s.mu.Lock()
	tableName, ok := s.indexTable[lowerIdx]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("index %q does not exist", indexName)
	}
	tbl := s.tables[tableName]
	delete(tbl.indexes, lowerIdx)
	delete(s.indexTable, lowerIdx)
	s.mu.Unlock()
	return nil
}

// LookupIndex finds a secondary index on the given table matching the given column indexes.
func (s *MemoryStorage) LookupIndex(tableName string, columnIdxs []int) storage.IndexReader {
	tbl, ok := s.getTable(tableName)
	if !ok {
		return nil
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
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

// LookupSingleColumnIndex finds a single-column index for the given table and column index.
func (s *MemoryStorage) LookupSingleColumnIndex(tableName string, colIdx int) storage.IndexReader {
	tbl, ok := s.getTable(tableName)
	if !ok {
		return nil
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
	for _, idx := range tbl.indexes {
		if len(idx.Info.ColumnIdxs) == 1 && idx.Info.ColumnIdxs[0] == colIdx {
			return idx
		}
	}
	return nil
}

// GetIndexes returns all secondary indexes for the given table.
func (s *MemoryStorage) GetIndexes(tableName string) []storage.IndexReader {
	tbl, ok := s.getTable(tableName)
	if !ok {
		return nil
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
	var indexes []storage.IndexReader
	for _, idx := range tbl.indexes {
		indexes = append(indexes, idx)
	}
	return indexes
}

// ResolveIndexTable returns the table name that owns the given index.
func (s *MemoryStorage) ResolveIndexTable(indexName string) (string, bool) {
	lowerIdx := strings.ToLower(indexName)
	s.mu.RLock()
	tableName, ok := s.indexTable[lowerIdx]
	s.mu.RUnlock()
	return tableName, ok
}

// HasIndex checks if an index with the given name exists.
func (s *MemoryStorage) HasIndex(indexName string) bool {
	s.mu.RLock()
	_, ok := s.indexTable[strings.ToLower(indexName)]
	s.mu.RUnlock()
	return ok
}

// GetByKeys retrieves rows by their BTree keys.
func (s *MemoryStorage) GetByKeys(tableName string, keys []int64) ([]storage.Row, error) {
	tbl, ok := s.getTable(tableName)
	if !ok {
		return nil, fmt.Errorf("table %q does not exist in storage", tableName)
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
	rows := make([]storage.Row, 0, len(keys))
	for _, key := range keys {
		val, found := tbl.tree.Get(key)
		if found {
			rows = append(rows, val.(storage.Row))
		}
	}
	return rows, nil
}

// GetKeyRowsByKeys retrieves rows with their BTree keys by their BTree keys.
func (s *MemoryStorage) GetKeyRowsByKeys(tableName string, keys []int64) ([]storage.KeyRow, error) {
	tbl, ok := s.getTable(tableName)
	if !ok {
		return nil, fmt.Errorf("table %q does not exist in storage", tableName)
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
	rows := make([]storage.KeyRow, 0, len(keys))
	for _, key := range keys {
		val, found := tbl.tree.Get(key)
		if found {
			rows = append(rows, storage.KeyRow{Key: key, Row: val.(storage.Row)})
		}
	}
	return rows, nil
}

// RowCount returns the number of rows in the table.
func (s *MemoryStorage) RowCount(tableName string) (int, error) {
	tbl, ok := s.getTable(tableName)
	if !ok {
		return 0, fmt.Errorf("table %q does not exist in storage", tableName)
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
	return tbl.tree.Len(), nil
}

// Scan returns all rows in key order.
func (s *MemoryStorage) Scan(tableName string) ([]storage.Row, error) {
	tbl, ok := s.getTable(tableName)
	if !ok {
		return nil, fmt.Errorf("table %q does not exist in storage", tableName)
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
	var rows []storage.Row
	tbl.tree.ForEach(func(key int64, value any) bool {
		rows = append(rows, value.(storage.Row))
		return true
	})
	return rows, nil
}

// ScanOrdered returns rows in PK order (ascending or descending).
// limit > 0 enables early termination after limit rows.
func (s *MemoryStorage) ScanOrdered(tableName string, reverse bool, limit int) ([]storage.Row, error) {
	tbl, ok := s.getTable(tableName)
	if !ok {
		return nil, fmt.Errorf("table %q does not exist in storage", tableName)
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
	cap := 64
	if limit > 0 {
		cap = limit
	}
	rows := make([]storage.Row, 0, cap)
	stopped := false
	iterFn := func(key int64, value any) bool {
		rows = append(rows, value.(storage.Row))
		if limit > 0 && len(rows) >= limit {
			stopped = true
			return false
		}
		return true
	}
	if reverse {
		tbl.tree.ForEachReverse(iterFn)
	} else {
		tbl.tree.ForEach(iterFn)
	}
	_ = stopped
	return rows, nil
}

// ScanWithKeys returns all rows with their BTree keys in key order.
func (s *MemoryStorage) ScanWithKeys(tableName string) ([]storage.KeyRow, error) {
	tbl, ok := s.getTable(tableName)
	if !ok {
		return nil, fmt.Errorf("table %q does not exist in storage", tableName)
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
	var rows []storage.KeyRow
	tbl.tree.ForEach(func(key int64, value any) bool {
		rows = append(rows, storage.KeyRow{Key: key, Row: value.(storage.Row)})
		return true
	})
	return rows, nil
}

// ScanWithKeysNoLock is like ScanWithKeys but does not acquire the table lock.
// The caller must ensure proper synchronization (e.g., via WithTableLocks).
func (s *MemoryStorage) ScanWithKeysNoLock(tableName string) ([]storage.KeyRow, error) {
	lower := strings.ToLower(tableName)
	s.mu.RLock()
	tbl, ok := s.tables[lower]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("table %q does not exist in storage", tableName)
	}
	var rows []storage.KeyRow
	tbl.tree.ForEach(func(key int64, value any) bool {
		rows = append(rows, storage.KeyRow{Key: key, Row: value.(storage.Row)})
		return true
	})
	return rows, nil
}

// ForEachRow iterates over all rows in key order, calling fn for each.
// If reverse is true, iterates in reverse key order.
// fn returning false stops the iteration.
//
// limit > 0: collect at most limit rows from the B-tree (for ORDER BY + LIMIT
// without WHERE). limit <= 0: collect all rows (safe for callbacks that re-read
// the same table via subqueries).
//
// Rows are collected under tbl.mu.RLock, then the lock is released before
// calling fn. This prevents deadlocks when fn contains subqueries that
// re-read the same table (Go's RWMutex blocks new RLock when a writer waits).
func (s *MemoryStorage) ForEachRow(tableName string, reverse bool, fn func(key int64, row storage.Row) bool, limit int) error {
	tbl, ok := s.getTable(tableName)
	if !ok {
		return fmt.Errorf("table %q does not exist in storage", tableName)
	}

	// Collect entries under lock
	type entry struct {
		key int64
		row storage.Row
	}
	cap := 64
	if limit > 0 && limit < cap {
		cap = limit
	}
	entries := make([]entry, 0, cap)

	tbl.mu.RLock()
	collected := 0
	iterFn := func(key int64, value any) bool {
		entries = append(entries, entry{key: key, row: value.(storage.Row)})
		collected++
		if limit > 0 && collected >= limit {
			return false
		}
		return true
	}
	if reverse {
		tbl.tree.ForEachReverse(iterFn)
	} else {
		tbl.tree.ForEach(iterFn)
	}
	tbl.mu.RUnlock()

	// Call fn after releasing the lock
	for _, e := range entries {
		if !fn(e.key, e.row) {
			break
		}
	}
	return nil
}

// ForEachRowKeyOnly iterates over primary keys without reading row values.
// It follows the same collect-then-iterate pattern as ForEachRow.
func (s *MemoryStorage) ForEachRowKeyOnly(tableName string, reverse bool, fn func(key int64) bool, limit int) error {
	tbl, ok := s.getTable(tableName)
	if !ok {
		return fmt.Errorf("table %q does not exist in storage", tableName)
	}

	cap := 64
	if limit > 0 && limit < cap {
		cap = limit
	}
	keys := make([]int64, 0, cap)

	tbl.mu.RLock()
	collected := 0
	iterFn := func(key int64, value any) bool {
		keys = append(keys, key)
		collected++
		if limit > 0 && collected >= limit {
			return false
		}
		return true
	}
	if reverse {
		tbl.tree.ForEachReverse(iterFn)
	} else {
		tbl.tree.ForEach(iterFn)
	}
	tbl.mu.RUnlock()

	for _, k := range keys {
		if !fn(k) {
			break
		}
	}
	return nil
}

// ScanEach iterates rows inline under the table read-lock, calling fn for each row.
// fn returning false stops the iteration. The callback runs while the lock is held,
// so fn must not re-read the same table (no subqueries).
func (s *MemoryStorage) ScanEach(tableName string, fn func(row storage.Row) bool) error {
	tbl, ok := s.getTable(tableName)
	if !ok {
		return fmt.Errorf("table %q does not exist in storage", tableName)
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
	tbl.tree.ForEach(func(key int64, value any) bool {
		return fn(value.(storage.Row))
	})
	return nil
}

// GetRow retrieves a single row by its BTree key.
func (s *MemoryStorage) GetRow(tableName string, key int64) (storage.Row, bool) {
	tbl, ok := s.getTable(tableName)
	if !ok {
		return nil, false
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
	val, found := tbl.tree.Get(key)
	if !found {
		return nil, false
	}
	return val.(storage.Row), true
}

// GetPrimaryTree returns the primary BTree for a table.
func (s *MemoryStorage) GetPrimaryTree(tableName string) *btree.BTree[int64] {
	lower := strings.ToLower(tableName)
	s.mu.RLock()
	tbl, ok := s.tables[lower]
	s.mu.RUnlock()
	if !ok {
		return nil
	}
	return tbl.tree
}

// SetPrimaryTree replaces the primary BTree for a table.
func (s *MemoryStorage) SetPrimaryTree(tableName string, tree *btree.BTree[int64]) {
	lower := strings.ToLower(tableName)
	s.mu.RLock()
	tbl, ok := s.tables[lower]
	s.mu.RUnlock()
	if !ok {
		return
	}
	tbl.tree = tree
}

// GetAllSecondaryTrees returns all secondary index trees for a table.
func (s *MemoryStorage) GetAllSecondaryTrees(tableName string) map[string]*SecondaryIndex {
	lower := strings.ToLower(tableName)
	s.mu.RLock()
	tbl, ok := s.tables[lower]
	s.mu.RUnlock()
	if !ok {
		return nil
	}
	return tbl.indexes
}

// Tree returns the BTree of a secondary index.
func (si *SecondaryIndex) Tree() *btree.BTree[storage.KeyEncoding] {
	return si.tree
}

// SetTree replaces the BTree of a secondary index.
func (si *SecondaryIndex) SetTree(tree *btree.BTree[storage.KeyEncoding]) {
	si.tree = tree
}

// CreateIndexEmpty creates a secondary index entry without building from existing data.
// Used for restoring index structures from disk snapshots.
func (s *MemoryStorage) CreateIndexEmpty(info *storage.IndexInfo) {
	lower := strings.ToLower(info.TableName)
	s.mu.RLock()
	tbl, ok := s.tables[lower]
	s.mu.RUnlock()
	if !ok {
		return
	}

	idx := &SecondaryIndex{
		Info: info,
		tree: btree.New[storage.KeyEncoding](32),
	}
	tbl.indexes[strings.ToLower(info.Name)] = idx
	s.mu.Lock()
	s.indexTable[strings.ToLower(info.Name)] = lower
	s.mu.Unlock()
}

// InsertWithKey inserts a row with a specific BTree key (used for restoring state from disk).
// Unlike Insert, this does not auto-generate a rowID.
func (s *MemoryStorage) InsertWithKey(tableName string, key int64, row storage.Row) error {
	tbl, ok := s.getTable(tableName)
	if !ok {
		return fmt.Errorf("table %q does not exist in storage", tableName)
	}

	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	tbl.tree.Put(key, row)

	// Update secondary indexes
	for _, idx := range tbl.indexes {
		idx.AddRow(key, row)
	}

	return nil
}

// SetNextRowID sets the auto-increment counter for a table (used for restoring state from disk).
func (s *MemoryStorage) SetNextRowID(tableName string, nextRowID int64) {
	tbl, ok := s.getTable(tableName)
	if !ok {
		return
	}
	tbl.mu.Lock()
	defer tbl.mu.Unlock()
	tbl.nextRowID = nextRowID
}

// GetTableMeta returns the table info, index infos, and nextRowID for a table.
// Note: This method only acquires s.mu.RLock for table lookup but does NOT acquire
// tbl.mu. The caller must ensure thread-safety — either by holding tbl.mu via
// WithTableLocks (DDL path) or via the FileStorage mutex (DML path).
func (s *MemoryStorage) GetTableMeta(tableName string) (*storage.TableInfo, []*storage.IndexInfo, int64) {
	lower := strings.ToLower(tableName)
	s.mu.RLock()
	tbl, ok := s.tables[lower]
	s.mu.RUnlock()
	if !ok {
		return nil, nil, 0
	}

	var indexes []*storage.IndexInfo
	for _, idx := range tbl.indexes {
		indexes = append(indexes, idx.Info)
	}

	return tbl.Info, indexes, tbl.nextRowID
}

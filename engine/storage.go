package engine

import (
	"encoding/binary"
	"fmt"
	"math"
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

// KeyEncoding is a binary-encoded index key.
// Each value is prefixed with a type byte (NULL=0x00, INT=0x01, FLOAT=0x02, TEXT=0x03)
// followed by fixed-size or length-prefixed data, making the encoding self-delimiting.
type KeyEncoding string

// SecondaryIndex is a BTree-based secondary index on one or more columns.
type SecondaryIndex struct {
	Info *IndexInfo
	tree *btree.BTree[KeyEncoding] // encoded value -> map[int64]struct{} (set of BTree keys)
}

// encodeValue encodes a single value with a type prefix into the builder.
// The encoding preserves sort order for byte-wise comparison:
//   - NULL: 0x00 (sorts before all other types)
//   - INT:  0x01 + 8-byte big-endian with sign bit flipped (so negative < positive)
//   - FLOAT: 0x02 + 8-byte order-preserving IEEE754 (positive: flip sign bit; negative: flip all bits)
//   - TEXT: 0x03 + raw bytes + 0x00 (null-terminated, preserves lexicographic order)
func encodeValue(buf *strings.Builder, val Value) {
	switch v := val.(type) {
	case nil:
		buf.WriteByte(0x00)
	case int64:
		buf.WriteByte(0x01)
		var b [8]byte
		// Flip the sign bit so that negative values sort before positive values
		binary.BigEndian.PutUint64(b[:], uint64(v)^0x8000000000000000)
		buf.Write(b[:])
	case float64:
		buf.WriteByte(0x02)
		bits := math.Float64bits(v)
		if v >= 0 {
			// Positive (and +0): flip sign bit
			bits ^= 0x8000000000000000
		} else {
			// Negative: flip all bits
			bits = ^bits
		}
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], bits)
		buf.Write(b[:])
	case string:
		buf.WriteByte(0x03)
		buf.WriteString(v)
		buf.WriteByte(0x00) // null terminator
	}
}

// encodeCompositeKey encodes multiple column values into a single binary key.
// NULL values are encoded (prefix 0x00), so all rows are indexable.
func encodeCompositeKey(row Row, columnIdxs []int) KeyEncoding {
	var buf strings.Builder
	for _, idx := range columnIdxs {
		encodeValue(&buf, row[idx])
	}
	return KeyEncoding(buf.String())
}

// encodeSingleValue encodes a single value into a KeyEncoding.
func encodeSingleValue(val Value) KeyEncoding {
	var buf strings.Builder
	encodeValue(&buf, val)
	return KeyEncoding(buf.String())
}

// encodeValues encodes multiple values into a single KeyEncoding.
func encodeValues(vals []Value) KeyEncoding {
	var buf strings.Builder
	for _, v := range vals {
		encodeValue(&buf, v)
	}
	return KeyEncoding(buf.String())
}

// CompositeRangeScan returns BTree keys for rows matching a composite index prefix
// with a range condition on a subsequent column.
// prefixVals are the equality values for the leading columns;
// the range condition applies to the next column after the prefix.
// When the range column is not the last index column (partial prefix),
// boundary keys are adjusted to account for trailing column suffixes.
func (si *SecondaryIndex) CompositeRangeScan(
	prefixVals []Value,
	fromVal *Value, fromInclusive bool,
	toVal *Value, toInclusive bool,
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

	var fromKey *KeyEncoding
	var toKey *KeyEncoding

	if fromVal != nil {
		vals := append(append([]Value{}, prefixVals...), *fromVal)
		k := encodeValues(vals)
		if isPartialPrefix && !fromInclusive {
			k = k + "\xff"
		}
		fromKey = &k
	} else {
		// No lower bound: start right after the prefix itself
		k := encodeValues(prefixVals)
		fromKey = &k
		fromInclusive = false
	}

	if toVal != nil {
		vals := append(append([]Value{}, prefixVals...), *toVal)
		k := encodeValues(vals)
		if isPartialPrefix && toInclusive {
			k = k + "\xff"
			toInclusive = false
		}
		toKey = &k
	} else {
		// No upper bound: stop at prefix + \xff (exclusive)
		k := encodeValues(prefixVals) + "\xff"
		toKey = &k
		toInclusive = false
	}

	var keys []int64
	si.tree.ForEachRange(fromKey, fromInclusive, toKey, toInclusive, func(key KeyEncoding, value any) bool {
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
func (si *SecondaryIndex) RangeScan(fromVal *Value, fromInclusive bool, toVal *Value, toInclusive bool) []int64 {
	if len(si.Info.ColumnIdxs) != 1 {
		return nil
	}

	var fromKey *KeyEncoding
	var toKey *KeyEncoding
	if fromVal != nil {
		k := encodeSingleValue(*fromVal)
		fromKey = &k
	}
	if toVal != nil {
		k := encodeSingleValue(*toVal)
		toKey = &k
	}

	var keys []int64
	si.tree.ForEachRange(fromKey, fromInclusive, toKey, toInclusive, func(key KeyEncoding, value any) bool {
		keySet := value.(map[int64]struct{})
		for k := range keySet {
			keys = append(keys, k)
		}
		return true
	})
	return keys
}

// Lookup returns the BTree keys matching the given composite values.
func (si *SecondaryIndex) Lookup(vals []Value) []int64 {
	var buf strings.Builder
	for _, v := range vals {
		encodeValue(&buf, v)
	}
	encoded := KeyEncoding(buf.String())
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

// isAllNull returns true if all indexed columns in the row are NULL.
func isAllNull(row Row, columnIdxs []int) bool {
	for _, idx := range columnIdxs {
		if row[idx] != nil {
			return false
		}
	}
	return true
}

// checkUnique checks if inserting or updating a row would violate the unique constraint.
// excludeKey is the BTree key of the row being updated (-1 for inserts).
func (si *SecondaryIndex) checkUnique(row Row, excludeKey int64) error {
	if !si.Info.Unique {
		return nil
	}
	if isAllNull(row, si.Info.ColumnIdxs) {
		return nil
	}
	encoded := encodeCompositeKey(row, si.Info.ColumnIdxs)
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

func (si *SecondaryIndex) addRow(key int64, row Row) {
	encoded := encodeCompositeKey(row, si.Info.ColumnIdxs)
	val, found := si.tree.Get(encoded)
	if found {
		keySet := val.(map[int64]struct{})
		keySet[key] = struct{}{}
	} else {
		si.tree.Put(encoded, map[int64]struct{}{key: {}})
	}
}

func (si *SecondaryIndex) removeRow(key int64, row Row) {
	encoded := encodeCompositeKey(row, si.Info.ColumnIdxs)
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
	tree      *btree.BTree[int64]
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
		tree:      btree.New[int64](32),
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

	// Check unique constraints before inserting
	for _, idx := range tbl.indexes {
		if err := idx.checkUnique(row, -1); err != nil {
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
	var oldRow Row
	if len(tbl.indexes) > 0 {
		oldVal, found := tbl.tree.Get(key)
		if found {
			oldRow = oldVal.(Row)
			for _, idx := range tbl.indexes {
				idx.removeRow(key, oldRow)
			}
		}
	}

	// Check unique constraints before applying update
	for _, idx := range tbl.indexes {
		if err := idx.checkUnique(row, key); err != nil {
			// Restore old index entries
			if oldRow != nil {
				for _, idx2 := range tbl.indexes {
					idx2.addRow(key, oldRow)
				}
			}
			return err
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
		tbl.tree = btree.New[int64](32)
		tbl.nextRowID = 1
		// Clear index entries but keep index structure
		for _, idx := range tbl.indexes {
			idx.tree = btree.New[KeyEncoding](32)
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

// AddColumn appends a value to every row in the table.
func (s *Storage) AddColumn(tableName string, defaultVal Value) error {
	lower := strings.ToLower(tableName)
	tbl, ok := s.tables[lower]
	if !ok {
		return fmt.Errorf("table %q does not exist in storage", tableName)
	}

	tbl.tree.ForEach(func(key int64, value any) bool {
		row := value.(Row)
		newRow := make(Row, len(row)+1)
		copy(newRow, row)
		newRow[len(row)] = defaultVal
		tbl.tree.Put(key, newRow)
		return true
	})
	return nil
}

// DropColumn removes a column from every row and adjusts indexes.
func (s *Storage) DropColumn(tableName string, colIdx int) error {
	lower := strings.ToLower(tableName)
	tbl, ok := s.tables[lower]
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
	for _, name := range toDelete {
		delete(tbl.indexes, name)
		delete(s.indexTable, name)
	}

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
		row := value.(Row)
		newRow := make(Row, len(row)-1)
		copy(newRow[:colIdx], row[:colIdx])
		copy(newRow[colIdx:], row[colIdx+1:])
		tbl.tree.Put(key, newRow)
		return true
	})

	// Rebuild affected indexes
	for _, idx := range toRebuild {
		idx.tree = btree.New[KeyEncoding](32)
		tbl.tree.ForEach(func(key int64, value any) bool {
			row := value.(Row)
			idx.addRow(key, row)
			return true
		})
	}

	return nil
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
		tree: btree.New[KeyEncoding](32),
	}

	// Build index from existing data
	var buildErr error
	tbl.tree.ForEach(func(key int64, value any) bool {
		row := value.(Row)
		if info.Unique {
			if err := idx.checkUnique(row, -1); err != nil {
				buildErr = err
				return false
			}
		}
		idx.addRow(key, row)
		return true
	})
	if buildErr != nil {
		return buildErr
	}

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

// LookupSingleColumnIndex finds a single-column index for the given table and column index.
func (s *Storage) LookupSingleColumnIndex(tableName string, colIdx int) *SecondaryIndex {
	lower := strings.ToLower(tableName)
	tbl, ok := s.tables[lower]
	if !ok {
		return nil
	}
	for _, idx := range tbl.indexes {
		if len(idx.Info.ColumnIdxs) == 1 && idx.Info.ColumnIdxs[0] == colIdx {
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

// GetKeyRowsByKeys retrieves rows with their BTree keys by their BTree keys.
func (s *Storage) GetKeyRowsByKeys(tableName string, keys []int64) ([]KeyRow, error) {
	lower := strings.ToLower(tableName)
	tbl, ok := s.tables[lower]
	if !ok {
		return nil, fmt.Errorf("table %q does not exist in storage", tableName)
	}
	var rows []KeyRow
	for _, key := range keys {
		val, found := tbl.tree.Get(key)
		if found {
			rows = append(rows, KeyRow{Key: key, Row: val.(Row)})
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

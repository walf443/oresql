package storage

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
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

// KeyEncoding is a binary-encoded index key.
// Each value is prefixed with a type byte (NULL=0x00, INT=0x01, FLOAT=0x02, TEXT=0x03)
// followed by fixed-size or length-prefixed data, making the encoding self-delimiting.
type KeyEncoding string

// TableLockMode represents the type of lock to acquire on a table.
type TableLockMode int

const (
	TableLockRead  TableLockMode = iota // shared read lock (SELECT)
	TableLockWrite                      // exclusive write lock (INSERT/UPDATE/DELETE/DDL)
)

// TableLock represents a lock request for a single table.
type TableLock struct {
	TableName string
	Mode      TableLockMode
}

// TableLocker is an optional interface that storage engines can implement
// to support table-level locking for concurrent access.
type TableLocker interface {
	WithTableLocks(locks []TableLock, catalogWrite bool, fn func() error) error
	WithCatalogLock(write bool, fn func() error) error
	ResolveIndexTable(indexName string) (string, bool)
}

// Engine is the interface for storage backends.
type Engine interface {
	// Table lifecycle
	CreateTable(info *TableInfo)
	DropTable(name string)
	TruncateTable(name string)

	// Row operations
	Insert(tableName string, row Row) error
	DeleteByKeys(tableName string, keys []int64) error
	UpdateRow(tableName string, key int64, row Row) error

	// Schema changes
	AddColumn(tableName string, defaultVal Value) error
	DropColumn(tableName string, colIdx int) error

	// Index management
	CreateIndex(info *IndexInfo) error
	DropIndex(indexName string) error
	HasIndex(indexName string) bool
	LookupIndex(tableName string, columnIdxs []int) IndexReader
	LookupSingleColumnIndex(tableName string, colIdx int) IndexReader
	GetIndexes(tableName string) []IndexReader

	// Query
	Scan(tableName string) ([]Row, error)
	ScanOrdered(tableName string, reverse bool, limit int) ([]Row, error)
	ScanWithKeys(tableName string) ([]KeyRow, error)
	GetByKeys(tableName string, keys []int64) ([]Row, error)
	GetKeyRowsByKeys(tableName string, keys []int64) ([]KeyRow, error)
	RowCount(tableName string) (int, error)

	// Row iteration (replaces direct BTree access)
	// limit > 0: collect at most limit rows from the B-tree before calling fn.
	// limit <= 0: collect all rows (safe for callbacks that re-read the same table).
	ForEachRow(tableName string, reverse bool, fn func(key int64, row Row) bool, limit int) error
	GetRow(tableName string, key int64) (Row, bool)
}

// IndexReader is the interface for reading index data.
type IndexReader interface {
	GetInfo() *IndexInfo
	Lookup(vals []Value) []int64
	RangeScan(fromVal *Value, fromInclusive bool, toVal *Value, toInclusive bool) []int64
	CompositeRangeScan(prefixVals []Value, fromVal *Value, fromInclusive bool, toVal *Value, toInclusive bool) []int64
	OrderedRangeScan(fromVal *Value, fromInclusive bool, toVal *Value, toInclusive bool, reverse bool, fn func(rowKey int64) bool)
}

// EncodeValue encodes a single value with a type prefix into the builder.
// The encoding preserves sort order for byte-wise comparison:
//   - NULL: 0x00 (sorts before all other types)
//   - INT:  0x01 + 8-byte big-endian with sign bit flipped (so negative < positive)
//   - FLOAT: 0x02 + 8-byte order-preserving IEEE754 (positive: flip sign bit; negative: flip all bits)
//   - TEXT: 0x03 + raw bytes + 0x00 (null-terminated, preserves lexicographic order)
func EncodeValue(buf *strings.Builder, val Value) {
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

// EncodeValues encodes multiple values into a single KeyEncoding.
func EncodeValues(vals []Value) KeyEncoding {
	var buf strings.Builder
	for _, v := range vals {
		EncodeValue(&buf, v)
	}
	return KeyEncoding(buf.String())
}

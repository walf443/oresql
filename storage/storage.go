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
	Type        string // "BTREE", "HASH", or "GIN"
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

	// GIN index management
	LookupGinIndex(tableName string, colIdx int) GinIndexReader

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
	ForEachRowKeyOnly(tableName string, reverse bool, fn func(key int64) bool, limit int) error
	GetRow(tableName string, key int64) (Row, bool)

	// ScanEach iterates rows inline under the table lock, calling fn for each row.
	// fn returning false stops the iteration early. Unlike ForEachRow, the callback
	// runs while the lock is held, so fn must not re-read the same table.
	ScanEach(tableName string, fn func(row Row) bool) error

	// ScanEachWithKey iterates rows inline under the table lock, calling fn for
	// each row with its key. Supports reverse iteration and limit. The Row passed
	// to fn may be reused across calls (disk storage), so callers must copy it if
	// they need to retain it beyond the callback. fn must not re-read the same table.
	ScanEachWithKey(tableName string, reverse bool, fn func(key int64, row Row) bool, limit int) error

	// ForEachByKeys iterates rows matching the given keys inline under the table
	// lock. Keys are sorted internally. The Row passed to fn may be reused across
	// calls (disk storage), so callers must copy it if they need to retain it
	// beyond the callback. fn returning false stops the iteration.
	ForEachByKeys(tableName string, keys []int64, fn func(key int64, row Row) bool) error
}

// IndexReader is the interface for reading index data.
type IndexReader interface {
	GetInfo() *IndexInfo
	Lookup(vals []Value) []int64
	RangeScan(fromVal *Value, fromInclusive bool, toVal *Value, toInclusive bool) []int64
	CompositeRangeScan(prefixVals []Value, fromVal *Value, fromInclusive bool, toVal *Value, toInclusive bool) []int64
	OrderedRangeScan(fromVal *Value, fromInclusive bool, toVal *Value, toInclusive bool, reverse bool, fn func(rowKey int64) bool)
}

// CoveringIndexReader is an optional interface that IndexReader implementations
// can support for covering index scans. When all columns needed by a query are
// contained in the index (+ PK), the executor can skip PK lookups entirely.
type CoveringIndexReader interface {
	LookupCovering(vals []Value, tableNumCols int, pkColIdx int) []Row
	OrderedCoveringScan(
		fromVal *Value, fromInclusive bool,
		toVal *Value, toInclusive bool,
		reverse bool, tableNumCols int, pkColIdx int,
		fn func(rowKey int64, row Row) bool,
	)
}

// GinIndexReader is the interface for reading GIN (inverted) index data.
type GinIndexReader interface {
	GetInfo() *IndexInfo
	// MatchToken returns row keys whose indexed TEXT column contains the given token.
	MatchToken(token string) []int64
}

// MetadataProvider is an optional interface that storage engines can implement
// to support loading table metadata on startup (for persistent storage).
type MetadataProvider interface {
	ListTables() []string
	LoadTableMeta(name string) (*TableInfo, []*IndexInfo, int64, error)
}

// EncodeRow encodes a row (slice of Values) into a byte slice.
// Each value is encoded as: [1 byte type] [N bytes data].
// Type tags: 0x00=NULL, 0x01=INT, 0x02=FLOAT, 0x03=TEXT.
func EncodeRow(row Row) []byte {
	var buf []byte
	for _, val := range row {
		switch v := val.(type) {
		case nil:
			buf = append(buf, 0x00)
		case int64:
			buf = append(buf, 0x01)
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], uint64(v))
			buf = append(buf, b[:]...)
		case float64:
			buf = append(buf, 0x02)
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], math.Float64bits(v))
			buf = append(buf, b[:]...)
		case string:
			buf = append(buf, 0x03)
			var lenBuf [4]byte
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(v)))
			buf = append(buf, lenBuf[:]...)
			buf = append(buf, v...)
		}
	}
	return buf
}

// DecodeRowN decodes a byte slice into a row (slice of Values) with
// pre-allocated capacity. When numCols > 0, the result slice is allocated
// with that capacity to avoid repeated grow/copy during append.
func DecodeRowN(data []byte, numCols int) (Row, error) {
	var row Row
	if numCols > 0 {
		row = make(Row, 0, numCols)
	}
	pos := 0
	for pos < len(data) {
		if pos >= len(data) {
			return nil, fmt.Errorf("unexpected end of row data")
		}
		tag := data[pos]
		pos++
		switch tag {
		case 0x00: // NULL
			row = append(row, nil)
		case 0x01: // INT
			if pos+8 > len(data) {
				return nil, fmt.Errorf("unexpected end of INT data")
			}
			v := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			row = append(row, v)
		case 0x02: // FLOAT
			if pos+8 > len(data) {
				return nil, fmt.Errorf("unexpected end of FLOAT data")
			}
			bits := binary.BigEndian.Uint64(data[pos : pos+8])
			pos += 8
			row = append(row, math.Float64frombits(bits))
		case 0x03: // TEXT
			if pos+4 > len(data) {
				return nil, fmt.Errorf("unexpected end of TEXT length")
			}
			length := int(binary.BigEndian.Uint32(data[pos : pos+4]))
			pos += 4
			if pos+length > len(data) {
				return nil, fmt.Errorf("unexpected end of TEXT data")
			}
			row = append(row, string(data[pos:pos+length]))
			pos += length
		default:
			return nil, fmt.Errorf("unknown value type tag: 0x%02x", tag)
		}
	}
	return row, nil
}

// DecodeRowInto decodes a byte slice into an existing Row slice, reusing the
// backing array to avoid allocation. If cap(dst) < numCols, a new slice is
// allocated (first call only). The returned Row shares the backing array of dst,
// so callers must copy it before the next call if they need to retain the data.
func DecodeRowInto(data []byte, dst Row, numCols int) (Row, error) {
	dst = dst[:0]
	if cap(dst) < numCols {
		dst = make(Row, 0, numCols)
	}
	pos := 0
	for pos < len(data) {
		tag := data[pos]
		pos++
		switch tag {
		case 0x00: // NULL
			dst = append(dst, nil)
		case 0x01: // INT
			if pos+8 > len(data) {
				return nil, fmt.Errorf("unexpected end of INT data")
			}
			v := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			dst = append(dst, v)
		case 0x02: // FLOAT
			if pos+8 > len(data) {
				return nil, fmt.Errorf("unexpected end of FLOAT data")
			}
			bits := binary.BigEndian.Uint64(data[pos : pos+8])
			pos += 8
			dst = append(dst, math.Float64frombits(bits))
		case 0x03: // TEXT
			if pos+4 > len(data) {
				return nil, fmt.Errorf("unexpected end of TEXT length")
			}
			length := int(binary.BigEndian.Uint32(data[pos : pos+4]))
			pos += 4
			if pos+length > len(data) {
				return nil, fmt.Errorf("unexpected end of TEXT data")
			}
			dst = append(dst, string(data[pos:pos+length]))
			pos += length
		default:
			return nil, fmt.Errorf("unknown value type tag: 0x%02x", tag)
		}
	}
	return dst, nil
}

// DecodeRowDirect decodes a byte slice into a pre-allocated Row using index
// writes (dst[idx] = value) instead of append. The caller must ensure
// len(dst) >= number of encoded columns. Returns the number of columns decoded.
func DecodeRowDirect(data []byte, dst Row) (int, error) {
	idx := 0
	pos := 0
	for pos < len(data) {
		tag := data[pos]
		pos++
		switch tag {
		case 0x00: // NULL
			dst[idx] = nil
			idx++
		case 0x01: // INT
			if pos+8 > len(data) {
				return idx, fmt.Errorf("unexpected end of INT data")
			}
			v := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			dst[idx] = v
			idx++
		case 0x02: // FLOAT
			if pos+8 > len(data) {
				return idx, fmt.Errorf("unexpected end of FLOAT data")
			}
			bits := binary.BigEndian.Uint64(data[pos : pos+8])
			pos += 8
			dst[idx] = math.Float64frombits(bits)
			idx++
		case 0x03: // TEXT
			if pos+4 > len(data) {
				return idx, fmt.Errorf("unexpected end of TEXT length")
			}
			length := int(binary.BigEndian.Uint32(data[pos : pos+4]))
			pos += 4
			if pos+length > len(data) {
				return idx, fmt.Errorf("unexpected end of TEXT data")
			}
			dst[idx] = string(data[pos : pos+length])
			pos += length
			idx++
		default:
			return idx, fmt.Errorf("unknown value type tag: 0x%02x", tag)
		}
	}
	return idx, nil
}

// DecodeRow decodes a byte slice into a row (slice of Values).
// Returns the row and any decoding error.
func DecodeRow(data []byte) (Row, error) {
	return DecodeRowN(data, 0)
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

// EncodeValueBytes encodes a single value with a type prefix into a byte slice.
// Same encoding as EncodeValue but appends directly to []byte instead of strings.Builder.
func EncodeValueBytes(buf []byte, val Value) []byte {
	switch v := val.(type) {
	case nil:
		return append(buf, 0x00)
	case int64:
		buf = append(buf, 0x01)
		var b [8]byte
		// Flip the sign bit so that negative values sort before positive values
		// (two's complement → unsigned order-preserving encoding)
		binary.BigEndian.PutUint64(b[:], uint64(v)^0x8000000000000000)
		return append(buf, b[:]...)
	case float64:
		buf = append(buf, 0x02)
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
		return append(buf, b[:]...)
	case string:
		buf = append(buf, 0x03)
		buf = append(buf, v...)
		return append(buf, 0x00) // null terminator
	}
	return buf
}

// DecodeValueBytes decodes a single value from a byte slice at the given position.
// Returns the decoded value, the new position after reading, and any error.
// This is the inverse of EncodeValueBytes.
func DecodeValueBytes(data []byte, pos int) (Value, int, error) {
	if pos >= len(data) {
		return nil, pos, fmt.Errorf("unexpected end of data at pos %d", pos)
	}
	tag := data[pos]
	pos++
	switch tag {
	case 0x00: // NULL
		return nil, pos, nil
	case 0x01: // INT
		if pos+8 > len(data) {
			return nil, pos, fmt.Errorf("unexpected end of INT data at pos %d", pos)
		}
		u := binary.BigEndian.Uint64(data[pos : pos+8])
		pos += 8
		return int64(u ^ 0x8000000000000000), pos, nil
	case 0x02: // FLOAT
		if pos+8 > len(data) {
			return nil, pos, fmt.Errorf("unexpected end of FLOAT data at pos %d", pos)
		}
		bits := binary.BigEndian.Uint64(data[pos : pos+8])
		pos += 8
		// Reverse the order-preserving encoding
		if bits&0x8000000000000000 != 0 {
			// Was positive: flip sign bit back
			bits ^= 0x8000000000000000
		} else {
			// Was negative: flip all bits back
			bits = ^bits
		}
		return math.Float64frombits(bits), pos, nil
	case 0x03: // TEXT
		// Find null terminator
		end := pos
		for end < len(data) && data[end] != 0x00 {
			end++
		}
		if end >= len(data) {
			return nil, pos, fmt.Errorf("TEXT value missing null terminator at pos %d", pos)
		}
		s := string(data[pos:end])
		return s, end + 1, nil // skip the null terminator
	default:
		return nil, pos, fmt.Errorf("unknown value type tag: 0x%02x at pos %d", tag, pos)
	}
}

// DecodeCompositeKeyValues decodes the first numCols values from a composite key.
// The composite key format is: EncodeValueBytes(col1) || ... || EncodeValueBytes(colN) || BigEndian(rowKey)
func DecodeCompositeKeyValues(compositeKey []byte, numCols int) ([]Value, error) {
	vals := make([]Value, numCols)
	pos := 0
	for i := 0; i < numCols; i++ {
		v, newPos, err := DecodeValueBytes(compositeKey, pos)
		if err != nil {
			return nil, fmt.Errorf("decoding column %d: %w", i, err)
		}
		vals[i] = v
		pos = newPos
	}
	return vals, nil
}

// EncodeValues encodes multiple values into a single KeyEncoding.
func EncodeValues(vals []Value) KeyEncoding {
	var buf []byte
	for _, v := range vals {
		buf = EncodeValueBytes(buf, v)
	}
	return KeyEncoding(buf)
}

// PutString appends a length-prefixed string to the buffer.
func PutString(buf *[]byte, s string) {
	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(len(s)))
	*buf = append(*buf, lenBuf[:]...)
	*buf = append(*buf, s...)
}

// GetString reads a length-prefixed string from data at the given position.
func GetString(data []byte, pos int) (string, int, error) {
	if pos+2 > len(data) {
		return "", pos, fmt.Errorf("unexpected end of data reading string length at pos %d", pos)
	}
	length := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if pos+length > len(data) {
		return "", pos, fmt.Errorf("unexpected end of data reading string at pos %d, length %d", pos, length)
	}
	s := string(data[pos : pos+length])
	pos += length
	return s, pos, nil
}

// EncodeOneValue encodes a single Value into bytes using EncodeRow format.
func EncodeOneValue(val Value) []byte {
	return EncodeRow(Row{val})
}

// DecodeOneValue decodes a single value from EncodeRow format at the given position.
func DecodeOneValue(data []byte, pos int) (Value, int, error) {
	if pos >= len(data) {
		return nil, pos, fmt.Errorf("unexpected end of data at pos %d", pos)
	}
	tag := data[pos]
	pos++
	switch tag {
	case 0x00: // NULL
		return nil, pos, nil
	case 0x01: // INT
		if pos+8 > len(data) {
			return nil, pos, fmt.Errorf("unexpected end of INT data")
		}
		v := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
		pos += 8
		return v, pos, nil
	case 0x02: // FLOAT
		if pos+8 > len(data) {
			return nil, pos, fmt.Errorf("unexpected end of FLOAT data")
		}
		bits := binary.BigEndian.Uint64(data[pos : pos+8])
		pos += 8
		return math.Float64frombits(bits), pos, nil
	case 0x03: // TEXT
		if pos+4 > len(data) {
			return nil, pos, fmt.Errorf("unexpected end of TEXT length")
		}
		length := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if pos+length > len(data) {
			return nil, pos, fmt.Errorf("unexpected end of TEXT data")
		}
		return string(data[pos : pos+length]), pos + length, nil
	default:
		return nil, pos, fmt.Errorf("unknown value type tag: 0x%02x", tag)
	}
}

// EncodeMeta encodes TableInfo and IndexInfo list into binary format.
func EncodeMeta(info *TableInfo, indexes []*IndexInfo) []byte {
	var buf []byte

	// Table name
	PutString(&buf, info.Name)

	// Number of columns
	var numColsBuf [2]byte
	binary.BigEndian.PutUint16(numColsBuf[:], uint16(len(info.Columns)))
	buf = append(buf, numColsBuf[:]...)

	// Per column
	for _, col := range info.Columns {
		PutString(&buf, col.Name)
		PutString(&buf, col.DataType)

		var idxBuf [2]byte
		binary.BigEndian.PutUint16(idxBuf[:], uint16(col.Index))
		buf = append(buf, idxBuf[:]...)

		var flags byte
		if col.NotNull {
			flags |= 0x01
		}
		if col.PrimaryKey {
			flags |= 0x02
		}
		if col.HasDefault {
			flags |= 0x04
		}
		buf = append(buf, flags)

		if col.HasDefault {
			buf = append(buf, EncodeOneValue(col.Default)...)
		}
	}

	// PrimaryKeyCol (int16, can be -1)
	var pkColBuf [2]byte
	binary.BigEndian.PutUint16(pkColBuf[:], uint16(int16(info.PrimaryKeyCol)))
	buf = append(buf, pkColBuf[:]...)

	// PrimaryKeyCols
	var numPKColsBuf [2]byte
	binary.BigEndian.PutUint16(numPKColsBuf[:], uint16(len(info.PrimaryKeyCols)))
	buf = append(buf, numPKColsBuf[:]...)
	for _, pkCol := range info.PrimaryKeyCols {
		var pkBuf [2]byte
		binary.BigEndian.PutUint16(pkBuf[:], uint16(pkCol))
		buf = append(buf, pkBuf[:]...)
	}

	// Indexes
	numIndexes := 0
	if indexes != nil {
		numIndexes = len(indexes)
	}
	var numIdxBuf [2]byte
	binary.BigEndian.PutUint16(numIdxBuf[:], uint16(numIndexes))
	buf = append(buf, numIdxBuf[:]...)

	for _, idx := range indexes {
		PutString(&buf, idx.Name)
		PutString(&buf, idx.TableName)

		// Column names
		var numColNamesBuf [2]byte
		binary.BigEndian.PutUint16(numColNamesBuf[:], uint16(len(idx.ColumnNames)))
		buf = append(buf, numColNamesBuf[:]...)
		for _, cn := range idx.ColumnNames {
			PutString(&buf, cn)
		}

		// Column indexes
		var numColIdxsBuf [2]byte
		binary.BigEndian.PutUint16(numColIdxsBuf[:], uint16(len(idx.ColumnIdxs)))
		buf = append(buf, numColIdxsBuf[:]...)
		for _, ci := range idx.ColumnIdxs {
			var ciBuf [2]byte
			binary.BigEndian.PutUint16(ciBuf[:], uint16(ci))
			buf = append(buf, ciBuf[:]...)
		}

		// Type
		PutString(&buf, idx.Type)

		// Unique
		if idx.Unique {
			buf = append(buf, 0x01)
		} else {
			buf = append(buf, 0x00)
		}
	}

	return buf
}

// DecodeMeta decodes binary metadata into TableInfo and IndexInfo list.
func DecodeMeta(data []byte) (*TableInfo, []*IndexInfo, error) {
	pos := 0
	info := &TableInfo{}

	// Table name
	var err error
	info.Name, pos, err = GetString(data, pos)
	if err != nil {
		return nil, nil, fmt.Errorf("reading table name: %w", err)
	}

	// Number of columns
	if pos+2 > len(data) {
		return nil, nil, fmt.Errorf("unexpected end reading num columns")
	}
	numCols := binary.BigEndian.Uint16(data[pos : pos+2])
	pos += 2

	info.Columns = make([]ColumnInfo, numCols)
	for i := 0; i < int(numCols); i++ {
		col := &info.Columns[i]

		col.Name, pos, err = GetString(data, pos)
		if err != nil {
			return nil, nil, fmt.Errorf("reading column name: %w", err)
		}

		col.DataType, pos, err = GetString(data, pos)
		if err != nil {
			return nil, nil, fmt.Errorf("reading column datatype: %w", err)
		}

		if pos+2 > len(data) {
			return nil, nil, fmt.Errorf("unexpected end reading column index")
		}
		colIdx := binary.BigEndian.Uint16(data[pos : pos+2])
		pos += 2
		col.Index = int(colIdx)

		if pos >= len(data) {
			return nil, nil, fmt.Errorf("unexpected end reading column flags")
		}
		flags := data[pos]
		pos++

		col.NotNull = flags&0x01 != 0
		col.PrimaryKey = flags&0x02 != 0
		col.HasDefault = flags&0x04 != 0

		if col.HasDefault {
			col.Default, pos, err = DecodeOneValue(data, pos)
			if err != nil {
				return nil, nil, fmt.Errorf("reading column default: %w", err)
			}
		}
	}

	// PrimaryKeyCol
	if pos+2 > len(data) {
		return nil, nil, fmt.Errorf("unexpected end reading primaryKeyCol")
	}
	pkCol := binary.BigEndian.Uint16(data[pos : pos+2])
	pos += 2
	info.PrimaryKeyCol = int(int16(pkCol))

	// PrimaryKeyCols
	if pos+2 > len(data) {
		return nil, nil, fmt.Errorf("unexpected end reading numPKCols")
	}
	numPKCols := binary.BigEndian.Uint16(data[pos : pos+2])
	pos += 2

	if numPKCols > 0 {
		info.PrimaryKeyCols = make([]int, numPKCols)
		for i := 0; i < int(numPKCols); i++ {
			if pos+2 > len(data) {
				return nil, nil, fmt.Errorf("unexpected end reading pk col index")
			}
			v := binary.BigEndian.Uint16(data[pos : pos+2])
			pos += 2
			info.PrimaryKeyCols[i] = int(v)
		}
	}

	// Indexes
	if pos+2 > len(data) {
		return nil, nil, fmt.Errorf("unexpected end reading num indexes")
	}
	numIndexes := binary.BigEndian.Uint16(data[pos : pos+2])
	pos += 2

	var indexes []*IndexInfo
	for i := 0; i < int(numIndexes); i++ {
		idx := &IndexInfo{}

		idx.Name, pos, err = GetString(data, pos)
		if err != nil {
			return nil, nil, fmt.Errorf("reading index name: %w", err)
		}

		idx.TableName, pos, err = GetString(data, pos)
		if err != nil {
			return nil, nil, fmt.Errorf("reading index table name: %w", err)
		}

		// Column names
		if pos+2 > len(data) {
			return nil, nil, fmt.Errorf("unexpected end reading num col names")
		}
		numColNames := binary.BigEndian.Uint16(data[pos : pos+2])
		pos += 2

		idx.ColumnNames = make([]string, numColNames)
		for j := 0; j < int(numColNames); j++ {
			idx.ColumnNames[j], pos, err = GetString(data, pos)
			if err != nil {
				return nil, nil, fmt.Errorf("reading index col name: %w", err)
			}
		}

		// Column indexes
		if pos+2 > len(data) {
			return nil, nil, fmt.Errorf("unexpected end reading num col idxs")
		}
		numColIdxs := binary.BigEndian.Uint16(data[pos : pos+2])
		pos += 2

		idx.ColumnIdxs = make([]int, numColIdxs)
		for j := 0; j < int(numColIdxs); j++ {
			if pos+2 > len(data) {
				return nil, nil, fmt.Errorf("unexpected end reading col idx")
			}
			v := binary.BigEndian.Uint16(data[pos : pos+2])
			pos += 2
			idx.ColumnIdxs[j] = int(v)
		}

		// Type
		idx.Type, pos, err = GetString(data, pos)
		if err != nil {
			return nil, nil, fmt.Errorf("reading index type: %w", err)
		}

		// Unique
		if pos >= len(data) {
			return nil, nil, fmt.Errorf("unexpected end reading index unique flag")
		}
		idx.Unique = data[pos] != 0
		pos++

		indexes = append(indexes, idx)
	}

	return info, indexes, nil
}

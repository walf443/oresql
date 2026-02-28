package disk

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/walf443/oresql/storage"
	"github.com/walf443/oresql/storage/file"
	"github.com/walf443/oresql/storage/pager"
)

// Compile-time checks.
var _ storage.Engine = (*DiskStorage)(nil)
var _ storage.TableLocker = (*DiskStorage)(nil)
var _ storage.MetadataProvider = (*DiskStorage)(nil)
var _ storage.IndexReader = (*DiskSecondaryIndex)(nil)

// Header page format (.db file, page 0):
//
//	[magic: "ORESQL" 6B]
//	[version: 0x11 1B]
//	[pageSize: 4B uint32 = 4096]
//	[rootPageID: 4B uint32]
//	[rowCount: 4B uint32]
//	[nextRowID: 8B uint64]
//	[freeListHead: 4B uint32]
//	[schemaLen: 2B uint16]
//	[schemaData: variable]
//	  -- after EncodeMeta data --
//	  [numSecBTrees: 2B]
//	  for each:
//	    [indexNameLen: 2B][indexName: N bytes]
//	    [rootPageID: 4B]
//	    [entryCount: 4B]
const (
	dbMagic      = "ORESQL"
	dbVersionV11 = byte(0x11) // new version with persisted secondary indexes
	dbVersionV10 = byte(0x10) // legacy version (rebuild indexes on load)
	bufPoolSize  = 256        // pages cached per table
	headerOffset = 0
)

// Header field offsets within page 0.
const (
	hdrMagicOff     = 0
	hdrVersionOff   = 6
	hdrPageSizeOff  = 7
	hdrRootOff      = 11
	hdrRowCountOff  = 15
	hdrNextRowOff   = 19
	hdrFreeHeadOff  = 27
	hdrSchemaLenOff = 31
	hdrSchemaOff    = 33
)

// DiskSecondaryIndex implements storage.IndexReader backed by a DiskSecondaryBTree.
// Each entry in the tree is a composite key: KeyEncoding(column_values) || BigEndian(rowKey).
type DiskSecondaryIndex struct {
	info *storage.IndexInfo
	tree *DiskSecondaryBTree
}

func (dsi *DiskSecondaryIndex) GetInfo() *storage.IndexInfo {
	return dsi.info
}

// encodeCompositeKey builds compositeKey = KeyEncoding(column_values) || BigEndian(rowKey)
func encodeCompositeKey(row storage.Row, columnIdxs []int, rowKey int64) []byte {
	var buf strings.Builder
	for _, idx := range columnIdxs {
		storage.EncodeValue(&buf, row[idx])
	}
	var keyBuf [8]byte
	binary.BigEndian.PutUint64(keyBuf[:], uint64(rowKey))
	buf.Write(keyBuf[:])
	return []byte(buf.String())
}

// encodeValuesPrefix builds the prefix part (without rowKey suffix).
func encodeValuesPrefix(vals []storage.Value) []byte {
	var buf strings.Builder
	for _, v := range vals {
		storage.EncodeValue(&buf, v)
	}
	return []byte(buf.String())
}

// extractRowKey extracts the int64 rowKey from the last 8 bytes of a composite key.
func extractRowKey(compositeKey []byte) int64 {
	if len(compositeKey) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(compositeKey[len(compositeKey)-8:]))
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

func (dsi *DiskSecondaryIndex) Lookup(vals []storage.Value) []int64 {
	prefix := encodeValuesPrefix(vals)
	var keys []int64
	dsi.tree.PrefixScan(prefix, func(compositeKey []byte) bool {
		keys = append(keys, extractRowKey(compositeKey))
		return true
	})
	return keys
}

func (dsi *DiskSecondaryIndex) RangeScan(fromVal *storage.Value, fromInclusive bool, toVal *storage.Value, toInclusive bool) []int64 {
	if len(dsi.info.ColumnIdxs) != 1 {
		return nil
	}

	var from []byte
	var to []byte
	if fromVal != nil {
		from = encodeValuesPrefix([]storage.Value{*fromVal})
	}
	if toVal != nil {
		to = encodeValuesPrefix([]storage.Value{*toVal})
	}

	// For range scans, we need to handle the rowKey suffix.
	// from inclusive: start at prefix (any rowKey suffix is >= prefix)
	// from exclusive: start after prefix + 0xff...
	// to inclusive: stop after prefix + 0xff...
	// to exclusive: stop at prefix

	var fromKey []byte
	var toKey []byte

	if from != nil {
		if fromInclusive {
			fromKey = from
		} else {
			// Skip past all entries with this prefix
			fromKey = append(append([]byte{}, from...), 0xff)
		}
	}

	if to != nil {
		if toInclusive {
			toKey = append(append([]byte{}, to...), 0xff)
		} else {
			toKey = to
		}
	}

	var keys []int64
	dsi.tree.RangeScan(fromKey, toKey, true, false, func(compositeKey []byte) bool {
		keys = append(keys, extractRowKey(compositeKey))
		return true
	})
	return keys
}

func (dsi *DiskSecondaryIndex) CompositeRangeScan(
	prefixVals []storage.Value,
	fromVal *storage.Value, fromInclusive bool,
	toVal *storage.Value, toInclusive bool,
) []int64 {
	if len(prefixVals)+1 > len(dsi.info.ColumnIdxs) || len(prefixVals) < 1 {
		return nil
	}

	isPartialPrefix := len(prefixVals)+1 < len(dsi.info.ColumnIdxs)

	var fromKey []byte
	var toKey []byte
	fromInc := true
	toInc := false

	if fromVal != nil {
		vals := append(append([]storage.Value{}, prefixVals...), *fromVal)
		k := encodeValuesPrefix(vals)
		if isPartialPrefix && !fromInclusive {
			k = append(k, 0xff)
		} else if !fromInclusive {
			k = append(k, 0xff)
		}
		fromKey = k
	} else {
		k := encodeValuesPrefix(prefixVals)
		fromKey = k
		fromInc = false
	}

	if toVal != nil {
		vals := append(append([]storage.Value{}, prefixVals...), *toVal)
		k := encodeValuesPrefix(vals)
		if isPartialPrefix && toInclusive {
			k = append(k, 0xff)
		} else if toInclusive {
			k = append(k, 0xff)
		}
		toKey = k
	} else {
		k := append(encodeValuesPrefix(prefixVals), 0xff)
		toKey = k
	}

	var keys []int64
	dsi.tree.RangeScan(fromKey, toKey, fromInc, toInc, func(compositeKey []byte) bool {
		keys = append(keys, extractRowKey(compositeKey))
		return true
	})
	return keys
}

func (dsi *DiskSecondaryIndex) OrderedRangeScan(
	fromVal *storage.Value, fromInclusive bool,
	toVal *storage.Value, toInclusive bool,
	reverse bool,
	fn func(rowKey int64) bool,
) {
	if len(dsi.info.ColumnIdxs) != 1 {
		return
	}

	var from []byte
	var to []byte
	if fromVal != nil {
		from = encodeValuesPrefix([]storage.Value{*fromVal})
	}
	if toVal != nil {
		to = encodeValuesPrefix([]storage.Value{*toVal})
	}

	var fromKey []byte
	var toKey []byte

	if from != nil {
		if fromInclusive {
			fromKey = from
		} else {
			fromKey = append(append([]byte{}, from...), 0xff)
		}
	}

	if to != nil {
		if toInclusive {
			toKey = append(append([]byte{}, to...), 0xff)
		} else {
			toKey = to
		}
	}

	iterFn := func(compositeKey []byte) bool {
		return fn(extractRowKey(compositeKey))
	}

	if reverse {
		dsi.tree.RangeScanReverse(fromKey, toKey, true, false, iterFn)
	} else {
		dsi.tree.RangeScan(fromKey, toKey, true, false, iterFn)
	}
}

func (dsi *DiskSecondaryIndex) CheckUnique(row storage.Row, excludeKey int64) error {
	if !dsi.info.Unique {
		return nil
	}
	if isAllNull(row, dsi.info.ColumnIdxs) {
		return nil
	}
	prefix := encodeValuesPrefix(extractIndexVals(row, dsi.info.ColumnIdxs))
	found := false
	dsi.tree.PrefixScan(prefix, func(compositeKey []byte) bool {
		rk := extractRowKey(compositeKey)
		if rk != excludeKey {
			found = true
			return false
		}
		return true
	})
	if found {
		return fmt.Errorf("duplicate key value violates unique constraint %q", dsi.info.Name)
	}
	return nil
}

func extractIndexVals(row storage.Row, columnIdxs []int) []storage.Value {
	vals := make([]storage.Value, len(columnIdxs))
	for i, idx := range columnIdxs {
		vals[i] = row[idx]
	}
	return vals
}

func (dsi *DiskSecondaryIndex) AddRow(key int64, row storage.Row) {
	compositeKey := encodeCompositeKey(row, dsi.info.ColumnIdxs, key)
	dsi.tree.Insert(compositeKey)
}

func (dsi *DiskSecondaryIndex) RemoveRow(key int64, row storage.Row) {
	compositeKey := encodeCompositeKey(row, dsi.info.ColumnIdxs, key)
	dsi.tree.Delete(compositeKey)
}

type diskTable struct {
	mu        sync.RWMutex
	info      *storage.TableInfo
	pager     *pager.Pager
	pool      *pager.BufferPool
	btree     *DiskBTree
	nextRowID int64
	indexes   map[string]*DiskSecondaryIndex
	indexInfo []*storage.IndexInfo
}

// DiskStorage is a disk-based storage engine.
// Each table is a separate .db file with a B+Tree.
// Secondary indexes are persisted as B+Trees in the same file.
type DiskStorage struct {
	mu         sync.RWMutex
	dataDir    string
	tables     map[string]*diskTable
	indexTable map[string]string // index name -> table name
}

// NewDiskStorage creates a new DiskStorage.
func NewDiskStorage(dataDir string) (*DiskStorage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("disk storage: create dir: %w", err)
	}
	return &DiskStorage{
		dataDir:    dataDir,
		tables:     make(map[string]*diskTable),
		indexTable: make(map[string]string),
	}, nil
}

func (ds *DiskStorage) tablePath(name string) string {
	return filepath.Join(ds.dataDir, strings.ToLower(name)+".db")
}

// --- Header read/write ---

func writeHeader(tbl *diskTable) error {
	// Fetch page 0
	data, err := tbl.pool.FetchPage(0)
	if err != nil {
		return err
	}
	copy(data[hdrMagicOff:hdrMagicOff+6], dbMagic)
	data[hdrVersionOff] = dbVersionV11
	binary.BigEndian.PutUint32(data[hdrPageSizeOff:hdrPageSizeOff+4], pager.PageSize)
	binary.BigEndian.PutUint32(data[hdrRootOff:hdrRootOff+4], tbl.btree.RootPageID())
	binary.BigEndian.PutUint32(data[hdrRowCountOff:hdrRowCountOff+4], uint32(tbl.btree.Len()))
	binary.BigEndian.PutUint64(data[hdrNextRowOff:hdrNextRowOff+8], uint64(tbl.nextRowID))
	binary.BigEndian.PutUint32(data[hdrFreeHeadOff:hdrFreeHeadOff+4], tbl.pager.FreeHead())

	// Schema (EncodeMeta)
	schema := file.EncodeMeta(tbl.info, tbl.indexInfo)

	// Append secondary index BTree metadata
	var secMeta []byte
	var numSecBuf [2]byte
	binary.BigEndian.PutUint16(numSecBuf[:], uint16(len(tbl.indexInfo)))
	secMeta = append(secMeta, numSecBuf[:]...)

	for _, idxInfo := range tbl.indexInfo {
		lowerName := strings.ToLower(idxInfo.Name)
		dsi, ok := tbl.indexes[lowerName]

		file.PutString(&secMeta, idxInfo.Name)

		var rootBuf [4]byte
		var countBuf [4]byte
		if ok && dsi.tree != nil {
			binary.BigEndian.PutUint32(rootBuf[:], dsi.tree.RootPageID())
			binary.BigEndian.PutUint32(countBuf[:], uint32(dsi.tree.Len()))
		} else {
			binary.BigEndian.PutUint32(rootBuf[:], uint32(pager.InvalidPageID))
			binary.BigEndian.PutUint32(countBuf[:], 0)
		}
		secMeta = append(secMeta, rootBuf[:]...)
		secMeta = append(secMeta, countBuf[:]...)
	}

	fullSchema := append(schema, secMeta...)
	if len(fullSchema) > pager.PageSize-hdrSchemaOff {
		tbl.pool.UnpinPage(0, false)
		return fmt.Errorf("schema too large: %d bytes", len(fullSchema))
	}
	// Clear rest of page after header to avoid stale data
	for i := hdrSchemaOff; i < pager.PageSize; i++ {
		data[i] = 0
	}
	binary.BigEndian.PutUint16(data[hdrSchemaLenOff:hdrSchemaLenOff+2], uint16(len(fullSchema)))
	copy(data[hdrSchemaOff:], fullSchema)

	tbl.pool.UnpinPage(0, true)
	return nil
}

// secIndexMeta holds secondary index BTree metadata read from header.
type secIndexMeta struct {
	indexName  string
	rootPageID pager.PageID
	entryCount int
}

func readHeader(data []byte) (version byte, rootPageID pager.PageID, rowCount uint32, nextRowID int64, freeHead pager.PageID, schema []byte, secMetas []secIndexMeta, err error) {
	if string(data[hdrMagicOff:hdrMagicOff+6]) != dbMagic {
		return 0, 0, 0, 0, 0, nil, nil, fmt.Errorf("invalid magic")
	}
	version = data[hdrVersionOff]
	if version != dbVersionV11 && version != dbVersionV10 {
		return 0, 0, 0, 0, 0, nil, nil, fmt.Errorf("unsupported version: 0x%02x", version)
	}
	rootPageID = binary.BigEndian.Uint32(data[hdrRootOff : hdrRootOff+4])
	rowCount = binary.BigEndian.Uint32(data[hdrRowCountOff : hdrRowCountOff+4])
	nextRowID = int64(binary.BigEndian.Uint64(data[hdrNextRowOff : hdrNextRowOff+8]))
	freeHead = binary.BigEndian.Uint32(data[hdrFreeHeadOff : hdrFreeHeadOff+4])
	schemaLen := binary.BigEndian.Uint16(data[hdrSchemaLenOff : hdrSchemaLenOff+2])
	fullSchema := data[hdrSchemaOff : hdrSchemaOff+int(schemaLen)]

	// Decode base schema to find where it ends
	schemaCopy := make([]byte, len(fullSchema))
	copy(schemaCopy, fullSchema)
	info, indexes, decErr := file.DecodeMeta(schemaCopy)
	if decErr != nil {
		// Return raw schema for caller to handle
		schema = schemaCopy
		return
	}

	// Re-encode to find boundary
	baseSchema := file.EncodeMeta(info, indexes)
	schema = schemaCopy[:len(baseSchema)]

	if version == dbVersionV11 && len(schemaCopy) > len(baseSchema) {
		secData := schemaCopy[len(baseSchema):]
		pos := 0
		if pos+2 <= len(secData) {
			numSec := int(binary.BigEndian.Uint16(secData[pos : pos+2]))
			pos += 2
			for i := 0; i < numSec; i++ {
				name, newPos, nameErr := file.GetString(secData, pos)
				if nameErr != nil {
					break
				}
				pos = newPos
				if pos+8 > len(secData) {
					break
				}
				rootPID := binary.BigEndian.Uint32(secData[pos : pos+4])
				pos += 4
				count := int(binary.BigEndian.Uint32(secData[pos : pos+4]))
				pos += 4
				secMetas = append(secMetas, secIndexMeta{
					indexName:  name,
					rootPageID: rootPID,
					entryCount: count,
				})
			}
		}
	}

	return
}

// --- Table lifecycle ---

func (ds *DiskStorage) CreateTable(info *storage.TableInfo) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	lower := strings.ToLower(info.Name)
	path := ds.tablePath(info.Name)

	// Create .db file
	p, err := pager.Create(path)
	if err != nil {
		panic(fmt.Sprintf("disk storage: create table file: %v", err))
	}
	pool := pager.NewBufferPool(p, bufPoolSize)

	// Allocate header page (page 0)
	_, _, err = pool.NewPage()
	if err != nil {
		panic(fmt.Sprintf("disk storage: alloc header page: %v", err))
	}
	pool.UnpinPage(0, false)

	// Create B+Tree (root starts at page 1+)
	bt, err := NewDiskBTree(pool)
	if err != nil {
		panic(fmt.Sprintf("disk storage: create btree: %v", err))
	}

	tbl := &diskTable{
		info:      info,
		pager:     p,
		pool:      pool,
		btree:     bt,
		nextRowID: 1,
		indexes:   make(map[string]*DiskSecondaryIndex),
	}

	if err := writeHeader(tbl); err != nil {
		panic(fmt.Sprintf("disk storage: write header: %v", err))
	}
	if err := pool.FlushAll(); err != nil {
		panic(fmt.Sprintf("disk storage: flush: %v", err))
	}

	ds.tables[lower] = tbl
}

func (ds *DiskStorage) DropTable(name string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	lower := strings.ToLower(name)
	tbl, ok := ds.tables[lower]
	if !ok {
		return
	}

	// Clean up index registry
	for idxName := range tbl.indexes {
		delete(ds.indexTable, strings.ToLower(idxName))
	}

	tbl.pool.Close()
	os.Remove(ds.tablePath(name))
	delete(ds.tables, lower)
}

func (ds *DiskStorage) TruncateTable(name string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	lower := strings.ToLower(name)
	tbl, ok := ds.tables[lower]
	if !ok {
		return
	}

	// Close old file
	tbl.pool.Close()

	// Remove and recreate
	path := ds.tablePath(name)
	os.Remove(path)

	p, err := pager.Create(path)
	if err != nil {
		return
	}
	pool := pager.NewBufferPool(p, bufPoolSize)

	// Allocate header page
	pool.NewPage()
	pool.UnpinPage(0, false)

	bt, err := NewDiskBTree(pool)
	if err != nil {
		pool.Close()
		return
	}

	tbl.pager = p
	tbl.pool = pool
	tbl.btree = bt
	tbl.nextRowID = 1

	// Recreate empty secondary index BTrees
	for lowerName, idx := range tbl.indexes {
		newTree, err := NewDiskSecondaryBTree(pool)
		if err != nil {
			continue
		}
		tbl.indexes[lowerName] = &DiskSecondaryIndex{
			info: idx.info,
			tree: newTree,
		}
	}

	writeHeader(tbl)
	pool.FlushAll()
}

// --- Row operations ---

func (ds *DiskStorage) getTable(name string) (*diskTable, bool) {
	lower := strings.ToLower(name)
	ds.mu.RLock()
	tbl, ok := ds.tables[lower]
	ds.mu.RUnlock()
	return tbl, ok
}

func (ds *DiskStorage) Insert(tableName string, row storage.Row) error {
	tbl, ok := ds.getTable(tableName)
	if !ok {
		return fmt.Errorf("table %q does not exist", tableName)
	}

	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	// Check unique constraints
	for _, idx := range tbl.indexes {
		if err := idx.CheckUnique(row, -1); err != nil {
			return err
		}
	}

	var key int64
	if tbl.info.PrimaryKeyCol >= 0 {
		pkVal := row[tbl.info.PrimaryKeyCol]
		key = pkVal.(int64)
		if !tbl.btree.Insert(key, row) {
			return fmt.Errorf("duplicate primary key value: %d", key)
		}
	} else {
		key = tbl.nextRowID
		tbl.nextRowID++
		tbl.btree.Insert(key, row)
	}

	// Update secondary indexes
	for _, idx := range tbl.indexes {
		idx.AddRow(key, row)
	}

	// Persist
	if err := writeHeader(tbl); err != nil {
		return err
	}
	return tbl.pool.FlushAll()
}

func (ds *DiskStorage) DeleteByKeys(tableName string, keys []int64) error {
	tbl, ok := ds.getTable(tableName)
	if !ok {
		return fmt.Errorf("table %q does not exist", tableName)
	}

	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	for _, key := range keys {
		if len(tbl.indexes) > 0 {
			row, found := tbl.btree.Get(key)
			if found {
				for _, idx := range tbl.indexes {
					idx.RemoveRow(key, row)
				}
			}
		}
		tbl.btree.Delete(key)
	}

	if err := writeHeader(tbl); err != nil {
		return err
	}
	return tbl.pool.FlushAll()
}

func (ds *DiskStorage) UpdateRow(tableName string, key int64, row storage.Row) error {
	tbl, ok := ds.getTable(tableName)
	if !ok {
		return fmt.Errorf("table %q does not exist", tableName)
	}

	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	// Remove old index entries
	var oldRow storage.Row
	if len(tbl.indexes) > 0 {
		oldRow, _ = tbl.btree.Get(key)
		if oldRow != nil {
			for _, idx := range tbl.indexes {
				idx.RemoveRow(key, oldRow)
			}
		}
	}

	// Check unique constraints
	for _, idx := range tbl.indexes {
		if err := idx.CheckUnique(row, key); err != nil {
			if oldRow != nil {
				for _, idx2 := range tbl.indexes {
					idx2.AddRow(key, oldRow)
				}
			}
			return err
		}
	}

	tbl.btree.Put(key, row)

	for _, idx := range tbl.indexes {
		idx.AddRow(key, row)
	}

	if err := writeHeader(tbl); err != nil {
		return err
	}
	return tbl.pool.FlushAll()
}

// --- Schema changes ---

// AddColumn adds a column to all rows.
// tbl.mu must be held by the caller (via WithTableLocks).
func (ds *DiskStorage) AddColumn(tableName string, defaultVal storage.Value) error {
	tbl, ok := ds.getTable(tableName)
	if !ok {
		return fmt.Errorf("table %q does not exist", tableName)
	}

	// Update all rows
	type kv struct {
		key int64
		row storage.Row
	}
	var all []kv
	tbl.btree.ForEach(func(key int64, row storage.Row) bool {
		all = append(all, kv{key, row})
		return true
	})
	for _, entry := range all {
		newRow := make(storage.Row, len(entry.row)+1)
		copy(newRow, entry.row)
		newRow[len(entry.row)] = defaultVal
		tbl.btree.Put(entry.key, newRow)
	}

	if err := writeHeader(tbl); err != nil {
		return err
	}
	return tbl.pool.FlushAll()
}

// DropColumn removes a column from all rows.
// tbl.mu must be held by the caller (via WithTableLocks).
func (ds *DiskStorage) DropColumn(tableName string, colIdx int) error {
	tbl, ok := ds.getTable(tableName)
	if !ok {
		return fmt.Errorf("table %q does not exist", tableName)
	}

	// Check composite indexes
	for _, idx := range tbl.indexes {
		info := idx.GetInfo()
		if len(info.ColumnIdxs) > 1 {
			for _, ci := range info.ColumnIdxs {
				if ci == colIdx {
					return fmt.Errorf("cannot drop column: composite index %q references it", info.Name)
				}
			}
		}
	}

	// Delete single-column indexes on this column
	var toDelete []string
	for name, idx := range tbl.indexes {
		info := idx.GetInfo()
		if len(info.ColumnIdxs) == 1 && info.ColumnIdxs[0] == colIdx {
			toDelete = append(toDelete, name)
		}
	}
	ds.mu.Lock()
	for _, name := range toDelete {
		delete(tbl.indexes, name)
		delete(ds.indexTable, name)
		// Remove from indexInfo
		for i, ii := range tbl.indexInfo {
			if strings.ToLower(ii.Name) == name {
				tbl.indexInfo = append(tbl.indexInfo[:i], tbl.indexInfo[i+1:]...)
				break
			}
		}
	}
	ds.mu.Unlock()

	// Adjust column indexes for remaining indexes and rebuild
	var toRebuild []*DiskSecondaryIndex
	for _, idx := range tbl.indexes {
		info := idx.GetInfo()
		needsRebuild := false
		for i, ci := range info.ColumnIdxs {
			if ci > colIdx {
				info.ColumnIdxs[i] = ci - 1
				needsRebuild = true
			}
		}
		if needsRebuild {
			toRebuild = append(toRebuild, idx)
		}
	}

	// Remove column from all rows
	type kv struct {
		key int64
		row storage.Row
	}
	var all []kv
	tbl.btree.ForEach(func(key int64, row storage.Row) bool {
		all = append(all, kv{key, row})
		return true
	})
	for _, entry := range all {
		row := entry.row
		newRow := make(storage.Row, len(row)-1)
		copy(newRow[:colIdx], row[:colIdx])
		copy(newRow[colIdx:], row[colIdx+1:])
		tbl.btree.Put(entry.key, newRow)
	}

	// Rebuild affected indexes
	for _, idx := range toRebuild {
		newTree, err := NewDiskSecondaryBTree(tbl.pool)
		if err != nil {
			continue
		}
		idx.tree = newTree
		tbl.btree.ForEach(func(key int64, row storage.Row) bool {
			idx.AddRow(key, row)
			return true
		})
	}

	if err := writeHeader(tbl); err != nil {
		return err
	}
	return tbl.pool.FlushAll()
}

// --- Index management ---

// CreateIndex creates a secondary index and builds it from existing data.
// tbl.mu must be held by the caller (via WithTableLocks).
func (ds *DiskStorage) CreateIndex(info *storage.IndexInfo) error {
	tbl, ok := ds.getTable(info.TableName)
	if !ok {
		return fmt.Errorf("table %q does not exist", info.TableName)
	}

	tree, err := NewDiskSecondaryBTree(tbl.pool)
	if err != nil {
		return fmt.Errorf("create secondary btree: %w", err)
	}

	idx := &DiskSecondaryIndex{
		info: info,
		tree: tree,
	}

	// Build from existing data
	var buildErr error
	tbl.btree.ForEach(func(key int64, row storage.Row) bool {
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
	tbl.indexInfo = append(tbl.indexInfo, info)
	ds.mu.Lock()
	ds.indexTable[strings.ToLower(info.Name)] = strings.ToLower(info.TableName)
	ds.mu.Unlock()

	if err := writeHeader(tbl); err != nil {
		return err
	}
	return tbl.pool.FlushAll()
}

func (ds *DiskStorage) DropIndex(indexName string) error {
	lowerIdx := strings.ToLower(indexName)
	ds.mu.Lock()
	tableName, ok := ds.indexTable[lowerIdx]
	if !ok {
		ds.mu.Unlock()
		return fmt.Errorf("index %q does not exist", indexName)
	}
	tbl := ds.tables[tableName]
	delete(tbl.indexes, lowerIdx)
	delete(ds.indexTable, lowerIdx)

	// Remove from indexInfo
	for i, ii := range tbl.indexInfo {
		if strings.ToLower(ii.Name) == lowerIdx {
			tbl.indexInfo = append(tbl.indexInfo[:i], tbl.indexInfo[i+1:]...)
			break
		}
	}
	ds.mu.Unlock()

	if err := writeHeader(tbl); err != nil {
		return err
	}
	return tbl.pool.FlushAll()
}

func (ds *DiskStorage) HasIndex(indexName string) bool {
	ds.mu.RLock()
	_, ok := ds.indexTable[strings.ToLower(indexName)]
	ds.mu.RUnlock()
	return ok
}

func (ds *DiskStorage) LookupIndex(tableName string, columnIdxs []int) storage.IndexReader {
	tbl, ok := ds.getTable(tableName)
	if !ok {
		return nil
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
	for _, idx := range tbl.indexes {
		info := idx.GetInfo()
		if len(info.ColumnIdxs) != len(columnIdxs) {
			continue
		}
		match := true
		for i := range columnIdxs {
			if info.ColumnIdxs[i] != columnIdxs[i] {
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

func (ds *DiskStorage) LookupSingleColumnIndex(tableName string, colIdx int) storage.IndexReader {
	tbl, ok := ds.getTable(tableName)
	if !ok {
		return nil
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
	for _, idx := range tbl.indexes {
		info := idx.GetInfo()
		if len(info.ColumnIdxs) == 1 && info.ColumnIdxs[0] == colIdx {
			return idx
		}
	}
	return nil
}

func (ds *DiskStorage) GetIndexes(tableName string) []storage.IndexReader {
	tbl, ok := ds.getTable(tableName)
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

// --- Query ---

func (ds *DiskStorage) Scan(tableName string) ([]storage.Row, error) {
	tbl, ok := ds.getTable(tableName)
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", tableName)
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
	var rows []storage.Row
	tbl.btree.ForEach(func(key int64, row storage.Row) bool {
		rows = append(rows, row)
		return true
	})
	return rows, nil
}

func (ds *DiskStorage) ScanOrdered(tableName string, reverse bool, limit int) ([]storage.Row, error) {
	tbl, ok := ds.getTable(tableName)
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", tableName)
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	cap := 64
	if limit > 0 {
		cap = limit
	}
	rows := make([]storage.Row, 0, cap)

	iterFn := func(key int64, row storage.Row) bool {
		rows = append(rows, row)
		if limit > 0 && len(rows) >= limit {
			return false
		}
		return true
	}

	if reverse {
		tbl.btree.ForEachReverse(iterFn)
	} else {
		tbl.btree.ForEach(iterFn)
	}
	return rows, nil
}

func (ds *DiskStorage) ScanWithKeys(tableName string) ([]storage.KeyRow, error) {
	tbl, ok := ds.getTable(tableName)
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", tableName)
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
	var rows []storage.KeyRow
	tbl.btree.ForEach(func(key int64, row storage.Row) bool {
		rows = append(rows, storage.KeyRow{Key: key, Row: row})
		return true
	})
	return rows, nil
}

func (ds *DiskStorage) GetByKeys(tableName string, keys []int64) ([]storage.Row, error) {
	tbl, ok := ds.getTable(tableName)
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", tableName)
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	sorted := make([]int64, len(keys))
	copy(sorted, keys)
	slices.Sort(sorted)

	keyRows := tbl.btree.GetByKeysSorted(sorted)
	rows := make([]storage.Row, len(keyRows))
	for i, kr := range keyRows {
		rows[i] = kr.Row
	}
	return rows, nil
}

func (ds *DiskStorage) GetKeyRowsByKeys(tableName string, keys []int64) ([]storage.KeyRow, error) {
	tbl, ok := ds.getTable(tableName)
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", tableName)
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	sorted := make([]int64, len(keys))
	copy(sorted, keys)
	slices.Sort(sorted)

	return tbl.btree.GetByKeysSorted(sorted), nil
}

func (ds *DiskStorage) RowCount(tableName string) (int, error) {
	tbl, ok := ds.getTable(tableName)
	if !ok {
		return 0, fmt.Errorf("table %q does not exist", tableName)
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
	return tbl.btree.Len(), nil
}

func (ds *DiskStorage) ForEachRow(tableName string, reverse bool, fn func(key int64, row storage.Row) bool, limit int) error {
	tbl, ok := ds.getTable(tableName)
	if !ok {
		return fmt.Errorf("table %q does not exist", tableName)
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
	iterFn := func(key int64, row storage.Row) bool {
		entries = append(entries, entry{key: key, row: row})
		collected++
		if limit > 0 && collected >= limit {
			return false
		}
		return true
	}
	if reverse {
		tbl.btree.ForEachReverse(iterFn)
	} else {
		tbl.btree.ForEach(iterFn)
	}
	tbl.mu.RUnlock()

	for _, e := range entries {
		if !fn(e.key, e.row) {
			break
		}
	}
	return nil
}

func (ds *DiskStorage) GetRow(tableName string, key int64) (storage.Row, bool) {
	tbl, ok := ds.getTable(tableName)
	if !ok {
		return nil, false
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
	return tbl.btree.Get(key)
}

// --- TableLocker ---

func (ds *DiskStorage) WithTableLocks(locks []storage.TableLock, catalogWrite bool, fn func() error) error {
	sorted := make([]storage.TableLock, len(locks))
	copy(sorted, locks)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].TableName < sorted[j].TableName
	})

	ds.mu.RLock()

	lockedTables := make([]*diskTable, 0, len(sorted))
	lockedModes := make([]storage.TableLockMode, 0, len(sorted))
	for _, lock := range sorted {
		tbl, ok := ds.tables[strings.ToLower(lock.TableName)]
		if !ok {
			for i := len(lockedTables) - 1; i >= 0; i-- {
				if lockedModes[i] == storage.TableLockWrite {
					lockedTables[i].mu.Unlock()
				} else {
					lockedTables[i].mu.RUnlock()
				}
			}
			ds.mu.RUnlock()
			return fn()
		}
		if lock.Mode == storage.TableLockWrite {
			tbl.mu.Lock()
		} else {
			tbl.mu.RLock()
		}
		lockedTables = append(lockedTables, tbl)
		lockedModes = append(lockedModes, lock.Mode)
	}

	ds.mu.RUnlock()

	err := fn()

	for i := len(lockedTables) - 1; i >= 0; i-- {
		if lockedModes[i] == storage.TableLockWrite {
			lockedTables[i].mu.Unlock()
		} else {
			lockedTables[i].mu.RUnlock()
		}
	}

	return err
}

func (ds *DiskStorage) WithCatalogLock(write bool, fn func() error) error {
	return fn()
}

func (ds *DiskStorage) ResolveIndexTable(indexName string) (string, bool) {
	lowerIdx := strings.ToLower(indexName)
	ds.mu.RLock()
	tableName, ok := ds.indexTable[lowerIdx]
	ds.mu.RUnlock()
	return tableName, ok
}

// --- MetadataProvider ---

func (ds *DiskStorage) ListTables() []string {
	entries, err := os.ReadDir(ds.dataDir)
	if err != nil {
		return nil
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".db") {
			names = append(names, strings.TrimSuffix(name, ".db"))
		}
	}
	return names
}

func (ds *DiskStorage) LoadTableMeta(name string) (*storage.TableInfo, []*storage.IndexInfo, int64, error) {
	tbl, ok := ds.getTable(name)
	if !ok {
		return nil, nil, 0, fmt.Errorf("table %q not loaded", name)
	}
	return tbl.info, tbl.indexInfo, tbl.nextRowID, nil
}

// --- LoadAll ---

// LoadAll loads all tables from the data directory.
func (ds *DiskStorage) LoadAll() error {
	entries, err := os.ReadDir(ds.dataDir)
	if err != nil {
		return fmt.Errorf("disk storage: read dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".db") {
			continue
		}
		tableName := strings.TrimSuffix(name, ".db")
		if err := ds.loadTable(tableName); err != nil {
			return fmt.Errorf("disk storage: load table %q: %w", tableName, err)
		}
	}
	return nil
}

func (ds *DiskStorage) loadTable(tableName string) error {
	path := ds.tablePath(tableName)

	p, err := pager.Open(path)
	if err != nil {
		return err
	}
	pool := pager.NewBufferPool(p, bufPoolSize)

	// Read header page
	data, err := pool.FetchPage(0)
	if err != nil {
		pool.Close()
		return err
	}

	version, rootPageID, _, nextRowID, freeHead, schemaBytes, secMetas, err := readHeader(data)
	if err != nil {
		pool.UnpinPage(0, false)
		pool.Close()
		return err
	}
	pool.UnpinPage(0, false)

	// Decode schema
	schemaCopy := make([]byte, len(schemaBytes))
	copy(schemaCopy, schemaBytes)
	info, indexes, err := file.DecodeMeta(schemaCopy)
	if err != nil {
		pool.Close()
		return fmt.Errorf("decode schema: %w", err)
	}

	// Set free-list head
	p.SetFreeHead(freeHead)

	// Load B+Tree
	bt := LoadDiskBTree(pool, rootPageID, 0)
	// Count actual rows by walking the tree
	count := 0
	bt.ForEach(func(key int64, row storage.Row) bool {
		count++
		return true
	})
	bt = LoadDiskBTree(pool, rootPageID, count)

	tbl := &diskTable{
		info:      info,
		pager:     p,
		pool:      pool,
		btree:     bt,
		nextRowID: nextRowID,
		indexes:   make(map[string]*DiskSecondaryIndex),
		indexInfo: indexes,
	}

	if version == dbVersionV11 && len(secMetas) > 0 {
		// Load secondary indexes from disk (no rebuild needed)
		secMetaMap := make(map[string]secIndexMeta)
		for _, sm := range secMetas {
			secMetaMap[strings.ToLower(sm.indexName)] = sm
		}

		for _, idxInfo := range indexes {
			lowerName := strings.ToLower(idxInfo.Name)
			sm, ok := secMetaMap[lowerName]
			if ok && sm.rootPageID != pager.InvalidPageID {
				tree := LoadDiskSecondaryBTree(pool, sm.rootPageID, sm.entryCount)
				tbl.indexes[lowerName] = &DiskSecondaryIndex{
					info: idxInfo,
					tree: tree,
				}
			} else {
				// No persisted tree, rebuild
				tree, treeErr := NewDiskSecondaryBTree(pool)
				if treeErr != nil {
					pool.Close()
					return fmt.Errorf("create sec btree: %w", treeErr)
				}
				idx := &DiskSecondaryIndex{info: idxInfo, tree: tree}
				tbl.btree.ForEach(func(key int64, row storage.Row) bool {
					idx.AddRow(key, row)
					return true
				})
				tbl.indexes[lowerName] = idx
			}
			ds.indexTable[lowerName] = strings.ToLower(tableName)
		}
	} else {
		// Legacy v0x10 format: rebuild secondary indexes from primary tree
		for _, idxInfo := range indexes {
			tree, treeErr := NewDiskSecondaryBTree(pool)
			if treeErr != nil {
				pool.Close()
				return fmt.Errorf("create sec btree: %w", treeErr)
			}
			idx := &DiskSecondaryIndex{info: idxInfo, tree: tree}
			tbl.btree.ForEach(func(key int64, row storage.Row) bool {
				idx.AddRow(key, row)
				return true
			})

			tbl.indexes[strings.ToLower(idxInfo.Name)] = idx
			ds.indexTable[strings.ToLower(idxInfo.Name)] = strings.ToLower(tableName)
		}

		// Upgrade to v0x11 by writing new header
		if len(indexes) > 0 {
			writeHeader(tbl)
			pool.FlushAll()
		}
	}

	ds.tables[strings.ToLower(tableName)] = tbl
	return nil
}

// Close closes all table files.
func (ds *DiskStorage) Close() error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	for _, tbl := range ds.tables {
		tbl.pool.Close()
	}
	return nil
}

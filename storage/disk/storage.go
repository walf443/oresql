package disk

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/walf443/oresql/btree"
	"github.com/walf443/oresql/storage"
	"github.com/walf443/oresql/storage/file"
	"github.com/walf443/oresql/storage/memory"
	"github.com/walf443/oresql/storage/pager"
)

// Compile-time checks.
var _ storage.Engine = (*DiskStorage)(nil)
var _ storage.TableLocker = (*DiskStorage)(nil)
var _ storage.MetadataProvider = (*DiskStorage)(nil)

// Header page format (.db file, page 0):
//
//	[magic: "ORESQL" 6B]
//	[version: 0x10 1B]
//	[pageSize: 4B uint32 = 4096]
//	[rootPageID: 4B uint32]
//	[rowCount: 4B uint32]
//	[nextRowID: 8B uint64]
//	[freeListHead: 4B uint32]
//	[schemaLen: 2B uint16]
//	[schemaData: rest of page]
const (
	dbMagic      = "ORESQL"
	dbVersion    = byte(0x10)
	bufPoolSize  = 256 // pages cached per table
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

type diskTable struct {
	mu        sync.RWMutex
	info      *storage.TableInfo
	pager     *pager.Pager
	pool      *pager.BufferPool
	btree     *DiskBTree
	nextRowID int64
	indexes   map[string]*memory.SecondaryIndex
	indexInfo []*storage.IndexInfo
}

// DiskStorage is a disk-based storage engine.
// Each table is a separate .db file with a B+Tree.
// Secondary indexes are kept in memory and rebuilt on startup.
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
	data[hdrVersionOff] = dbVersion
	binary.BigEndian.PutUint32(data[hdrPageSizeOff:hdrPageSizeOff+4], pager.PageSize)
	binary.BigEndian.PutUint32(data[hdrRootOff:hdrRootOff+4], tbl.btree.RootPageID())
	binary.BigEndian.PutUint32(data[hdrRowCountOff:hdrRowCountOff+4], uint32(tbl.btree.Len()))
	binary.BigEndian.PutUint64(data[hdrNextRowOff:hdrNextRowOff+8], uint64(tbl.nextRowID))
	binary.BigEndian.PutUint32(data[hdrFreeHeadOff:hdrFreeHeadOff+4], tbl.pager.FreeHead())

	// Schema
	schema := file.EncodeMeta(tbl.info, tbl.indexInfo)
	if len(schema) > pager.PageSize-hdrSchemaOff {
		tbl.pool.UnpinPage(0, false)
		return fmt.Errorf("schema too large: %d bytes", len(schema))
	}
	binary.BigEndian.PutUint16(data[hdrSchemaLenOff:hdrSchemaLenOff+2], uint16(len(schema)))
	copy(data[hdrSchemaOff:], schema)

	tbl.pool.UnpinPage(0, true)
	return nil
}

func readHeader(data []byte) (rootPageID pager.PageID, rowCount uint32, nextRowID int64, freeHead pager.PageID, schema []byte, err error) {
	if string(data[hdrMagicOff:hdrMagicOff+6]) != dbMagic {
		return 0, 0, 0, 0, nil, fmt.Errorf("invalid magic")
	}
	if data[hdrVersionOff] != dbVersion {
		return 0, 0, 0, 0, nil, fmt.Errorf("unsupported version: 0x%02x", data[hdrVersionOff])
	}
	rootPageID = binary.BigEndian.Uint32(data[hdrRootOff : hdrRootOff+4])
	rowCount = binary.BigEndian.Uint32(data[hdrRowCountOff : hdrRowCountOff+4])
	nextRowID = int64(binary.BigEndian.Uint64(data[hdrNextRowOff : hdrNextRowOff+8]))
	freeHead = binary.BigEndian.Uint32(data[hdrFreeHeadOff : hdrFreeHeadOff+4])
	schemaLen := binary.BigEndian.Uint16(data[hdrSchemaLenOff : hdrSchemaLenOff+2])
	schema = data[hdrSchemaOff : hdrSchemaOff+int(schemaLen)]
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
		indexes:   make(map[string]*memory.SecondaryIndex),
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

	// Clear index data but keep structure
	for _, idx := range tbl.indexes {
		idx.SetTree(btree.New[storage.KeyEncoding](32))
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

	// Adjust column indexes for remaining indexes
	var toRebuild []*memory.SecondaryIndex
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
		idx.SetTree(btree.New[storage.KeyEncoding](32))
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

	idx := &memory.SecondaryIndex{
		Info: info,
	}
	idx.SetTree(btree.New[storage.KeyEncoding](32))

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
	rows := make([]storage.Row, 0, len(keys))
	for _, key := range keys {
		row, found := tbl.btree.Get(key)
		if found {
			rows = append(rows, row)
		}
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
	rows := make([]storage.KeyRow, 0, len(keys))
	for _, key := range keys {
		row, found := tbl.btree.Get(key)
		if found {
			rows = append(rows, storage.KeyRow{Key: key, Row: row})
		}
	}
	return rows, nil
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

	rootPageID, _, nextRowID, freeHead, schemaBytes, err := readHeader(data)
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
		indexes:   make(map[string]*memory.SecondaryIndex),
		indexInfo: indexes,
	}

	// Rebuild secondary indexes from primary tree
	for _, idxInfo := range indexes {
		idx := &memory.SecondaryIndex{
			Info: idxInfo,
		}
		idx.SetTree(btree.New[storage.KeyEncoding](32))

		tbl.btree.ForEach(func(key int64, row storage.Row) bool {
			idx.AddRow(key, row)
			return true
		})

		tbl.indexes[strings.ToLower(idxInfo.Name)] = idx
		ds.indexTable[strings.ToLower(idxInfo.Name)] = strings.ToLower(tableName)
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

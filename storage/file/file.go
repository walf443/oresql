package file

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/walf443/oresql/btree"
	"github.com/walf443/oresql/storage"
	"github.com/walf443/oresql/storage/memory"
)

// Compile-time checks.
var _ storage.Engine = (*FileStorage)(nil)
var _ storage.TableLocker = (*FileStorage)(nil)
var _ storage.MetadataProvider = (*FileStorage)(nil)

const (
	statusActive  byte = 0x01
	statusDeleted byte = 0x00

	fileMagic       = "ORESQL"
	fileVersionV1   = byte(0x01)
	fileVersionV2   = byte(0x02)
	fileVersion     = fileVersionV2 // current version for new files
	nextRowIDOffset = 7             // offset of nextRowID field in file
	headerFixedSize = 19            // magic(6) + version(1) + nextRowID(8) + metaLength(4)
	noRootPageID    = uint32(0xFFFFFFFF)
)

// FileStorage provides persistent storage backed by files on disk.
// Each table is stored as a single binary .dat file containing a header,
// metadata, and append-only row entries.
// All read operations are served from an in-memory MemoryStorage.
// Write operations are persisted to disk in addition to updating memory.
type FileStorage struct {
	mu      sync.Mutex // protects disk writes
	dataDir string
	mem     *memory.MemoryStorage
}

// NewFileStorage creates a new FileStorage with the given data directory.
// The directory is created if it does not exist.
func NewFileStorage(dataDir string) (*FileStorage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	fs := &FileStorage{
		dataDir: dataDir,
		mem:     memory.NewMemoryStorage(),
	}
	return fs, nil
}

// tablePath returns the path to the .dat file for a table.
func (fs *FileStorage) tablePath(name string) string {
	return filepath.Join(fs.dataDir, strings.ToLower(name)+".dat")
}

// --- Binary metadata encoding/decoding helpers ---

func putString(buf *[]byte, s string) {
	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(len(s)))
	*buf = append(*buf, lenBuf[:]...)
	*buf = append(*buf, s...)
}

func getString(data []byte, pos int) (string, int, error) {
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

// encodeOneValue encodes a single storage.Value into bytes using EncodeRow format.
func encodeOneValue(val storage.Value) []byte {
	return storage.EncodeRow(storage.Row{val})
}

// decodeOneValue decodes a single value from EncodeRow format at the given position.
func decodeOneValue(data []byte, pos int) (storage.Value, int, error) {
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

// encodeMeta encodes TableInfo and IndexInfo list into binary format.
func encodeMeta(info *storage.TableInfo, indexes []*storage.IndexInfo) []byte {
	var buf []byte

	// Table name
	putString(&buf, info.Name)

	// Number of columns
	var numColsBuf [2]byte
	binary.BigEndian.PutUint16(numColsBuf[:], uint16(len(info.Columns)))
	buf = append(buf, numColsBuf[:]...)

	// Per column
	for _, col := range info.Columns {
		putString(&buf, col.Name)
		putString(&buf, col.DataType)

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
			buf = append(buf, encodeOneValue(col.Default)...)
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
		putString(&buf, idx.Name)
		putString(&buf, idx.TableName)

		// Column names
		var numColNamesBuf [2]byte
		binary.BigEndian.PutUint16(numColNamesBuf[:], uint16(len(idx.ColumnNames)))
		buf = append(buf, numColNamesBuf[:]...)
		for _, cn := range idx.ColumnNames {
			putString(&buf, cn)
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
		putString(&buf, idx.Type)

		// Unique
		if idx.Unique {
			buf = append(buf, 0x01)
		} else {
			buf = append(buf, 0x00)
		}
	}

	return buf
}

// decodeMeta decodes binary metadata into TableInfo and IndexInfo list.
func decodeMeta(data []byte) (*storage.TableInfo, []*storage.IndexInfo, error) {
	pos := 0
	info := &storage.TableInfo{}

	// Table name
	var err error
	info.Name, pos, err = getString(data, pos)
	if err != nil {
		return nil, nil, fmt.Errorf("reading table name: %w", err)
	}

	// Number of columns
	if pos+2 > len(data) {
		return nil, nil, fmt.Errorf("unexpected end reading num columns")
	}
	numCols := binary.BigEndian.Uint16(data[pos : pos+2])
	pos += 2

	info.Columns = make([]storage.ColumnInfo, numCols)
	for i := 0; i < int(numCols); i++ {
		col := &info.Columns[i]

		col.Name, pos, err = getString(data, pos)
		if err != nil {
			return nil, nil, fmt.Errorf("reading column name: %w", err)
		}

		col.DataType, pos, err = getString(data, pos)
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
			col.Default, pos, err = decodeOneValue(data, pos)
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

	var indexes []*storage.IndexInfo
	for i := 0; i < int(numIndexes); i++ {
		idx := &storage.IndexInfo{}

		idx.Name, pos, err = getString(data, pos)
		if err != nil {
			return nil, nil, fmt.Errorf("reading index name: %w", err)
		}

		idx.TableName, pos, err = getString(data, pos)
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
			idx.ColumnNames[j], pos, err = getString(data, pos)
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
		idx.Type, pos, err = getString(data, pos)
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

// --- File-level read/write functions ---

// writeFullFileV1 writes a complete v1 .dat file (header + metadata + all row entries).
func writeFullFileV1(path string, info *storage.TableInfo, indexes []*storage.IndexInfo, nextRowID int64, keyRows []storage.KeyRow) error {
	metaBytes := encodeMeta(info, indexes)

	var header [headerFixedSize]byte
	copy(header[0:6], fileMagic)
	header[6] = fileVersionV1
	binary.BigEndian.PutUint64(header[7:15], uint64(nextRowID))
	binary.BigEndian.PutUint32(header[15:19], uint32(len(metaBytes)))

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(header[:]); err != nil {
		return err
	}
	if _, err := f.Write(metaBytes); err != nil {
		return err
	}

	// Write all row entries
	for _, kr := range keyRows {
		encoded := storage.EncodeRow(kr.Row)
		var rowHeader [1 + 8 + 4]byte
		rowHeader[0] = statusActive
		binary.BigEndian.PutUint64(rowHeader[1:9], uint64(kr.Key))
		binary.BigEndian.PutUint32(rowHeader[9:13], uint32(len(encoded)))
		if _, err := f.Write(rowHeader[:]); err != nil {
			return err
		}
		if _, err := f.Write(encoded); err != nil {
			return err
		}
	}

	return f.Sync()
}

// btreeMetaInfo holds BTree metadata for serialization.
type btreeMetaInfo struct {
	rootPageID uint32
	degree     byte
	length     uint32
}

// secondaryTreeMeta holds secondary index BTree metadata.
type secondaryTreeMeta struct {
	indexName  string
	rootPageID uint32
	degree     byte
	length     uint32
}

// writeFullFileV2 writes a complete v2 .dat file with BTree snapshots.
// Format: [Header v2][Metadata + BTree root info][Page Directory][Page Data][Empty Log]
func writeFullFileV2(path string, info *storage.TableInfo, indexes []*storage.IndexInfo,
	nextRowID int64, primaryTree *btree.BTree[int64],
	secondaryIndexes map[string]*memory.SecondaryIndex) error {

	// Collect all pages from BTree walks
	var pages [][]byte // page data
	nextPageID := uint32(0)

	allocPage := func(data []byte) uint32 {
		id := nextPageID
		nextPageID++
		pages = append(pages, data)
		return id
	}

	// Walk primary tree
	var primaryMeta btreeMetaInfo
	if primaryTree != nil {
		rootID, hasRoot := primaryTree.WalkNodes(func(data btree.NodeData[int64]) uint32 {
			return allocPage(encodePrimaryPage(data))
		})
		if hasRoot {
			primaryMeta.rootPageID = rootID
		} else {
			primaryMeta.rootPageID = noRootPageID
		}
		primaryMeta.degree = byte(primaryTree.Degree())
		primaryMeta.length = uint32(primaryTree.Len())
	} else {
		primaryMeta.rootPageID = noRootPageID
		primaryMeta.degree = 32
		primaryMeta.length = 0
	}

	// Walk secondary trees
	var secondaryMetas []secondaryTreeMeta
	if secondaryIndexes != nil {
		for _, idxInfo := range indexes {
			lowerName := strings.ToLower(idxInfo.Name)
			si, ok := secondaryIndexes[lowerName]
			if !ok {
				continue
			}
			tree := si.Tree()
			meta := secondaryTreeMeta{indexName: idxInfo.Name}
			if tree != nil {
				rootID, hasRoot := tree.WalkNodes(func(data btree.NodeData[storage.KeyEncoding]) uint32 {
					return allocPage(encodeSecondaryPage(data))
				})
				if hasRoot {
					meta.rootPageID = rootID
				} else {
					meta.rootPageID = noRootPageID
				}
				meta.degree = byte(tree.Degree())
				meta.length = uint32(tree.Len())
			} else {
				meta.rootPageID = noRootPageID
				meta.degree = 32
				meta.length = 0
			}
			secondaryMetas = append(secondaryMetas, meta)
		}
	}

	// Encode metadata (existing schema + index defs)
	metaBytes := encodeMeta(info, indexes)

	// Encode BTree meta section (appended after existing metadata)
	var btreeMeta []byte

	// Primary tree info
	var primaryRootBuf [4]byte
	binary.BigEndian.PutUint32(primaryRootBuf[:], primaryMeta.rootPageID)
	btreeMeta = append(btreeMeta, primaryRootBuf[:]...)
	btreeMeta = append(btreeMeta, primaryMeta.degree)
	var primaryLenBuf [4]byte
	binary.BigEndian.PutUint32(primaryLenBuf[:], primaryMeta.length)
	btreeMeta = append(btreeMeta, primaryLenBuf[:]...)

	// Secondary trees
	var numSecBuf [2]byte
	binary.BigEndian.PutUint16(numSecBuf[:], uint16(len(secondaryMetas)))
	btreeMeta = append(btreeMeta, numSecBuf[:]...)

	for _, sm := range secondaryMetas {
		putString(&btreeMeta, sm.indexName)
		var rootBuf [4]byte
		binary.BigEndian.PutUint32(rootBuf[:], sm.rootPageID)
		btreeMeta = append(btreeMeta, rootBuf[:]...)
		btreeMeta = append(btreeMeta, sm.degree)
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], sm.length)
		btreeMeta = append(btreeMeta, lenBuf[:]...)
	}

	// Combine metadata
	fullMeta := append(metaBytes, btreeMeta...)

	// Page directory
	var pageDir []byte
	var pageCountBuf [4]byte
	binary.BigEndian.PutUint32(pageCountBuf[:], uint32(len(pages)))
	pageDir = append(pageDir, pageCountBuf[:]...)

	// Calculate page offsets (relative to start of page data section)
	offset := uint32(0)
	for _, p := range pages {
		var offsetBuf [4]byte
		binary.BigEndian.PutUint32(offsetBuf[:], offset)
		pageDir = append(pageDir, offsetBuf[:]...)
		var sizeBuf [4]byte
		binary.BigEndian.PutUint32(sizeBuf[:], uint32(len(p)))
		pageDir = append(pageDir, sizeBuf[:]...)
		offset += uint32(len(p))
	}

	// Write file
	var header [headerFixedSize]byte
	copy(header[0:6], fileMagic)
	header[6] = fileVersionV2
	binary.BigEndian.PutUint64(header[7:15], uint64(nextRowID))
	binary.BigEndian.PutUint32(header[15:19], uint32(len(fullMeta)))

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(header[:]); err != nil {
		return err
	}
	if _, err := f.Write(fullMeta); err != nil {
		return err
	}
	if _, err := f.Write(pageDir); err != nil {
		return err
	}
	for _, p := range pages {
		if _, err := f.Write(p); err != nil {
			return err
		}
	}

	// No append-only log entries (clean snapshot)
	return f.Sync()
}

// writeFullFile writes a complete .dat file in current version (v2) format.
func writeFullFile(path string, info *storage.TableInfo, indexes []*storage.IndexInfo, nextRowID int64, keyRows []storage.KeyRow) error {
	// For simple callers that don't have BTree access (CreateTable, TruncateTable),
	// use v1 format as the trees will be rebuilt anyway.
	return writeFullFileV1(path, info, indexes, nextRowID, keyRows)
}

// readFileHeader reads just the header to determine the version.
func readFileHeader(data []byte) (version byte, nextRowID int64, metaLength int, err error) {
	if len(data) < headerFixedSize {
		return 0, 0, 0, fmt.Errorf("file too short: %d bytes", len(data))
	}
	if string(data[0:6]) != fileMagic {
		return 0, 0, 0, fmt.Errorf("invalid magic: %q", string(data[0:6]))
	}
	version = data[6]
	nextRowID = int64(binary.BigEndian.Uint64(data[7:15]))
	metaLength = int(binary.BigEndian.Uint32(data[15:19]))
	return version, nextRowID, metaLength, nil
}

// readFile reads a v1 .dat file and returns the table info, indexes, nextRowID, and raw row data bytes.
func readFile(path string) (*storage.TableInfo, []*storage.IndexInfo, int64, []byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, 0, nil, err
	}

	version, nextRowID, metaLength, err := readFileHeader(data)
	if err != nil {
		return nil, nil, 0, nil, err
	}

	if version != fileVersionV1 {
		return nil, nil, 0, nil, fmt.Errorf("expected v1 but got version: %d", version)
	}

	if headerFixedSize+metaLength > len(data) {
		return nil, nil, 0, nil, fmt.Errorf("file too short for metadata: need %d, have %d", headerFixedSize+metaLength, len(data))
	}

	metaData := data[headerFixedSize : headerFixedSize+metaLength]
	info, indexes, err := decodeMeta(metaData)
	if err != nil {
		return nil, nil, 0, nil, fmt.Errorf("failed to decode metadata: %w", err)
	}

	rowData := data[headerFixedSize+metaLength:]
	return info, indexes, nextRowID, rowData, nil
}

// readFileV2 reads a v2 .dat file and restores BTree snapshots into memory storage.
func readFileV2(path string, mem *memory.MemoryStorage) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	version, nextRowID, metaLength, err := readFileHeader(data)
	if err != nil {
		return err
	}
	if version != fileVersionV2 {
		return fmt.Errorf("expected v2 but got version: %d", version)
	}

	if headerFixedSize+metaLength > len(data) {
		return fmt.Errorf("file too short for metadata: need %d, have %d", headerFixedSize+metaLength, len(data))
	}

	fullMeta := data[headerFixedSize : headerFixedSize+metaLength]

	// Decode the standard metadata first
	info, indexes, err := decodeMeta(fullMeta)
	if err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}

	// Find where standard metadata ends by re-encoding and checking length
	standardMeta := encodeMeta(info, indexes)
	btreeMetaStart := len(standardMeta)

	if btreeMetaStart+9 > len(fullMeta) {
		return fmt.Errorf("metadata too short for BTree info")
	}

	btreeMetaData := fullMeta[btreeMetaStart:]
	pos := 0

	// Primary tree info
	primaryRootPageID := binary.BigEndian.Uint32(btreeMetaData[pos : pos+4])
	pos += 4
	primaryDegree := int(btreeMetaData[pos])
	pos++
	primaryLength := int(binary.BigEndian.Uint32(btreeMetaData[pos : pos+4]))
	pos += 4

	// Secondary trees
	if pos+2 > len(btreeMetaData) {
		return fmt.Errorf("metadata too short for secondary tree count")
	}
	numSecondary := int(binary.BigEndian.Uint16(btreeMetaData[pos : pos+2]))
	pos += 2

	type secMeta struct {
		indexName  string
		rootPageID uint32
		degree     int
		length     int
	}
	secMetas := make([]secMeta, numSecondary)
	for i := 0; i < numSecondary; i++ {
		name, newPos, err := getString(btreeMetaData, pos)
		if err != nil {
			return fmt.Errorf("reading secondary tree name: %w", err)
		}
		pos = newPos

		if pos+9 > len(btreeMetaData) {
			return fmt.Errorf("metadata too short for secondary tree %d info", i)
		}
		rootPageID := binary.BigEndian.Uint32(btreeMetaData[pos : pos+4])
		pos += 4
		degree := int(btreeMetaData[pos])
		pos++
		length := int(binary.BigEndian.Uint32(btreeMetaData[pos : pos+4]))
		pos += 4

		secMetas[i] = secMeta{name, rootPageID, degree, length}
	}

	// Read page directory
	afterMeta := headerFixedSize + metaLength
	if afterMeta+4 > len(data) {
		return fmt.Errorf("file too short for page directory")
	}

	pageDirStart := afterMeta
	pageCount := int(binary.BigEndian.Uint32(data[pageDirStart : pageDirStart+4]))
	pageDirEnd := pageDirStart + 4 + pageCount*8 // 4 bytes offset + 4 bytes size per page

	if pageDirEnd > len(data) {
		return fmt.Errorf("file too short for page directory entries")
	}

	type pageEntry struct {
		offset uint32
		size   uint32
	}
	pageEntries := make([]pageEntry, pageCount)
	dirPos := pageDirStart + 4
	for i := 0; i < pageCount; i++ {
		pageEntries[i].offset = binary.BigEndian.Uint32(data[dirPos : dirPos+4])
		dirPos += 4
		pageEntries[i].size = binary.BigEndian.Uint32(data[dirPos : dirPos+4])
		dirPos += 4
	}

	pageDataStart := pageDirEnd

	getPageData := func(pageID uint32) []byte {
		if int(pageID) >= pageCount {
			return nil
		}
		pe := pageEntries[pageID]
		start := pageDataStart + int(pe.offset)
		end := start + int(pe.size)
		if end > len(data) {
			return nil
		}
		return data[start:end]
	}

	// Create table in memory
	mem.CreateTable(info)

	// Restore primary BTree
	if primaryRootPageID != noRootPageID {
		primaryTree := btree.BuildFromNodes[int64](primaryDegree, primaryLength, primaryRootPageID,
			func(pageID uint32) btree.NodeData[int64] {
				pageData := getPageData(pageID)
				if pageData == nil {
					return btree.NodeData[int64]{Leaf: true}
				}
				nd, err := decodePrimaryPage(pageData)
				if err != nil {
					return btree.NodeData[int64]{Leaf: true}
				}
				return nd
			})
		mem.SetPrimaryTree(info.Name, primaryTree)
	}

	// Restore secondary indexes
	for _, idxInfo := range indexes {
		mem.CreateIndexEmpty(idxInfo)
	}

	for _, sm := range secMetas {
		if sm.rootPageID == noRootPageID {
			continue
		}
		secondaryTree := btree.BuildFromNodes[storage.KeyEncoding](sm.degree, sm.length, sm.rootPageID,
			func(pageID uint32) btree.NodeData[storage.KeyEncoding] {
				pageData := getPageData(pageID)
				if pageData == nil {
					return btree.NodeData[storage.KeyEncoding]{Leaf: true}
				}
				nd, err := decodeSecondaryPage(pageData)
				if err != nil {
					return btree.NodeData[storage.KeyEncoding]{Leaf: true}
				}
				return nd
			})

		allIndexes := mem.GetAllSecondaryTrees(info.Name)
		if si, ok := allIndexes[strings.ToLower(sm.indexName)]; ok {
			si.SetTree(secondaryTree)
		}
	}

	// Restore nextRowID
	mem.SetNextRowID(info.Name, nextRowID)

	// Replay any append-only log entries after the page data
	totalPageDataSize := 0
	for _, pe := range pageEntries {
		totalPageDataSize += int(pe.size)
	}
	logStart := pageDataStart + totalPageDataSize

	if logStart < len(data) {
		logData := data[logStart:]
		if err := replayLog(info, mem, logData); err != nil {
			return fmt.Errorf("failed to replay log: %w", err)
		}
	}

	return nil
}

// replayLog replays append-only log entries into memory storage.
func replayLog(info *storage.TableInfo, mem *memory.MemoryStorage, logData []byte) error {
	pos := 0
	for pos < len(logData) {
		if pos+1+8+4 > len(logData) {
			break
		}
		status := logData[pos]
		pos++
		key := int64(binary.BigEndian.Uint64(logData[pos : pos+8]))
		pos += 8
		dataLen := int(binary.BigEndian.Uint32(logData[pos : pos+4]))
		pos += 4
		if pos+dataLen > len(logData) {
			break
		}
		rowBytes := logData[pos : pos+dataLen]
		pos += dataLen

		if status == statusActive {
			row, err := storage.DecodeRow(rowBytes)
			if err != nil {
				return fmt.Errorf("failed to decode log row (key=%d): %w", key, err)
			}
			if info.PrimaryKeyCol >= 0 {
				// For PK tables, use Put semantics (insert or update)
				mem.InsertWithKey(info.Name, key, row)
			} else {
				mem.InsertWithKey(info.Name, key, row)
			}
		} else {
			mem.DeleteByKeys(info.Name, []int64{key})
		}
	}
	return nil
}

// updateNextRowID writes the nextRowID at offset 7 in the .dat file.
func updateNextRowID(path string, nextRowID int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(nextRowID))
	if _, err := f.WriteAt(buf[:], nextRowIDOffset); err != nil {
		return err
	}
	return f.Sync()
}

// --- Table loading ---

// LoadAll loads all tables from the data directory into memory.
// Should be called once at startup before any queries.
func (fs *FileStorage) LoadAll() error {
	entries, err := os.ReadDir(fs.dataDir)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".dat") {
			continue
		}
		tableName := strings.TrimSuffix(name, ".dat")
		if err := fs.loadTable(tableName); err != nil {
			return fmt.Errorf("failed to load table %q: %w", tableName, err)
		}
	}
	return nil
}

// loadTable loads a single table from its .dat file into memory.
func (fs *FileStorage) loadTable(tableName string) error {
	path := fs.tablePath(tableName)

	// Read the file header to determine version
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", path, err)
	}

	version, _, _, err := readFileHeader(data)
	if err != nil {
		return fmt.Errorf("failed to read header of %s: %w", path, err)
	}

	switch version {
	case fileVersionV2:
		return fs.loadTableV2(path)
	case fileVersionV1:
		return fs.loadTableV1(path)
	default:
		return fmt.Errorf("unsupported file version: %d", version)
	}
}

// loadTableV1 loads a table from a v1 .dat file (append-only log replay).
func (fs *FileStorage) loadTableV1(path string) error {
	info, indexes, nextRowID, rowsData, err := readFile(path)
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", path, err)
	}

	// Create the table in memory
	fs.mem.CreateTable(info)

	// Parse rows and track active/deleted state
	activeRows := make(map[int64]storage.Row)
	pos := 0
	for pos < len(rowsData) {
		if pos+1+8+4 > len(rowsData) {
			break // incomplete record at end
		}
		status := rowsData[pos]
		pos++
		key := int64(binary.BigEndian.Uint64(rowsData[pos : pos+8]))
		pos += 8
		dataLen := int(binary.BigEndian.Uint32(rowsData[pos : pos+4]))
		pos += 4
		if pos+dataLen > len(rowsData) {
			break // incomplete record at end
		}
		data := rowsData[pos : pos+dataLen]
		pos += dataLen

		if status == statusActive {
			row, err := storage.DecodeRow(data)
			if err != nil {
				return fmt.Errorf("failed to decode row (key=%d): %w", key, err)
			}
			activeRows[key] = row
		} else {
			delete(activeRows, key)
		}
	}

	// Insert active rows into memory storage
	for key, row := range activeRows {
		if info.PrimaryKeyCol >= 0 {
			if err := fs.mem.Insert(info.Name, row); err != nil {
				return fmt.Errorf("failed to restore row (key=%d): %w", key, err)
			}
		} else {
			if err := fs.mem.InsertWithKey(info.Name, key, row); err != nil {
				return fmt.Errorf("failed to restore row (key=%d): %w", key, err)
			}
		}
	}

	// Restore nextRowID
	fs.mem.SetNextRowID(info.Name, nextRowID)

	// Create indexes
	for _, idxInfo := range indexes {
		if err := fs.mem.CreateIndex(idxInfo); err != nil {
			return fmt.Errorf("failed to restore index %q: %w", idxInfo.Name, err)
		}
	}

	// Migrate to v2 on load
	tableName := info.Name
	return fs.rewriteTableV2(tableName)
}

// loadTableV2 loads a table from a v2 .dat file (BTree snapshot + optional log replay).
func (fs *FileStorage) loadTableV2(path string) error {
	return readFileV2(path, fs.mem)
}

// ListTables returns all table names from the data directory.
func (fs *FileStorage) ListTables() []string {
	entries, err := os.ReadDir(fs.dataDir)
	if err != nil {
		return nil
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".dat") {
			names = append(names, strings.TrimSuffix(name, ".dat"))
		}
	}
	return names
}

// LoadTableMeta loads the metadata for a single table from disk.
func (fs *FileStorage) LoadTableMeta(name string) (*storage.TableInfo, []*storage.IndexInfo, int64, error) {
	path := fs.tablePath(name)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("failed to read %s: %w", path, err)
	}

	_, nextRowID, metaLength, err := readFileHeader(data)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("failed to read header of %s: %w", path, err)
	}

	if headerFixedSize+metaLength > len(data) {
		return nil, nil, 0, fmt.Errorf("file too short for metadata")
	}

	metaData := data[headerFixedSize : headerFixedSize+metaLength]
	info, indexes, err := decodeMeta(metaData)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("failed to decode metadata: %w", err)
	}

	return info, indexes, nextRowID, nil
}

// --- Disk write helpers ---

// appendRowEntry appends a row entry to the .dat file.
func (fs *FileStorage) appendRowEntry(tableName string, status byte, key int64, row storage.Row) error {
	path := fs.tablePath(tableName)

	f, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Seek to end for append
	if _, err := f.Seek(0, 2); err != nil {
		return err
	}

	var encoded []byte
	if row != nil {
		encoded = storage.EncodeRow(row)
	}

	// Write: [status:1][key:8][length:4][data:N]
	var header [1 + 8 + 4]byte
	header[0] = status
	binary.BigEndian.PutUint64(header[1:9], uint64(key))
	binary.BigEndian.PutUint32(header[9:13], uint32(len(encoded)))

	if _, err := f.Write(header[:]); err != nil {
		return err
	}
	if len(encoded) > 0 {
		if _, err := f.Write(encoded); err != nil {
			return err
		}
	}
	return f.Sync()
}

// appendDeleteEntry appends a deletion marker to the .dat file.
func (fs *FileStorage) appendDeleteEntry(tableName string, key int64) error {
	return fs.appendRowEntry(tableName, statusDeleted, key, nil)
}

// getTableMeta retrieves the current meta from the in-memory storage.
func (fs *FileStorage) getTableMeta(tableName string) (*storage.TableInfo, []*storage.IndexInfo, int64) {
	return fs.mem.GetTableMeta(tableName)
}

// rewriteTable rewrites the entire .dat file from the current in-memory state.
// Used after schema changes (AddColumn, DropColumn) that modify all rows.
// Uses ScanWithKeysNoLock because the caller (via WithTableLocks) already holds the table lock.
func (fs *FileStorage) rewriteTable(tableName string) error {
	return fs.rewriteTableV2(tableName)
}

// rewriteTableV2 rewrites the entire .dat file using v2 format with BTree snapshots.
func (fs *FileStorage) rewriteTableV2(tableName string) error {
	info, indexes, nextRowID := fs.getTableMeta(tableName)
	if info == nil {
		return nil
	}

	primaryTree := fs.mem.GetPrimaryTree(tableName)
	secondaryIndexes := fs.mem.GetAllSecondaryTrees(tableName)

	return writeFullFileV2(fs.tablePath(tableName), info, indexes, nextRowID,
		primaryTree, secondaryIndexes)
}

// rewriteTableMetaOnly rewrites the .dat file with updated metadata.
// For v2, this does a full v2 rewrite since metadata includes BTree info.
func (fs *FileStorage) rewriteTableMetaOnly(tableName string) error {
	return fs.rewriteTableV2(tableName)
}

// --- storage.Engine implementation ---

func (fs *FileStorage) CreateTable(info *storage.TableInfo) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.mem.CreateTable(info)
	// Write .dat file with v2 format (empty BTree snapshot)
	writeFullFileV2(fs.tablePath(info.Name), info, nil, 1, nil, nil)
}

func (fs *FileStorage) DropTable(name string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.mem.DropTable(name)
	os.Remove(fs.tablePath(name))
}

func (fs *FileStorage) TruncateTable(name string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.mem.TruncateTable(name)
	// Rewrite .dat file with v2 format (empty BTree snapshot)
	info, indexes, nextRowID := fs.getTableMeta(name)
	if info != nil {
		writeFullFileV2(fs.tablePath(name), info, indexes, nextRowID, nil, nil)
	}
}

func (fs *FileStorage) Insert(tableName string, row storage.Row) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Get key before insert to know what key will be used
	_, _, nextRowID := fs.getTableMeta(tableName)

	if err := fs.mem.Insert(tableName, row); err != nil {
		return err
	}

	// Determine the key that was used
	info, _, newNextRowID := fs.getTableMeta(tableName)
	var key int64
	if info != nil && info.PrimaryKeyCol >= 0 {
		key = row[info.PrimaryKeyCol].(int64)
	} else {
		key = nextRowID // the auto-increment key used
	}

	// Append row entry to .dat file
	if err := fs.appendRowEntry(tableName, statusActive, key, row); err != nil {
		return err
	}

	// Update nextRowID in-place at fixed offset
	if newNextRowID != nextRowID {
		return updateNextRowID(fs.tablePath(tableName), newNextRowID)
	}
	return nil
}

func (fs *FileStorage) DeleteByKeys(tableName string, keys []int64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if err := fs.mem.DeleteByKeys(tableName, keys); err != nil {
		return err
	}

	// Append delete entries
	for _, key := range keys {
		if err := fs.appendDeleteEntry(tableName, key); err != nil {
			return err
		}
	}
	return nil
}

func (fs *FileStorage) UpdateRow(tableName string, key int64, row storage.Row) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if err := fs.mem.UpdateRow(tableName, key, row); err != nil {
		return err
	}

	// Append updated row (overwrites previous entry for this key on reload)
	return fs.appendRowEntry(tableName, statusActive, key, row)
}

func (fs *FileStorage) AddColumn(tableName string, defaultVal storage.Value) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if err := fs.mem.AddColumn(tableName, defaultVal); err != nil {
		return err
	}

	return fs.rewriteTable(tableName)
}

func (fs *FileStorage) DropColumn(tableName string, colIdx int) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if err := fs.mem.DropColumn(tableName, colIdx); err != nil {
		return err
	}

	return fs.rewriteTable(tableName)
}

func (fs *FileStorage) CreateIndex(info *storage.IndexInfo) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if err := fs.mem.CreateIndex(info); err != nil {
		return err
	}

	// Only metadata changed (rows unchanged), rewrite with raw row data from disk
	return fs.rewriteTableMetaOnly(info.TableName)
}

func (fs *FileStorage) DropIndex(indexName string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Get table name before dropping index
	tableName, ok := fs.mem.ResolveIndexTable(indexName)
	if !ok {
		return fs.mem.DropIndex(indexName) // let MemoryStorage return the error
	}

	if err := fs.mem.DropIndex(indexName); err != nil {
		return err
	}

	// Only metadata changed (rows unchanged), rewrite with raw row data from disk
	return fs.rewriteTableMetaOnly(tableName)
}

func (fs *FileStorage) HasIndex(indexName string) bool {
	return fs.mem.HasIndex(indexName)
}

func (fs *FileStorage) LookupIndex(tableName string, columnIdxs []int) storage.IndexReader {
	return fs.mem.LookupIndex(tableName, columnIdxs)
}

func (fs *FileStorage) LookupSingleColumnIndex(tableName string, colIdx int) storage.IndexReader {
	return fs.mem.LookupSingleColumnIndex(tableName, colIdx)
}

func (fs *FileStorage) GetIndexes(tableName string) []storage.IndexReader {
	return fs.mem.GetIndexes(tableName)
}

func (fs *FileStorage) Scan(tableName string) ([]storage.Row, error) {
	return fs.mem.Scan(tableName)
}

func (fs *FileStorage) ScanOrdered(tableName string, reverse bool, limit int) ([]storage.Row, error) {
	return fs.mem.ScanOrdered(tableName, reverse, limit)
}

func (fs *FileStorage) ScanWithKeys(tableName string) ([]storage.KeyRow, error) {
	return fs.mem.ScanWithKeys(tableName)
}

func (fs *FileStorage) GetByKeys(tableName string, keys []int64) ([]storage.Row, error) {
	return fs.mem.GetByKeys(tableName, keys)
}

func (fs *FileStorage) GetKeyRowsByKeys(tableName string, keys []int64) ([]storage.KeyRow, error) {
	return fs.mem.GetKeyRowsByKeys(tableName, keys)
}

func (fs *FileStorage) RowCount(tableName string) (int, error) {
	return fs.mem.RowCount(tableName)
}

func (fs *FileStorage) ForEachRow(tableName string, reverse bool, fn func(key int64, row storage.Row) bool, limit int) error {
	return fs.mem.ForEachRow(tableName, reverse, fn, limit)
}

func (fs *FileStorage) GetRow(tableName string, key int64) (storage.Row, bool) {
	return fs.mem.GetRow(tableName, key)
}

// --- storage.TableLocker implementation (delegates to MemoryStorage) ---

func (fs *FileStorage) WithTableLocks(locks []storage.TableLock, catalogWrite bool, fn func() error) error {
	return fs.mem.WithTableLocks(locks, catalogWrite, fn)
}

func (fs *FileStorage) WithCatalogLock(write bool, fn func() error) error {
	return fs.mem.WithCatalogLock(write, fn)
}

func (fs *FileStorage) ResolveIndexTable(indexName string) (string, bool) {
	return fs.mem.ResolveIndexTable(indexName)
}

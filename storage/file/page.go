package file

import (
	"encoding/binary"
	"fmt"

	"github.com/walf443/oresql/btree"
	"github.com/walf443/oresql/storage"
)

// Page format:
//   [Flags: 1B]       bit0: leaf=1
//   [EntryCount: 2B]  uint16
//   [ChildCount: 2B]  uint16 (leaf: 0)
//   Entries (EntryCount times):
//     Primary: [Key: 8B int64] [ValueLen: 4B] [Value: EncodeRow format]
//     Secondary: [KeyLen: 2B] [Key: N B] [RowIDCount: 4B] [RowIDs: each 8B int64]
//   Children (ChildCount times):
//     [ChildPageID: 4B uint32]

const (
	pageFlagLeaf byte = 0x01
)

// encodePrimaryPage encodes a primary BTree node into a page byte slice.
// Entry values are storage.Row encoded via storage.EncodeRow.
func encodePrimaryPage(data btree.NodeData[int64]) []byte {
	var buf []byte

	// Flags
	var flags byte
	if data.Leaf {
		flags |= pageFlagLeaf
	}
	buf = append(buf, flags)

	// EntryCount
	var entryCountBuf [2]byte
	binary.BigEndian.PutUint16(entryCountBuf[:], uint16(len(data.Entries)))
	buf = append(buf, entryCountBuf[:]...)

	// ChildCount
	var childCountBuf [2]byte
	childCount := uint16(0)
	if !data.Leaf {
		childCount = uint16(len(data.Children))
	}
	binary.BigEndian.PutUint16(childCountBuf[:], childCount)
	buf = append(buf, childCountBuf[:]...)

	// Entries
	for _, e := range data.Entries {
		// Key: 8B int64
		var keyBuf [8]byte
		binary.BigEndian.PutUint64(keyBuf[:], uint64(e.Key))
		buf = append(buf, keyBuf[:]...)

		// Value: encode as Row
		var encoded []byte
		if e.Value != nil {
			row := e.Value.(storage.Row)
			encoded = storage.EncodeRow(row)
		}
		var valLenBuf [4]byte
		binary.BigEndian.PutUint32(valLenBuf[:], uint32(len(encoded)))
		buf = append(buf, valLenBuf[:]...)
		buf = append(buf, encoded...)
	}

	// Children
	if !data.Leaf {
		for _, childID := range data.Children {
			var childBuf [4]byte
			binary.BigEndian.PutUint32(childBuf[:], childID)
			buf = append(buf, childBuf[:]...)
		}
	}

	return buf
}

// decodePrimaryPage decodes a page byte slice into a primary BTree node.
func decodePrimaryPage(buf []byte) (btree.NodeData[int64], error) {
	var data btree.NodeData[int64]
	if len(buf) < 5 {
		return data, fmt.Errorf("page too short: %d bytes", len(buf))
	}

	pos := 0

	// Flags
	flags := buf[pos]
	pos++
	data.Leaf = flags&pageFlagLeaf != 0

	// EntryCount
	entryCount := int(binary.BigEndian.Uint16(buf[pos : pos+2]))
	pos += 2

	// ChildCount
	childCount := int(binary.BigEndian.Uint16(buf[pos : pos+2]))
	pos += 2

	// Entries
	data.Entries = make([]btree.EntryData[int64], entryCount)
	for i := 0; i < entryCount; i++ {
		if pos+8 > len(buf) {
			return data, fmt.Errorf("unexpected end reading entry key at entry %d", i)
		}
		key := int64(binary.BigEndian.Uint64(buf[pos : pos+8]))
		pos += 8

		if pos+4 > len(buf) {
			return data, fmt.Errorf("unexpected end reading value length at entry %d", i)
		}
		valLen := int(binary.BigEndian.Uint32(buf[pos : pos+4]))
		pos += 4

		var value any
		if valLen > 0 {
			if pos+valLen > len(buf) {
				return data, fmt.Errorf("unexpected end reading value data at entry %d", i)
			}
			row, err := storage.DecodeRow(buf[pos : pos+valLen])
			if err != nil {
				return data, fmt.Errorf("failed to decode row at entry %d: %w", i, err)
			}
			value = row
			pos += valLen
		}

		data.Entries[i] = btree.EntryData[int64]{Key: key, Value: value}
	}

	// Children
	if childCount > 0 {
		data.Children = make([]uint32, childCount)
		for i := 0; i < childCount; i++ {
			if pos+4 > len(buf) {
				return data, fmt.Errorf("unexpected end reading child page ID at child %d", i)
			}
			data.Children[i] = binary.BigEndian.Uint32(buf[pos : pos+4])
			pos += 4
		}
	}

	return data, nil
}

// encodeSecondaryPage encodes a secondary index BTree node into a page byte slice.
// Entry values are map[int64]struct{} (set of row keys).
func encodeSecondaryPage(data btree.NodeData[storage.KeyEncoding]) []byte {
	var buf []byte

	// Flags
	var flags byte
	if data.Leaf {
		flags |= pageFlagLeaf
	}
	buf = append(buf, flags)

	// EntryCount
	var entryCountBuf [2]byte
	binary.BigEndian.PutUint16(entryCountBuf[:], uint16(len(data.Entries)))
	buf = append(buf, entryCountBuf[:]...)

	// ChildCount
	var childCountBuf [2]byte
	childCount := uint16(0)
	if !data.Leaf {
		childCount = uint16(len(data.Children))
	}
	binary.BigEndian.PutUint16(childCountBuf[:], childCount)
	buf = append(buf, childCountBuf[:]...)

	// Entries
	for _, e := range data.Entries {
		// KeyLen + Key
		keyBytes := []byte(e.Key)
		var keyLenBuf [2]byte
		binary.BigEndian.PutUint16(keyLenBuf[:], uint16(len(keyBytes)))
		buf = append(buf, keyLenBuf[:]...)
		buf = append(buf, keyBytes...)

		// RowIDs
		var rowIDs []int64
		if e.Value != nil {
			keySet := e.Value.(map[int64]struct{})
			rowIDs = make([]int64, 0, len(keySet))
			for k := range keySet {
				rowIDs = append(rowIDs, k)
			}
		}
		var rowIDCountBuf [4]byte
		binary.BigEndian.PutUint32(rowIDCountBuf[:], uint32(len(rowIDs)))
		buf = append(buf, rowIDCountBuf[:]...)
		for _, rowID := range rowIDs {
			var ridBuf [8]byte
			binary.BigEndian.PutUint64(ridBuf[:], uint64(rowID))
			buf = append(buf, ridBuf[:]...)
		}
	}

	// Children
	if !data.Leaf {
		for _, childID := range data.Children {
			var childBuf [4]byte
			binary.BigEndian.PutUint32(childBuf[:], childID)
			buf = append(buf, childBuf[:]...)
		}
	}

	return buf
}

// decodeSecondaryPage decodes a page byte slice into a secondary index BTree node.
func decodeSecondaryPage(buf []byte) (btree.NodeData[storage.KeyEncoding], error) {
	var data btree.NodeData[storage.KeyEncoding]
	if len(buf) < 5 {
		return data, fmt.Errorf("page too short: %d bytes", len(buf))
	}

	pos := 0

	// Flags
	flags := buf[pos]
	pos++
	data.Leaf = flags&pageFlagLeaf != 0

	// EntryCount
	entryCount := int(binary.BigEndian.Uint16(buf[pos : pos+2]))
	pos += 2

	// ChildCount
	childCount := int(binary.BigEndian.Uint16(buf[pos : pos+2]))
	pos += 2

	// Entries
	data.Entries = make([]btree.EntryData[storage.KeyEncoding], entryCount)
	for i := 0; i < entryCount; i++ {
		if pos+2 > len(buf) {
			return data, fmt.Errorf("unexpected end reading key length at entry %d", i)
		}
		keyLen := int(binary.BigEndian.Uint16(buf[pos : pos+2]))
		pos += 2

		if pos+keyLen > len(buf) {
			return data, fmt.Errorf("unexpected end reading key data at entry %d", i)
		}
		key := storage.KeyEncoding(buf[pos : pos+keyLen])
		pos += keyLen

		if pos+4 > len(buf) {
			return data, fmt.Errorf("unexpected end reading rowID count at entry %d", i)
		}
		rowIDCount := int(binary.BigEndian.Uint32(buf[pos : pos+4]))
		pos += 4

		keySet := make(map[int64]struct{}, rowIDCount)
		for j := 0; j < rowIDCount; j++ {
			if pos+8 > len(buf) {
				return data, fmt.Errorf("unexpected end reading rowID at entry %d, rowID %d", i, j)
			}
			rowID := int64(binary.BigEndian.Uint64(buf[pos : pos+8]))
			pos += 8
			keySet[rowID] = struct{}{}
		}

		data.Entries[i] = btree.EntryData[storage.KeyEncoding]{Key: key, Value: keySet}
	}

	// Children
	if childCount > 0 {
		data.Children = make([]uint32, childCount)
		for i := 0; i < childCount; i++ {
			if pos+4 > len(buf) {
				return data, fmt.Errorf("unexpected end reading child page ID at child %d", i)
			}
			data.Children[i] = binary.BigEndian.Uint32(buf[pos : pos+4])
			pos += 4
		}
	}

	return data, nil
}

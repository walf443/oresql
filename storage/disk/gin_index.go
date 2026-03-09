package disk

import (
	"encoding/binary"
	"sort"
	"strings"
	"unicode"

	"github.com/walf443/oresql/storage"
)

// Compile-time verification that DiskGinIndex satisfies GinIndexReader.
var _ storage.GinIndexReader = (*DiskGinIndex)(nil)

// DiskGinIndex implements storage.GinIndexReader backed by a DiskSecondaryBTree.
// Each entry in the tree is: EncodeValue(lowercase_token) || BigEndian(rowKey).
// This allows prefix scan by token to find all matching row keys.
type DiskGinIndex struct {
	info *storage.IndexInfo
	tree *DiskSecondaryBTree
}

func (dgi *DiskGinIndex) GetInfo() *storage.IndexInfo {
	return dgi.info
}

// MatchToken returns sorted row keys whose indexed column contains the given token.
func (dgi *DiskGinIndex) MatchToken(token string) []int64 {
	lower := strings.ToLower(token)
	prefix := ginEncodeToken(lower)

	var keys []int64
	dgi.tree.PrefixScan(prefix, func(compositeKey []byte) bool {
		// compositeKey = EncodeValue(token) || BigEndian(rowKey)
		// rowKey is the last 8 bytes
		if len(compositeKey) >= 8 {
			rowKey := int64(binary.BigEndian.Uint64(compositeKey[len(compositeKey)-8:]))
			keys = append(keys, rowKey)
		}
		return true
	})
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// AddRow indexes all tokens from the TEXT value in the specified column.
func (dgi *DiskGinIndex) AddRow(key int64, row storage.Row) {
	colIdx := dgi.info.ColumnIdxs[0]
	val := row[colIdx]
	text, ok := val.(string)
	if !ok {
		return // NULL or non-string values are not indexed
	}
	for _, tok := range ginTokenize(text) {
		compositeKey := ginEncodeEntry(tok, key)
		dgi.tree.Insert(compositeKey)
	}
}

// RemoveRow removes all token entries for the given row.
func (dgi *DiskGinIndex) RemoveRow(key int64, row storage.Row) {
	colIdx := dgi.info.ColumnIdxs[0]
	val := row[colIdx]
	text, ok := val.(string)
	if !ok {
		return
	}
	for _, tok := range ginTokenize(text) {
		compositeKey := ginEncodeEntry(tok, key)
		dgi.tree.Delete(compositeKey)
	}
}

// ginEncodeToken encodes a token as a sort-preserving key prefix.
// Uses TEXT encoding: 0x03 + raw bytes + 0x00 (null terminator).
func ginEncodeToken(token string) []byte {
	return storage.EncodeValueBytes(nil, token)
}

// ginEncodeEntry encodes a token + rowKey pair as a composite key
// for the DiskSecondaryBTree: EncodeValue(token) || BigEndian(rowKey).
func ginEncodeEntry(token string, rowKey int64) []byte {
	buf := storage.EncodeValueBytes(nil, token)
	var keyBuf [8]byte
	binary.BigEndian.PutUint64(keyBuf[:], uint64(rowKey))
	return append(buf, keyBuf[:]...)
}

// ginTokenize splits text into lowercase tokens by whitespace and punctuation.
func ginTokenize(text string) []string {
	words := strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	tokens := make([]string, len(words))
	for i, w := range words {
		tokens[i] = strings.ToLower(w)
	}
	return tokens
}

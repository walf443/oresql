package disk

import (
	"sort"
	"strings"
	"unicode"

	"github.com/walf443/oresql/storage"
)

// Compile-time verification that DiskGinIndex satisfies GinIndexReader.
var _ storage.GinIndexReader = (*DiskGinIndex)(nil)

// DiskGinIndex implements storage.GinIndexReader backed by a DiskSecondaryBTree.
//
// Each entry in the tree stores a posting list:
//
//	key = EncodeValue(token) || roaringBitmapBytes
//
// where roaringBitmapBytes is a Roaring Bitmap encoded sorted set of row keys.
// This achieves one BTree entry per unique token instead of one per (token, rowKey) pair.
type DiskGinIndex struct {
	info *storage.IndexInfo
	tree *DiskSecondaryBTree
}

func (dgi *DiskGinIndex) GetInfo() *storage.IndexInfo {
	return dgi.info
}

// MatchToken returns sorted row keys whose indexed column contains the given token.
// For bigram tokenizer, the search term is split into bigrams and the intersection
// of all posting lists is returned.
func (dgi *DiskGinIndex) MatchToken(token string) []int64 {
	if dgi.info.Tokenizer == "bigram" {
		return dgi.matchBigram(token)
	}
	lower := strings.ToLower(token)
	prefix := ginEncodeToken(lower)

	var keys []int64
	dgi.tree.PrefixScan(prefix, func(compositeKey []byte) bool {
		postingData := compositeKey[len(prefix):]
		keys = decodePostingList(postingData)
		return false // only one entry per token
	})
	return keys
}

// matchBigram splits the search term into bigrams and returns the intersection
// of all posting lists. Bigrams are processed in ascending order of posting list
// size to minimize intermediate result sizes. Uses Roaring Bitmap And operation
// to intersect posting lists without repeated encode/decode cycles.
func (dgi *DiskGinIndex) matchBigram(token string) []int64 {
	bigrams := ginBigramTokenize(token)
	if len(bigrams) == 0 {
		return nil
	}

	// Get posting list sizes for all bigrams
	type bigramInfo struct {
		token string
		count uint64
	}
	infos := make([]bigramInfo, 0, len(bigrams))
	for _, bg := range bigrams {
		c := dgi.postingListCount(bg)
		if c == 0 {
			return nil // any empty posting list means no results
		}
		infos = append(infos, bigramInfo{bg, c})
	}

	// Sort by posting list size (smallest first)
	sort.Slice(infos, func(i, j int) bool { return infos[i].count < infos[j].count })

	// Start with the smallest posting list as a RoaringBitmap
	result := dgi.lookupRoaringBitmap(infos[0].token)
	if result == nil || result.Cardinality() == 0 {
		return nil
	}

	// Intersect with remaining bigrams using Roaring Bitmap And
	for _, info := range infos[1:] {
		other := dgi.lookupRoaringBitmap(info.token)
		if other == nil || other.Cardinality() == 0 {
			return nil
		}
		result = result.And(other)
		if result.Cardinality() == 0 {
			return nil
		}
	}

	return result.ToInt64Slice()
}

// postingListCount returns the number of entries in the posting list for a token
// by decoding the Roaring Bitmap and reading its cardinality.
func (dgi *DiskGinIndex) postingListCount(token string) uint64 {
	rb := dgi.lookupRoaringBitmap(token)
	if rb == nil {
		return 0
	}
	return uint64(rb.Cardinality())
}

// lookupRoaringBitmap returns the Roaring Bitmap for a single token.
func (dgi *DiskGinIndex) lookupRoaringBitmap(token string) *storage.RoaringBitmap {
	prefix := ginEncodeToken(token)
	var rb *storage.RoaringBitmap
	dgi.tree.PrefixScan(prefix, func(compositeKey []byte) bool {
		postingData := compositeKey[len(prefix):]
		rb = storage.DecodeRoaringBitmap(postingData)
		return false
	})
	return rb
}

// MatchTokenBitmap returns a RoaringBitmap of row keys for the given token.
// For bigram tokenizer, returns the intersection of all bigram posting lists.
func (dgi *DiskGinIndex) MatchTokenBitmap(token string) *storage.RoaringBitmap {
	if dgi.info.Tokenizer == "bigram" {
		return dgi.matchBigramBitmap(token)
	}
	lower := strings.ToLower(token)
	return dgi.lookupRoaringBitmap(lower)
}

// matchBigramBitmap is the bitmap-returning variant of matchBigram.
func (dgi *DiskGinIndex) matchBigramBitmap(token string) *storage.RoaringBitmap {
	bigrams := ginBigramTokenize(token)
	if len(bigrams) == 0 {
		return nil
	}

	type bigramInfo struct {
		token string
		count uint64
	}
	infos := make([]bigramInfo, 0, len(bigrams))
	for _, bg := range bigrams {
		c := dgi.postingListCount(bg)
		if c == 0 {
			return nil
		}
		infos = append(infos, bigramInfo{bg, c})
	}

	sort.Slice(infos, func(i, j int) bool { return infos[i].count < infos[j].count })

	result := dgi.lookupRoaringBitmap(infos[0].token)
	if result == nil || result.Cardinality() == 0 {
		return nil
	}

	for _, info := range infos[1:] {
		other := dgi.lookupRoaringBitmap(info.token)
		if other == nil || other.Cardinality() == 0 {
			return nil
		}
		result = result.And(other)
		if result.Cardinality() == 0 {
			return nil
		}
	}

	return result
}

// MatchPrefix returns sorted row keys whose indexed column contains a token
// that starts with the given prefix. Uses PrefixScan for efficient lookup.
func (dgi *DiskGinIndex) MatchPrefix(prefix string) []int64 {
	lower := strings.ToLower(prefix)
	// TEXT encoding uses 0x03 + raw bytes; we scan by the prefix bytes without
	// the null terminator so that all tokens starting with `lower` are matched.
	scanPrefix := append([]byte{0x03}, []byte(lower)...)

	keyMap := make(map[int64]struct{})
	dgi.tree.PrefixScan(scanPrefix, func(compositeKey []byte) bool {
		// Extract the full token prefix (including null terminator) to find posting list data
		tokenEnd := -1
		for i := 1; i < len(compositeKey); i++ {
			if compositeKey[i] == 0x00 {
				tokenEnd = i
				break
			}
		}
		if tokenEnd < 0 {
			return true
		}
		postingData := compositeKey[tokenEnd+1:]
		for _, k := range decodePostingList(postingData) {
			keyMap[k] = struct{}{}
		}
		return true // continue scanning
	})
	keys := make([]int64, 0, len(keyMap))
	for k := range keyMap {
		keys = append(keys, k)
	}
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
	for _, tok := range dgi.tokenizeText(text) {
		dgi.addToPostingList(tok, key)
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
	for _, tok := range dgi.tokenizeText(text) {
		dgi.removeFromPostingList(tok, key)
	}
}

// tokenizeText dispatches to the appropriate tokenizer based on the index configuration.
func (dgi *DiskGinIndex) tokenizeText(text string) []string {
	switch dgi.info.Tokenizer {
	case "bigram":
		return ginBigramTokenize(text)
	default:
		return ginTokenize(text)
	}
}

// addToPostingList adds a rowKey to the posting list for the given token.
func (dgi *DiskGinIndex) addToPostingList(token string, rowKey int64) {
	prefix := ginEncodeToken(token)

	// Find existing posting list
	var oldEntry []byte
	var existing []int64
	dgi.tree.PrefixScan(prefix, func(compositeKey []byte) bool {
		oldEntry = make([]byte, len(compositeKey))
		copy(oldEntry, compositeKey)
		existing = decodePostingList(compositeKey[len(prefix):])
		return false
	})

	// Insert rowKey in sorted position
	pos := sort.Search(len(existing), func(i int) bool { return existing[i] >= rowKey })
	if pos < len(existing) && existing[pos] == rowKey {
		return // already exists
	}
	existing = append(existing, 0)
	copy(existing[pos+1:], existing[pos:])
	existing[pos] = rowKey

	// Replace entry
	if oldEntry != nil {
		dgi.tree.Delete(oldEntry)
	}
	newEntry := append(prefix, encodePostingList(existing)...)
	dgi.tree.Insert(newEntry)
}

// removeFromPostingList removes a rowKey from the posting list for the given token.
func (dgi *DiskGinIndex) removeFromPostingList(token string, rowKey int64) {
	prefix := ginEncodeToken(token)

	var oldEntry []byte
	var existing []int64
	dgi.tree.PrefixScan(prefix, func(compositeKey []byte) bool {
		oldEntry = make([]byte, len(compositeKey))
		copy(oldEntry, compositeKey)
		existing = decodePostingList(compositeKey[len(prefix):])
		return false
	})

	if oldEntry == nil {
		return
	}

	// Remove rowKey
	pos := sortSearchInt64s(existing, rowKey)
	if pos >= len(existing) || existing[pos] != rowKey {
		return // not found
	}
	existing = append(existing[:pos], existing[pos+1:]...)

	dgi.tree.Delete(oldEntry)
	if len(existing) > 0 {
		newEntry := append(prefix, encodePostingList(existing)...)
		dgi.tree.Insert(newEntry)
	}
}

// encodePostingList encodes a sorted list of row keys as a Roaring Bitmap.
func encodePostingList(keys []int64) []byte {
	rb := storage.RoaringFromInt64Slice(keys)
	return rb.Encode()
}

// decodePostingList decodes a Roaring Bitmap encoded posting list to sorted int64 keys.
func decodePostingList(data []byte) []int64 {
	rb := storage.DecodeRoaringBitmap(data)
	return rb.ToInt64Slice()
}

// appendVarint appends a varint-encoded uint64 to buf.
func appendVarint(buf []byte, v uint64) []byte {
	for v >= 0x80 {
		buf = append(buf, byte(v)|0x80)
		v >>= 7
	}
	return append(buf, byte(v))
}

// readVarint reads a varint from data at the given position.
func readVarint(data []byte, pos int) (uint64, int) {
	var v uint64
	var shift uint
	for pos < len(data) {
		b := data[pos]
		pos++
		v |= uint64(b&0x7F) << shift
		if b < 0x80 {
			return v, pos
		}
		shift += 7
	}
	return v, pos
}

// sortSearchInt64s is not in stdlib, so we provide it here.
func sortSearchInt64s(a []int64, x int64) int {
	return sort.Search(len(a), func(i int) bool { return a[i] >= x })
}

// ginEncodeToken encodes a token as a sort-preserving key prefix.
// Uses TEXT encoding: 0x03 + raw bytes + 0x00 (null terminator).
func ginEncodeToken(token string) []byte {
	return storage.EncodeValueBytes(nil, token)
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

// ginBigramTokenize splits text into 2-character overlapping tokens (bigrams).
func ginBigramTokenize(text string) []string {
	lower := strings.ToLower(text)
	runes := []rune(lower)
	if len(runes) < 2 {
		if len(runes) == 1 {
			return []string{string(runes)}
		}
		return nil
	}
	seen := make(map[string]struct{})
	tokens := make([]string, 0, len(runes)-1)
	for i := 0; i < len(runes)-1; i++ {
		bigram := string(runes[i : i+2])
		if _, ok := seen[bigram]; !ok {
			seen[bigram] = struct{}{}
			tokens = append(tokens, bigram)
		}
	}
	return tokens
}

package memory

import (
	"sort"
	"strings"
	"unicode"

	"github.com/walf443/oresql/storage"
)

// Compile-time verification that GinIndex satisfies GinIndexReader.
var _ storage.GinIndexReader = (*GinIndex)(nil)

// GinIndex is an inverted index for full-text search on TEXT columns.
// It maps each token (lowercase word) to the set of row keys containing that token.
type GinIndex struct {
	Info   *storage.IndexInfo
	tokens map[string]map[int64]struct{} // token -> set of row keys
}

// NewGinIndex creates a new empty GinIndex.
func NewGinIndex(info *storage.IndexInfo) *GinIndex {
	return &GinIndex{
		Info:   info,
		tokens: make(map[string]map[int64]struct{}),
	}
}

func (gi *GinIndex) GetInfo() *storage.IndexInfo {
	return gi.Info
}

// MatchToken returns sorted row keys whose indexed column contains the given token.
func (gi *GinIndex) MatchToken(token string) []int64 {
	lower := strings.ToLower(token)
	keySet, ok := gi.tokens[lower]
	if !ok {
		return nil
	}
	keys := make([]int64, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// AddRow indexes all tokens from the TEXT value in the specified column.
func (gi *GinIndex) AddRow(key int64, row storage.Row) {
	colIdx := gi.Info.ColumnIdxs[0]
	val := row[colIdx]
	text, ok := val.(string)
	if !ok {
		return // NULL or non-string values are not indexed
	}
	for _, tok := range tokenize(text) {
		if gi.tokens[tok] == nil {
			gi.tokens[tok] = make(map[int64]struct{})
		}
		gi.tokens[tok][key] = struct{}{}
	}
}

// RemoveRow removes all token entries for the given row.
func (gi *GinIndex) RemoveRow(key int64, row storage.Row) {
	colIdx := gi.Info.ColumnIdxs[0]
	val := row[colIdx]
	text, ok := val.(string)
	if !ok {
		return
	}
	for _, tok := range tokenize(text) {
		if keySet, exists := gi.tokens[tok]; exists {
			delete(keySet, key)
			if len(keySet) == 0 {
				delete(gi.tokens, tok)
			}
		}
	}
}

// Clear removes all entries from the index.
func (gi *GinIndex) Clear() {
	gi.tokens = make(map[string]map[int64]struct{})
}

// tokenize splits text into lowercase tokens by whitespace and punctuation.
func tokenize(text string) []string {
	words := strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	tokens := make([]string, len(words))
	for i, w := range words {
		tokens[i] = strings.ToLower(w)
	}
	return tokens
}

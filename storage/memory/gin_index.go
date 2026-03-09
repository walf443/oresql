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
// For bigram tokenizer, the search term is split into bigrams and the intersection
// of all posting lists is returned.
func (gi *GinIndex) MatchToken(token string) []int64 {
	if gi.Info.Tokenizer == "bigram" {
		return gi.matchBigram(token)
	}
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

// matchBigram splits the search term into bigrams and returns the intersection
// of all posting lists. Bigrams are processed in ascending order of posting list
// size to minimize intermediate result sizes.
func (gi *GinIndex) matchBigram(token string) []int64 {
	bigrams := bigramTokenize(token)
	if len(bigrams) == 0 {
		return nil
	}

	// Sort bigrams by posting list size (smallest first)
	sort.Slice(bigrams, func(i, j int) bool {
		return len(gi.tokens[bigrams[i]]) < len(gi.tokens[bigrams[j]])
	})

	// Start with the smallest posting list
	first, ok := gi.tokens[bigrams[0]]
	if !ok || len(first) == 0 {
		return nil
	}
	result := make(map[int64]struct{}, len(first))
	for k := range first {
		result[k] = struct{}{}
	}

	// Intersect with remaining bigrams in ascending size order
	for _, bg := range bigrams[1:] {
		keySet, ok := gi.tokens[bg]
		if !ok {
			return nil
		}
		for k := range result {
			if _, exists := keySet[k]; !exists {
				delete(result, k)
			}
		}
		if len(result) == 0 {
			return nil
		}
	}

	keys := make([]int64, 0, len(result))
	for k := range result {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// MatchTokenBitmap returns a RoaringBitmap of row keys for the given token.
func (gi *GinIndex) MatchTokenBitmap(token string) *storage.RoaringBitmap {
	keys := gi.MatchToken(token)
	return storage.RoaringFromInt64Slice(keys)
}

// MatchPrefix returns sorted row keys whose indexed column contains a token
// that starts with the given prefix.
func (gi *GinIndex) MatchPrefix(prefix string) []int64 {
	lower := strings.ToLower(prefix)
	keyMap := make(map[int64]struct{})
	for tok, keySet := range gi.tokens {
		if strings.HasPrefix(tok, lower) {
			for k := range keySet {
				keyMap[k] = struct{}{}
			}
		}
	}
	keys := make([]int64, 0, len(keyMap))
	for k := range keyMap {
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
	for _, tok := range gi.tokenizeText(text) {
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
	for _, tok := range gi.tokenizeText(text) {
		if keySet, exists := gi.tokens[tok]; exists {
			delete(keySet, key)
			if len(keySet) == 0 {
				delete(gi.tokens, tok)
			}
		}
	}
}

// tokenizeText dispatches to the appropriate tokenizer based on the index configuration.
func (gi *GinIndex) tokenizeText(text string) []string {
	switch gi.Info.Tokenizer {
	case "bigram":
		return bigramTokenize(text)
	default:
		return tokenize(text)
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

// bigramTokenize splits text into 2-character overlapping tokens (bigrams).
// For example, "東京都" produces ["東京", "京都"].
func bigramTokenize(text string) []string {
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

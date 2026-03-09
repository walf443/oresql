package disk

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/walf443/oresql/storage"
	"github.com/walf443/oresql/storage/pager"
)

func setupGinTestTree(t *testing.T) (*DiskGinIndex, *pager.BufferPool) {
	t.Helper()
	tmpFile := t.TempDir() + "/gin_test.db"
	p, err := pager.Create(tmpFile)
	require.NoError(t, err)
	pool := pager.NewBufferPool(p, 64)

	// Allocate header page
	pool.NewPage()
	pool.UnpinPage(0, false)

	tree, err := NewDiskSecondaryBTree(pool)
	require.NoError(t, err)

	info := &storage.IndexInfo{
		Name:        "idx_gin",
		TableName:   "test",
		ColumnNames: []string{"body"},
		ColumnIdxs:  []int{1},
		Type:        "GIN",
	}

	return &DiskGinIndex{info: info, tree: tree}, pool
}

func TestDiskGinPostingListCompression(t *testing.T) {
	idx, pool := setupGinTestTree(t)
	defer pool.Close()

	// Insert 10 rows all containing "hello"
	for i := int64(1); i <= 10; i++ {
		row := storage.Row{i, "hello world"}
		idx.AddRow(i, row)
	}

	// With posting list, there should be far fewer BTree entries than 10*2 (tokens * rows)
	// "hello" and "world" each appear 10 times.
	// Posting list: 2 entries (one per unique token), each with 10 rowKeys
	// Old approach would be 20 entries (one per token-row pair)
	assert.Equal(t, 2, idx.tree.Len(), "posting list should have 1 entry per unique token")

	// Verify search still works
	keys := idx.MatchToken("hello")
	require.Len(t, keys, 10)
	for i := int64(1); i <= 10; i++ {
		assert.Equal(t, i, keys[i-1])
	}
}

func TestDiskGinPostingListRemove(t *testing.T) {
	idx, pool := setupGinTestTree(t)
	defer pool.Close()

	// Insert 5 rows containing "hello"
	for i := int64(1); i <= 5; i++ {
		row := storage.Row{i, "hello"}
		idx.AddRow(i, row)
	}
	assert.Equal(t, 1, idx.tree.Len())

	// Remove middle row
	idx.RemoveRow(3, storage.Row{int64(3), "hello"})

	keys := idx.MatchToken("hello")
	require.Len(t, keys, 4)
	assert.Equal(t, []int64{1, 2, 4, 5}, keys)

	// BTree should still have 1 entry
	assert.Equal(t, 1, idx.tree.Len())
}

func TestDiskGinPostingListRemoveLastEntry(t *testing.T) {
	idx, pool := setupGinTestTree(t)
	defer pool.Close()

	row := storage.Row{int64(1), "hello"}
	idx.AddRow(1, row)
	assert.Equal(t, 1, idx.tree.Len())

	idx.RemoveRow(1, row)
	assert.Equal(t, 0, idx.tree.Len())

	keys := idx.MatchToken("hello")
	require.Len(t, keys, 0)
}

func TestDiskGinPostingListDeltaEncoding(t *testing.T) {
	// Test that posting list encoding/decoding works correctly with varied rowKeys
	keys := []int64{1, 5, 10, 100, 1000, 10000}
	encoded := encodePostingList(keys)
	decoded := decodePostingList(encoded)
	assert.Equal(t, keys, decoded)
}

func TestDiskGinPostingListEmpty(t *testing.T) {
	encoded := encodePostingList(nil)
	decoded := decodePostingList(encoded)
	assert.Len(t, decoded, 0)
}

func TestDiskGinPostingListSingleEntry(t *testing.T) {
	keys := []int64{42}
	encoded := encodePostingList(keys)
	decoded := decodePostingList(encoded)
	assert.Equal(t, keys, decoded)
}

// --- Blocked posting list tests ---

func TestBlockedPostingListSmall(t *testing.T) {
	// Less than one block — should still work correctly
	keys := []int64{1, 5, 10, 100, 1000, 10000}
	encoded := encodePostingList(keys)
	decoded := decodePostingList(encoded)
	assert.Equal(t, keys, decoded)

	// Parse as blocked and verify structure
	bpl := parseBlockedPostingList(encoded)
	assert.Equal(t, len(keys), bpl.totalCount)
	assert.Equal(t, 1, len(bpl.headers)) // single block
	assert.Equal(t, int64(1), bpl.headers[0].base)
	assert.Equal(t, int64(10000), bpl.headers[0].last)
}

func TestBlockedPostingListMultipleBlocks(t *testing.T) {
	// Create a posting list that spans multiple blocks
	n := postingBlockSize*3 + 50 // 3 full blocks + partial
	keys := make([]int64, n)
	for i := 0; i < n; i++ {
		keys[i] = int64(i*10 + 1) // 1, 11, 21, 31, ...
	}
	encoded := encodePostingList(keys)
	decoded := decodePostingList(encoded)
	assert.Equal(t, keys, decoded)

	bpl := parseBlockedPostingList(encoded)
	assert.Equal(t, n, bpl.totalCount)
	assert.Equal(t, 4, len(bpl.headers)) // 3 full + 1 partial

	// Verify block bases
	assert.Equal(t, int64(1), bpl.headers[0].base)
	assert.Equal(t, int64(postingBlockSize*10+1), bpl.headers[1].base)

	// Verify each block decodes correctly
	for i, hdr := range bpl.headers {
		blockKeys := bpl.decodeBlock(i)
		assert.Equal(t, hdr.count, len(blockKeys))
		assert.Equal(t, hdr.base, blockKeys[0])
		assert.Equal(t, hdr.last, blockKeys[len(blockKeys)-1])
	}
}

func TestBlockedPostingListDecodeBlock(t *testing.T) {
	// Verify that decoding individual blocks gives the same result as full decode
	n := postingBlockSize*2 + 10
	keys := make([]int64, n)
	for i := 0; i < n; i++ {
		keys[i] = int64(i + 1)
	}
	encoded := encodePostingList(keys)
	bpl := parseBlockedPostingList(encoded)

	// Concatenate all block decodes and compare
	var allKeys []int64
	for i := range bpl.headers {
		allKeys = append(allKeys, bpl.decodeBlock(i)...)
	}
	assert.Equal(t, keys, allKeys)
}

func TestBlockedPostingListExactBlockSize(t *testing.T) {
	// Exactly one full block
	keys := make([]int64, postingBlockSize)
	for i := 0; i < postingBlockSize; i++ {
		keys[i] = int64(i + 1)
	}
	encoded := encodePostingList(keys)
	decoded := decodePostingList(encoded)
	assert.Equal(t, keys, decoded)

	bpl := parseBlockedPostingList(encoded)
	assert.Equal(t, 1, len(bpl.headers))
	assert.Equal(t, postingBlockSize, bpl.headers[0].count)
}

func TestIntersectBlockedPostingLists(t *testing.T) {
	// Two posting lists with partial overlap
	a := []int64{1, 5, 10, 15, 20, 25, 30}
	b := []int64{3, 5, 15, 22, 25, 40}
	expected := []int64{5, 15, 25}

	encA := encodePostingList(a)
	encB := encodePostingList(b)
	result := intersectBlockedPostingLists(encA, encB)
	assert.Equal(t, expected, result)
}

func TestIntersectBlockedPostingListsNoOverlap(t *testing.T) {
	a := []int64{1, 2, 3}
	b := []int64{10, 20, 30}

	encA := encodePostingList(a)
	encB := encodePostingList(b)
	result := intersectBlockedPostingLists(encA, encB)
	assert.Len(t, result, 0)
}

func TestIntersectBlockedPostingListsMultiBlock(t *testing.T) {
	// Create large posting lists with known overlap across blocks
	// a: even numbers 2, 4, 6, ..., 2000
	// b: multiples of 3: 3, 6, 9, ..., 1998
	// intersection: multiples of 6: 6, 12, 18, ..., 1998
	var a, b, expected []int64
	for i := int64(1); i <= 1000; i++ {
		a = append(a, i*2)
	}
	for i := int64(1); i <= 666; i++ {
		b = append(b, i*3)
	}
	for i := int64(1); i <= 333; i++ {
		expected = append(expected, i*6)
	}

	encA := encodePostingList(a)
	encB := encodePostingList(b)
	result := intersectBlockedPostingLists(encA, encB)
	assert.Equal(t, expected, result)
}

func TestIntersectBlockedSkipsBlocks(t *testing.T) {
	// a has keys 1..128 (one block) and 10001..10128 (another block)
	// b has keys 10050..10060
	// Only the second block of a should need to be decoded
	var a []int64
	for i := int64(1); i <= int64(postingBlockSize); i++ {
		a = append(a, i)
	}
	for i := int64(10001); i <= int64(10000+postingBlockSize); i++ {
		a = append(a, i)
	}
	b := []int64{10050, 10055, 10060}
	expected := []int64{10050, 10055, 10060}

	encA := encodePostingList(a)
	encB := encodePostingList(b)
	result := intersectBlockedPostingLists(encA, encB)
	assert.Equal(t, expected, result)
}

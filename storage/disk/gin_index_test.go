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

// --- Roaring Bitmap posting list tests ---

func TestRoaringPostingListLarge(t *testing.T) {
	// Verify encode/decode round-trip with many keys
	n := 5000
	keys := make([]int64, n)
	for i := 0; i < n; i++ {
		keys[i] = int64(i*10 + 1)
	}
	encoded := encodePostingList(keys)
	decoded := decodePostingList(encoded)
	assert.Equal(t, keys, decoded)
}

func TestRoaringPostingListIntersect(t *testing.T) {
	// Two posting lists with partial overlap, intersected via Roaring Bitmap
	a := []int64{1, 5, 10, 15, 20, 25, 30}
	b := []int64{3, 5, 15, 22, 25, 40}
	expected := []int64{5, 15, 25}

	rbA := storage.RoaringFromInt64Slice(a)
	rbB := storage.RoaringFromInt64Slice(b)
	result := rbA.And(rbB).ToInt64Slice()
	assert.Equal(t, expected, result)
}

func TestRoaringPostingListUnion(t *testing.T) {
	a := []int64{1, 5, 10}
	b := []int64{3, 5, 20}
	expected := []int64{1, 3, 5, 10, 20}

	rbA := storage.RoaringFromInt64Slice(a)
	rbB := storage.RoaringFromInt64Slice(b)
	result := rbA.Or(rbB).ToInt64Slice()
	assert.Equal(t, expected, result)
}

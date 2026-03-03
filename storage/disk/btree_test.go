package disk

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/walf443/oresql/storage"
	"github.com/walf443/oresql/storage/pager"
)

func newTestBTree(t *testing.T) *DiskBTree {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	p, err := pager.Create(path)
	require.NoError(t, err)
	pool := pager.NewBufferPool(p, 256)
	bt, err := NewDiskBTree(pool, 2)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	return bt
}

func TestInsertAndGet(t *testing.T) {
	bt := newTestBTree(t)

	ok := bt.Insert(10, storage.Row{int64(10), "ten"})
	require.True(t, ok)
	ok = bt.Insert(20, storage.Row{int64(20), "twenty"})
	require.True(t, ok)
	ok = bt.Insert(5, storage.Row{int64(5), "five"})
	require.True(t, ok)

	row, ok := bt.Get(10)
	assert.True(t, ok)
	assert.Equal(t, int64(10), row[0])
	assert.Equal(t, "ten", row[1])

	row, ok = bt.Get(20)
	assert.True(t, ok)
	assert.Equal(t, "twenty", row[1])

	row, ok = bt.Get(5)
	assert.True(t, ok)
	assert.Equal(t, "five", row[1])

	_, ok = bt.Get(999)
	assert.False(t, ok)
}

func TestInsertDuplicate(t *testing.T) {
	bt := newTestBTree(t)

	bt.Insert(10, storage.Row{int64(10), "ten"})
	ok := bt.Insert(10, storage.Row{int64(10), "ten-again"})
	assert.False(t, ok)
	row, _ := bt.Get(10)
	assert.Equal(t, "ten", row[1])
}

func TestPut(t *testing.T) {
	bt := newTestBTree(t)

	bt.Put(10, storage.Row{int64(10), "ten"})
	bt.Put(10, storage.Row{int64(10), "TEN"})
	row, ok := bt.Get(10)
	assert.True(t, ok)
	assert.Equal(t, "TEN", row[1])
}

func TestDelete(t *testing.T) {
	bt := newTestBTree(t)

	bt.Insert(10, storage.Row{int64(10), "ten"})
	bt.Insert(20, storage.Row{int64(20), "twenty"})
	bt.Insert(5, storage.Row{int64(5), "five"})

	ok := bt.Delete(10)
	require.True(t, ok)
	_, ok = bt.Get(10)
	assert.False(t, ok)
	assert.Equal(t, 2, bt.Len())

	ok = bt.Delete(999)
	assert.False(t, ok)
}

func TestHas(t *testing.T) {
	bt := newTestBTree(t)

	bt.Insert(10, storage.Row{int64(10), "ten"})
	assert.True(t, bt.Has(10))
	assert.False(t, bt.Has(999))
}

func TestLen(t *testing.T) {
	bt := newTestBTree(t)

	assert.Equal(t, 0, bt.Len())
	bt.Insert(1, storage.Row{int64(1)})
	bt.Insert(2, storage.Row{int64(2)})
	bt.Insert(3, storage.Row{int64(3)})
	assert.Equal(t, 3, bt.Len())
}

func TestForEachInOrder(t *testing.T) {
	bt := newTestBTree(t)

	keys := []int64{50, 20, 80, 10, 30, 60, 90, 5, 15, 25, 35}
	for _, k := range keys {
		bt.Insert(k, storage.Row{k})
	}

	var result []int64
	bt.ForEach(func(key int64, row storage.Row) bool {
		result = append(result, key)
		return true
	})

	require.Len(t, result, len(keys))
	for i := 1; i < len(result); i++ {
		assert.Less(t, result[i-1], result[i], "keys not in order: %v", result)
	}
}

func TestForEachEarlyTermination(t *testing.T) {
	bt := newTestBTree(t)

	for i := int64(1); i <= 100; i++ {
		bt.Insert(i, storage.Row{i})
	}

	count := 0
	bt.ForEach(func(key int64, row storage.Row) bool {
		count++
		return count < 5
	})
	assert.Equal(t, 5, count)
}

func TestForEachReverse(t *testing.T) {
	bt := newTestBTree(t)

	keys := []int64{50, 20, 80, 10, 30, 60, 90}
	for _, k := range keys {
		bt.Insert(k, storage.Row{k})
	}

	var result []int64
	bt.ForEachReverse(func(key int64, row storage.Row) bool {
		result = append(result, key)
		return true
	})

	require.Len(t, result, len(keys))
	for i := 1; i < len(result); i++ {
		assert.Greater(t, result[i-1], result[i], "keys not in descending order: %v", result)
	}
}

func TestForEachReverseEarlyTermination(t *testing.T) {
	bt := newTestBTree(t)

	for i := int64(1); i <= 100; i++ {
		bt.Insert(i, storage.Row{i})
	}

	count := 0
	bt.ForEachReverse(func(key int64, row storage.Row) bool {
		count++
		return count < 5
	})
	assert.Equal(t, 5, count)
}

func TestForEachReverseEmpty(t *testing.T) {
	bt := newTestBTree(t)

	count := 0
	bt.ForEachReverse(func(key int64, row storage.Row) bool {
		count++
		return true
	})
	assert.Equal(t, 0, count)
}

func TestForEachRange(t *testing.T) {
	bt := newTestBTree(t)

	for i := int64(1); i <= 10; i++ {
		bt.Insert(i, storage.Row{i})
	}

	tests := []struct {
		name          string
		from          *int64
		fromInclusive bool
		to            *int64
		toInclusive   bool
		wantKeys      []int64
	}{
		{
			name:          "closed [3,7]",
			from:          ptr(int64(3)),
			fromInclusive: true,
			to:            ptr(int64(7)),
			toInclusive:   true,
			wantKeys:      []int64{3, 4, 5, 6, 7},
		},
		{
			name:          "open (3,7)",
			from:          ptr(int64(3)),
			fromInclusive: false,
			to:            ptr(int64(7)),
			toInclusive:   false,
			wantKeys:      []int64{4, 5, 6},
		},
		{
			name:          "no lower bound (,5]",
			from:          nil,
			fromInclusive: false,
			to:            ptr(int64(5)),
			toInclusive:   true,
			wantKeys:      []int64{1, 2, 3, 4, 5},
		},
		{
			name:          "no upper bound [5,)",
			from:          ptr(int64(5)),
			fromInclusive: true,
			to:            nil,
			toInclusive:   false,
			wantKeys:      []int64{5, 6, 7, 8, 9, 10},
		},
		{
			name:          "no bounds (all)",
			from:          nil,
			fromInclusive: false,
			to:            nil,
			toInclusive:   false,
			wantKeys:      []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []int64
			bt.ForEachRange(tt.from, tt.fromInclusive, tt.to, tt.toInclusive, func(key int64, row storage.Row) bool {
				got = append(got, key)
				return true
			})
			require.Len(t, got, len(tt.wantKeys), "got: %v", got)
			for i := range tt.wantKeys {
				assert.Equal(t, tt.wantKeys[i], got[i], "position %d", i)
			}
		})
	}
}

func TestLargeDataSet(t *testing.T) {
	bt := newTestBTree(t)

	n := 1000
	perm := rand.Perm(n)

	for _, i := range perm {
		k := int64(i)
		bt.Insert(k, storage.Row{k, fmt.Sprintf("val-%d", i)})
	}

	require.Equal(t, n, bt.Len())

	// Verify all values
	for i := 0; i < n; i++ {
		row, ok := bt.Get(int64(i))
		require.True(t, ok, "Get(%d)", i)
		assert.Equal(t, int64(i), row[0])
	}

	// Verify sorted order
	var prev int64 = -1
	bt.ForEach(func(key int64, row storage.Row) bool {
		assert.Greater(t, key, prev, "keys not in order")
		prev = key
		return true
	})

	// Delete half
	for i := 0; i < n/2; i++ {
		k := int64(perm[i])
		require.True(t, bt.Delete(k), "Delete(%d)", k)
	}

	require.Equal(t, n/2, bt.Len())

	// Verify remaining are sorted
	prev = -1
	count := 0
	bt.ForEach(func(key int64, row storage.Row) bool {
		assert.Greater(t, key, prev, "keys not in order after delete")
		prev = key
		count++
		return true
	})
	assert.Equal(t, n/2, count)
}

func TestDeleteAndReinsert(t *testing.T) {
	bt := newTestBTree(t)

	bt.Insert(10, storage.Row{int64(10), "first"})
	bt.Delete(10)

	ok := bt.Insert(10, storage.Row{int64(10), "second"})
	require.True(t, ok)
	row, ok := bt.Get(10)
	assert.True(t, ok)
	assert.Equal(t, "second", row[1])
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Phase 1: Write data
	p1, err := pager.Create(path)
	require.NoError(t, err)
	pool1 := pager.NewBufferPool(p1, 256)

	bt1, err := NewDiskBTree(pool1, 2)
	require.NoError(t, err)

	for i := int64(1); i <= 50; i++ {
		bt1.Insert(i, storage.Row{i, fmt.Sprintf("val-%d", i)})
	}

	rootID := bt1.RootPageID()
	length := bt1.Len()
	require.NoError(t, pool1.FlushAll())
	require.NoError(t, pool1.Close())

	// Phase 2: Reload
	p2, err := pager.Open(path)
	require.NoError(t, err)
	pool2 := pager.NewBufferPool(p2, 256)
	defer pool2.Close()

	bt2 := LoadDiskBTree(pool2, rootID, length, 2)
	assert.Equal(t, 50, bt2.Len())

	for i := int64(1); i <= 50; i++ {
		row, ok := bt2.Get(i)
		require.True(t, ok, "Get(%d)", i)
		assert.Equal(t, i, row[0])
		assert.Equal(t, fmt.Sprintf("val-%d", i), row[1])
	}

	// Verify order
	var prev int64 = 0
	bt2.ForEach(func(key int64, row storage.Row) bool {
		assert.Greater(t, key, prev)
		prev = key
		return true
	})
}

func TestNullValues(t *testing.T) {
	bt := newTestBTree(t)

	bt.Insert(1, storage.Row{int64(1), nil})
	bt.Insert(2, storage.Row{int64(2), "hello"})

	row, ok := bt.Get(1)
	require.True(t, ok)
	assert.Nil(t, row[1])

	row, ok = bt.Get(2)
	require.True(t, ok)
	assert.Equal(t, "hello", row[1])
}

func TestFloatValues(t *testing.T) {
	bt := newTestBTree(t)

	bt.Insert(1, storage.Row{int64(1), float64(3.14)})
	bt.Insert(2, storage.Row{int64(2), float64(-2.71)})

	row, ok := bt.Get(1)
	require.True(t, ok)
	assert.Equal(t, float64(3.14), row[1])

	row, ok = bt.Get(2)
	require.True(t, ok)
	assert.Equal(t, float64(-2.71), row[1])
}

func TestGetByKeysSorted(t *testing.T) {
	t.Run("all keys exist", func(t *testing.T) {
		bt := newTestBTree(t)
		for i := int64(1); i <= 10; i++ {
			bt.Insert(i, storage.Row{i, fmt.Sprintf("val-%d", i)})
		}

		result := bt.GetByKeysSorted([]int64{2, 5, 8})
		require.Len(t, result, 3)
		assert.Equal(t, int64(2), result[0].Key)
		assert.Equal(t, "val-2", result[0].Row[1])
		assert.Equal(t, int64(5), result[1].Key)
		assert.Equal(t, "val-5", result[1].Row[1])
		assert.Equal(t, int64(8), result[2].Key)
		assert.Equal(t, "val-8", result[2].Row[1])
	})

	t.Run("some keys missing", func(t *testing.T) {
		bt := newTestBTree(t)
		for i := int64(1); i <= 10; i++ {
			bt.Insert(i*2, storage.Row{i * 2, fmt.Sprintf("val-%d", i*2)})
		}
		// Keys: 2,4,6,8,10,12,14,16,18,20
		// Query: 3(missing),4(hit),7(missing),10(hit),15(missing),20(hit)
		result := bt.GetByKeysSorted([]int64{3, 4, 7, 10, 15, 20})
		require.Len(t, result, 3)
		assert.Equal(t, int64(4), result[0].Key)
		assert.Equal(t, int64(10), result[1].Key)
		assert.Equal(t, int64(20), result[2].Key)
	})

	t.Run("empty keys", func(t *testing.T) {
		bt := newTestBTree(t)
		bt.Insert(1, storage.Row{int64(1)})

		result := bt.GetByKeysSorted([]int64{})
		assert.Nil(t, result)
	})

	t.Run("all keys missing", func(t *testing.T) {
		bt := newTestBTree(t)
		for i := int64(1); i <= 5; i++ {
			bt.Insert(i*2, storage.Row{i * 2})
		}
		// Keys: 2,4,6,8,10. Query: 1,3,5,7,9 (all missing)
		result := bt.GetByKeysSorted([]int64{1, 3, 5, 7, 9})
		assert.Len(t, result, 0)
	})

	t.Run("large dataset scattered keys", func(t *testing.T) {
		bt := newTestBTree(t)
		n := 1000
		for i := int64(0); i < int64(n); i++ {
			bt.Insert(i, storage.Row{i, fmt.Sprintf("val-%d", i)})
		}

		// Pick every 10th key
		var keys []int64
		for i := int64(5); i < int64(n); i += 10 {
			keys = append(keys, i)
		}
		result := bt.GetByKeysSorted(keys)
		require.Len(t, result, len(keys))
		for i, kr := range result {
			assert.Equal(t, keys[i], kr.Key)
			assert.Equal(t, fmt.Sprintf("val-%d", keys[i]), kr.Row[1])
		}
	})

	t.Run("single key", func(t *testing.T) {
		bt := newTestBTree(t)
		bt.Insert(42, storage.Row{int64(42), "answer"})

		result := bt.GetByKeysSorted([]int64{42})
		require.Len(t, result, 1)
		assert.Equal(t, int64(42), result[0].Key)
		assert.Equal(t, "answer", result[0].Row[1])
	})

	t.Run("sparse keys with header skip", func(t *testing.T) {
		bt := newTestBTree(t)
		n := 10000
		for i := int64(0); i < int64(n); i++ {
			bt.Insert(i, storage.Row{i, fmt.Sprintf("val-%d", i)})
		}

		// Sparse keys spread across the key space — most pages should be skipped
		sparseKeys := []int64{50, 500, 501, 5000, 5001, 5099}
		result := bt.GetByKeysSorted(sparseKeys)
		require.Len(t, result, len(sparseKeys))
		for i, kr := range result {
			assert.Equal(t, sparseKeys[i], kr.Key)
			assert.Equal(t, fmt.Sprintf("val-%d", sparseKeys[i]), kr.Row[1])
		}
	})

	t.Run("gap jump with large gaps", func(t *testing.T) {
		bt := newTestBTree(t)
		n := 10000
		for i := int64(0); i < int64(n); i++ {
			bt.Insert(i, storage.Row{i, fmt.Sprintf("val-%d", i)})
		}

		// Keys with large gaps — should trigger gap jump (findLeaf)
		gapKeys := []int64{10, 11, 12, 4990, 4991, 4992}
		result := bt.GetByKeysSorted(gapKeys)
		require.Len(t, result, len(gapKeys))
		for i, kr := range result {
			assert.Equal(t, gapKeys[i], kr.Key)
			assert.Equal(t, fmt.Sprintf("val-%d", gapKeys[i]), kr.Row[1])
		}
	})

	t.Run("single page all entries", func(t *testing.T) {
		bt := newTestBTree(t)
		// Insert few entries that fit in a single leaf page
		for i := int64(1); i <= 5; i++ {
			bt.Insert(i, storage.Row{i, fmt.Sprintf("val-%d", i)})
		}

		result := bt.GetByKeysSorted([]int64{1, 3, 5})
		require.Len(t, result, 3)
		assert.Equal(t, int64(1), result[0].Key)
		assert.Equal(t, int64(3), result[1].Key)
		assert.Equal(t, int64(5), result[2].Key)
	})

	t.Run("dense keys all match", func(t *testing.T) {
		bt := newTestBTree(t)
		n := 1000
		for i := int64(0); i < int64(n); i++ {
			bt.Insert(i, storage.Row{i, fmt.Sprintf("val-%d", i)})
		}

		// Query a contiguous block of 100 keys — all should match
		var keys []int64
		for i := int64(100); i < 200; i++ {
			keys = append(keys, i)
		}
		result := bt.GetByKeysSorted(keys)
		require.Len(t, result, 100)
		for i, kr := range result {
			assert.Equal(t, keys[i], kr.Key)
			assert.Equal(t, fmt.Sprintf("val-%d", keys[i]), kr.Row[1])
		}
	})

	t.Run("alternating hits", func(t *testing.T) {
		bt := newTestBTree(t)
		// Insert only even keys
		for i := int64(0); i < 1000; i += 2 {
			bt.Insert(i, storage.Row{i, fmt.Sprintf("val-%d", i)})
		}

		// Query with consecutive keys (mix of hits and misses)
		var keys []int64
		for i := int64(0); i < 100; i++ {
			keys = append(keys, i)
		}
		result := bt.GetByKeysSorted(keys)
		// Only even keys should match: 0,2,4,...,98 = 50 entries
		require.Len(t, result, 50)
		for i, kr := range result {
			expected := int64(i * 2)
			assert.Equal(t, expected, kr.Key)
			assert.Equal(t, fmt.Sprintf("val-%d", expected), kr.Row[1])
		}
	})
}

func TestGetByKeysSortedSlabIsolation(t *testing.T) {
	bt := newTestBTree(t)
	for i := int64(1); i <= 100; i++ {
		bt.Insert(i, storage.Row{i, fmt.Sprintf("val-%d", i)})
	}

	result := bt.GetByKeysSorted([]int64{10, 20, 30})
	require.Len(t, result, 3)

	// Mutate row 0 — should NOT affect rows 1 or 2
	result[0].Row[0] = int64(999)
	result[0].Row[1] = "mutated"

	assert.Equal(t, int64(20), result[1].Row[0])
	assert.Equal(t, "val-20", result[1].Row[1])
	assert.Equal(t, int64(30), result[2].Row[0])
	assert.Equal(t, "val-30", result[2].Row[1])
}

func TestForEachByKeysSorted(t *testing.T) {
	t.Run("all keys exist", func(t *testing.T) {
		bt := newTestBTree(t)
		for i := int64(1); i <= 10; i++ {
			bt.Insert(i, storage.Row{i, fmt.Sprintf("val-%d", i)})
		}

		var collected []storage.KeyRow
		bt.ForEachByKeysSorted([]int64{2, 5, 8}, func(key int64, row storage.Row) bool {
			// Must copy since row may be reused
			cp := make(storage.Row, len(row))
			copy(cp, row)
			collected = append(collected, storage.KeyRow{Key: key, Row: cp})
			return true
		})
		require.Len(t, collected, 3)
		assert.Equal(t, int64(2), collected[0].Key)
		assert.Equal(t, "val-2", collected[0].Row[1])
		assert.Equal(t, int64(5), collected[1].Key)
		assert.Equal(t, "val-5", collected[1].Row[1])
		assert.Equal(t, int64(8), collected[2].Key)
		assert.Equal(t, "val-8", collected[2].Row[1])
	})

	t.Run("some keys missing", func(t *testing.T) {
		bt := newTestBTree(t)
		for i := int64(1); i <= 10; i++ {
			bt.Insert(i*2, storage.Row{i * 2, fmt.Sprintf("val-%d", i*2)})
		}

		var keys []int64
		bt.ForEachByKeysSorted([]int64{3, 4, 7, 10, 15, 20}, func(key int64, row storage.Row) bool {
			keys = append(keys, key)
			return true
		})
		require.Len(t, keys, 3)
		assert.Equal(t, []int64{4, 10, 20}, keys)
	})

	t.Run("empty keys", func(t *testing.T) {
		bt := newTestBTree(t)
		bt.Insert(1, storage.Row{int64(1)})

		called := false
		bt.ForEachByKeysSorted([]int64{}, func(key int64, row storage.Row) bool {
			called = true
			return true
		})
		assert.False(t, called)
	})

	t.Run("early stop", func(t *testing.T) {
		bt := newTestBTree(t)
		for i := int64(1); i <= 100; i++ {
			bt.Insert(i, storage.Row{i, fmt.Sprintf("val-%d", i)})
		}

		queryKeys := []int64{10, 20, 30, 40, 50}
		var collected []int64
		bt.ForEachByKeysSorted(queryKeys, func(key int64, row storage.Row) bool {
			collected = append(collected, key)
			return len(collected) < 3 // stop after 3
		})
		require.Len(t, collected, 3)
		assert.Equal(t, []int64{10, 20, 30}, collected)
	})

	t.Run("large dataset with gap jump", func(t *testing.T) {
		bt := newTestBTree(t)
		n := 10000
		for i := int64(0); i < int64(n); i++ {
			bt.Insert(i, storage.Row{i, fmt.Sprintf("val-%d", i)})
		}

		queryKeys := []int64{10, 11, 12, 4990, 4991, 4992}
		var collected []int64
		bt.ForEachByKeysSorted(queryKeys, func(key int64, row storage.Row) bool {
			collected = append(collected, key)
			return true
		})
		assert.Equal(t, queryKeys, collected)
	})
}

func TestPrevLeafIntegrity(t *testing.T) {
	bt := newTestBTree(t)

	// Insert enough keys to cause multiple splits
	for i := int64(0); i < 500; i++ {
		bt.Insert(i, storage.Row{i, fmt.Sprintf("val-%d", i)})
	}

	// Forward traverse: collect all leaf pageIDs and their nextLeaf/prevLeaf
	type leafInfo struct {
		pageID   pager.PageID
		prevLeaf pager.PageID
		nextLeaf pager.PageID
	}

	// Find leftmost leaf
	pageID := bt.rootPageID
	for {
		data, err := bt.pool.FetchPage(pageID)
		require.NoError(t, err)
		if isLeafPage(data) {
			bt.pool.UnpinPage(pageID, false)
			break
		}
		ip := decodeInternalPage(data)
		bt.pool.UnpinPage(pageID, false)
		pageID = ip.children[0]
	}

	// Collect leaf chain forward
	var leaves []leafInfo
	for pageID != pager.InvalidPageID {
		data, err := bt.pool.FetchPage(pageID)
		require.NoError(t, err)
		prev := pager.PageID(binary.BigEndian.Uint32(data[leafOffPrevLeaf : leafOffPrevLeaf+4]))
		next := pager.PageID(binary.BigEndian.Uint32(data[leafOffNextLeaf : leafOffNextLeaf+4]))
		leaves = append(leaves, leafInfo{
			pageID:   pageID,
			prevLeaf: prev,
			nextLeaf: next,
		})
		bt.pool.UnpinPage(pageID, false)
		pageID = next
	}

	require.Greater(t, len(leaves), 1, "need multiple leaves for meaningful test")

	// Verify: first leaf has prevLeaf == InvalidPageID
	assert.Equal(t, pager.InvalidPageID, leaves[0].prevLeaf, "first leaf prevLeaf should be InvalidPageID")

	// Verify: last leaf has nextLeaf == InvalidPageID
	assert.Equal(t, pager.InvalidPageID, leaves[len(leaves)-1].nextLeaf, "last leaf nextLeaf should be InvalidPageID")

	// Verify symmetry: for each pair of adjacent leaves, nextLeaf/prevLeaf match
	for i := 0; i < len(leaves)-1; i++ {
		assert.Equal(t, leaves[i+1].pageID, leaves[i].nextLeaf,
			"leaf %d nextLeaf should point to leaf %d", i, i+1)
		assert.Equal(t, leaves[i].pageID, leaves[i+1].prevLeaf,
			"leaf %d prevLeaf should point to leaf %d", i+1, i)
	}

	// Verify backward traversal via prevLeaf produces same leaves in reverse
	rightmostID, err := bt.findRightmostLeaf()
	require.NoError(t, err)
	assert.Equal(t, leaves[len(leaves)-1].pageID, rightmostID)

	var backwardIDs []pager.PageID
	pageID = rightmostID
	for pageID != pager.InvalidPageID {
		backwardIDs = append(backwardIDs, pageID)
		data, err := bt.pool.FetchPage(pageID)
		require.NoError(t, err)
		prev := pager.PageID(binary.BigEndian.Uint32(data[leafOffPrevLeaf : leafOffPrevLeaf+4]))
		bt.pool.UnpinPage(pageID, false)
		pageID = prev
	}

	require.Len(t, backwardIDs, len(leaves))
	for i, id := range backwardIDs {
		assert.Equal(t, leaves[len(leaves)-1-i].pageID, id,
			"backward traversal mismatch at position %d", i)
	}
}

func TestPrevLeafAfterDelete(t *testing.T) {
	bt := newTestBTree(t)

	// Insert enough keys to cause splits
	n := 200
	for i := int64(0); i < int64(n); i++ {
		bt.Insert(i, storage.Row{i, fmt.Sprintf("val-%d", i)})
	}

	// Delete half to trigger merges
	for i := int64(0); i < int64(n); i += 2 {
		bt.Delete(i)
	}

	// Verify prevLeaf/nextLeaf symmetry after merges
	pageID := bt.rootPageID
	for {
		data, err := bt.pool.FetchPage(pageID)
		require.NoError(t, err)
		if isLeafPage(data) {
			bt.pool.UnpinPage(pageID, false)
			break
		}
		ip := decodeInternalPage(data)
		bt.pool.UnpinPage(pageID, false)
		pageID = ip.children[0]
	}

	type leafInfo struct {
		pageID   pager.PageID
		prevLeaf pager.PageID
		nextLeaf pager.PageID
	}
	var leaves []leafInfo
	for pageID != pager.InvalidPageID {
		data, err := bt.pool.FetchPage(pageID)
		require.NoError(t, err)
		prev := pager.PageID(binary.BigEndian.Uint32(data[leafOffPrevLeaf : leafOffPrevLeaf+4]))
		next := pager.PageID(binary.BigEndian.Uint32(data[leafOffNextLeaf : leafOffNextLeaf+4]))
		leaves = append(leaves, leafInfo{
			pageID:   pageID,
			prevLeaf: prev,
			nextLeaf: next,
		})
		bt.pool.UnpinPage(pageID, false)
		pageID = next
	}

	if len(leaves) > 1 {
		assert.Equal(t, pager.InvalidPageID, leaves[0].prevLeaf)
		assert.Equal(t, pager.InvalidPageID, leaves[len(leaves)-1].nextLeaf)

		for i := 0; i < len(leaves)-1; i++ {
			assert.Equal(t, leaves[i+1].pageID, leaves[i].nextLeaf,
				"leaf %d nextLeaf mismatch after delete", i)
			assert.Equal(t, leaves[i].pageID, leaves[i+1].prevLeaf,
				"leaf %d prevLeaf mismatch after delete", i+1)
		}
	}

	// Verify ForEachReverse still produces correct results
	var keys []int64
	bt.ForEachReverse(func(key int64, row storage.Row) bool {
		keys = append(keys, key)
		return true
	})

	require.Equal(t, bt.Len(), len(keys))
	for i := 1; i < len(keys); i++ {
		assert.Greater(t, keys[i-1], keys[i], "keys not in descending order after delete")
	}
}

func TestDecodeLeafPageSlabIsolation(t *testing.T) {
	// Build a leaf page with multiple entries
	page := make([]byte, pager.PageSize)
	lp := leafPage{
		entryCount: 3,
		nextLeaf:   pager.InvalidPageID,
		prevLeaf:   pager.InvalidPageID,
		highKey:    30,
	}
	lp.entries = []leafEntry{
		{key: 10, valLen: 4, val: []byte{0xAA, 0xBB, 0xCC, 0xDD}},
		{key: 20, valLen: 4, val: []byte{0x11, 0x22, 0x33, 0x44}},
		{key: 30, valLen: 4, val: []byte{0x55, 0x66, 0x77, 0x88}},
	}
	encodeLeafPage(lp, page)

	// Decode with slab
	decoded := decodeLeafPageSlab(page)
	require.Equal(t, uint16(3), decoded.entryCount)

	// Mutate entry 0's val — should NOT affect entry 1 or 2
	decoded.entries[0].val[0] = 0xFF
	decoded.entries[0].val[1] = 0xFF

	assert.Equal(t, []byte{0x11, 0x22, 0x33, 0x44}, decoded.entries[1].val)
	assert.Equal(t, []byte{0x55, 0x66, 0x77, 0x88}, decoded.entries[2].val)
}

func BenchmarkBTreeInsert(b *testing.B) {
	b.ReportAllocs()
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.db")
	p, err := pager.Create(path)
	require.NoError(b, err)
	pool := pager.NewBufferPool(p, 1024)
	bt, err := NewDiskBTree(pool, 2)
	require.NoError(b, err)
	defer pool.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := int64(i)
		bt.Insert(k, storage.Row{k, fmt.Sprintf("val-%d", i)})
	}
}

func BenchmarkBTreeDelete(b *testing.B) {
	b.ReportAllocs()
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.db")
	p, err := pager.Create(path)
	require.NoError(b, err)
	pool := pager.NewBufferPool(p, 1024)
	bt, err := NewDiskBTree(pool, 2)
	require.NoError(b, err)
	defer pool.Close()

	// Pre-populate
	n := b.N
	for i := 0; i < n; i++ {
		k := int64(i)
		bt.Insert(k, storage.Row{k, fmt.Sprintf("val-%d", i)})
	}

	b.ResetTimer()
	for i := 0; i < n; i++ {
		bt.Delete(int64(i))
	}
}

func BenchmarkBTreePut(b *testing.B) {
	b.ReportAllocs()
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.db")
	p, err := pager.Create(path)
	require.NoError(b, err)
	pool := pager.NewBufferPool(p, 1024)
	bt, err := NewDiskBTree(pool, 2)
	require.NoError(b, err)
	defer pool.Close()

	// Pre-populate with 1000 keys
	for i := int64(0); i < 1000; i++ {
		bt.Insert(i, storage.Row{i, fmt.Sprintf("val-%d", i)})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := int64(i % 1000)
		bt.Put(k, storage.Row{k, fmt.Sprintf("updated-%d", i)})
	}
}

func ptr(v int64) *int64 {
	return &v
}

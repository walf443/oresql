package btree

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsertAndGet(t *testing.T) {
	tree := New[int64](4) // small degree for easier testing

	ok := tree.Insert(10, "ten")
	require.True(t, ok, "expected Insert to return true for new key")
	ok = tree.Insert(20, "twenty")
	require.True(t, ok, "expected Insert to return true for new key")
	ok = tree.Insert(5, "five")
	require.True(t, ok, "expected Insert to return true for new key")

	val, ok := tree.Get(10)
	assert.True(t, ok, "Get(10): expected ok=true")
	assert.Equal(t, "ten", val)
	val, ok = tree.Get(20)
	assert.True(t, ok, "Get(20): expected ok=true")
	assert.Equal(t, "twenty", val)
	val, ok = tree.Get(5)
	assert.True(t, ok, "Get(5): expected ok=true")
	assert.Equal(t, "five", val)

	_, ok = tree.Get(999)
	assert.False(t, ok, "Get(999): expected ok=false for missing key")
}

func TestInsertDuplicate(t *testing.T) {
	tree := New[int64](4)
	tree.Insert(10, "ten")
	ok := tree.Insert(10, "ten-again")
	assert.False(t, ok, "expected Insert to return false for duplicate key")
	// Original value should be preserved
	val, _ := tree.Get(10)
	assert.Equal(t, "ten", val, "expected 'ten' after duplicate insert")
}

func TestPut(t *testing.T) {
	tree := New[int64](4)
	tree.Put(10, "ten")
	tree.Put(10, "TEN") // upsert
	val, ok := tree.Get(10)
	assert.True(t, ok, "Put upsert: expected ok=true")
	assert.Equal(t, "TEN", val, "Put upsert: expected 'TEN'")
}

func TestDelete(t *testing.T) {
	tree := New[int64](4)
	tree.Insert(10, "ten")
	tree.Insert(20, "twenty")
	tree.Insert(5, "five")

	ok := tree.Delete(10)
	require.True(t, ok, "expected Delete to return true for existing key")
	_, ok = tree.Get(10)
	assert.False(t, ok, "Get(10) should return false after delete")
	assert.Equal(t, 2, tree.Len(), "expected Len()=2 after delete")

	// Delete non-existent key
	ok = tree.Delete(999)
	assert.False(t, ok, "expected Delete to return false for non-existent key")
}

func TestHas(t *testing.T) {
	tree := New[int64](4)
	tree.Insert(10, "ten")

	assert.True(t, tree.Has(10), "Has(10): expected true")
	assert.False(t, tree.Has(999), "Has(999): expected false")
}

func TestLen(t *testing.T) {
	tree := New[int64](4)
	assert.Equal(t, 0, tree.Len(), "expected Len()=0 for empty tree")
	tree.Insert(1, nil)
	tree.Insert(2, nil)
	tree.Insert(3, nil)
	assert.Equal(t, 3, tree.Len(), "expected Len()=3")
}

func TestForEachInOrder(t *testing.T) {
	tree := New[int64](4)
	keys := []int64{50, 20, 80, 10, 30, 60, 90, 5, 15, 25, 35}
	for _, k := range keys {
		tree.Insert(k, nil)
	}

	var result []int64
	tree.ForEach(func(key int64, value any) bool {
		result = append(result, key)
		return true
	})

	require.Len(t, result, len(keys), "expected same number of keys")
	for i := 1; i < len(result); i++ {
		if result[i] <= result[i-1] {
			assert.Fail(t, fmt.Sprintf("keys not in order: %v", result))
			break
		}
	}
}

func TestForEachEarlyTermination(t *testing.T) {
	tree := New[int64](4)
	for i := int64(1); i <= 100; i++ {
		tree.Insert(i, nil)
	}

	count := 0
	tree.ForEach(func(key int64, value any) bool {
		count++
		return count < 5 // stop after 5
	})

	assert.Equal(t, 5, count, "expected ForEach to stop after 5 items")
}

func TestLargeDataSet(t *testing.T) {
	tree := New[int64](32) // production degree

	n := 2000
	perm := rand.Perm(n)

	// Insert in random order
	for _, i := range perm {
		k := int64(i)
		tree.Insert(k, i*10)
	}

	require.Equal(t, n, tree.Len(), "expected Len() to match n")

	// Verify all values retrievable
	for i := 0; i < n; i++ {
		val, ok := tree.Get(int64(i))
		require.True(t, ok, "Get(%d): expected ok=true", i)
		require.Equal(t, i*10, val, "Get(%d): expected %d", i, i*10)
	}

	// Verify sorted order
	var prev int64 = -1
	tree.ForEach(func(key int64, value any) bool {
		if key <= prev {
			assert.Fail(t, fmt.Sprintf("keys not in order: %d after %d", key, prev))
			return false
		}
		prev = key
		return true
	})

	// Delete half
	for i := 0; i < n/2; i++ {
		k := int64(perm[i])
		require.True(t, tree.Delete(k), "Delete(%d): expected true", k)
	}

	require.Equal(t, n/2, tree.Len(), "expected Len() after deletes")

	// Verify remaining are still accessible and sorted
	prev = -1
	count := 0
	tree.ForEach(func(key int64, value any) bool {
		if key <= prev {
			assert.Fail(t, fmt.Sprintf("keys not in order after delete: %d after %d", key, prev))
			return false
		}
		prev = key
		count++
		return true
	})
	assert.Equal(t, n/2, count, "expected correct number of items in ForEach")
}

func TestDeleteAndReinsert(t *testing.T) {
	tree := New[int64](4)
	tree.Insert(10, "first")
	tree.Delete(10)

	ok := tree.Insert(10, "second")
	require.True(t, ok, "expected Insert to return true after delete")
	val, ok := tree.Get(10)
	assert.True(t, ok, "expected ok=true after reinsert")
	assert.Equal(t, "second", val, "expected 'second' after reinsert")
}

// --- String key tests ---

func TestStringBTreePutAndGet(t *testing.T) {
	tree := New[string](4)

	tree.Put("banana", 2)
	tree.Put("apple", 1)
	tree.Put("cherry", 3)

	val, ok := tree.Get("banana")
	assert.True(t, ok, "Get(banana): expected ok=true")
	assert.Equal(t, 2, val)
	val, ok = tree.Get("apple")
	assert.True(t, ok, "Get(apple): expected ok=true")
	assert.Equal(t, 1, val)
	val, ok = tree.Get("cherry")
	assert.True(t, ok, "Get(cherry): expected ok=true")
	assert.Equal(t, 3, val)

	_, ok = tree.Get("missing")
	assert.False(t, ok, "Get(missing): expected ok=false")
}

func TestStringBTreePutUpsert(t *testing.T) {
	tree := New[string](4)
	tree.Put("key", "first")
	tree.Put("key", "second")
	val, ok := tree.Get("key")
	assert.True(t, ok, "Put upsert: expected ok=true")
	assert.Equal(t, "second", val, "Put upsert: expected 'second'")
	assert.Equal(t, 1, tree.Len(), "expected Len()=1 after upsert")
}

func TestStringBTreeDelete(t *testing.T) {
	tree := New[string](4)
	tree.Put("a", 1)
	tree.Put("b", 2)
	tree.Put("c", 3)

	ok := tree.Delete("b")
	require.True(t, ok, "expected Delete to return true for existing key")
	_, ok = tree.Get("b")
	assert.False(t, ok, "Get(b) should return false after delete")
	assert.Equal(t, 2, tree.Len(), "expected Len()=2 after delete")

	ok = tree.Delete("missing")
	assert.False(t, ok, "expected Delete to return false for non-existent key")
}

func TestStringBTreeForEachInOrder(t *testing.T) {
	tree := New[string](4)
	keys := []string{"delta", "bravo", "foxtrot", "alpha", "charlie", "echo"}
	for _, k := range keys {
		tree.Put(k, nil)
	}

	var result []string
	tree.ForEach(func(key string, value any) bool {
		result = append(result, key)
		return true
	})

	require.Len(t, result, len(keys), "expected same number of keys")
	for i := 1; i < len(result); i++ {
		if result[i] <= result[i-1] {
			assert.Fail(t, fmt.Sprintf("keys not in order: %v", result))
			break
		}
	}
}

func TestStringBTreeForEachEarlyTermination(t *testing.T) {
	tree := New[string](4)
	for i := 0; i < 100; i++ {
		tree.Put(fmt.Sprintf("key-%03d", i), nil)
	}

	count := 0
	tree.ForEach(func(key string, value any) bool {
		count++
		return count < 5
	})

	assert.Equal(t, 5, count, "expected ForEach to stop after 5 items")
}

func TestStringBTreeLargeDataSet(t *testing.T) {
	tree := New[string](32)

	n := 2000
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("key-%04d", i)
	}

	perm := rand.Perm(n)
	for _, i := range perm {
		tree.Put(keys[i], i*10)
	}

	require.Equal(t, n, tree.Len(), "expected Len() to match n")

	for i := 0; i < n; i++ {
		val, ok := tree.Get(keys[i])
		require.True(t, ok, "Get(%s): expected ok=true", keys[i])
		require.Equal(t, i*10, val, "Get(%s): expected %d", keys[i], i*10)
	}

	// Verify sorted order
	var prev string
	tree.ForEach(func(key string, value any) bool {
		if prev != "" && key <= prev {
			assert.Fail(t, fmt.Sprintf("keys not in order: %s after %s", key, prev))
			return false
		}
		prev = key
		return true
	})

	// Delete half
	for i := 0; i < n/2; i++ {
		k := keys[perm[i]]
		require.True(t, tree.Delete(k), "Delete(%s): expected true", k)
	}

	require.Equal(t, n/2, tree.Len(), "expected Len() after deletes")

	// Verify remaining are sorted
	prev = ""
	count := 0
	tree.ForEach(func(key string, value any) bool {
		if prev != "" && key <= prev {
			assert.Fail(t, fmt.Sprintf("keys not in order after delete: %s after %s", key, prev))
			return false
		}
		prev = key
		count++
		return true
	})
	assert.Equal(t, n/2, count, "expected correct number of items in ForEach")
}

func TestStringBTreeDeleteAndReinsert(t *testing.T) {
	tree := New[string](4)
	tree.Put("key", "first")
	tree.Delete("key")

	tree.Put("key", "second")
	val, ok := tree.Get("key")
	assert.True(t, ok, "expected ok=true after reinsert")
	assert.Equal(t, "second", val, "expected 'second' after reinsert")
}

// --- ForEachRange tests ---

func TestForEachRange(t *testing.T) {
	tree := New[int64](4)
	for i := int64(1); i <= 10; i++ {
		tree.Insert(i, i*10)
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
			name:          "closed interval [3,7]",
			from:          ptr(int64(3)),
			fromInclusive: true,
			to:            ptr(int64(7)),
			toInclusive:   true,
			wantKeys:      []int64{3, 4, 5, 6, 7},
		},
		{
			name:          "open interval (3,7)",
			from:          ptr(int64(3)),
			fromInclusive: false,
			to:            ptr(int64(7)),
			toInclusive:   false,
			wantKeys:      []int64{4, 5, 6},
		},
		{
			name:          "half-open [3,7)",
			from:          ptr(int64(3)),
			fromInclusive: true,
			to:            ptr(int64(7)),
			toInclusive:   false,
			wantKeys:      []int64{3, 4, 5, 6},
		},
		{
			name:          "half-open (3,7]",
			from:          ptr(int64(3)),
			fromInclusive: false,
			to:            ptr(int64(7)),
			toInclusive:   true,
			wantKeys:      []int64{4, 5, 6, 7},
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
			tree.ForEachRange(tt.from, tt.fromInclusive, tt.to, tt.toInclusive, func(key int64, value any) bool {
				got = append(got, key)
				return true
			})
			require.Len(t, got, len(tt.wantKeys), "unexpected number of keys: %v", got)
			for i := range tt.wantKeys {
				assert.Equal(t, tt.wantKeys[i], got[i], "position %d", i)
			}
		})
	}
}

func TestForEachRangeNoMatch(t *testing.T) {
	tree := New[int64](4)
	for i := int64(1); i <= 5; i++ {
		tree.Insert(i, nil)
	}

	var got []int64
	from := int64(10)
	to := int64(20)
	tree.ForEachRange(&from, true, &to, true, func(key int64, value any) bool {
		got = append(got, key)
		return true
	})
	assert.Len(t, got, 0, "expected 0 keys")
}

func TestForEachRangeEarlyTermination(t *testing.T) {
	tree := New[int64](4)
	for i := int64(1); i <= 100; i++ {
		tree.Insert(i, nil)
	}

	count := 0
	from := int64(10)
	tree.ForEachRange(&from, true, nil, false, func(key int64, value any) bool {
		count++
		return count < 3
	})
	assert.Equal(t, 3, count, "expected 3 items before early termination")
}

func TestForEachRangeStringKeys(t *testing.T) {
	tree := New[string](4)
	words := []string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape"}
	for _, w := range words {
		tree.Put(w, nil)
	}

	var got []string
	from := "cherry"
	to := "fig"
	tree.ForEachRange(&from, true, &to, true, func(key string, value any) bool {
		got = append(got, key)
		return true
	})
	expected := []string{"cherry", "date", "elderberry", "fig"}
	require.Len(t, got, len(expected), "unexpected number of keys: %v", got)
	for i := range expected {
		assert.Equal(t, expected[i], got[i], "position %d", i)
	}
}

func ptr[T any](v T) *T {
	return &v
}

// --- ForEachReverse tests ---

func TestForEachReverse(t *testing.T) {
	tree := New[int64](4)
	keys := []int64{50, 20, 80, 10, 30, 60, 90, 5, 15, 25, 35}
	for _, k := range keys {
		tree.Insert(k, nil)
	}

	var result []int64
	tree.ForEachReverse(func(key int64, value any) bool {
		result = append(result, key)
		return true
	})

	require.Len(t, result, len(keys), "expected same number of keys")
	for i := 1; i < len(result); i++ {
		if result[i] >= result[i-1] {
			assert.Fail(t, fmt.Sprintf("keys not in descending order: %v", result))
			break
		}
	}
}

func TestForEachReverseEarlyTermination(t *testing.T) {
	tree := New[int64](4)
	for i := int64(1); i <= 100; i++ {
		tree.Insert(i, nil)
	}

	count := 0
	tree.ForEachReverse(func(key int64, value any) bool {
		count++
		return count < 5
	})

	assert.Equal(t, 5, count, "expected ForEachReverse to stop after 5 items")
}

func TestForEachRangeReverse(t *testing.T) {
	tree := New[int64](4)
	for i := int64(1); i <= 10; i++ {
		tree.Insert(i, i*10)
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
			name:          "closed interval [3,7] descending",
			from:          ptr(int64(3)),
			fromInclusive: true,
			to:            ptr(int64(7)),
			toInclusive:   true,
			wantKeys:      []int64{7, 6, 5, 4, 3},
		},
		{
			name:          "open interval (3,7) descending",
			from:          ptr(int64(3)),
			fromInclusive: false,
			to:            ptr(int64(7)),
			toInclusive:   false,
			wantKeys:      []int64{6, 5, 4},
		},
		{
			name:          "half-open [3,7) descending",
			from:          ptr(int64(3)),
			fromInclusive: true,
			to:            ptr(int64(7)),
			toInclusive:   false,
			wantKeys:      []int64{6, 5, 4, 3},
		},
		{
			name:          "half-open (3,7] descending",
			from:          ptr(int64(3)),
			fromInclusive: false,
			to:            ptr(int64(7)),
			toInclusive:   true,
			wantKeys:      []int64{7, 6, 5, 4},
		},
		{
			name:          "no lower bound (,5] descending",
			from:          nil,
			fromInclusive: false,
			to:            ptr(int64(5)),
			toInclusive:   true,
			wantKeys:      []int64{5, 4, 3, 2, 1},
		},
		{
			name:          "no upper bound [5,) descending",
			from:          ptr(int64(5)),
			fromInclusive: true,
			to:            nil,
			toInclusive:   false,
			wantKeys:      []int64{10, 9, 8, 7, 6, 5},
		},
		{
			name:          "no bounds (all) descending",
			from:          nil,
			fromInclusive: false,
			to:            nil,
			toInclusive:   false,
			wantKeys:      []int64{10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []int64
			tree.ForEachRangeReverse(tt.from, tt.fromInclusive, tt.to, tt.toInclusive, func(key int64, value any) bool {
				got = append(got, key)
				return true
			})
			require.Len(t, got, len(tt.wantKeys), "unexpected number of keys: %v", got)
			for i := range tt.wantKeys {
				assert.Equal(t, tt.wantKeys[i], got[i], "position %d", i)
			}
		})
	}
}

func TestForEachRangeReverseNoMatch(t *testing.T) {
	tree := New[int64](4)
	for i := int64(1); i <= 5; i++ {
		tree.Insert(i, nil)
	}

	var got []int64
	from := int64(10)
	to := int64(20)
	tree.ForEachRangeReverse(&from, true, &to, true, func(key int64, value any) bool {
		got = append(got, key)
		return true
	})
	assert.Len(t, got, 0, "expected 0 keys")
}

func TestForEachReverseStringKeys(t *testing.T) {
	tree := New[string](4)
	words := []string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape"}
	for _, w := range words {
		tree.Put(w, nil)
	}

	var got []string
	tree.ForEachReverse(func(key string, value any) bool {
		got = append(got, key)
		return true
	})

	expected := []string{"grape", "fig", "elderberry", "date", "cherry", "banana", "apple"}
	require.Len(t, got, len(expected), "unexpected number of keys: %v", got)
	for i := range expected {
		assert.Equal(t, expected[i], got[i], "position %d", i)
	}
}

func TestStringBTreeSortedKeys(t *testing.T) {
	tree := New[string](4)
	input := []string{"z", "m", "a", "f", "x", "b"}
	for _, k := range input {
		tree.Put(k, nil)
	}

	var result []string
	tree.ForEach(func(key string, value any) bool {
		result = append(result, key)
		return true
	})

	expected := make([]string, len(input))
	copy(expected, input)
	sort.Strings(expected)

	require.Len(t, result, len(expected), "unexpected number of keys")
	for i := range expected {
		assert.Equal(t, expected[i], result[i], "position %d", i)
	}
}

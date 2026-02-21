package btree

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestInsertAndGet(t *testing.T) {
	tree := New[int64](4) // small degree for easier testing

	if ok := tree.Insert(10, "ten"); !ok {
		t.Fatal("expected Insert to return true for new key")
	}
	if ok := tree.Insert(20, "twenty"); !ok {
		t.Fatal("expected Insert to return true for new key")
	}
	if ok := tree.Insert(5, "five"); !ok {
		t.Fatal("expected Insert to return true for new key")
	}

	val, ok := tree.Get(10)
	if !ok || val != "ten" {
		t.Errorf("Get(10): expected 'ten', got %v (ok=%v)", val, ok)
	}
	val, ok = tree.Get(20)
	if !ok || val != "twenty" {
		t.Errorf("Get(20): expected 'twenty', got %v (ok=%v)", val, ok)
	}
	val, ok = tree.Get(5)
	if !ok || val != "five" {
		t.Errorf("Get(5): expected 'five', got %v (ok=%v)", val, ok)
	}

	_, ok = tree.Get(999)
	if ok {
		t.Error("Get(999): expected ok=false for missing key")
	}
}

func TestInsertDuplicate(t *testing.T) {
	tree := New[int64](4)
	tree.Insert(10, "ten")
	if ok := tree.Insert(10, "ten-again"); ok {
		t.Error("expected Insert to return false for duplicate key")
	}
	// Original value should be preserved
	val, _ := tree.Get(10)
	if val != "ten" {
		t.Errorf("expected 'ten' after duplicate insert, got %v", val)
	}
}

func TestPut(t *testing.T) {
	tree := New[int64](4)
	tree.Put(10, "ten")
	tree.Put(10, "TEN") // upsert
	val, ok := tree.Get(10)
	if !ok || val != "TEN" {
		t.Errorf("Put upsert: expected 'TEN', got %v", val)
	}
}

func TestDelete(t *testing.T) {
	tree := New[int64](4)
	tree.Insert(10, "ten")
	tree.Insert(20, "twenty")
	tree.Insert(5, "five")

	if ok := tree.Delete(10); !ok {
		t.Fatal("expected Delete to return true for existing key")
	}
	if _, ok := tree.Get(10); ok {
		t.Error("Get(10) should return false after delete")
	}
	if tree.Len() != 2 {
		t.Errorf("expected Len()=2 after delete, got %d", tree.Len())
	}

	// Delete non-existent key
	if ok := tree.Delete(999); ok {
		t.Error("expected Delete to return false for non-existent key")
	}
}

func TestHas(t *testing.T) {
	tree := New[int64](4)
	tree.Insert(10, "ten")

	if !tree.Has(10) {
		t.Error("Has(10): expected true")
	}
	if tree.Has(999) {
		t.Error("Has(999): expected false")
	}
}

func TestLen(t *testing.T) {
	tree := New[int64](4)
	if tree.Len() != 0 {
		t.Errorf("expected Len()=0 for empty tree, got %d", tree.Len())
	}
	tree.Insert(1, nil)
	tree.Insert(2, nil)
	tree.Insert(3, nil)
	if tree.Len() != 3 {
		t.Errorf("expected Len()=3, got %d", tree.Len())
	}
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

	if len(result) != len(keys) {
		t.Fatalf("expected %d keys, got %d", len(keys), len(result))
	}
	for i := 1; i < len(result); i++ {
		if result[i] <= result[i-1] {
			t.Errorf("keys not in order: %v", result)
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

	if count != 5 {
		t.Errorf("expected ForEach to stop after 5 items, got %d", count)
	}
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

	if tree.Len() != n {
		t.Fatalf("expected Len()=%d, got %d", n, tree.Len())
	}

	// Verify all values retrievable
	for i := 0; i < n; i++ {
		val, ok := tree.Get(int64(i))
		if !ok {
			t.Fatalf("Get(%d): expected ok=true", i)
		}
		if val != i*10 {
			t.Fatalf("Get(%d): expected %d, got %v", i, i*10, val)
		}
	}

	// Verify sorted order
	var prev int64 = -1
	tree.ForEach(func(key int64, value any) bool {
		if key <= prev {
			t.Errorf("keys not in order: %d after %d", key, prev)
			return false
		}
		prev = key
		return true
	})

	// Delete half
	for i := 0; i < n/2; i++ {
		k := int64(perm[i])
		if !tree.Delete(k) {
			t.Fatalf("Delete(%d): expected true", k)
		}
	}

	if tree.Len() != n/2 {
		t.Fatalf("expected Len()=%d after deletes, got %d", n/2, tree.Len())
	}

	// Verify remaining are still accessible and sorted
	prev = -1
	count := 0
	tree.ForEach(func(key int64, value any) bool {
		if key <= prev {
			t.Errorf("keys not in order after delete: %d after %d", key, prev)
			return false
		}
		prev = key
		count++
		return true
	})
	if count != n/2 {
		t.Errorf("expected %d items in ForEach, got %d", n/2, count)
	}
}

func TestDeleteAndReinsert(t *testing.T) {
	tree := New[int64](4)
	tree.Insert(10, "first")
	tree.Delete(10)

	if ok := tree.Insert(10, "second"); !ok {
		t.Fatal("expected Insert to return true after delete")
	}
	val, ok := tree.Get(10)
	if !ok || val != "second" {
		t.Errorf("expected 'second' after reinsert, got %v", val)
	}
}

// --- String key tests ---

func TestStringBTreePutAndGet(t *testing.T) {
	tree := New[string](4)

	tree.Put("banana", 2)
	tree.Put("apple", 1)
	tree.Put("cherry", 3)

	val, ok := tree.Get("banana")
	if !ok || val != 2 {
		t.Errorf("Get(banana): expected 2, got %v (ok=%v)", val, ok)
	}
	val, ok = tree.Get("apple")
	if !ok || val != 1 {
		t.Errorf("Get(apple): expected 1, got %v (ok=%v)", val, ok)
	}
	val, ok = tree.Get("cherry")
	if !ok || val != 3 {
		t.Errorf("Get(cherry): expected 3, got %v (ok=%v)", val, ok)
	}

	_, ok = tree.Get("missing")
	if ok {
		t.Error("Get(missing): expected ok=false")
	}
}

func TestStringBTreePutUpsert(t *testing.T) {
	tree := New[string](4)
	tree.Put("key", "first")
	tree.Put("key", "second")
	val, ok := tree.Get("key")
	if !ok || val != "second" {
		t.Errorf("Put upsert: expected 'second', got %v", val)
	}
	if tree.Len() != 1 {
		t.Errorf("expected Len()=1 after upsert, got %d", tree.Len())
	}
}

func TestStringBTreeDelete(t *testing.T) {
	tree := New[string](4)
	tree.Put("a", 1)
	tree.Put("b", 2)
	tree.Put("c", 3)

	if ok := tree.Delete("b"); !ok {
		t.Fatal("expected Delete to return true for existing key")
	}
	if _, ok := tree.Get("b"); ok {
		t.Error("Get(b) should return false after delete")
	}
	if tree.Len() != 2 {
		t.Errorf("expected Len()=2 after delete, got %d", tree.Len())
	}

	if ok := tree.Delete("missing"); ok {
		t.Error("expected Delete to return false for non-existent key")
	}
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

	if len(result) != len(keys) {
		t.Fatalf("expected %d keys, got %d", len(keys), len(result))
	}
	for i := 1; i < len(result); i++ {
		if result[i] <= result[i-1] {
			t.Errorf("keys not in order: %v", result)
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

	if count != 5 {
		t.Errorf("expected ForEach to stop after 5 items, got %d", count)
	}
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

	if tree.Len() != n {
		t.Fatalf("expected Len()=%d, got %d", n, tree.Len())
	}

	for i := 0; i < n; i++ {
		val, ok := tree.Get(keys[i])
		if !ok {
			t.Fatalf("Get(%s): expected ok=true", keys[i])
		}
		if val != i*10 {
			t.Fatalf("Get(%s): expected %d, got %v", keys[i], i*10, val)
		}
	}

	// Verify sorted order
	var prev string
	tree.ForEach(func(key string, value any) bool {
		if prev != "" && key <= prev {
			t.Errorf("keys not in order: %s after %s", key, prev)
			return false
		}
		prev = key
		return true
	})

	// Delete half
	for i := 0; i < n/2; i++ {
		k := keys[perm[i]]
		if !tree.Delete(k) {
			t.Fatalf("Delete(%s): expected true", k)
		}
	}

	if tree.Len() != n/2 {
		t.Fatalf("expected Len()=%d after deletes, got %d", n/2, tree.Len())
	}

	// Verify remaining are sorted
	prev = ""
	count := 0
	tree.ForEach(func(key string, value any) bool {
		if prev != "" && key <= prev {
			t.Errorf("keys not in order after delete: %s after %s", key, prev)
			return false
		}
		prev = key
		count++
		return true
	})
	if count != n/2 {
		t.Errorf("expected %d items in ForEach, got %d", n/2, count)
	}
}

func TestStringBTreeDeleteAndReinsert(t *testing.T) {
	tree := New[string](4)
	tree.Put("key", "first")
	tree.Delete("key")

	tree.Put("key", "second")
	val, ok := tree.Get("key")
	if !ok || val != "second" {
		t.Errorf("expected 'second' after reinsert, got %v", val)
	}
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
			if len(got) != len(tt.wantKeys) {
				t.Fatalf("expected %d keys, got %d: %v", len(tt.wantKeys), len(got), got)
			}
			for i := range tt.wantKeys {
				if got[i] != tt.wantKeys[i] {
					t.Errorf("position %d: expected %d, got %d", i, tt.wantKeys[i], got[i])
				}
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
	if len(got) != 0 {
		t.Errorf("expected 0 keys, got %d: %v", len(got), got)
	}
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
	if count != 3 {
		t.Errorf("expected 3 items before early termination, got %d", count)
	}
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
	if len(got) != len(expected) {
		t.Fatalf("expected %d keys, got %d: %v", len(expected), len(got), got)
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Errorf("position %d: expected %s, got %s", i, expected[i], got[i])
		}
	}
}

func ptr[T any](v T) *T {
	return &v
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

	if len(result) != len(expected) {
		t.Fatalf("expected %d keys, got %d", len(expected), len(result))
	}
	for i := range expected {
		if result[i] != expected[i] {
			t.Errorf("position %d: expected %s, got %s", i, expected[i], result[i])
		}
	}
}

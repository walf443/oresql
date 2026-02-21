package btree

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestStringBTreePutAndGet(t *testing.T) {
	tree := NewStringBTree(4)

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
	tree := NewStringBTree(4)
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
	tree := NewStringBTree(4)
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
	tree := NewStringBTree(4)
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
	tree := NewStringBTree(4)
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
	tree := NewStringBTree(32)

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
	tree := NewStringBTree(4)
	tree.Put("key", "first")
	tree.Delete("key")

	tree.Put("key", "second")
	val, ok := tree.Get("key")
	if !ok || val != "second" {
		t.Errorf("expected 'second' after reinsert, got %v", val)
	}
}

func TestStringBTreeSortedKeys(t *testing.T) {
	tree := NewStringBTree(4)
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

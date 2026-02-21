package btree

import (
	"math/rand"
	"testing"
)

func TestInsertAndGet(t *testing.T) {
	tree := New(4) // small degree for easier testing

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
	tree := New(4)
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
	tree := New(4)
	tree.Put(10, "ten")
	tree.Put(10, "TEN") // upsert
	val, ok := tree.Get(10)
	if !ok || val != "TEN" {
		t.Errorf("Put upsert: expected 'TEN', got %v", val)
	}
}

func TestDelete(t *testing.T) {
	tree := New(4)
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
	tree := New(4)
	tree.Insert(10, "ten")

	if !tree.Has(10) {
		t.Error("Has(10): expected true")
	}
	if tree.Has(999) {
		t.Error("Has(999): expected false")
	}
}

func TestLen(t *testing.T) {
	tree := New(4)
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
	tree := New(4)
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
	tree := New(4)
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
	tree := New(32) // production degree

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
	tree := New(4)
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

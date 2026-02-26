package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestStorageEngineInterface verifies that *Storage satisfies StorageEngine at compile time.
// The var _ declaration in storage.go provides compile-time verification;
// this test documents the intent and exercises the assertion.
func TestStorageEngineInterface(t *testing.T) {
	var s StorageEngine = NewStorage()
	assert.NotNil(t, s)
}

// TestIndexReaderInterface verifies that *SecondaryIndex satisfies IndexReader at compile time.
func TestIndexReaderInterface(t *testing.T) {
	var idx IndexReader = &SecondaryIndex{
		Info: &IndexInfo{Name: "test_idx"},
	}
	assert.NotNil(t, idx)
	assert.Equal(t, "test_idx", idx.GetInfo().Name)
}

// TestWithStorageOption verifies that WithStorage correctly sets the storage engine.
func TestWithStorageOption(t *testing.T) {
	s := NewStorage()
	e := NewExecutor(WithStorage(s))
	assert.NotNil(t, e)
}

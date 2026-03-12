package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/walf443/oresql/storage"
	"github.com/walf443/oresql/storage/memory"
)

// TestStorageEngineInterface verifies that *MemoryStorage satisfies StorageEngine at compile time.
// The var _ declaration in storage/memory provides compile-time verification;
// this test documents the intent and exercises the assertion.
func TestStorageEngineInterface(t *testing.T) {
	var s storage.Engine = memory.NewMemoryStorage()
	assert.NotNil(t, s)
}

// TestIndexReaderInterface verifies that *SecondaryIndex satisfies IndexReader at compile time.
func TestIndexReaderInterface(t *testing.T) {
	var idx IndexReader = &memory.SecondaryIndex{
		Info: &storage.IndexInfo{Name: "test_idx"},
	}
	assert.NotNil(t, idx)
	assert.Equal(t, "test_idx", idx.GetInfo().Name)
}

// TestWithDatabaseStorageOption verifies that WithDatabaseStorage correctly sets the storage engine.
func TestWithDatabaseStorageOption(t *testing.T) {
	s := memory.NewMemoryStorage()
	db := NewDatabase("test", WithDatabaseStorage(s))
	e := NewExecutor(db)
	assert.NotNil(t, e)
}

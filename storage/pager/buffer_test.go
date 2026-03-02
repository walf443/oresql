package pager

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestPool(t *testing.T, capacity int) *BufferPool {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	p, err := Create(path)
	require.NoError(t, err)
	return NewBufferPool(p, capacity)
}

func TestBufferPoolFetchAndUnpin(t *testing.T) {
	bp := newTestPool(t, 10)
	defer bp.Close()

	// Allocate and write a page through the pool
	id, data, err := bp.NewPage()
	require.NoError(t, err)
	data[0] = 0xAA
	data[1] = 0xBB
	bp.UnpinPage(id, true)

	// Flush and re-fetch
	require.NoError(t, bp.FlushAll())

	fetched, err := bp.FetchPage(id)
	require.NoError(t, err)
	assert.Equal(t, byte(0xAA), fetched[0])
	assert.Equal(t, byte(0xBB), fetched[1])
	bp.UnpinPage(id, false)
}

func TestBufferPoolLRUEviction(t *testing.T) {
	bp := newTestPool(t, 4)
	defer bp.Close()

	// Allocate 4 pages (fills the pool)
	ids := make([]PageID, 4)
	for i := range ids {
		id, data, err := bp.NewPage()
		require.NoError(t, err)
		data[0] = byte(i + 1)
		bp.UnpinPage(id, true)
		ids[i] = id
	}

	// Fetch page 0 to make it most-recently-used
	_, err := bp.FetchPage(ids[0])
	require.NoError(t, err)
	bp.UnpinPage(ids[0], false)

	// Allocate page 5 → should evict page 1 (least recently used)
	id5, _, err := bp.NewPage()
	require.NoError(t, err)
	bp.UnpinPage(id5, true)

	// Page 1 should have been evicted and written to disk
	// Fetch it back should work (from disk)
	fetched, err := bp.FetchPage(ids[1])
	require.NoError(t, err)
	assert.Equal(t, byte(2), fetched[0])
	bp.UnpinPage(ids[1], false)
}

func TestBufferPoolPinnedProtection(t *testing.T) {
	// Use numShards as capacity so each shard holds exactly 1 page.
	bp := newTestPool(t, numShards)
	defer bp.Close()

	// Pin all pages (one per shard)
	ids := make([]PageID, numShards)
	for i := range ids {
		id, _, err := bp.NewPage()
		require.NoError(t, err)
		ids[i] = id
		// Don't unpin — all are pinned
	}

	// Try to allocate one more → should fail (all pinned)
	_, _, err := bp.NewPage()
	assert.Error(t, err, "should fail when all frames are pinned")

	// Unpin all → now allocation should succeed (eviction possible in any shard)
	for _, id := range ids {
		bp.UnpinPage(id, false)
	}
	idNew, _, err := bp.NewPage()
	require.NoError(t, err)
	assert.NotEqual(t, InvalidPageID, idNew)
	bp.UnpinPage(idNew, false)
}

func TestBufferPoolDirtyFlush(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	p, err := Create(path)
	require.NoError(t, err)

	bp := NewBufferPool(p, 10)

	// Create a page, write data, mark dirty
	id, data, err := bp.NewPage()
	require.NoError(t, err)
	data[0] = 0xFF
	bp.UnpinPage(id, true)
	require.NoError(t, bp.FlushAll())

	// Close and reopen
	require.NoError(t, bp.Close())

	p2, err := Open(path)
	require.NoError(t, err)
	bp2 := NewBufferPool(p2, 10)
	defer bp2.Close()

	fetched, err := bp2.FetchPage(id)
	require.NoError(t, err)
	assert.Equal(t, byte(0xFF), fetched[0])
	bp2.UnpinPage(id, false)
}

func TestBufferPoolMultiplePin(t *testing.T) {
	bp := newTestPool(t, 10)
	defer bp.Close()

	id, _, err := bp.NewPage()
	require.NoError(t, err)

	// Fetch again (double pin)
	_, err = bp.FetchPage(id)
	require.NoError(t, err)

	// Unpin once → still pinned
	bp.UnpinPage(id, false)

	// Unpin again → now unpinned
	bp.UnpinPage(id, false)
}

func TestBufferPoolConcurrentFetch(t *testing.T) {
	bp := newTestPool(t, 64)
	defer bp.Close()

	const numPages = 32
	ids := make([]PageID, numPages)
	for i := range ids {
		id, data, err := bp.NewPage()
		require.NoError(t, err)
		data[0] = byte(i + 1)
		bp.UnpinPage(id, true)
		ids[i] = id
	}
	require.NoError(t, bp.FlushAll())

	// Concurrent FetchPage / UnpinPage from multiple goroutines
	var wg sync.WaitGroup
	const numGoroutines = 16
	const iterations = 100
	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for iter := 0; iter < iterations; iter++ {
				idx := (gid*iterations + iter) % numPages
				data, err := bp.FetchPage(ids[idx])
				if err != nil {
					t.Errorf("goroutine %d: FetchPage(%d): %v", gid, ids[idx], err)
					return
				}
				if data[0] != byte(idx+1) {
					t.Errorf("goroutine %d: page %d: got %d, want %d", gid, ids[idx], data[0], idx+1)
				}
				bp.UnpinPage(ids[idx], false)
			}
		}(g)
	}
	wg.Wait()
}

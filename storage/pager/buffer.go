package pager

import (
	"container/list"
	"fmt"
	"sync"
)

// frame holds a cached page in the buffer pool.
type frame struct {
	pageID  PageID
	data    [PageSize]byte
	dirty   bool
	pinned  int // reference count
	lruElem *list.Element
}

// BufferPool is an LRU cache over a Pager.
type BufferPool struct {
	mu       sync.Mutex
	pager    *Pager
	frames   map[PageID]*frame
	lruList  *list.List // LRU list of unpinned frames (back = least recently used)
	capacity int
}

// NewBufferPool creates a new BufferPool with the given capacity (max pages in memory).
func NewBufferPool(pager *Pager, capacity int) *BufferPool {
	if capacity < 4 {
		capacity = 4
	}
	return &BufferPool{
		pager:    pager,
		frames:   make(map[PageID]*frame),
		lruList:  list.New(),
		capacity: capacity,
	}
}

// FetchPage fetches a page into the buffer pool, pins it, and returns its data slice.
// The caller must call UnpinPage when done.
func (bp *BufferPool) FetchPage(id PageID) ([]byte, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Check if already in pool
	if f, ok := bp.frames[id]; ok {
		f.pinned++
		// Remove from LRU if it was there (it's now pinned)
		if f.lruElem != nil {
			bp.lruList.Remove(f.lruElem)
			f.lruElem = nil
		}
		return f.data[:], nil
	}

	// Need to load from disk; make room if needed
	if err := bp.evictIfNeeded(); err != nil {
		return nil, err
	}

	f := &frame{
		pageID: id,
		pinned: 1,
	}
	if err := bp.pager.ReadPage(id, f.data[:]); err != nil {
		return nil, err
	}
	bp.frames[id] = f
	return f.data[:], nil
}

// NewPage allocates a new page, pins it, and returns its ID and data slice.
func (bp *BufferPool) NewPage() (PageID, []byte, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if err := bp.evictIfNeeded(); err != nil {
		return InvalidPageID, nil, err
	}

	id, err := bp.pager.AllocPage()
	if err != nil {
		return InvalidPageID, nil, err
	}

	f := &frame{
		pageID: id,
		pinned: 1,
		dirty:  true,
	}
	bp.frames[id] = f
	return id, f.data[:], nil
}

// UnpinPage unpins a page. If dirty is true, marks it for flushing.
func (bp *BufferPool) UnpinPage(id PageID, dirty bool) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	f, ok := bp.frames[id]
	if !ok {
		return
	}
	if dirty {
		f.dirty = true
	}
	if f.pinned > 0 {
		f.pinned--
	}
	if f.pinned == 0 && f.lruElem == nil {
		f.lruElem = bp.lruList.PushFront(f)
	}
}

// FlushPage writes a dirty page to disk.
func (bp *BufferPool) FlushPage(id PageID) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	f, ok := bp.frames[id]
	if !ok {
		return nil
	}
	if f.dirty {
		if err := bp.pager.WritePage(f.pageID, f.data[:]); err != nil {
			return err
		}
		f.dirty = false
	}
	return nil
}

// FlushAll writes all dirty pages to disk and syncs.
func (bp *BufferPool) FlushAll() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for _, f := range bp.frames {
		if f.dirty {
			if err := bp.pager.WritePage(f.pageID, f.data[:]); err != nil {
				return err
			}
			f.dirty = false
		}
	}
	return bp.pager.Sync()
}

// Close flushes all pages and closes the underlying pager.
func (bp *BufferPool) Close() error {
	if err := bp.FlushAll(); err != nil {
		return err
	}
	return bp.pager.Close()
}

// Pager returns the underlying Pager.
func (bp *BufferPool) Pager() *Pager {
	return bp.pager
}

// evictIfNeeded evicts the least recently used unpinned frame if the pool is at capacity.
// Must be called with bp.mu held.
func (bp *BufferPool) evictIfNeeded() error {
	for len(bp.frames) >= bp.capacity {
		elem := bp.lruList.Back()
		if elem == nil {
			return fmt.Errorf("buffer pool: all %d frames are pinned, cannot evict", bp.capacity)
		}
		victim := elem.Value.(*frame)
		bp.lruList.Remove(elem)

		if victim.dirty {
			if err := bp.pager.WritePage(victim.pageID, victim.data[:]); err != nil {
				return err
			}
		}
		delete(bp.frames, victim.pageID)
	}
	return nil
}

package pager

import (
	"container/list"
	"fmt"
	"sync"
)

const numShards = 8

// frame holds a cached page in the buffer pool.
type frame struct {
	pageID  PageID
	data    [PageSize]byte
	dirty   bool
	pinned  int // reference count
	lruElem *list.Element
}

// shard is an independently-locked partition of the buffer pool.
type shard struct {
	mu       sync.Mutex
	frames   map[PageID]*frame
	lruList  *list.List
	capacity int
}

// BufferPool is an LRU cache over a Pager, sharded by pageID for reduced lock contention.
type BufferPool struct {
	pager  *Pager
	shards [numShards]shard
}

// NewBufferPool creates a new BufferPool with the given capacity (max pages in memory).
func NewBufferPool(pager *Pager, capacity int) *BufferPool {
	if capacity < numShards {
		capacity = numShards
	}
	bp := &BufferPool{pager: pager}
	base := capacity / numShards
	extra := capacity % numShards
	for i := range bp.shards {
		c := base
		if i < extra {
			c++
		}
		bp.shards[i] = shard{
			frames:   make(map[PageID]*frame),
			lruList:  list.New(),
			capacity: c,
		}
	}
	return bp
}

// FetchPage fetches a page into the buffer pool, pins it, and returns its data slice.
// The caller must call UnpinPage when done.
func (bp *BufferPool) FetchPage(id PageID) ([]byte, error) {
	s := &bp.shards[id%numShards]
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already in pool
	if f, ok := s.frames[id]; ok {
		f.pinned++
		// Remove from LRU if it was there (it's now pinned)
		if f.lruElem != nil {
			s.lruList.Remove(f.lruElem)
			f.lruElem = nil
		}
		return f.data[:], nil
	}

	// Need to load from disk; make room if needed
	if err := s.evictIfNeeded(bp.pager); err != nil {
		return nil, err
	}

	f := &frame{
		pageID: id,
		pinned: 1,
	}
	if err := bp.pager.ReadPage(id, f.data[:]); err != nil {
		return nil, err
	}
	s.frames[id] = f
	return f.data[:], nil
}

// NewPage allocates a new page, pins it, and returns its ID and data slice.
func (bp *BufferPool) NewPage() (PageID, []byte, error) {
	id, err := bp.pager.AllocPage()
	if err != nil {
		return InvalidPageID, nil, err
	}

	s := &bp.shards[id%numShards]
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.evictIfNeeded(bp.pager); err != nil {
		return InvalidPageID, nil, err
	}

	f := &frame{
		pageID: id,
		pinned: 1,
		dirty:  true,
	}
	s.frames[id] = f
	return id, f.data[:], nil
}

// UnpinPage unpins a page. If dirty is true, marks it for flushing.
func (bp *BufferPool) UnpinPage(id PageID, dirty bool) {
	s := &bp.shards[id%numShards]
	s.mu.Lock()
	defer s.mu.Unlock()

	f, ok := s.frames[id]
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
		f.lruElem = s.lruList.PushFront(f)
	}
}

// FlushPage writes a dirty page to disk.
func (bp *BufferPool) FlushPage(id PageID) error {
	s := &bp.shards[id%numShards]
	s.mu.Lock()
	defer s.mu.Unlock()

	f, ok := s.frames[id]
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
	for i := range bp.shards {
		s := &bp.shards[i]
		s.mu.Lock()
		for _, f := range s.frames {
			if f.dirty {
				if err := bp.pager.WritePage(f.pageID, f.data[:]); err != nil {
					s.mu.Unlock()
					return err
				}
				f.dirty = false
			}
		}
		s.mu.Unlock()
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

// evictIfNeeded evicts the least recently used unpinned frame if the shard is at capacity.
// Must be called with s.mu held.
func (s *shard) evictIfNeeded(p *Pager) error {
	for len(s.frames) >= s.capacity {
		elem := s.lruList.Back()
		if elem == nil {
			return fmt.Errorf("buffer pool: all %d frames are pinned, cannot evict", s.capacity)
		}
		victim := elem.Value.(*frame)
		s.lruList.Remove(elem)

		if victim.dirty {
			if err := p.WritePage(victim.pageID, victim.data[:]); err != nil {
				return err
			}
		}
		delete(s.frames, victim.pageID)
	}
	return nil
}

package pager

import (
	"encoding/binary"
	"fmt"
	"os"
)

const PageSize = 4096

type PageID = uint32

const InvalidPageID PageID = 0xFFFFFFFF

// freeListPageHeader is the overhead per free-list page:
// [nextFreePage: 4B] [count: 2B]
const freeListHeaderSize = 6

// maxFreeEntriesPerPage is how many PageIDs fit in one free-list page.
const maxFreeEntriesPerPage = (PageSize - freeListHeaderSize) / 4

// Pager manages page-level I/O on a single file.
// Page 0 is reserved for internal file-level metadata managed by the caller.
type Pager struct {
	file      *os.File
	pageCount uint32
	freeHead  PageID // head of the free-list chain (InvalidPageID = none)
}

// Create creates a new pager file. The file must not already exist.
func Create(path string) (*Pager, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, fmt.Errorf("pager create: %w", err)
	}
	p := &Pager{
		file:      f,
		pageCount: 0,
		freeHead:  InvalidPageID,
	}
	return p, nil
}

// Open opens an existing pager file.
func Open(path string) (*Pager, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("pager open: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("pager stat: %w", err)
	}
	size := info.Size()
	pageCount := uint32(0)
	if size > 0 {
		pageCount = uint32(size / int64(PageSize))
	}
	p := &Pager{
		file:      f,
		pageCount: pageCount,
		freeHead:  InvalidPageID,
	}
	return p, nil
}

// ReadPage reads the page with the given ID into buf.
// buf must be at least PageSize bytes.
func (p *Pager) ReadPage(id PageID, buf []byte) error {
	if id >= p.pageCount {
		return fmt.Errorf("pager: read page %d out of range (count=%d)", id, p.pageCount)
	}
	offset := int64(id) * int64(PageSize)
	n, err := p.file.ReadAt(buf[:PageSize], offset)
	if err != nil {
		return fmt.Errorf("pager: read page %d: %w", id, err)
	}
	if n < PageSize {
		return fmt.Errorf("pager: short read page %d: got %d bytes", id, n)
	}
	return nil
}

// WritePage writes data to the page with the given ID.
// data must be exactly PageSize bytes.
func (p *Pager) WritePage(id PageID, data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("pager: write page data must be %d bytes, got %d", PageSize, len(data))
	}
	if id >= p.pageCount {
		return fmt.Errorf("pager: write page %d out of range (count=%d)", id, p.pageCount)
	}
	offset := int64(id) * int64(PageSize)
	_, err := p.file.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("pager: write page %d: %w", id, err)
	}
	return nil
}

// AllocPage allocates a new page and returns its ID.
// It reuses freed pages from the free-list when available.
func (p *Pager) AllocPage() (PageID, error) {
	// Try free-list first
	if p.freeHead != InvalidPageID {
		id := p.freeHead
		// Read the free-list page to get next pointer
		var buf [PageSize]byte
		if err := p.ReadPage(id, buf[:]); err != nil {
			return InvalidPageID, err
		}
		nextFree := binary.BigEndian.Uint32(buf[0:4])
		p.freeHead = nextFree
		// Zero the page
		for i := range buf {
			buf[i] = 0
		}
		if err := p.WritePage(id, buf[:]); err != nil {
			return InvalidPageID, err
		}
		return id, nil
	}

	// Extend the file
	id := p.pageCount
	var zeroBuf [PageSize]byte
	offset := int64(id) * int64(PageSize)
	if _, err := p.file.WriteAt(zeroBuf[:], offset); err != nil {
		return InvalidPageID, fmt.Errorf("pager: alloc page: %w", err)
	}
	p.pageCount++
	return id, nil
}

// FreePage adds a page to the free-list for reuse.
func (p *Pager) FreePage(id PageID) error {
	if id >= p.pageCount {
		return fmt.Errorf("pager: free page %d out of range (count=%d)", id, p.pageCount)
	}
	// Write a free-list entry: [nextFreePage: 4B] followed by zeroes
	var buf [PageSize]byte
	binary.BigEndian.PutUint32(buf[0:4], p.freeHead)
	if err := p.WritePage(id, buf[:]); err != nil {
		return err
	}
	p.freeHead = id
	return nil
}

// PageCount returns the total number of pages in the file.
func (p *Pager) PageCount() uint32 {
	return p.pageCount
}

// FreeHead returns the current free-list head page ID.
func (p *Pager) FreeHead() PageID {
	return p.freeHead
}

// SetFreeHead sets the free-list head (used when restoring from file header).
func (p *Pager) SetFreeHead(id PageID) {
	p.freeHead = id
}

// Sync flushes the file to disk.
func (p *Pager) Sync() error {
	return p.file.Sync()
}

// Close closes the pager file.
func (p *Pager) Close() error {
	return p.file.Close()
}

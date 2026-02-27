package pager

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateAndOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Create
	p1, err := Create(path)
	require.NoError(t, err)

	// Alloc 3 pages
	id0, err := p1.AllocPage()
	require.NoError(t, err)
	assert.Equal(t, PageID(0), id0)

	id1, err := p1.AllocPage()
	require.NoError(t, err)
	assert.Equal(t, PageID(1), id1)

	id2, err := p1.AllocPage()
	require.NoError(t, err)
	assert.Equal(t, PageID(2), id2)

	// Write some data
	var buf [PageSize]byte
	buf[0] = 0xAB
	buf[PageSize-1] = 0xCD
	require.NoError(t, p1.WritePage(id1, buf[:]))
	require.NoError(t, p1.Sync())

	assert.Equal(t, uint32(3), p1.PageCount())
	require.NoError(t, p1.Close())

	// Open
	p2, err := Open(path)
	require.NoError(t, err)
	defer p2.Close()

	assert.Equal(t, uint32(3), p2.PageCount())

	var readBuf [PageSize]byte
	require.NoError(t, p2.ReadPage(id1, readBuf[:]))
	assert.Equal(t, byte(0xAB), readBuf[0])
	assert.Equal(t, byte(0xCD), readBuf[PageSize-1])
}

func TestAllocAndFree(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := Create(path)
	require.NoError(t, err)
	defer p.Close()

	// Alloc 3 pages
	id0, _ := p.AllocPage()
	id1, _ := p.AllocPage()
	id2, _ := p.AllocPage()
	assert.Equal(t, PageID(0), id0)
	assert.Equal(t, PageID(1), id1)
	assert.Equal(t, PageID(2), id2)
	assert.Equal(t, uint32(3), p.PageCount())

	// Free page 1
	require.NoError(t, p.FreePage(id1))

	// Next alloc should reuse page 1
	reused, err := p.AllocPage()
	require.NoError(t, err)
	assert.Equal(t, id1, reused)

	// Next alloc should extend
	id3, err := p.AllocPage()
	require.NoError(t, err)
	assert.Equal(t, PageID(3), id3)
}

func TestReadWriteRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := Create(path)
	require.NoError(t, err)
	defer p.Close()

	id, err := p.AllocPage()
	require.NoError(t, err)

	// Write pattern
	var writeBuf [PageSize]byte
	for i := range writeBuf {
		writeBuf[i] = byte(i % 256)
	}
	require.NoError(t, p.WritePage(id, writeBuf[:]))

	// Read back
	var readBuf [PageSize]byte
	require.NoError(t, p.ReadPage(id, readBuf[:]))
	assert.Equal(t, writeBuf, readBuf)
}

func TestFreeListChain(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := Create(path)
	require.NoError(t, err)
	defer p.Close()

	// Alloc pages 0..4
	ids := make([]PageID, 5)
	for i := range ids {
		id, err := p.AllocPage()
		require.NoError(t, err)
		ids[i] = id
	}

	// Free pages 1, 3 (in that order)
	require.NoError(t, p.FreePage(ids[1]))
	require.NoError(t, p.FreePage(ids[3]))

	// Alloc should return freed pages in LIFO order
	a1, _ := p.AllocPage()
	a2, _ := p.AllocPage()
	assert.Equal(t, ids[3], a1, "first alloc should reuse last freed")
	assert.Equal(t, ids[1], a2, "second alloc should reuse first freed")

	// Next alloc extends
	a3, _ := p.AllocPage()
	assert.Equal(t, PageID(5), a3)
}

func TestReadOutOfRange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := Create(path)
	require.NoError(t, err)
	defer p.Close()

	var buf [PageSize]byte
	err = p.ReadPage(0, buf[:])
	assert.Error(t, err, "reading from empty pager should error")
}

func TestWriteWrongSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := Create(path)
	require.NoError(t, err)
	defer p.Close()

	id, _ := p.AllocPage()
	err = p.WritePage(id, make([]byte, 100))
	assert.Error(t, err, "writing wrong size should error")
}

package disk

import (
	"encoding/binary"
	"math/bits"
	"sort"
)

// arrayMaxSize is the maximum number of elements in an array container.
// At 4096 elements, the array (4096 * 2 = 8192 bytes) equals the bitmap size (8192 bytes).
const arrayMaxSize = 4096

// --- Container interface ---

type roaringContainer interface {
	add(val uint16) roaringContainer
	remove(val uint16) roaringContainer
	contains(val uint16) bool
	cardinality() int
	toSortedSlice() []uint16
	and(other roaringContainer) roaringContainer
	or(other roaringContainer) roaringContainer
	encode(buf []byte) []byte
}

// --- Array Container ---

type arrayContainer struct {
	values []uint16
}

func newArrayContainer() *arrayContainer {
	return &arrayContainer{}
}

func newArrayContainerWithValues(vals []uint16) *arrayContainer {
	return &arrayContainer{values: vals}
}

func (c *arrayContainer) add(val uint16) roaringContainer {
	pos := sort.Search(len(c.values), func(i int) bool { return c.values[i] >= val })
	if pos < len(c.values) && c.values[pos] == val {
		return c // already present
	}
	c.values = append(c.values, 0)
	copy(c.values[pos+1:], c.values[pos:])
	c.values[pos] = val

	if len(c.values) >= arrayMaxSize {
		return c.toBitmapContainer()
	}
	return c
}

func (c *arrayContainer) remove(val uint16) roaringContainer {
	pos := sort.Search(len(c.values), func(i int) bool { return c.values[i] >= val })
	if pos >= len(c.values) || c.values[pos] != val {
		return c
	}
	c.values = append(c.values[:pos], c.values[pos+1:]...)
	return c
}

func (c *arrayContainer) contains(val uint16) bool {
	pos := sort.Search(len(c.values), func(i int) bool { return c.values[i] >= val })
	return pos < len(c.values) && c.values[pos] == val
}

func (c *arrayContainer) cardinality() int {
	return len(c.values)
}

func (c *arrayContainer) toSortedSlice() []uint16 {
	if len(c.values) == 0 {
		return nil
	}
	out := make([]uint16, len(c.values))
	copy(out, c.values)
	return out
}

func (c *arrayContainer) toBitmapContainer() *bitmapContainer {
	bc := &bitmapContainer{card: len(c.values)}
	for _, v := range c.values {
		bc.bitmap[v/64] |= 1 << (v % 64)
	}
	return bc
}

func (c *arrayContainer) and(other roaringContainer) roaringContainer {
	switch o := other.(type) {
	case *arrayContainer:
		return c.andArray(o)
	case *bitmapContainer:
		return c.andBitmap(o)
	}
	return newArrayContainer()
}

func (c *arrayContainer) andArray(o *arrayContainer) roaringContainer {
	result := make([]uint16, 0)
	i, j := 0, 0
	for i < len(c.values) && j < len(o.values) {
		if c.values[i] == o.values[j] {
			result = append(result, c.values[i])
			i++
			j++
		} else if c.values[i] < o.values[j] {
			i++
		} else {
			j++
		}
	}
	return newArrayContainerWithValues(result)
}

func (c *arrayContainer) andBitmap(o *bitmapContainer) roaringContainer {
	result := make([]uint16, 0)
	for _, v := range c.values {
		if o.bitmap[v/64]&(1<<(v%64)) != 0 {
			result = append(result, v)
		}
	}
	return newArrayContainerWithValues(result)
}

func (c *arrayContainer) or(other roaringContainer) roaringContainer {
	switch o := other.(type) {
	case *arrayContainer:
		return c.orArray(o)
	case *bitmapContainer:
		return o.orArray(c)
	}
	return c
}

func (c *arrayContainer) orArray(o *arrayContainer) roaringContainer {
	result := make([]uint16, 0, len(c.values)+len(o.values))
	i, j := 0, 0
	for i < len(c.values) && j < len(o.values) {
		if c.values[i] == o.values[j] {
			result = append(result, c.values[i])
			i++
			j++
		} else if c.values[i] < o.values[j] {
			result = append(result, c.values[i])
			i++
		} else {
			result = append(result, o.values[j])
			j++
		}
	}
	for ; i < len(c.values); i++ {
		result = append(result, c.values[i])
	}
	for ; j < len(o.values); j++ {
		result = append(result, o.values[j])
	}
	ac := newArrayContainerWithValues(result)
	if len(result) >= arrayMaxSize {
		return ac.toBitmapContainer()
	}
	return ac
}

func (c *arrayContainer) encode(buf []byte) []byte {
	buf = append(buf, 0) // type: array
	buf = appendVarint(buf, uint64(len(c.values)))
	for _, v := range c.values {
		buf = append(buf, byte(v), byte(v>>8))
	}
	return buf
}

// --- Bitmap Container ---

type bitmapContainer struct {
	bitmap [1024]uint64 // 65536 bits
	card   int          // cached cardinality
}

func (c *bitmapContainer) add(val uint16) roaringContainer {
	idx := val / 64
	bit := uint64(1) << (val % 64)
	if c.bitmap[idx]&bit == 0 {
		c.bitmap[idx] |= bit
		c.card++
	}
	return c
}

func (c *bitmapContainer) remove(val uint16) roaringContainer {
	idx := val / 64
	bit := uint64(1) << (val % 64)
	if c.bitmap[idx]&bit != 0 {
		c.bitmap[idx] &^= bit
		c.card--
	}
	if c.card < arrayMaxSize {
		return c.toArrayContainer()
	}
	return c
}

func (c *bitmapContainer) contains(val uint16) bool {
	return c.bitmap[val/64]&(1<<(val%64)) != 0
}

func (c *bitmapContainer) cardinality() int {
	return c.card
}

func (c *bitmapContainer) toSortedSlice() []uint16 {
	if c.card == 0 {
		return nil
	}
	result := make([]uint16, 0, c.card)
	for i := 0; i < 1024; i++ {
		w := c.bitmap[i]
		for w != 0 {
			t := w & (-w) // lowest set bit
			result = append(result, uint16(i*64+bits.TrailingZeros64(t)))
			w ^= t
		}
	}
	return result
}

func (c *bitmapContainer) toArrayContainer() *arrayContainer {
	return newArrayContainerWithValues(c.toSortedSlice())
}

func (c *bitmapContainer) and(other roaringContainer) roaringContainer {
	switch o := other.(type) {
	case *arrayContainer:
		return o.andBitmap(c)
	case *bitmapContainer:
		return c.andBitmap(o)
	}
	return newArrayContainer()
}

func (c *bitmapContainer) andBitmap(o *bitmapContainer) roaringContainer {
	result := &bitmapContainer{}
	for i := 0; i < 1024; i++ {
		result.bitmap[i] = c.bitmap[i] & o.bitmap[i]
		result.card += bits.OnesCount64(result.bitmap[i])
	}
	if result.card < arrayMaxSize {
		return result.toArrayContainer()
	}
	return result
}

func (c *bitmapContainer) or(other roaringContainer) roaringContainer {
	switch o := other.(type) {
	case *arrayContainer:
		return c.orArray(o)
	case *bitmapContainer:
		return c.orBitmap(o)
	}
	return c
}

func (c *bitmapContainer) orArray(o *arrayContainer) roaringContainer {
	result := &bitmapContainer{card: c.card}
	copy(result.bitmap[:], c.bitmap[:])
	for _, v := range o.values {
		idx := v / 64
		bit := uint64(1) << (v % 64)
		if result.bitmap[idx]&bit == 0 {
			result.bitmap[idx] |= bit
			result.card++
		}
	}
	return result
}

func (c *bitmapContainer) orBitmap(o *bitmapContainer) roaringContainer {
	result := &bitmapContainer{}
	for i := 0; i < 1024; i++ {
		result.bitmap[i] = c.bitmap[i] | o.bitmap[i]
		result.card += bits.OnesCount64(result.bitmap[i])
	}
	return result
}

func (c *bitmapContainer) encode(buf []byte) []byte {
	buf = append(buf, 1) // type: bitmap
	buf = appendVarint(buf, uint64(c.card))
	var tmp [8]byte
	for i := 0; i < 1024; i++ {
		binary.LittleEndian.PutUint64(tmp[:], c.bitmap[i])
		buf = append(buf, tmp[:]...)
	}
	return buf
}

// --- Roaring Bitmap ---

// RoaringBitmap is a compressed bitmap data structure that stores a set of uint32 values.
// Values are partitioned by their high 16 bits into containers.
// Each container uses either an array (for sparse data) or a bitmap (for dense data).
type RoaringBitmap struct {
	keys       []uint16
	containers []roaringContainer
}

func NewRoaringBitmap() *RoaringBitmap {
	return &RoaringBitmap{}
}

func (rb *RoaringBitmap) findContainer(key uint16) int {
	return sort.Search(len(rb.keys), func(i int) bool { return rb.keys[i] >= key })
}

func (rb *RoaringBitmap) Add(val uint32) {
	hi := uint16(val >> 16)
	lo := uint16(val)
	pos := rb.findContainer(hi)
	if pos < len(rb.keys) && rb.keys[pos] == hi {
		rb.containers[pos] = rb.containers[pos].add(lo)
	} else {
		// Insert new container
		rb.keys = append(rb.keys, 0)
		rb.containers = append(rb.containers, nil)
		copy(rb.keys[pos+1:], rb.keys[pos:])
		copy(rb.containers[pos+1:], rb.containers[pos:])
		rb.keys[pos] = hi
		c := newArrayContainer()
		rb.containers[pos] = c.add(lo)
	}
}

func (rb *RoaringBitmap) Remove(val uint32) {
	hi := uint16(val >> 16)
	lo := uint16(val)
	pos := rb.findContainer(hi)
	if pos >= len(rb.keys) || rb.keys[pos] != hi {
		return
	}
	rb.containers[pos] = rb.containers[pos].remove(lo)
	if rb.containers[pos].cardinality() == 0 {
		rb.keys = append(rb.keys[:pos], rb.keys[pos+1:]...)
		rb.containers = append(rb.containers[:pos], rb.containers[pos+1:]...)
	}
}

func (rb *RoaringBitmap) Contains(val uint32) bool {
	hi := uint16(val >> 16)
	lo := uint16(val)
	pos := rb.findContainer(hi)
	if pos >= len(rb.keys) || rb.keys[pos] != hi {
		return false
	}
	return rb.containers[pos].contains(lo)
}

func (rb *RoaringBitmap) Cardinality() int {
	total := 0
	for _, c := range rb.containers {
		total += c.cardinality()
	}
	return total
}

func (rb *RoaringBitmap) ToSortedSlice() []uint32 {
	total := rb.Cardinality()
	if total == 0 {
		return nil
	}
	result := make([]uint32, 0, total)
	for i, key := range rb.keys {
		base := uint32(key) << 16
		for _, lo := range rb.containers[i].toSortedSlice() {
			result = append(result, base|uint32(lo))
		}
	}
	return result
}

// And returns a new RoaringBitmap that is the intersection of rb and other.
func (rb *RoaringBitmap) And(other *RoaringBitmap) *RoaringBitmap {
	result := NewRoaringBitmap()
	i, j := 0, 0
	for i < len(rb.keys) && j < len(other.keys) {
		if rb.keys[i] == other.keys[j] {
			c := rb.containers[i].and(other.containers[j])
			if c.cardinality() > 0 {
				result.keys = append(result.keys, rb.keys[i])
				result.containers = append(result.containers, c)
			}
			i++
			j++
		} else if rb.keys[i] < other.keys[j] {
			i++
		} else {
			j++
		}
	}
	return result
}

// Or returns a new RoaringBitmap that is the union of rb and other.
func (rb *RoaringBitmap) Or(other *RoaringBitmap) *RoaringBitmap {
	result := NewRoaringBitmap()
	i, j := 0, 0
	for i < len(rb.keys) && j < len(other.keys) {
		if rb.keys[i] == other.keys[j] {
			c := rb.containers[i].or(other.containers[j])
			result.keys = append(result.keys, rb.keys[i])
			result.containers = append(result.containers, c)
			i++
			j++
		} else if rb.keys[i] < other.keys[j] {
			result.keys = append(result.keys, rb.keys[i])
			result.containers = append(result.containers, rb.containers[i])
			i++
		} else {
			result.keys = append(result.keys, other.keys[j])
			result.containers = append(result.containers, other.containers[j])
			j++
		}
	}
	for ; i < len(rb.keys); i++ {
		result.keys = append(result.keys, rb.keys[i])
		result.containers = append(result.containers, rb.containers[i])
	}
	for ; j < len(other.keys); j++ {
		result.keys = append(result.keys, other.keys[j])
		result.containers = append(result.containers, other.containers[j])
	}
	return result
}

// Encode serializes the RoaringBitmap to bytes.
//
// Format:
//
//	varint(numContainers) ||
//	[uint16(key) || byte(containerType) || varint(cardinality) || containerData] * numContainers
func (rb *RoaringBitmap) Encode() []byte {
	buf := make([]byte, 0, 64)
	buf = appendVarint(buf, uint64(len(rb.keys)))
	for i, key := range rb.keys {
		buf = append(buf, byte(key), byte(key>>8))
		buf = rb.containers[i].encode(buf)
	}
	return buf
}

// DecodeRoaringBitmap deserializes a RoaringBitmap from bytes.
func DecodeRoaringBitmap(data []byte) *RoaringBitmap {
	if len(data) == 0 {
		return NewRoaringBitmap()
	}
	numContainers, pos := readVarint(data, 0)
	rb := &RoaringBitmap{
		keys:       make([]uint16, 0, numContainers),
		containers: make([]roaringContainer, 0, numContainers),
	}
	for i := uint64(0); i < numContainers; i++ {
		if pos+2 > len(data) {
			break
		}
		key := uint16(data[pos]) | uint16(data[pos+1])<<8
		pos += 2
		containerType := data[pos]
		pos++
		card, newPos := readVarint(data, pos)
		pos = newPos

		switch containerType {
		case 0: // array
			vals := make([]uint16, card)
			for j := uint64(0); j < card; j++ {
				vals[j] = uint16(data[pos]) | uint16(data[pos+1])<<8
				pos += 2
			}
			rb.keys = append(rb.keys, key)
			rb.containers = append(rb.containers, newArrayContainerWithValues(vals))
		case 1: // bitmap
			bc := &bitmapContainer{card: int(card)}
			for j := 0; j < 1024; j++ {
				bc.bitmap[j] = binary.LittleEndian.Uint64(data[pos:])
				pos += 8
			}
			rb.keys = append(rb.keys, key)
			rb.containers = append(rb.containers, bc)
		}
	}
	return rb
}

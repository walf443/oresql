package storage

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoaringAddAndContains(t *testing.T) {
	rb := NewRoaringBitmap()
	rb.Add(1)
	rb.Add(100)
	rb.Add(70000) // different chunk (high16 = 1)

	assert.True(t, rb.Contains(1))
	assert.True(t, rb.Contains(100))
	assert.True(t, rb.Contains(70000))
	assert.False(t, rb.Contains(2))
	assert.False(t, rb.Contains(0))
	assert.Equal(t, 3, rb.Cardinality())
}

func TestRoaringRemove(t *testing.T) {
	rb := NewRoaringBitmap()
	rb.Add(1)
	rb.Add(2)
	rb.Add(3)
	rb.Remove(2)

	assert.True(t, rb.Contains(1))
	assert.False(t, rb.Contains(2))
	assert.True(t, rb.Contains(3))
	assert.Equal(t, 2, rb.Cardinality())

	// Remove non-existent
	rb.Remove(999)
	assert.Equal(t, 2, rb.Cardinality())
}

func TestRoaringToSortedSlice(t *testing.T) {
	rb := NewRoaringBitmap()
	vals := []uint32{70000, 1, 200, 65536, 100}
	for _, v := range vals {
		rb.Add(v)
	}
	result := rb.ToSortedSlice()
	assert.Equal(t, []uint32{1, 100, 200, 65536, 70000}, result)
}

func TestRoaringEmpty(t *testing.T) {
	rb := NewRoaringBitmap()
	assert.Equal(t, 0, rb.Cardinality())
	assert.Nil(t, rb.ToSortedSlice())
	assert.False(t, rb.Contains(0))
}

func TestRoaringArrayToBitmapUpgrade(t *testing.T) {
	rb := NewRoaringBitmap()
	// Add 4096 values in the same chunk → should upgrade to bitmap container
	for i := uint32(0); i < 4096; i++ {
		rb.Add(i)
	}
	assert.Equal(t, 4096, rb.Cardinality())
	assert.True(t, rb.Contains(0))
	assert.True(t, rb.Contains(4095))
	assert.False(t, rb.Contains(4096))

	// Verify all values present
	result := rb.ToSortedSlice()
	assert.Len(t, result, 4096)
	for i := uint32(0); i < 4096; i++ {
		assert.Equal(t, i, result[i])
	}
}

func TestRoaringBitmapToArrayDowngrade(t *testing.T) {
	rb := NewRoaringBitmap()
	// Add 4096 to get bitmap, then remove one to downgrade to array
	for i := uint32(0); i < 4096; i++ {
		rb.Add(i)
	}
	rb.Remove(0)
	assert.Equal(t, 4095, rb.Cardinality())
	assert.False(t, rb.Contains(0))
	assert.True(t, rb.Contains(1))
	assert.True(t, rb.Contains(4095))
}

func TestRoaringAndIntersection(t *testing.T) {
	tests := []struct {
		name     string
		a        []uint32
		b        []uint32
		expected []uint32
	}{
		{"both empty", nil, nil, nil},
		{"one empty", []uint32{1, 2, 3}, nil, nil},
		{"no overlap", []uint32{1, 2, 3}, []uint32{4, 5, 6}, nil},
		{"full overlap", []uint32{1, 2, 3}, []uint32{1, 2, 3}, []uint32{1, 2, 3}},
		{"partial overlap", []uint32{1, 5, 10, 15}, []uint32{5, 10, 20}, []uint32{5, 10}},
		{"cross chunk", []uint32{1, 70000}, []uint32{70000, 80000}, []uint32{70000}},
		{"single element", []uint32{42}, []uint32{42}, []uint32{42}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewRoaringBitmap()
			for _, v := range tt.a {
				a.Add(v)
			}
			b := NewRoaringBitmap()
			for _, v := range tt.b {
				b.Add(v)
			}
			result := a.And(b)
			got := result.ToSortedSlice()
			if tt.expected == nil {
				assert.Nil(t, got)
			} else {
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestRoaringOrUnion(t *testing.T) {
	tests := []struct {
		name     string
		a        []uint32
		b        []uint32
		expected []uint32
	}{
		{"both empty", nil, nil, nil},
		{"one empty", []uint32{1, 2, 3}, nil, []uint32{1, 2, 3}},
		{"no overlap", []uint32{1, 2}, []uint32{3, 4}, []uint32{1, 2, 3, 4}},
		{"full overlap", []uint32{1, 2, 3}, []uint32{1, 2, 3}, []uint32{1, 2, 3}},
		{"partial overlap", []uint32{1, 5, 10}, []uint32{5, 20}, []uint32{1, 5, 10, 20}},
		{"cross chunk", []uint32{1, 70000}, []uint32{2, 80000}, []uint32{1, 2, 70000, 80000}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewRoaringBitmap()
			for _, v := range tt.a {
				a.Add(v)
			}
			b := NewRoaringBitmap()
			for _, v := range tt.b {
				b.Add(v)
			}
			result := a.Or(b)
			got := result.ToSortedSlice()
			if tt.expected == nil {
				assert.Nil(t, got)
			} else {
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestRoaringEncodeDecode(t *testing.T) {
	tests := []struct {
		name string
		vals []uint32
	}{
		{"empty", nil},
		{"single", []uint32{42}},
		{"small", []uint32{1, 5, 10, 100, 1000}},
		{"cross chunk", []uint32{1, 65536, 70000, 131072}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb := NewRoaringBitmap()
			for _, v := range tt.vals {
				rb.Add(v)
			}
			encoded := rb.Encode()
			decoded := DecodeRoaringBitmap(encoded)

			assert.Equal(t, rb.Cardinality(), decoded.Cardinality())
			assert.Equal(t, rb.ToSortedSlice(), decoded.ToSortedSlice())
		})
	}
}

func TestRoaringEncodeDecodeLarge(t *testing.T) {
	rb := NewRoaringBitmap()
	// Add 100K values spanning multiple chunks
	for i := uint32(0); i < 100000; i++ {
		rb.Add(i * 3) // 0, 3, 6, ..., 299997
	}
	assert.Equal(t, 100000, rb.Cardinality())

	encoded := rb.Encode()
	decoded := DecodeRoaringBitmap(encoded)
	assert.Equal(t, 100000, decoded.Cardinality())
	assert.Equal(t, rb.ToSortedSlice(), decoded.ToSortedSlice())
}

func TestRoaringAndWithBitmapContainers(t *testing.T) {
	// Both bitmaps have dense data in the same chunk → bitmap AND bitmap
	a := NewRoaringBitmap()
	b := NewRoaringBitmap()
	for i := uint32(0); i < 5000; i++ {
		a.Add(i * 2)   // even: 0, 2, 4, ..., 9998
		b.Add(i*2 + 1) // odd: 1, 3, 5, ..., 9999
	}
	// No overlap
	result := a.And(b)
	assert.Equal(t, 0, result.Cardinality())

	// Add some shared values
	for i := uint32(0); i < 100; i++ {
		b.Add(i * 2) // add evens to b
	}
	result = a.And(b)
	assert.Equal(t, 100, result.Cardinality())
}

func TestRoaringOrWithBitmapContainers(t *testing.T) {
	a := NewRoaringBitmap()
	b := NewRoaringBitmap()
	for i := uint32(0); i < 5000; i++ {
		a.Add(i * 2)   // even: 0, 2, ..., 9998
		b.Add(i*2 + 1) // odd: 1, 3, ..., 9999
	}
	result := a.Or(b)
	assert.Equal(t, 10000, result.Cardinality())
	// Should be 0, 1, 2, 3, ..., 9999
	slice := result.ToSortedSlice()
	require.Len(t, slice, 10000)
	for i := uint32(0); i < 10000; i++ {
		assert.Equal(t, i, slice[i])
	}
}

func TestRoaringEncodeDecodeWithBitmapContainer(t *testing.T) {
	rb := NewRoaringBitmap()
	// 5000 values in chunk 0 → bitmap container
	for i := uint32(0); i < 5000; i++ {
		rb.Add(i)
	}
	// 100 values in chunk 1 → array container
	for i := uint32(65536); i < 65636; i++ {
		rb.Add(i)
	}
	assert.Equal(t, 5100, rb.Cardinality())

	encoded := rb.Encode()
	decoded := DecodeRoaringBitmap(encoded)
	assert.Equal(t, 5100, decoded.Cardinality())
	assert.Equal(t, rb.ToSortedSlice(), decoded.ToSortedSlice())
}

func TestRoaringDuplicateAdd(t *testing.T) {
	rb := NewRoaringBitmap()
	rb.Add(42)
	rb.Add(42)
	rb.Add(42)
	assert.Equal(t, 1, rb.Cardinality())
}

func TestRoaringLargeRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	a := NewRoaringBitmap()
	b := NewRoaringBitmap()
	aSet := make(map[uint32]struct{})
	bSet := make(map[uint32]struct{})

	for i := 0; i < 10000; i++ {
		v := rng.Uint32() % 200000
		a.Add(v)
		aSet[v] = struct{}{}
	}
	for i := 0; i < 10000; i++ {
		v := rng.Uint32() % 200000
		b.Add(v)
		bSet[v] = struct{}{}
	}

	// Verify And
	andResult := a.And(b)
	var expectedAnd []uint32
	for v := range aSet {
		if _, ok := bSet[v]; ok {
			expectedAnd = append(expectedAnd, v)
		}
	}
	sort.Slice(expectedAnd, func(i, j int) bool { return expectedAnd[i] < expectedAnd[j] })
	got := andResult.ToSortedSlice()
	if len(expectedAnd) == 0 {
		assert.Empty(t, got)
	} else {
		assert.Equal(t, expectedAnd, got)
	}

	// Verify Or
	orResult := a.Or(b)
	expectedOrSet := make(map[uint32]struct{})
	for v := range aSet {
		expectedOrSet[v] = struct{}{}
	}
	for v := range bSet {
		expectedOrSet[v] = struct{}{}
	}
	var expectedOr []uint32
	for v := range expectedOrSet {
		expectedOr = append(expectedOr, v)
	}
	sort.Slice(expectedOr, func(i, j int) bool { return expectedOr[i] < expectedOr[j] })
	assert.Equal(t, expectedOr, orResult.ToSortedSlice())
}

func TestRoaringToInt64Slice(t *testing.T) {
	rb := NewRoaringBitmap()
	rb.Add(1)
	rb.Add(100)
	rb.Add(70000)
	result := rb.ToInt64Slice()
	assert.Equal(t, []int64{1, 100, 70000}, result)
}

func TestRoaringFromInt64Slice(t *testing.T) {
	keys := []int64{1, 5, 10, 100}
	rb := RoaringFromInt64Slice(keys)
	assert.Equal(t, 4, rb.Cardinality())
	assert.Equal(t, keys, rb.ToInt64Slice())
}

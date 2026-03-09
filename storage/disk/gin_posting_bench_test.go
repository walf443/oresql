package disk

import (
	"testing"
)

// generatePostingList creates a sorted posting list with n keys, starting at
// start and incrementing by step.
func generatePostingList(n int, start, step int64) []int64 {
	keys := make([]int64, n)
	for i := range keys {
		keys[i] = start + int64(i)*step
	}
	return keys
}

// BenchmarkDecodePostingList measures full decode performance at various sizes.
func BenchmarkDecodePostingList(b *testing.B) {
	sizes := []struct {
		name string
		n    int
	}{
		{"100", 100},
		{"1000", 1000},
		{"10000", 10000},
		{"100000", 100000},
	}
	for _, sz := range sizes {
		encoded := encodePostingList(generatePostingList(sz.n, 1, 3))
		b.Run(sz.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				decodePostingList(encoded)
			}
		})
	}
}

// BenchmarkEncodePostingList measures encode performance at various sizes.
func BenchmarkEncodePostingList(b *testing.B) {
	sizes := []struct {
		name string
		n    int
	}{
		{"100", 100},
		{"1000", 1000},
		{"10000", 10000},
	}
	for _, sz := range sizes {
		keys := generatePostingList(sz.n, 1, 3)
		b.Run(sz.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				encodePostingList(keys)
			}
		})
	}
}

// BenchmarkRoaringIntersect measures Roaring Bitmap intersection performance.
func BenchmarkRoaringIntersect(b *testing.B) {
	benchmarks := []struct {
		name string
		a    []int64
		bk   []int64
	}{
		{
			"Small_100x100",
			generatePostingList(100, 1, 2),
			generatePostingList(100, 1, 3),
		},
		{
			"Large_100Kx100K_Overlap",
			generatePostingList(100000, 1, 2),
			generatePostingList(100000, 1, 3),
		},
		{
			"Large_100Kx1K_Overlap",
			generatePostingList(100000, 1, 2),
			generatePostingList(1000, 1, 3),
		},
		{
			"Large_100Kx100K_NoOverlap",
			generatePostingList(100000, 1, 2),
			generatePostingList(100000, 300000, 2),
		},
	}

	for _, bm := range benchmarks {
		rbA := NewRoaringBitmap()
		for _, k := range bm.a {
			rbA.Add(uint32(k))
		}
		rbB := NewRoaringBitmap()
		for _, k := range bm.bk {
			rbB.Add(uint32(k))
		}

		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				rbA.And(rbB)
			}
		})
	}
}

// BenchmarkRoaringUnion measures Roaring Bitmap union performance.
func BenchmarkRoaringUnion(b *testing.B) {
	benchmarks := []struct {
		name string
		a    []int64
		bk   []int64
	}{
		{
			"Small_100x100",
			generatePostingList(100, 1, 2),
			generatePostingList(100, 1, 3),
		},
		{
			"Large_100Kx100K_Overlap",
			generatePostingList(100000, 1, 2),
			generatePostingList(100000, 1, 3),
		},
	}

	for _, bm := range benchmarks {
		rbA := NewRoaringBitmap()
		for _, k := range bm.a {
			rbA.Add(uint32(k))
		}
		rbB := NewRoaringBitmap()
		for _, k := range bm.bk {
			rbB.Add(uint32(k))
		}

		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				rbA.Or(rbB)
			}
		})
	}
}

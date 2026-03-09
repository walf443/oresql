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

// BenchmarkDecodeBlock measures decoding a single block (last block) vs full decode.
func BenchmarkDecodeBlock(b *testing.B) {
	sizes := []struct {
		name string
		n    int
	}{
		{"1000", 1000},
		{"10000", 10000},
		{"100000", 100000},
	}
	for _, sz := range sizes {
		encoded := encodePostingList(generatePostingList(sz.n, 1, 3))
		bpl := parseBlockedPostingList(encoded)
		lastBlock := len(bpl.headers) - 1

		b.Run(sz.name+"/FullDecode", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				decodePostingList(encoded)
			}
		})
		b.Run(sz.name+"/SingleBlock", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				bpl.decodeBlock(lastBlock)
			}
		})
	}
}

// BenchmarkIntersectPostingLists compares blocked intersection vs naive (decode+map) intersection.
func BenchmarkIntersectPostingLists(b *testing.B) {
	benchmarks := []struct {
		name string
		a    []int64
		bk   []int64
	}{
		{
			// Both small, overlapping — baseline
			"Small_100x100",
			generatePostingList(100, 1, 2),  // 1,3,5,...,199
			generatePostingList(100, 1, 3),  // 1,4,7,...,298
		},
		{
			// Large lists, overlapping ranges
			"Large_100Kx100K_Overlap",
			generatePostingList(100000, 1, 2),  // 1,3,5,...,199999
			generatePostingList(100000, 1, 3),  // 1,4,7,...,299999
		},
		{
			// Large vs small, overlapping
			"Large_100Kx1K_Overlap",
			generatePostingList(100000, 1, 2),
			generatePostingList(1000, 1, 3),
		},
		{
			// Non-overlapping ranges — blocks should be skipped entirely
			"Large_100Kx100K_NoOverlap",
			generatePostingList(100000, 1, 2),          // 1..199999
			generatePostingList(100000, 300000, 2),      // 300000..499998
		},
		{
			// Partial overlap — first half of A overlaps with B
			"Large_100Kx100K_PartialOverlap",
			generatePostingList(100000, 1, 2),           // 1..199999
			generatePostingList(100000, 100000, 2),      // 100000..299998
		},
	}

	for _, bm := range benchmarks {
		encA := encodePostingList(bm.a)
		encB := encodePostingList(bm.bk)

		b.Run(bm.name+"/Blocked", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				intersectBlockedPostingLists(encA, encB)
			}
		})
		b.Run(bm.name+"/Naive", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				naiveIntersect(encA, encB)
			}
		})
	}
}

// naiveIntersect decodes both posting lists fully and uses a map for intersection.
// This simulates the old matchBigram approach.
func naiveIntersect(dataA, dataB []byte) []int64 {
	a := decodePostingList(dataA)
	b := decodePostingList(dataB)
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	set := make(map[int64]struct{}, len(b))
	for _, k := range b {
		set[k] = struct{}{}
	}
	var result []int64
	for _, k := range a {
		if _, ok := set[k]; ok {
			result = append(result, k)
		}
	}
	return result
}

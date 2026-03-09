package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLongestLiteralSegment(t *testing.T) {
	tests := []struct {
		pattern string
		want    string
	}{
		// No wildcards
		{"東京都", "東京都"},
		{"hello", "hello"},

		// Prefix pattern
		{"東京%", "東京"},
		{"hello%", "hello"},

		// Suffix pattern
		{"%タワー", "タワー"},
		{"%world", "world"},

		// Contains pattern
		{"%東京%", "東京"},
		{"%hello%", "hello"},

		// Multiple segments — longest wins
		{"%東京%スカイツリー%", "スカイツリー"},
		{"%ab%cdef%gh%", "cdef"},

		// Underscore wildcard splits segments
		{"東_京都", "京都"},
		{"a_bcde_f", "bcde"},

		// Mixed wildcards
		{"%東京_タワー%", "タワー"},

		// Single character segments only
		{"%京%", "京"},
		{"%a%", "a"},

		// All wildcards
		{"%%%", ""},
		{"%_%", ""},

		// Empty pattern
		{"", ""},

		// Single character
		{"a", "a"},
		{"東", "東"},
	}
	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			got := longestLiteralSegment(tt.pattern)
			assert.Equal(t, tt.want, got)
		})
	}
}

package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- EncodeValue tests ---

func TestEncodeValueNil(t *testing.T) {
	var buf strings.Builder
	EncodeValue(&buf, nil)
	encoded := buf.String()
	assert.Equal(t, 1, len(encoded))
	assert.Equal(t, byte(0x00), encoded[0])
}

func TestEncodeValueInt64(t *testing.T) {
	tests := []struct {
		name  string
		value int64
	}{
		{"zero", 0},
		{"positive", 42},
		{"negative", -42},
		{"max", int64(^uint64(0) >> 1)},    // math.MaxInt64
		{"min", -int64(^uint64(0)>>1) - 1}, // math.MinInt64
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf strings.Builder
			EncodeValue(&buf, tt.value)
			encoded := buf.String()
			assert.Equal(t, 9, len(encoded), "INT encoding should be 1 prefix + 8 bytes")
			assert.Equal(t, byte(0x01), encoded[0], "INT prefix should be 0x01")
		})
	}
}

func TestEncodeValueFloat64(t *testing.T) {
	tests := []struct {
		name  string
		value float64
	}{
		{"zero", 0.0},
		{"positive", 3.14},
		{"negative", -2.71},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf strings.Builder
			EncodeValue(&buf, tt.value)
			encoded := buf.String()
			assert.Equal(t, 9, len(encoded), "FLOAT encoding should be 1 prefix + 8 bytes")
			assert.Equal(t, byte(0x02), encoded[0], "FLOAT prefix should be 0x02")
		})
	}
}

func TestEncodeValueString(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"empty", ""},
		{"hello", "hello"},
		{"unicode", "日本語"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf strings.Builder
			EncodeValue(&buf, tt.value)
			encoded := buf.String()
			// 1 prefix + len(string) + 1 null terminator
			assert.Equal(t, 1+len(tt.value)+1, len(encoded))
			assert.Equal(t, byte(0x03), encoded[0], "TEXT prefix should be 0x03")
			assert.Equal(t, byte(0x00), encoded[len(encoded)-1], "TEXT should end with null terminator")
		})
	}
}

// --- EncodeValues tests ---

func TestEncodeValues(t *testing.T) {
	t.Run("single value", func(t *testing.T) {
		enc := EncodeValues([]Value{int64(1)})
		var buf strings.Builder
		EncodeValue(&buf, int64(1))
		assert.Equal(t, KeyEncoding(buf.String()), enc)
	})

	t.Run("multiple values", func(t *testing.T) {
		enc := EncodeValues([]Value{int64(1), "hello"})
		var buf strings.Builder
		EncodeValue(&buf, int64(1))
		EncodeValue(&buf, "hello")
		assert.Equal(t, KeyEncoding(buf.String()), enc)
	})

	t.Run("empty", func(t *testing.T) {
		enc := EncodeValues([]Value{})
		assert.Equal(t, KeyEncoding(""), enc)
	})
}

// --- Sort order tests ---

func TestSortOrderInt(t *testing.T) {
	values := []int64{-100, -1, 0, 1, 100}
	encodings := make([]KeyEncoding, len(values))
	for i, v := range values {
		encodings[i] = EncodeValues([]Value{v})
	}
	for i := 0; i < len(encodings)-1; i++ {
		assert.True(t, encodings[i] < encodings[i+1],
			"expected encode(%d) < encode(%d)", values[i], values[i+1])
	}
}

func TestSortOrderFloat(t *testing.T) {
	values := []float64{-100.5, -1.0, -0.1, 0.0, 0.1, 1.0, 100.5}
	encodings := make([]KeyEncoding, len(values))
	for i, v := range values {
		encodings[i] = EncodeValues([]Value{v})
	}
	for i := 0; i < len(encodings)-1; i++ {
		assert.True(t, encodings[i] < encodings[i+1],
			"expected encode(%f) < encode(%f)", values[i], values[i+1])
	}
}

func TestSortOrderString(t *testing.T) {
	values := []string{"apple", "banana", "cherry"}
	encodings := make([]KeyEncoding, len(values))
	for i, v := range values {
		encodings[i] = EncodeValues([]Value{v})
	}
	for i := 0; i < len(encodings)-1; i++ {
		assert.True(t, encodings[i] < encodings[i+1],
			"expected encode(%q) < encode(%q)", values[i], values[i+1])
	}
}

func TestSortOrderCrossType(t *testing.T) {
	// NULL < INT < FLOAT < TEXT
	nullEnc := EncodeValues([]Value{nil})
	intEnc := EncodeValues([]Value{int64(0)})
	floatEnc := EncodeValues([]Value{float64(0)})
	textEnc := EncodeValues([]Value{""})

	assert.True(t, nullEnc < intEnc, "NULL should sort before INT")
	assert.True(t, intEnc < floatEnc, "INT should sort before FLOAT")
	assert.True(t, floatEnc < textEnc, "FLOAT should sort before TEXT")
}

// --- TableInfo.FindColumn tests ---

func TestFindColumn(t *testing.T) {
	info := &TableInfo{
		Name: "users",
		Columns: []ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
			{Name: "name", DataType: "TEXT", Index: 1},
			{Name: "Age", DataType: "INT", Index: 2},
		},
	}

	t.Run("exact match", func(t *testing.T) {
		col, err := info.FindColumn("id")
		require.NoError(t, err)
		assert.Equal(t, "id", col.Name)
		assert.Equal(t, 0, col.Index)
	})

	t.Run("case insensitive", func(t *testing.T) {
		col, err := info.FindColumn("NAME")
		require.NoError(t, err)
		assert.Equal(t, "name", col.Name)

		col, err = info.FindColumn("age")
		require.NoError(t, err)
		assert.Equal(t, "Age", col.Name)
	})

	t.Run("not found", func(t *testing.T) {
		_, err := info.FindColumn("nonexistent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

// --- EncodeRow / DecodeRow tests ---

func TestEncodeDecodeRowRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		row  Row
	}{
		{"empty row", Row{}},
		{"null only", Row{nil}},
		{"int only", Row{int64(42)}},
		{"float only", Row{float64(3.14)}},
		{"text only", Row{"hello"}},
		{"mixed types", Row{int64(1), "alice", nil, float64(2.5)}},
		{"negative int", Row{int64(-100)}},
		{"empty string", Row{""}},
		{"unicode text", Row{"日本語テスト"}},
		{"multiple nulls", Row{nil, nil, nil}},
		{"all types", Row{nil, int64(0), float64(0.0), ""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeRow(tt.row)
			decoded, err := DecodeRow(encoded)
			require.NoError(t, err)
			require.Len(t, decoded, len(tt.row))
			for i := range tt.row {
				assert.Equal(t, tt.row[i], decoded[i], "mismatch at index %d", i)
			}
		})
	}
}

func TestDecodeRowError(t *testing.T) {
	t.Run("truncated INT", func(t *testing.T) {
		_, err := DecodeRow([]byte{0x01, 0x00, 0x00}) // INT needs 8 bytes
		assert.Error(t, err)
	})

	t.Run("truncated FLOAT", func(t *testing.T) {
		_, err := DecodeRow([]byte{0x02, 0x00}) // FLOAT needs 8 bytes
		assert.Error(t, err)
	})

	t.Run("truncated TEXT length", func(t *testing.T) {
		_, err := DecodeRow([]byte{0x03, 0x00}) // TEXT needs 4 bytes for length
		assert.Error(t, err)
	})

	t.Run("truncated TEXT data", func(t *testing.T) {
		_, err := DecodeRow([]byte{0x03, 0x00, 0x00, 0x00, 0x05, 'h', 'i'}) // claims 5 bytes, only 2
		assert.Error(t, err)
	})

	t.Run("unknown tag", func(t *testing.T) {
		_, err := DecodeRow([]byte{0xFF})
		assert.Error(t, err)
	})
}

package jsonb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeNull(t *testing.T) {
	b, err := Encode(nil)
	require.NoError(t, err)
	assert.Equal(t, byte(TagNull), b[0])

	val, err := Decode(b)
	require.NoError(t, err)
	assert.Nil(t, val)
}

func TestEncodeDecodeBool(t *testing.T) {
	for _, v := range []bool{true, false} {
		b, err := Encode(v)
		require.NoError(t, err)
		assert.Equal(t, byte(TagBool), b[0])

		val, err := Decode(b)
		require.NoError(t, err)
		assert.Equal(t, v, val)
	}
}

func TestEncodeDecodeInt(t *testing.T) {
	for _, v := range []int64{0, 1, -1, 42, -100, 1<<31 - 1, -(1 << 31)} {
		b, err := Encode(v)
		require.NoError(t, err)
		assert.Equal(t, byte(TagInt), b[0])

		val, err := Decode(b)
		require.NoError(t, err)
		assert.Equal(t, v, val)
	}
}

func TestEncodeDecodeFloat(t *testing.T) {
	for _, v := range []float64{0.0, 3.14, -2.718, 1e100} {
		b, err := Encode(v)
		require.NoError(t, err)
		assert.Equal(t, byte(TagFloat), b[0])

		val, err := Decode(b)
		require.NoError(t, err)
		assert.Equal(t, v, val)
	}
}

func TestEncodeDecodeString(t *testing.T) {
	for _, v := range []string{"", "hello", "日本語", "a longer string with spaces"} {
		b, err := Encode(v)
		require.NoError(t, err)
		assert.Equal(t, byte(TagString), b[0])

		val, err := Decode(b)
		require.NoError(t, err)
		assert.Equal(t, v, val)
	}
}

func TestEncodeDecodeArray(t *testing.T) {
	arr := []any{float64(1), "hello", true, nil}
	b, err := Encode(arr)
	require.NoError(t, err)
	assert.Equal(t, byte(TagArray), b[0])

	val, err := Decode(b)
	require.NoError(t, err)
	decoded, ok := val.([]any)
	require.True(t, ok)
	require.Len(t, decoded, 4)
	assert.Equal(t, float64(1), decoded[0])
	assert.Equal(t, "hello", decoded[1])
	assert.Equal(t, true, decoded[2])
	assert.Nil(t, decoded[3])
}

func TestEncodeDecodeObject(t *testing.T) {
	obj := map[string]any{
		"name": "alice",
		"age":  float64(30),
	}
	b, err := Encode(obj)
	require.NoError(t, err)
	assert.Equal(t, byte(TagObject), b[0])

	val, err := Decode(b)
	require.NoError(t, err)
	decoded, ok := val.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "alice", decoded["name"])
	assert.Equal(t, float64(30), decoded["age"])
}

func TestObjectKeysAreSorted(t *testing.T) {
	obj := map[string]any{
		"zebra": float64(1),
		"apple": float64(2),
		"mango": float64(3),
	}
	b, err := Encode(obj)
	require.NoError(t, err)

	// Decode and re-check values - keys should be accessible regardless of input order
	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.(map[string]any)
	assert.Equal(t, float64(1), decoded["zebra"])
	assert.Equal(t, float64(2), decoded["apple"])
	assert.Equal(t, float64(3), decoded["mango"])
}

func TestEncodeDecodeNested(t *testing.T) {
	obj := map[string]any{
		"users": []any{
			map[string]any{"name": "alice", "tags": []any{"go", "sql"}},
			map[string]any{"name": "bob", "tags": []any{"rust"}},
		},
	}
	b, err := Encode(obj)
	require.NoError(t, err)

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.(map[string]any)
	users := decoded["users"].([]any)
	require.Len(t, users, 2)
	assert.Equal(t, "alice", users[0].(map[string]any)["name"])
	tags := users[0].(map[string]any)["tags"].([]any)
	assert.Equal(t, "go", tags[0])
	assert.Equal(t, "sql", tags[1])
}

// Partial access tests (binary search without full decode)

func TestLookupObjectKey(t *testing.T) {
	obj := map[string]any{
		"name":  "alice",
		"age":   float64(30),
		"email": "alice@example.com",
	}
	b, err := Encode(obj)
	require.NoError(t, err)

	// Lookup existing key
	val, found, err := LookupKey(b, "name")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "alice", val)

	val, found, err = LookupKey(b, "age")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, float64(30), val)

	// Lookup missing key
	_, found, err = LookupKey(b, "missing")
	require.NoError(t, err)
	assert.False(t, found)
}

func TestLookupArrayIndex(t *testing.T) {
	arr := []any{float64(10), "hello", true}
	b, err := Encode(arr)
	require.NoError(t, err)

	val, found, err := LookupIndex(b, 0)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, float64(10), val)

	val, found, err = LookupIndex(b, 1)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "hello", val)

	// Out of bounds
	_, found, err = LookupIndex(b, 5)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestLookupKeyExists(t *testing.T) {
	obj := map[string]any{
		"name": "alice",
		"v":    nil, // JSON null value
	}
	b, err := Encode(obj)
	require.NoError(t, err)

	// Existing key with value
	assert.True(t, KeyExists(b, "name"))
	// Existing key with null value
	assert.True(t, KeyExists(b, "v"))
	// Missing key
	assert.False(t, KeyExists(b, "missing"))
}

// JSON round-trip tests

func TestFromJSON(t *testing.T) {
	tests := []string{
		`null`,
		`true`,
		`false`,
		`42`,
		`3.14`,
		`"hello"`,
		`[1,2,3]`,
		`{"a":1,"b":"two"}`,
		`{"nested":{"arr":[1,null,true]}}`,
	}
	for _, jsonStr := range tests {
		t.Run(jsonStr, func(t *testing.T) {
			b, err := FromJSON(jsonStr)
			require.NoError(t, err)

			out, err := ToJSON(b)
			require.NoError(t, err)

			// Re-parse both to compare semantically
			b2, err := FromJSON(out)
			require.NoError(t, err)
			assert.Equal(t, b, b2, "round-trip should produce identical binary")
		})
	}
}

func TestEmptyObject(t *testing.T) {
	b, err := FromJSON(`{}`)
	require.NoError(t, err)

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.(map[string]any)
	assert.Len(t, decoded, 0)
}

func TestEmptyArray(t *testing.T) {
	b, err := FromJSON(`[]`)
	require.NoError(t, err)

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.([]any)
	assert.Len(t, decoded, 0)
}

// Typed array tests (compact encoding for homogeneous arrays)

func TestIntArrayCompact(t *testing.T) {
	// Small ints (0-255) should use 1-byte width
	arr := []any{int64(0), int64(42), int64(255)}
	b, err := Encode(arr)
	require.NoError(t, err)
	assert.Equal(t, byte(TagIntArray), b[0])

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.([]any)
	require.Len(t, decoded, 3)
	assert.Equal(t, int64(0), decoded[0])
	assert.Equal(t, int64(42), decoded[1])
	assert.Equal(t, int64(255), decoded[2])
}

func TestIntArrayWidth2(t *testing.T) {
	// Values requiring 2-byte width
	arr := []any{int64(0), int64(256), int64(65535)}
	b, err := Encode(arr)
	require.NoError(t, err)
	assert.Equal(t, byte(TagIntArray), b[0])

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.([]any)
	require.Len(t, decoded, 3)
	assert.Equal(t, int64(0), decoded[0])
	assert.Equal(t, int64(256), decoded[1])
	assert.Equal(t, int64(65535), decoded[2])
}

func TestIntArrayWidth4(t *testing.T) {
	arr := []any{int64(0), int64(65536), int64(1<<32 - 1)}
	b, err := Encode(arr)
	require.NoError(t, err)
	assert.Equal(t, byte(TagIntArray), b[0])

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.([]any)
	require.Len(t, decoded, 3)
	assert.Equal(t, int64(0), decoded[0])
	assert.Equal(t, int64(65536), decoded[1])
	assert.Equal(t, int64(1<<32-1), decoded[2])
}

func TestIntArrayWidth8(t *testing.T) {
	// Negative values or large values require 8-byte width
	arr := []any{int64(-1), int64(0), int64(1 << 40)}
	b, err := Encode(arr)
	require.NoError(t, err)
	assert.Equal(t, byte(TagIntArray), b[0])

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.([]any)
	require.Len(t, decoded, 3)
	assert.Equal(t, int64(-1), decoded[0])
	assert.Equal(t, int64(0), decoded[1])
	assert.Equal(t, int64(1<<40), decoded[2])
}

func TestIntArraySpaceEfficiency(t *testing.T) {
	// 100 small ints: should be tag(1) + count(4) + width(1) + 100*1 = 106 bytes
	arr := make([]any, 100)
	for i := range arr {
		arr[i] = int64(i)
	}
	b, err := Encode(arr)
	require.NoError(t, err)
	assert.Equal(t, byte(TagIntArray), b[0])
	assert.Equal(t, 106, len(b), "100 small ints should be 106 bytes")

	// Verify round-trip
	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.([]any)
	require.Len(t, decoded, 100)
	for i := 0; i < 100; i++ {
		assert.Equal(t, int64(i), decoded[i])
	}
}

func TestFloatArrayCompact(t *testing.T) {
	arr := []any{float64(1.5), float64(2.7), float64(3.14)}
	b, err := Encode(arr)
	require.NoError(t, err)
	assert.Equal(t, byte(TagFloatArray), b[0])

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.([]any)
	require.Len(t, decoded, 3)
	assert.Equal(t, float64(1.5), decoded[0])
	assert.Equal(t, float64(2.7), decoded[1])
	assert.Equal(t, float64(3.14), decoded[2])
}

func TestMixedArrayUsesGenericFormat(t *testing.T) {
	// Mixed types should fall back to TagArray
	arr := []any{int64(1), "hello", true}
	b, err := Encode(arr)
	require.NoError(t, err)
	assert.Equal(t, byte(TagArray), b[0])

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.([]any)
	require.Len(t, decoded, 3)
	assert.Equal(t, int64(1), decoded[0])
	assert.Equal(t, "hello", decoded[1])
	assert.Equal(t, true, decoded[2])
}

func TestIntArrayLookupIndex(t *testing.T) {
	arr := []any{int64(10), int64(20), int64(30)}
	b, err := Encode(arr)
	require.NoError(t, err)

	val, found, err := LookupIndex(b, 0)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, int64(10), val)

	val, found, err = LookupIndex(b, 2)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, int64(30), val)

	// Out of bounds
	_, found, err = LookupIndex(b, 5)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestFloatArrayLookupIndex(t *testing.T) {
	arr := []any{float64(1.1), float64(2.2), float64(3.3)}
	b, err := Encode(arr)
	require.NoError(t, err)

	val, found, err := LookupIndex(b, 1)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, float64(2.2), val)
}

func TestIntArrayFromJSON(t *testing.T) {
	b, err := FromJSON(`[1, 2, 3]`)
	require.NoError(t, err)
	assert.Equal(t, byte(TagIntArray), b[0])

	out, err := ToJSON(b)
	require.NoError(t, err)
	assert.Equal(t, `[1,2,3]`, out)
}

func TestFloatArrayFromJSON(t *testing.T) {
	b, err := FromJSON(`[1.5, 2.5, 3.5]`)
	require.NoError(t, err)
	assert.Equal(t, byte(TagFloatArray), b[0])

	out, err := ToJSON(b)
	require.NoError(t, err)
	assert.Equal(t, `[1.5,2.5,3.5]`, out)
}

func TestNestedIntArray(t *testing.T) {
	obj := map[string]any{
		"ids": []any{int64(1), int64(2), int64(3)},
	}
	b, err := Encode(obj)
	require.NoError(t, err)

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.(map[string]any)
	ids := decoded["ids"].([]any)
	require.Len(t, ids, 3)
	assert.Equal(t, int64(1), ids[0])
	assert.Equal(t, int64(2), ids[1])
	assert.Equal(t, int64(3), ids[2])
}

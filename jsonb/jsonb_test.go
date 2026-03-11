package jsonb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/walf443/oresql/json_path"
)

func TestEncodeDecodeNull(t *testing.T) {
	b, err := Encode(nil)
	require.NoError(t, err)
	assert.Equal(t, byte(TagNull), BodyTag(b))

	val, err := Decode(b)
	require.NoError(t, err)
	assert.Nil(t, val)
}

func TestEncodeDecodeBool(t *testing.T) {
	for _, v := range []bool{true, false} {
		b, err := Encode(v)
		require.NoError(t, err)
		if v {
			assert.Equal(t, byte(TagTrue), BodyTag(b))
		} else {
			assert.Equal(t, byte(TagFalse), BodyTag(b))
		}

		val, err := Decode(b)
		require.NoError(t, err)
		assert.Equal(t, v, val)
	}
}

func TestEncodeDecodeInt(t *testing.T) {
	for _, v := range []int64{0, 1, -1, 42, -100, 127, 128, 1<<31 - 1, -(1 << 31)} {
		b, err := Encode(v)
		require.NoError(t, err)
		if v >= 0 && v <= 127 {
			// Inline small integer: tag = 0x80 | value
			assert.Equal(t, byte(TagInlineIntBase|byte(v)), BodyTag(b))
		} else {
			assert.Equal(t, byte(TagInt), BodyTag(b))
		}

		val, err := Decode(b)
		require.NoError(t, err)
		assert.Equal(t, v, val)
	}
}

func TestEncodeDecodeFloat(t *testing.T) {
	for _, v := range []float64{0.0, 3.14, -2.718, 1e100} {
		b, err := Encode(v)
		require.NoError(t, err)
		assert.Equal(t, byte(TagFloat), BodyTag(b))

		val, err := Decode(b)
		require.NoError(t, err)
		assert.Equal(t, v, val)
	}
}

func TestEncodeDecodeString(t *testing.T) {
	for _, v := range []string{"", "hello", "日本語", "a longer string with spaces"} {
		b, err := Encode(v)
		require.NoError(t, err)
		assert.Equal(t, byte(TagString), BodyTag(b))

		val, err := Decode(b)
		require.NoError(t, err)
		assert.Equal(t, v, val)
	}
}

func TestEncodeDecodeArray(t *testing.T) {
	arr := []any{float64(1), "hello", true, nil}
	b, err := Encode(arr)
	require.NoError(t, err)
	assert.Equal(t, byte(TagArray), BodyTag(b))

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
	assert.Equal(t, byte(TagObject), BodyTag(b))

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

	assert.True(t, KeyExists(b, "name"))
	assert.True(t, KeyExists(b, "v"))
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
	arr := []any{int64(0), int64(42), int64(255)}
	b, err := Encode(arr)
	require.NoError(t, err)
	assert.Equal(t, byte(TagIntArray), BodyTag(b))

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.([]any)
	require.Len(t, decoded, 3)
	assert.Equal(t, int64(0), decoded[0])
	assert.Equal(t, int64(42), decoded[1])
	assert.Equal(t, int64(255), decoded[2])
}

func TestIntArrayWidth2(t *testing.T) {
	arr := []any{int64(0), int64(256), int64(65535)}
	b, err := Encode(arr)
	require.NoError(t, err)
	assert.Equal(t, byte(TagIntArray), BodyTag(b))

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
	assert.Equal(t, byte(TagIntArray), BodyTag(b))

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.([]any)
	require.Len(t, decoded, 3)
	assert.Equal(t, int64(0), decoded[0])
	assert.Equal(t, int64(65536), decoded[1])
	assert.Equal(t, int64(1<<32-1), decoded[2])
}

func TestIntArrayWidth8(t *testing.T) {
	arr := []any{int64(-1), int64(0), int64(1 << 40)}
	b, err := Encode(arr)
	require.NoError(t, err)
	assert.Equal(t, byte(TagIntArray), BodyTag(b))

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.([]any)
	require.Len(t, decoded, 3)
	assert.Equal(t, int64(-1), decoded[0])
	assert.Equal(t, int64(0), decoded[1])
	assert.Equal(t, int64(1<<40), decoded[2])
}

func TestIntArraySpaceEfficiency(t *testing.T) {
	// 100 small ints: header(2) + tag(1) + count(4) + width(1) + 100*1 = 108 bytes
	arr := make([]any, 100)
	for i := range arr {
		arr[i] = int64(i)
	}
	b, err := Encode(arr)
	require.NoError(t, err)
	assert.Equal(t, byte(TagIntArray), BodyTag(b))
	assert.Equal(t, 108, len(b), "100 small ints should be 108 bytes (including 2-byte dict header)")

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
	assert.Equal(t, byte(TagFloatArray), BodyTag(b))

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.([]any)
	require.Len(t, decoded, 3)
	assert.Equal(t, float64(1.5), decoded[0])
	assert.Equal(t, float64(2.7), decoded[1])
	assert.Equal(t, float64(3.14), decoded[2])
}

func TestMixedArrayUsesGenericFormat(t *testing.T) {
	arr := []any{int64(1), "hello", true}
	b, err := Encode(arr)
	require.NoError(t, err)
	assert.Equal(t, byte(TagArray), BodyTag(b))

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
	assert.Equal(t, byte(TagIntArray), BodyTag(b))

	out, err := ToJSON(b)
	require.NoError(t, err)
	assert.Equal(t, `[1,2,3]`, out)
}

func TestFloatArrayFromJSON(t *testing.T) {
	b, err := FromJSON(`[1.5, 2.5, 3.5]`)
	require.NoError(t, err)
	assert.Equal(t, byte(TagFloatArray), BodyTag(b))

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

// Key dictionary tests

func TestKeyDictArrayOfObjects(t *testing.T) {
	arr := []any{
		map[string]any{"name": "alice", "age": int64(30)},
		map[string]any{"name": "bob", "age": int64(25)},
		map[string]any{"name": "carol", "age": int64(35)},
	}
	b, err := Encode(arr)
	require.NoError(t, err)

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.([]any)
	require.Len(t, decoded, 3)
	assert.Equal(t, "alice", decoded[0].(map[string]any)["name"])
	assert.Equal(t, int64(30), decoded[0].(map[string]any)["age"])
	assert.Equal(t, "bob", decoded[1].(map[string]any)["name"])
	assert.Equal(t, int64(25), decoded[1].(map[string]any)["age"])
	assert.Equal(t, "carol", decoded[2].(map[string]any)["name"])
	assert.Equal(t, int64(35), decoded[2].(map[string]any)["age"])
}

func TestKeyDictNestedArrayOfObjects(t *testing.T) {
	obj := map[string]any{
		"users": []any{
			map[string]any{"name": "alice", "score": float64(95.5)},
			map[string]any{"name": "bob", "score": float64(87.0)},
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
	assert.Equal(t, float64(95.5), users[0].(map[string]any)["score"])
	assert.Equal(t, "bob", users[1].(map[string]any)["name"])
	assert.Equal(t, float64(87.0), users[1].(map[string]any)["score"])
}

func TestKeyDictSavesSpace(t *testing.T) {
	// 10 objects with keys "product", "qty", "price".
	// Key strings should only be stored once in the dictionary.
	items := make([]any, 10)
	for i := range items {
		items[i] = map[string]any{
			"product": fmt.Sprintf("item_%d", i),
			"qty":     int64(i + 1),
			"price":   float64(float64(i)*10.5 + 100),
		}
	}
	withDict, err := Encode(items)
	require.NoError(t, err)

	// Verify round-trip correctness.
	val, err := Decode(withDict)
	require.NoError(t, err)
	decoded := val.([]any)
	require.Len(t, decoded, 10)
	assert.Equal(t, "item_0", decoded[0].(map[string]any)["product"])
	assert.Equal(t, int64(1), decoded[0].(map[string]any)["qty"])
	assert.Equal(t, float64(100), decoded[0].(map[string]any)["price"])
	assert.Equal(t, "item_9", decoded[9].(map[string]any)["product"])

	// Count occurrences of key strings in binary data.
	// With dictionary, "product" should appear only once.
	count := 0
	needle := []byte("product")
	for i := 0; i <= len(withDict)-len(needle); i++ {
		match := true
		for j := range needle {
			if withDict[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			count++
		}
	}
	assert.Equal(t, 1, count, "key 'product' should appear only once in binary (in dictionary)")
}

func TestKeyDictLookupKey(t *testing.T) {
	arr := []any{
		map[string]any{"name": "alice", "age": int64(30)},
		map[string]any{"name": "bob", "age": int64(25)},
	}
	b, err := Encode(arr)
	require.NoError(t, err)

	// Decode the array, then encode one element and look up keys.
	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.([]any)

	elem0, err := Encode(decoded[0])
	require.NoError(t, err)
	v, found, err := LookupKey(elem0, "name")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "alice", v)
}

func TestKeyDictJSONRoundTrip(t *testing.T) {
	jsonStr := `[{"x":1,"y":2},{"x":3,"y":4},{"x":5,"y":6}]`
	b, err := FromJSON(jsonStr)
	require.NoError(t, err)

	out, err := ToJSON(b)
	require.NoError(t, err)

	b2, err := FromJSON(out)
	require.NoError(t, err)
	assert.Equal(t, b, b2, "round-trip should produce identical binary")
}

func TestKeyDictMixedObjects(t *testing.T) {
	// Objects with different key sets — dictionary contains all keys.
	arr := []any{
		map[string]any{"name": "alice", "age": int64(30)},
		map[string]any{"name": "bob", "email": "bob@example.com"},
	}
	b, err := Encode(arr)
	require.NoError(t, err)

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.([]any)
	require.Len(t, decoded, 2)
	assert.Equal(t, "alice", decoded[0].(map[string]any)["name"])
	assert.Equal(t, int64(30), decoded[0].(map[string]any)["age"])
	assert.Equal(t, "bob", decoded[1].(map[string]any)["name"])
	assert.Equal(t, "bob@example.com", decoded[1].(map[string]any)["email"])
}

func TestKeyDictDeeplyNested(t *testing.T) {
	// Same key "value" at different nesting levels — stored once in dictionary.
	obj := map[string]any{
		"a": map[string]any{
			"value": int64(1),
			"b": map[string]any{
				"value": int64(2),
			},
		},
	}
	b, err := Encode(obj)
	require.NoError(t, err)

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.(map[string]any)
	assert.Equal(t, int64(1), decoded["a"].(map[string]any)["value"])
	assert.Equal(t, int64(2), decoded["a"].(map[string]any)["b"].(map[string]any)["value"])

	// "value" should appear only once in binary.
	count := 0
	needle := []byte("value")
	for i := 0; i <= len(b)-len(needle); i++ {
		match := true
		for j := range needle {
			if b[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			count++
		}
	}
	assert.Equal(t, 1, count, "key 'value' should appear only once in dictionary")
}

func TestKeyDictSingleObject(t *testing.T) {
	arr := []any{
		map[string]any{"key": "value"},
	}
	b, err := Encode(arr)
	require.NoError(t, err)

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.([]any)
	require.Len(t, decoded, 1)
	assert.Equal(t, "value", decoded[0].(map[string]any)["key"])
}

func TestKeyDictScalarNoDictOverhead(t *testing.T) {
	// Scalar values should have minimal dictionary overhead (just 2-byte keyCount=0).
	b, err := Encode(int64(42))
	require.NoError(t, err)
	// 2 bytes header + 1 inline int tag = 3
	assert.Equal(t, 3, len(b))
}

// Compact encoding tests

func TestCompactIntSmall(t *testing.T) {
	// Inline small ints (0-127): header(2) + inline tag(1) = 3 bytes
	for _, v := range []int64{0, 1, 42, 127} {
		b, err := Encode(v)
		require.NoError(t, err)
		assert.Equal(t, 3, len(b), "int64(%d) should be 3 bytes (inline)", v)

		val, err := Decode(b)
		require.NoError(t, err)
		assert.Equal(t, v, val)
	}
	// 128-255: header(2) + tag(1) + width(1) + value(1) = 5 bytes
	for _, v := range []int64{128, 255} {
		b, err := Encode(v)
		require.NoError(t, err)
		assert.Equal(t, 5, len(b), "int64(%d) should be 5 bytes", v)

		val, err := Decode(b)
		require.NoError(t, err)
		assert.Equal(t, v, val)
	}
}

func TestCompactInt16(t *testing.T) {
	for _, v := range []int64{256, 1000, 65535} {
		b, err := Encode(v)
		require.NoError(t, err)
		// header(2) + tag(1) + width(1) + value(2) = 6
		assert.Equal(t, 6, len(b), "int64(%d) should be 6 bytes", v)

		val, err := Decode(b)
		require.NoError(t, err)
		assert.Equal(t, v, val)
	}
}

func TestCompactInt32(t *testing.T) {
	for _, v := range []int64{65536, 1<<32 - 1} {
		b, err := Encode(v)
		require.NoError(t, err)
		// header(2) + tag(1) + width(1) + value(4) = 8
		assert.Equal(t, 8, len(b), "int64(%d) should be 8 bytes", v)

		val, err := Decode(b)
		require.NoError(t, err)
		assert.Equal(t, v, val)
	}
}

func TestCompactInt64(t *testing.T) {
	for _, v := range []int64{-1, -100, 1 << 40, -(1 << 31)} {
		b, err := Encode(v)
		require.NoError(t, err)
		// header(2) + tag(1) + width(1) + value(8) = 12
		assert.Equal(t, 12, len(b), "int64(%d) should be 12 bytes", v)

		val, err := Decode(b)
		require.NoError(t, err)
		assert.Equal(t, v, val)
	}
}

func TestCompactStringShort(t *testing.T) {
	// Short strings (< 128 bytes) should use 1-byte length
	for _, v := range []string{"", "hi", "hello", "日本語"} {
		b, err := Encode(v)
		require.NoError(t, err)
		// header(2) + tag(1) + len(1) + data
		expected := 2 + 1 + 1 + len(v)
		assert.Equal(t, expected, len(b), "string %q should be %d bytes", v, expected)

		val, err := Decode(b)
		require.NoError(t, err)
		assert.Equal(t, v, val)
	}
}

func TestCompactStringLong(t *testing.T) {
	// Strings >= 128 bytes should use 4-byte length
	long := string(make([]byte, 200))
	b, err := Encode(long)
	require.NoError(t, err)
	// header(2) + tag(1) + marker(1) + len(4) + data(200) = 208
	assert.Equal(t, 208, len(b))

	val, err := Decode(b)
	require.NoError(t, err)
	assert.Equal(t, long, val)
}

func TestCompactEncodingRoundTrip(t *testing.T) {
	// Complex structure with compact values
	obj := map[string]any{
		"id":     int64(42),
		"name":   "alice",
		"score":  float64(95.5),
		"active": true,
		"tags":   []any{"go", "sql"},
		"nested": map[string]any{
			"count": int64(3),
			"big":   int64(1 << 40),
		},
	}
	b, err := Encode(obj)
	require.NoError(t, err)

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.(map[string]any)
	assert.Equal(t, int64(42), decoded["id"])
	assert.Equal(t, "alice", decoded["name"])
	assert.Equal(t, float64(95.5), decoded["score"])
	assert.Equal(t, true, decoded["active"])
	assert.Equal(t, int64(3), decoded["nested"].(map[string]any)["count"])
	assert.Equal(t, int64(1<<40), decoded["nested"].(map[string]any)["big"])
}

func TestCompactEncodingLookupKey(t *testing.T) {
	obj := map[string]any{
		"id":   int64(1),
		"name": "alice",
	}
	b, err := Encode(obj)
	require.NoError(t, err)

	val, found, err := LookupKey(b, "id")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, int64(1), val)

	val, found, err = LookupKey(b, "name")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "alice", val)
}

func TestCompactEncodingJSONRoundTrip(t *testing.T) {
	tests := []string{
		`42`,
		`"hello"`,
		`{"id":1,"name":"test"}`,
		`[1,2,3]`,
		`{"items":[{"qty":1},{"qty":2}]}`,
	}
	for _, jsonStr := range tests {
		t.Run(jsonStr, func(t *testing.T) {
			b, err := FromJSON(jsonStr)
			require.NoError(t, err)

			out, err := ToJSON(b)
			require.NoError(t, err)

			b2, err := FromJSON(out)
			require.NoError(t, err)
			assert.Equal(t, b, b2, "round-trip should produce identical binary")
		})
	}
}

// Compact entry table tests

func TestCompactEntryTableSmallObject(t *testing.T) {
	// Small object: dict < 256 keys, value data < 256B
	// Entry table should use 1-byte keyIdx + 1-byte valOffset = 2B per key
	obj := map[string]any{
		"a": int64(1),
		"b": int64(2),
	}
	b, err := Encode(obj)
	require.NoError(t, err)

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.(map[string]any)
	assert.Equal(t, int64(1), decoded["a"])
	assert.Equal(t, int64(2), decoded["b"])

	// LookupKey should still work
	v, found, err := LookupKey(b, "a")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, int64(1), v)

	v, found, err = LookupKey(b, "b")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, int64(2), v)
}

func TestCompactEntryTableSizeReduction(t *testing.T) {
	// 3-key object: {"a":1, "b":2, "c":3}
	// Dict header: 2 + 3*(tag+len+1) = 2 + 9 = 11
	// Object: tag(1) + count(1, <256 keys) + keyIdxW(1) + valOffW(1) + entries(3 * 2) + values(3 * 3) = 19
	// Total = 11 + 19 = 30
	obj := map[string]any{
		"a": int64(1),
		"b": int64(2),
		"c": int64(3),
	}
	b, err := Encode(obj)
	require.NoError(t, err)

	// Verify correctness
	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.(map[string]any)
	assert.Equal(t, int64(1), decoded["a"])
	assert.Equal(t, int64(2), decoded["b"])
	assert.Equal(t, int64(3), decoded["c"])
}

func TestCompactEntryTableLookupKeyNested(t *testing.T) {
	obj := map[string]any{
		"user": map[string]any{
			"name":  "alice",
			"email": "alice@example.com",
			"age":   int64(30),
		},
	}
	b, err := Encode(obj)
	require.NoError(t, err)

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.(map[string]any)
	user := decoded["user"].(map[string]any)
	assert.Equal(t, "alice", user["name"])
	assert.Equal(t, "alice@example.com", user["email"])
	assert.Equal(t, int64(30), user["age"])
}

func TestCompactEntryTableManyKeys(t *testing.T) {
	// Object with > 255 keys to test wider key index
	obj := make(map[string]any)
	for i := 0; i < 300; i++ {
		obj[fmt.Sprintf("key_%03d", i)] = int64(i)
	}
	b, err := Encode(obj)
	require.NoError(t, err)

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.(map[string]any)
	assert.Len(t, decoded, 300)
	assert.Equal(t, int64(0), decoded["key_000"])
	assert.Equal(t, int64(299), decoded["key_299"])

	// LookupKey should work
	v, found, err := LookupKey(b, "key_150")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, int64(150), v)
}

func TestCompactEntryTableEmptyObject(t *testing.T) {
	obj := map[string]any{}
	b, err := Encode(obj)
	require.NoError(t, err)

	val, err := Decode(b)
	require.NoError(t, err)
	decoded := val.(map[string]any)
	assert.Len(t, decoded, 0)
}

// --- LookupKeys tests ---

func TestLookupKeysNestedObject(t *testing.T) {
	data := map[string]any{
		"address": map[string]any{
			"city": "Tokyo",
			"zip":  "100-0001",
		},
	}
	b, err := Encode(data)
	require.NoError(t, err)

	// $.address.city
	val, found, err := LookupKeys(b, "address", "city")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "Tokyo", val)
}

func TestLookupKeysArrayIndex(t *testing.T) {
	data := map[string]any{
		"items": []any{
			map[string]any{"name": "apple"},
			map[string]any{"name": "banana"},
		},
	}
	b, err := Encode(data)
	require.NoError(t, err)

	// $.items[1].name
	val, found, err := LookupKeys(b, "items", 1, "name")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "banana", val)
}

func TestLookupKeysSingleKey(t *testing.T) {
	data := map[string]any{"x": int64(42)}
	b, err := Encode(data)
	require.NoError(t, err)

	val, found, err := LookupKeys(b, "x")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, int64(42), val)
}

func TestLookupKeysNotFound(t *testing.T) {
	data := map[string]any{
		"a": map[string]any{"b": int64(1)},
	}
	b, err := Encode(data)
	require.NoError(t, err)

	// key doesn't exist
	val, found, err := LookupKeys(b, "a", "z")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, val)
}

func TestLookupKeysTypeMismatch(t *testing.T) {
	// "x" is a scalar, "y" exists in dictionary but traversing "x" then "y" is a type mismatch
	data := map[string]any{"x": int64(42), "y": int64(99)}
	b, err := Encode(data)
	require.NoError(t, err)

	// try to traverse into a scalar value
	_, _, err = LookupKeys(b, "x", "y")
	assert.Error(t, err)
}

func TestLookupKeysEmptyPath(t *testing.T) {
	data := map[string]any{"x": int64(1)}
	b, err := Encode(data)
	require.NoError(t, err)

	// empty path returns root value
	val, found, err := LookupKeys(b)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, map[string]any{"x": int64(1)}, val)
}

func TestLookupKeysDeeplyNested(t *testing.T) {
	data := map[string]any{
		"a": map[string]any{
			"b": map[string]any{
				"c": map[string]any{
					"d": "deep",
				},
			},
		},
	}
	b, err := Encode(data)
	require.NoError(t, err)

	val, found, err := LookupKeys(b, "a", "b", "c", "d")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "deep", val)
}

// --- KeysExists tests ---

func TestKeysExistsFound(t *testing.T) {
	data := map[string]any{
		"address": map[string]any{
			"city": "Tokyo",
		},
	}
	b, err := Encode(data)
	require.NoError(t, err)

	assert.True(t, KeysExists(b, "address", "city"))
}

func TestKeysExistsNotFound(t *testing.T) {
	data := map[string]any{
		"address": map[string]any{
			"city": "Tokyo",
		},
	}
	b, err := Encode(data)
	require.NoError(t, err)

	assert.False(t, KeysExists(b, "address", "zip"))
	assert.False(t, KeysExists(b, "missing"))
}

func TestKeysExistsWithArrayIndex(t *testing.T) {
	data := map[string]any{
		"items": []any{"a", "b", "c"},
	}
	b, err := Encode(data)
	require.NoError(t, err)

	assert.True(t, KeysExists(b, "items", 1))
	assert.False(t, KeysExists(b, "items", 5))
}

// --- QueryPath tests ---

func TestQueryPathSimpleKey(t *testing.T) {
	data := map[string]any{"name": "alice"}
	b, err := Encode(data)
	require.NoError(t, err)

	p, err := json_path.Parse("$.name")
	require.NoError(t, err)

	val, found, err := QueryPath(b, p)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "alice", val)
}

func TestQueryPathNested(t *testing.T) {
	data := map[string]any{
		"address": map[string]any{
			"city": "Tokyo",
		},
	}
	b, err := Encode(data)
	require.NoError(t, err)

	p, err := json_path.Parse("$.address.city")
	require.NoError(t, err)

	val, found, err := QueryPath(b, p)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "Tokyo", val)
}

func TestQueryPathArrayIndex(t *testing.T) {
	data := map[string]any{
		"items": []any{
			map[string]any{"name": "apple"},
			map[string]any{"name": "banana"},
		},
	}
	b, err := Encode(data)
	require.NoError(t, err)

	p, err := json_path.Parse("$.items[1].name")
	require.NoError(t, err)

	val, found, err := QueryPath(b, p)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "banana", val)
}

func TestQueryPathRoot(t *testing.T) {
	data := map[string]any{"x": int64(1)}
	b, err := Encode(data)
	require.NoError(t, err)

	p, err := json_path.Parse("$")
	require.NoError(t, err)

	val, found, err := QueryPath(b, p)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, map[string]any{"x": int64(1)}, val)
}

func TestQueryPathNotFound(t *testing.T) {
	data := map[string]any{"a": int64(1)}
	b, err := Encode(data)
	require.NoError(t, err)

	p, err := json_path.Parse("$.z")
	require.NoError(t, err)

	val, found, err := QueryPath(b, p)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, val)
}

func TestExistsPathFound(t *testing.T) {
	data := map[string]any{
		"a": map[string]any{"b": int64(1)},
	}
	b, err := Encode(data)
	require.NoError(t, err)

	p, _ := json_path.Parse("$.a.b")
	assert.True(t, ExistsPath(b, p))
}

func TestExistsPathNotFound(t *testing.T) {
	data := map[string]any{
		"a": map[string]any{"b": int64(1)},
	}
	b, err := Encode(data)
	require.NoError(t, err)

	p, _ := json_path.Parse("$.a.z")
	assert.False(t, ExistsPath(b, p))

	p2, _ := json_path.Parse("$.missing")
	assert.False(t, ExistsPath(b, p2))
}

func TestExistsPathArrayIndex(t *testing.T) {
	data := map[string]any{
		"items": []any{
			map[string]any{"name": "apple"},
			map[string]any{"name": "banana"},
		},
	}
	b, err := Encode(data)
	require.NoError(t, err)

	p, _ := json_path.Parse("$.items[1].name")
	assert.True(t, ExistsPath(b, p))

	p2, _ := json_path.Parse("$.items[5]")
	assert.False(t, ExistsPath(b, p2))
}

func TestExistsPathRoot(t *testing.T) {
	data := map[string]any{"x": int64(1)}
	b, err := Encode(data)
	require.NoError(t, err)

	p, _ := json_path.Parse("$")
	assert.True(t, ExistsPath(b, p))
}

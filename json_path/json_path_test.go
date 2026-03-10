package json_path

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTraverse_ObjectMember(t *testing.T) {
	tests := []struct {
		name     string
		json     interface{}
		path     string
		expected interface{}
	}{
		{
			"simple string",
			map[string]interface{}{"name": "alice"},
			"$.name",
			"alice",
		},
		{
			"simple number",
			map[string]interface{}{"age": float64(30)},
			"$.age",
			float64(30),
		},
		{
			"nested member",
			map[string]interface{}{"a": map[string]interface{}{"b": "deep"}},
			"$.a.b",
			"deep",
		},
		{
			"deeply nested",
			map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": float64(42)}}},
			"$.a.b.c",
			float64(42),
		},
		{
			"missing key returns nil",
			map[string]interface{}{"a": float64(1)},
			"$.b",
			nil,
		},
		{
			"missing nested returns nil",
			map[string]interface{}{"a": float64(1)},
			"$.a.b",
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Traverse(tt.json, tt.path)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTraverse_ArrayIndex(t *testing.T) {
	tests := []struct {
		name     string
		json     interface{}
		path     string
		expected interface{}
	}{
		{
			"first element",
			[]interface{}{float64(10), float64(20), float64(30)},
			"$[0]",
			float64(10),
		},
		{
			"second element",
			[]interface{}{float64(10), float64(20), float64(30)},
			"$[1]",
			float64(20),
		},
		{
			"last element",
			[]interface{}{float64(10), float64(20), float64(30)},
			"$[2]",
			float64(30),
		},
		{
			"out of bounds returns nil",
			[]interface{}{float64(1), float64(2)},
			"$[5]",
			nil,
		},
		{
			"negative index returns nil",
			[]interface{}{float64(1), float64(2)},
			"$[-1]",
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Traverse(tt.json, tt.path)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTraverse_MixedAccess(t *testing.T) {
	tests := []struct {
		name     string
		json     interface{}
		path     string
		expected interface{}
	}{
		{
			"object then array",
			map[string]interface{}{"items": []interface{}{float64(1), float64(2), float64(3)}},
			"$.items[1]",
			float64(2),
		},
		{
			"array then object",
			[]interface{}{
				map[string]interface{}{"id": float64(1)},
				map[string]interface{}{"id": float64(2)},
			},
			"$[1].id",
			float64(2),
		},
		{
			"complex nested",
			map[string]interface{}{
				"users": []interface{}{
					map[string]interface{}{"name": "alice", "tags": []interface{}{"go", "sql"}},
				},
			},
			"$.users[0].tags[1]",
			"sql",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Traverse(tt.json, tt.path)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTraverse_Root(t *testing.T) {
	obj := map[string]interface{}{"a": float64(1)}
	result, err := Traverse(obj, "$")
	require.NoError(t, err)
	assert.Equal(t, obj, result)

	arr := []interface{}{float64(1), float64(2)}
	result, err = Traverse(arr, "$")
	require.NoError(t, err)
	assert.Equal(t, arr, result)

	result, err = Traverse("hello", "$")
	require.NoError(t, err)
	assert.Equal(t, "hello", result)
}

func TestTraverse_NullValues(t *testing.T) {
	json := map[string]interface{}{"v": nil}
	result, err := Traverse(json, "$.v")
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestTraverse_StructureResults(t *testing.T) {
	// Traverse should return objects and arrays as-is
	nested := map[string]interface{}{"b": float64(1)}
	json := map[string]interface{}{"a": nested}
	result, err := Traverse(json, "$.a")
	require.NoError(t, err)
	assert.Equal(t, nested, result)

	arr := []interface{}{float64(1), float64(2)}
	json2 := map[string]interface{}{"items": arr}
	result, err = Traverse(json2, "$.items")
	require.NoError(t, err)
	assert.Equal(t, arr, result)
}

func TestTraverse_ErrorCases(t *testing.T) {
	tests := []struct {
		name string
		json interface{}
		path string
	}{
		{"empty path", map[string]interface{}{}, ""},
		{"no dollar prefix", map[string]interface{}{}, "a"},
		{"missing closing bracket", []interface{}{float64(1)}, "$[0"},
		{"non-numeric index", []interface{}{float64(1)}, "$[abc]"},
		{"unexpected character", map[string]interface{}{}, "$!key"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Traverse(tt.json, tt.path)
			require.Error(t, err)
		})
	}
}

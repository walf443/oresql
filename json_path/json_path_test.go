package json_path

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_Root(t *testing.T) {
	p, err := Parse("$")
	require.NoError(t, err)
	assert.Empty(t, p.Steps)
}

func TestParse_ObjectMember(t *testing.T) {
	p, err := Parse("$.name")
	require.NoError(t, err)
	require.Len(t, p.Steps, 1)
	assert.Equal(t, Step{Kind: StepMember, Key: "name"}, p.Steps[0])
}

func TestParse_NestedMembers(t *testing.T) {
	p, err := Parse("$.a.b.c")
	require.NoError(t, err)
	require.Len(t, p.Steps, 3)
	assert.Equal(t, Step{Kind: StepMember, Key: "a"}, p.Steps[0])
	assert.Equal(t, Step{Kind: StepMember, Key: "b"}, p.Steps[1])
	assert.Equal(t, Step{Kind: StepMember, Key: "c"}, p.Steps[2])
}

func TestParse_ArrayIndex(t *testing.T) {
	p, err := Parse("$[0]")
	require.NoError(t, err)
	require.Len(t, p.Steps, 1)
	assert.Equal(t, Step{Kind: StepIndex, Index: 0}, p.Steps[0])
}

func TestParse_MixedPath(t *testing.T) {
	p, err := Parse("$.users[0].tags[1]")
	require.NoError(t, err)
	require.Len(t, p.Steps, 4)
	assert.Equal(t, Step{Kind: StepMember, Key: "users"}, p.Steps[0])
	assert.Equal(t, Step{Kind: StepIndex, Index: 0}, p.Steps[1])
	assert.Equal(t, Step{Kind: StepMember, Key: "tags"}, p.Steps[2])
	assert.Equal(t, Step{Kind: StepIndex, Index: 1}, p.Steps[3])
}

func TestParse_ErrorCases(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{"empty path", ""},
		{"no dollar prefix", "a"},
		{"missing closing bracket", "$[0"},
		{"non-numeric index", "$[abc]"},
		{"unexpected character", "$!key"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.path)
			require.Error(t, err)
		})
	}
}

func TestPath_Execute_ObjectMember(t *testing.T) {
	tests := []struct {
		name     string
		json     any
		path     string
		expected any
	}{
		{
			"simple string",
			map[string]any{"name": "alice"},
			"$.name",
			"alice",
		},
		{
			"simple number",
			map[string]any{"age": float64(30)},
			"$.age",
			float64(30),
		},
		{
			"nested member",
			map[string]any{"a": map[string]any{"b": "deep"}},
			"$.a.b",
			"deep",
		},
		{
			"deeply nested",
			map[string]any{"a": map[string]any{"b": map[string]any{"c": float64(42)}}},
			"$.a.b.c",
			float64(42),
		},
		{
			"missing key returns nil",
			map[string]any{"a": float64(1)},
			"$.b",
			nil,
		},
		{
			"missing nested returns nil",
			map[string]any{"a": float64(1)},
			"$.a.b",
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := Parse(tt.path)
			require.NoError(t, err)
			result := p.Execute(tt.json)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPath_Execute_ArrayIndex(t *testing.T) {
	tests := []struct {
		name     string
		json     any
		path     string
		expected any
	}{
		{
			"first element",
			[]any{float64(10), float64(20), float64(30)},
			"$[0]",
			float64(10),
		},
		{
			"second element",
			[]any{float64(10), float64(20), float64(30)},
			"$[1]",
			float64(20),
		},
		{
			"last element",
			[]any{float64(10), float64(20), float64(30)},
			"$[2]",
			float64(30),
		},
		{
			"out of bounds returns nil",
			[]any{float64(1), float64(2)},
			"$[5]",
			nil,
		},
		{
			"negative index returns nil",
			[]any{float64(1), float64(2)},
			"$[-1]",
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := Parse(tt.path)
			require.NoError(t, err)
			result := p.Execute(tt.json)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPath_Execute_MixedAccess(t *testing.T) {
	tests := []struct {
		name     string
		json     any
		path     string
		expected any
	}{
		{
			"object then array",
			map[string]any{"items": []any{float64(1), float64(2), float64(3)}},
			"$.items[1]",
			float64(2),
		},
		{
			"array then object",
			[]any{
				map[string]any{"id": float64(1)},
				map[string]any{"id": float64(2)},
			},
			"$[1].id",
			float64(2),
		},
		{
			"complex nested",
			map[string]any{
				"users": []any{
					map[string]any{"name": "alice", "tags": []any{"go", "sql"}},
				},
			},
			"$.users[0].tags[1]",
			"sql",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := Parse(tt.path)
			require.NoError(t, err)
			result := p.Execute(tt.json)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPath_Execute_Root(t *testing.T) {
	p, err := Parse("$")
	require.NoError(t, err)

	obj := map[string]any{"a": float64(1)}
	assert.Equal(t, obj, p.Execute(obj))

	arr := []any{float64(1), float64(2)}
	assert.Equal(t, arr, p.Execute(arr))

	assert.Equal(t, "hello", p.Execute("hello"))
}

func TestPath_Execute_NullValues(t *testing.T) {
	p, err := Parse("$.v")
	require.NoError(t, err)

	json := map[string]any{"v": nil}
	assert.Nil(t, p.Execute(json))
}

func TestPath_Execute_StructureResults(t *testing.T) {
	nested := map[string]any{"b": float64(1)}
	json := map[string]any{"a": nested}
	p, err := Parse("$.a")
	require.NoError(t, err)
	assert.Equal(t, nested, p.Execute(json))

	arr := []any{float64(1), float64(2)}
	json2 := map[string]any{"items": arr}
	p2, err := Parse("$.items")
	require.NoError(t, err)
	assert.Equal(t, arr, p2.Execute(json2))
}

func TestPath_Execute_Reuse(t *testing.T) {
	// Same parsed path can be reused across multiple JSON values
	p, err := Parse("$.name")
	require.NoError(t, err)

	assert.Equal(t, "alice", p.Execute(map[string]any{"name": "alice"}))
	assert.Equal(t, "bob", p.Execute(map[string]any{"name": "bob"}))
	assert.Nil(t, p.Execute(map[string]any{"id": float64(1)}))
}

// Traverse tests (backward compatibility)
func TestTraverse_ObjectMember(t *testing.T) {
	tests := []struct {
		name     string
		json     any
		path     string
		expected any
	}{
		{
			"simple string",
			map[string]any{"name": "alice"},
			"$.name",
			"alice",
		},
		{
			"nested member",
			map[string]any{"a": map[string]any{"b": "deep"}},
			"$.a.b",
			"deep",
		},
		{
			"missing key returns nil",
			map[string]any{"a": float64(1)},
			"$.b",
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

func TestTraverse_ErrorCases(t *testing.T) {
	tests := []struct {
		name string
		json any
		path string
	}{
		{"empty path", map[string]any{}, ""},
		{"no dollar prefix", map[string]any{}, "a"},
		{"missing closing bracket", []any{float64(1)}, "$[0"},
		{"non-numeric index", []any{float64(1)}, "$[abc]"},
		{"unexpected character", map[string]any{}, "$!key"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Traverse(tt.json, tt.path)
			require.Error(t, err)
		})
	}
}

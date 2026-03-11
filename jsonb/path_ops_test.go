package jsonb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPathOpsTokenize_SimpleObject(t *testing.T) {
	b, err := FromJSON(`{"status": "active", "count": 42}`)
	require.NoError(t, err)

	tokens, err := PathOpsTokenize(b)
	require.NoError(t, err)

	// Should produce 2 tokens: one for status=active, one for count=42
	assert.Len(t, tokens, 2)
	// Tokens should be unique
	assert.NotEqual(t, tokens[0], tokens[1])
}

func TestPathOpsTokenize_NestedObject(t *testing.T) {
	b, err := FromJSON(`{"user": {"name": "alice", "age": 30}}`)
	require.NoError(t, err)

	tokens, err := PathOpsTokenize(b)
	require.NoError(t, err)

	// Should produce 2 tokens: hash(user,name,alice), hash(user,age,30)
	assert.Len(t, tokens, 2)
}

func TestPathOpsTokenize_Array(t *testing.T) {
	b, err := FromJSON(`{"tags": ["go", "sql"]}`)
	require.NoError(t, err)

	tokens, err := PathOpsTokenize(b)
	require.NoError(t, err)

	// Should produce 2 tokens: hash(tags,go), hash(tags,sql)
	assert.Len(t, tokens, 2)
	assert.NotEqual(t, tokens[0], tokens[1])
}

func TestPathOpsTokenize_ContainmentMatch(t *testing.T) {
	// The key property: tokenizing a document and tokenizing the query pattern
	// should produce overlapping tokens for containment matches.
	doc, err := FromJSON(`{"status": "active", "role": "admin", "count": 5}`)
	require.NoError(t, err)
	docTokens, err := PathOpsTokenize(doc)
	require.NoError(t, err)

	// Query: @> '{"status": "active"}'
	query, err := FromJSON(`{"status": "active"}`)
	require.NoError(t, err)
	queryTokens, err := PathOpsTokenize(query)
	require.NoError(t, err)

	// All query tokens should be present in doc tokens
	require.Len(t, queryTokens, 1)
	assert.Contains(t, docTokens, queryTokens[0])
}

func TestPathOpsTokenize_DifferentValuesProduceDifferentTokens(t *testing.T) {
	b1, err := FromJSON(`{"status": "active"}`)
	require.NoError(t, err)
	t1, err := PathOpsTokenize(b1)
	require.NoError(t, err)

	b2, err := FromJSON(`{"status": "inactive"}`)
	require.NoError(t, err)
	t2, err := PathOpsTokenize(b2)
	require.NoError(t, err)

	// Different values should produce different tokens
	assert.NotEqual(t, t1[0], t2[0])
}

func TestPathOpsTokenize_DifferentKeysProduceDifferentTokens(t *testing.T) {
	b1, err := FromJSON(`{"status": "active"}`)
	require.NoError(t, err)
	t1, err := PathOpsTokenize(b1)
	require.NoError(t, err)

	// Same value but different key
	b2, err := FromJSON(`{"role": "active"}`)
	require.NoError(t, err)
	t2, err := PathOpsTokenize(b2)
	require.NoError(t, err)

	assert.NotEqual(t, t1[0], t2[0])
}

func TestPathOpsTokenize_BooleanValues(t *testing.T) {
	b, err := FromJSON(`{"enabled": true, "deleted": false}`)
	require.NoError(t, err)

	tokens, err := PathOpsTokenize(b)
	require.NoError(t, err)
	assert.Len(t, tokens, 2)
	assert.NotEqual(t, tokens[0], tokens[1])
}

func TestPathOpsTokenize_NullValue(t *testing.T) {
	b, err := FromJSON(`{"data": null}`)
	require.NoError(t, err)

	tokens, err := PathOpsTokenize(b)
	require.NoError(t, err)
	assert.Len(t, tokens, 1)
}

func TestPathOpsTokenize_FloatValue(t *testing.T) {
	b, err := FromJSON(`{"price": 19.99}`)
	require.NoError(t, err)

	tokens, err := PathOpsTokenize(b)
	require.NoError(t, err)
	assert.Len(t, tokens, 1)
}

func TestPathOpsTokenize_EmptyObject(t *testing.T) {
	b, err := FromJSON(`{}`)
	require.NoError(t, err)

	tokens, err := PathOpsTokenize(b)
	require.NoError(t, err)
	assert.Len(t, tokens, 0)
}

func TestPathOpsTokenize_EmptyArray(t *testing.T) {
	b, err := FromJSON(`{"items": []}`)
	require.NoError(t, err)

	tokens, err := PathOpsTokenize(b)
	require.NoError(t, err)
	assert.Len(t, tokens, 0)
}

func TestPathOpsTokenize_NestedArray(t *testing.T) {
	b, err := FromJSON(`{"matrix": [[1, 2], [3, 4]]}`)
	require.NoError(t, err)

	tokens, err := PathOpsTokenize(b)
	require.NoError(t, err)
	// 4 tokens: hash(matrix,1), hash(matrix,2), hash(matrix,3), hash(matrix,4)
	assert.Len(t, tokens, 4)
}

func TestPathOpsTokenize_DuplicateTokensDeduped(t *testing.T) {
	// Array with duplicate values should produce deduplicated tokens
	b, err := FromJSON(`{"tags": ["go", "go", "sql"]}`)
	require.NoError(t, err)

	tokens, err := PathOpsTokenize(b)
	require.NoError(t, err)
	// "go" appears twice but should be deduped: hash(tags,go), hash(tags,sql)
	assert.Len(t, tokens, 2)
}

func TestPathOpsTokenize_TopLevelArray(t *testing.T) {
	b, err := FromJSON(`[1, 2, 3]`)
	require.NoError(t, err)

	tokens, err := PathOpsTokenize(b)
	require.NoError(t, err)
	// Top-level array with no path keys: hash(1), hash(2), hash(3)
	assert.Len(t, tokens, 3)
}

func TestPathOpsTokenize_StringPooledValues(t *testing.T) {
	// Repeated strings that would use the string pool
	b, err := FromJSON(`[{"status": "active"}, {"status": "active"}, {"status": "pending"}]`)
	require.NoError(t, err)

	tokens, err := PathOpsTokenize(b)
	require.NoError(t, err)
	// hash(status,active) and hash(status,pending) - deduped
	assert.Len(t, tokens, 2)
}

package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONType_CreateTableAndInsert(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	result := run(t, exec, "CREATE TABLE docs (id INT, data JSON)")
	assert.Equal(t, "table created", result.Message)

	// Insert valid JSON object
	result = run(t, exec, "INSERT INTO docs VALUES (1, '{\"name\": \"alice\"}')")
	assert.Equal(t, "1 row inserted", result.Message)

	// Insert valid JSON array
	result = run(t, exec, "INSERT INTO docs VALUES (2, '[1, 2, 3]')")
	assert.Equal(t, "1 row inserted", result.Message)

	// Insert valid JSON string
	result = run(t, exec, "INSERT INTO docs VALUES (3, '\"hello\"')")
	assert.Equal(t, "1 row inserted", result.Message)

	// Insert valid JSON number
	result = run(t, exec, "INSERT INTO docs VALUES (4, '42')")
	assert.Equal(t, "1 row inserted", result.Message)

	// Insert valid JSON boolean
	result = run(t, exec, "INSERT INTO docs VALUES (5, 'true')")
	assert.Equal(t, "1 row inserted", result.Message)

	// Insert valid JSON null
	result = run(t, exec, "INSERT INTO docs VALUES (6, 'null')")
	assert.Equal(t, "1 row inserted", result.Message)

	// SELECT back
	result = run(t, exec, "SELECT * FROM docs WHERE id = 1")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "{\"name\": \"alice\"}", result.Rows[0][1])
}

func TestJSONType_InvalidJSON(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE docs (id INT, data JSON)")

	// Insert invalid JSON should fail
	runExpectError(t, exec, "INSERT INTO docs VALUES (1, 'not json')")
}

func TestJSONType_NullValue(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE docs (id INT, data JSON)")

	// SQL NULL is allowed (not JSON null, but SQL NULL)
	result := run(t, exec, "INSERT INTO docs VALUES (1, NULL)")
	assert.Equal(t, "1 row inserted", result.Message)

	result = run(t, exec, "SELECT * FROM docs WHERE data IS NULL")
	require.Len(t, result.Rows, 1)
}

func TestJSONType_NotNull(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE docs (id INT, data JSON NOT NULL)")

	runExpectError(t, exec, "INSERT INTO docs VALUES (1, NULL)")
}

func TestJSONType_CastToJSON(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE docs (id INT, data JSON)")

	// CAST text to JSON
	result := run(t, exec, "SELECT CAST('{\"a\": 1}' AS JSON)")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "{\"a\": 1}", result.Rows[0][0])
}

func TestJSONType_CastInvalidJSON(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	// CAST invalid text to JSON should fail
	_, err := runWithError(exec, "SELECT CAST('invalid' AS JSON)")
	require.Error(t, err)
}

func TestJSONType_CastFromJSON(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE docs (id INT, data JSON)")
	run(t, exec, "INSERT INTO docs VALUES (1, '{\"name\": \"alice\"}')")

	// CAST JSON to TEXT
	result := run(t, exec, "SELECT CAST(data AS TEXT) FROM docs")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "{\"name\": \"alice\"}", result.Rows[0][0])
}

func TestJSONType_TypeMismatch(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE docs (id INT, data JSON)")

	// INT value into JSON column should fail
	runExpectError(t, exec, "INSERT INTO docs VALUES (1, 42)")
}

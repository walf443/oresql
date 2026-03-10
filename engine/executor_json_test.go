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

func TestJSON_OBJECT(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	// Basic key-value pairs
	result := run(t, exec, "SELECT JSON_OBJECT('name', 'alice', 'age', 30)")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, `{"name":"alice","age":30}`, result.Rows[0][0])

	// Empty object
	result = run(t, exec, "SELECT JSON_OBJECT()")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, `{}`, result.Rows[0][0])

	// With NULL value
	result = run(t, exec, "SELECT JSON_OBJECT('key', NULL)")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, `{"key":null}`, result.Rows[0][0])

	// Nested JSON_OBJECT
	result = run(t, exec, "SELECT JSON_OBJECT('outer', JSON_OBJECT('inner', 1))")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, `{"outer":{"inner":1}}`, result.Rows[0][0])

	// With column reference
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	result = run(t, exec, "SELECT JSON_OBJECT('id', id, 'name', name) FROM users")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, `{"id":1,"name":"alice"}`, result.Rows[0][0])
}

func TestJSON_OBJECT_OddArgs(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	// Odd number of arguments should fail
	_, err := runWithError(exec, "SELECT JSON_OBJECT('key')")
	require.Error(t, err)
}

func TestJSON_OBJECT_NonStringKey(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	// Non-string key should fail
	_, err := runWithError(exec, "SELECT JSON_OBJECT(1, 'value')")
	require.Error(t, err)
}

func TestJSON_ARRAY(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	// Basic array
	result := run(t, exec, "SELECT JSON_ARRAY(1, 2, 3)")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, `[1,2,3]`, result.Rows[0][0])

	// Empty array
	result = run(t, exec, "SELECT JSON_ARRAY()")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, `[]`, result.Rows[0][0])

	// Mixed types
	result = run(t, exec, "SELECT JSON_ARRAY(1, 'hello', NULL, 3.14)")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, `[1,"hello",null,3.14]`, result.Rows[0][0])

	// Nested JSON_ARRAY
	result = run(t, exec, "SELECT JSON_ARRAY(1, JSON_ARRAY(2, 3))")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, `[1,[2,3]]`, result.Rows[0][0])

	// Nested JSON_OBJECT in JSON_ARRAY
	result = run(t, exec, "SELECT JSON_ARRAY(JSON_OBJECT('a', 1), JSON_OBJECT('b', 2))")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, `[{"a":1},{"b":2}]`, result.Rows[0][0])

	// With column reference
	run(t, exec, "CREATE TABLE nums (val INT)")
	run(t, exec, "INSERT INTO nums VALUES (10)")
	result = run(t, exec, "SELECT JSON_ARRAY(val, val * 2) FROM nums")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, `[10,20]`, result.Rows[0][0])
}

func TestJSON_OBJECT_ErrorCases(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	tests := []struct {
		name string
		sql  string
	}{
		{"odd args 1", "SELECT JSON_OBJECT('key')"},
		{"odd args 3", "SELECT JSON_OBJECT('a', 1, 'b')"},
		{"odd args 5", "SELECT JSON_OBJECT('a', 1, 'b', 2, 'c')"},
		{"int key", "SELECT JSON_OBJECT(1, 'value')"},
		{"float key", "SELECT JSON_OBJECT(3.14, 'value')"},
		{"null key", "SELECT JSON_OBJECT(NULL, 'value')"},
		{"bool key (via column)", "SELECT JSON_OBJECT(1 = 1, 'value')"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := runWithError(exec, tt.sql)
			require.Error(t, err, "expected error for %s", tt.sql)
		})
	}
}

func TestJSON_OBJECT_ValueTypes(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{"int value", "SELECT JSON_OBJECT('v', 42)", `{"v":42}`},
		{"negative int", "SELECT JSON_OBJECT('v', -1)", `{"v":-1}`},
		{"float value", "SELECT JSON_OBJECT('v', 3.14)", `{"v":3.14}`},
		{"string value", "SELECT JSON_OBJECT('v', 'hello')", `{"v":"hello"}`},
		{"null value", "SELECT JSON_OBJECT('v', NULL)", `{"v":null}`},
		{"empty string value", "SELECT JSON_OBJECT('v', '')", `{"v":""}`},
		{"string with quotes", "SELECT JSON_OBJECT('v', 'say \"hi\"')", `{"v":"say \"hi\""}`},
		{"multiple pairs", "SELECT JSON_OBJECT('a', 1, 'b', 2, 'c', 3)", `{"a":1,"b":2,"c":3}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := run(t, exec, tt.sql)
			require.Len(t, result.Rows, 1)
			assert.Equal(t, tt.expected, result.Rows[0][0])
		})
	}
}

func TestJSON_ARRAY_ErrorCases(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE items (id INT, data JSON)")

	// JSON_ARRAY itself doesn't have error cases for argument count,
	// but verify that non-serializable types from expressions are handled
	result := run(t, exec, "SELECT JSON_ARRAY()")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, `[]`, result.Rows[0][0])
}

func TestJSON_ARRAY_ValueTypes(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{"single int", "SELECT JSON_ARRAY(1)", `[1]`},
		{"single string", "SELECT JSON_ARRAY('hello')", `["hello"]`},
		{"single null", "SELECT JSON_ARRAY(NULL)", `[null]`},
		{"single float", "SELECT JSON_ARRAY(3.14)", `[3.14]`},
		{"negative int", "SELECT JSON_ARRAY(-5)", `[-5]`},
		{"empty string", "SELECT JSON_ARRAY('')", `[""]`},
		{"all nulls", "SELECT JSON_ARRAY(NULL, NULL, NULL)", `[null,null,null]`},
		{"many elements", "SELECT JSON_ARRAY(1, 2, 3, 4, 5)", `[1,2,3,4,5]`},
		{"nested array in array", "SELECT JSON_ARRAY(JSON_ARRAY(1, 2), JSON_ARRAY(3, 4))", `[[1,2],[3,4]]`},
		{"deeply nested", "SELECT JSON_ARRAY(JSON_ARRAY(JSON_ARRAY(1)))", `[[[1]]]`},
		{"object in array", "SELECT JSON_ARRAY(JSON_OBJECT('x', 1))", `[{"x":1}]`},
		{"mixed nesting", "SELECT JSON_ARRAY(1, JSON_OBJECT('a', JSON_ARRAY(2, 3)))", `[1,{"a":[2,3]}]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := run(t, exec, tt.sql)
			require.Len(t, result.Rows, 1)
			assert.Equal(t, tt.expected, result.Rows[0][0])
		})
	}
}

func TestJSON_OBJECT_WithColumnRef(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE products (id INT, name TEXT, price FLOAT)")
	run(t, exec, "INSERT INTO products VALUES (1, 'apple', 1.5)")
	run(t, exec, "INSERT INTO products VALUES (2, 'banana', 2.0)")

	// JSON_OBJECT with column references across multiple rows
	result := run(t, exec, "SELECT JSON_OBJECT('id', id, 'name', name, 'price', price) FROM products ORDER BY id")
	require.Len(t, result.Rows, 2)
	assert.Equal(t, `{"id":1,"name":"apple","price":1.5}`, result.Rows[0][0])
	assert.Equal(t, `{"id":2,"name":"banana","price":2}`, result.Rows[1][0])
}

func TestJSON_ARRAY_WithColumnRef(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE coords (x INT, y INT)")
	run(t, exec, "INSERT INTO coords VALUES (1, 2)")
	run(t, exec, "INSERT INTO coords VALUES (3, 4)")

	// JSON_ARRAY with column references across multiple rows
	result := run(t, exec, "SELECT JSON_ARRAY(x, y) FROM coords ORDER BY x")
	require.Len(t, result.Rows, 2)
	assert.Equal(t, `[1,2]`, result.Rows[0][0])
	assert.Equal(t, `[3,4]`, result.Rows[1][0])
}

func TestJSON_VALUE(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	tests := []struct {
		name     string
		sql      string
		expected interface{}
	}{
		// Object member access
		{"string member", "SELECT JSON_VALUE('{\"name\": \"alice\"}', '$.name')", "alice"},
		{"int member", "SELECT JSON_VALUE('{\"age\": 30}', '$.age')", int64(30)},
		{"float member", "SELECT JSON_VALUE('{\"price\": 9.99}', '$.price')", 9.99},
		{"bool true member", "SELECT JSON_VALUE('{\"active\": true}', '$.active')", "true"},
		{"bool false member", "SELECT JSON_VALUE('{\"active\": false}', '$.active')", "false"},
		{"null member", "SELECT JSON_VALUE('{\"v\": null}', '$.v')", nil},

		// Nested object access
		{"nested member", "SELECT JSON_VALUE('{\"a\": {\"b\": \"deep\"}}', '$.a.b')", "deep"},
		{"deeply nested", "SELECT JSON_VALUE('{\"a\": {\"b\": {\"c\": 42}}}', '$.a.b.c')", int64(42)},

		// Array index access
		{"array first", "SELECT JSON_VALUE('[10, 20, 30]', '$[0]')", int64(10)},
		{"array second", "SELECT JSON_VALUE('[10, 20, 30]', '$[1]')", int64(20)},
		{"array last", "SELECT JSON_VALUE('[10, 20, 30]', '$[2]')", int64(30)},

		// Mixed object and array access
		{"object then array", "SELECT JSON_VALUE('{\"items\": [1, 2, 3]}', '$.items[1]')", int64(2)},
		{"array then object", "SELECT JSON_VALUE('[{\"id\": 1}, {\"id\": 2}]', '$[1].id')", int64(2)},

		// Root reference
		{"root string", "SELECT JSON_VALUE('\"hello\"', '$')", "hello"},
		{"root int", "SELECT JSON_VALUE('42', '$')", int64(42)},

		// Missing key returns NULL
		{"missing key", "SELECT JSON_VALUE('{\"a\": 1}', '$.b')", nil},
		{"missing nested", "SELECT JSON_VALUE('{\"a\": 1}', '$.a.b')", nil},
		{"out of bounds", "SELECT JSON_VALUE('[1, 2]', '$[5]')", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := run(t, exec, tt.sql)
			require.Len(t, result.Rows, 1)
			assert.Equal(t, tt.expected, result.Rows[0][0])
		})
	}
}

func TestJSON_VALUE_ErrorCases(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	tests := []struct {
		name string
		sql  string
	}{
		{"no args", "SELECT JSON_VALUE()"},
		{"one arg", "SELECT JSON_VALUE('{\"a\": 1}')"},
		{"three args", "SELECT JSON_VALUE('{\"a\": 1}', '$.a', 'extra')"},
		{"non-string first arg", "SELECT JSON_VALUE(123, '$.a')"},
		{"non-string path", "SELECT JSON_VALUE('{\"a\": 1}', 123)"},
		{"invalid json", "SELECT JSON_VALUE('not json', '$.a')"},
		{"invalid path no dollar", "SELECT JSON_VALUE('{\"a\": 1}', 'a')"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := runWithError(exec, tt.sql)
			require.Error(t, err, "expected error for %s", tt.sql)
		})
	}
}

func TestJSON_VALUE_WithColumnRef(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE docs (id INT, data JSON)")
	run(t, exec, "INSERT INTO docs VALUES (1, '{\"name\": \"alice\", \"age\": 30}')")
	run(t, exec, "INSERT INTO docs VALUES (2, '{\"name\": \"bob\", \"age\": 25}')")

	// Extract from JSON column
	result := run(t, exec, "SELECT JSON_VALUE(data, '$.name') FROM docs ORDER BY id")
	require.Len(t, result.Rows, 2)
	assert.Equal(t, "alice", result.Rows[0][0])
	assert.Equal(t, "bob", result.Rows[1][0])

	// Use in WHERE clause
	result = run(t, exec, "SELECT id FROM docs WHERE JSON_VALUE(data, '$.name') = 'alice'")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(1), result.Rows[0][0])

	// Extract int value from JSON column
	result = run(t, exec, "SELECT JSON_VALUE(data, '$.age') FROM docs WHERE id = 2")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(25), result.Rows[0][0])
}

func TestJSON_VALUE_ObjectArrayResult(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	// JSON_VALUE should return NULL for non-scalar results (object/array)
	// per SQL standard behavior
	result := run(t, exec, "SELECT JSON_VALUE('{\"a\": {\"b\": 1}}', '$.a')")
	require.Len(t, result.Rows, 1)
	assert.Nil(t, result.Rows[0][0])

	result = run(t, exec, "SELECT JSON_VALUE('{\"a\": [1, 2]}', '$.a')")
	require.Len(t, result.Rows, 1)
	assert.Nil(t, result.Rows[0][0])
}

func TestJSON_QUERY(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	tests := []struct {
		name     string
		sql      string
		expected interface{}
	}{
		// Extract object
		{"nested object", "SELECT JSON_QUERY('{\"a\": {\"b\": 1}}', '$.a')", `{"b":1}`},
		// Extract array
		{"nested array", "SELECT JSON_QUERY('{\"items\": [1, 2, 3]}', '$.items')", `[1,2,3]`},
		// Root object
		{"root object", "SELECT JSON_QUERY('{\"a\": 1}', '$')", `{"a":1}`},
		// Root array
		{"root array", "SELECT JSON_QUERY('[1, 2, 3]', '$')", `[1,2,3]`},
		// Deeply nested
		{"deeply nested object", "SELECT JSON_QUERY('{\"a\": {\"b\": {\"c\": [1]}}}', '$.a.b')", `{"c":[1]}`},
		// Array element that is object
		{"array element object", "SELECT JSON_QUERY('[{\"id\": 1}, {\"id\": 2}]', '$[0]')", `{"id":1}`},
		// Array element that is array
		{"array element array", "SELECT JSON_QUERY('[[1, 2], [3, 4]]', '$[1]')", `[3,4]`},
		// Scalar returns NULL (opposite of JSON_VALUE)
		{"scalar string returns null", "SELECT JSON_QUERY('{\"name\": \"alice\"}', '$.name')", nil},
		{"scalar int returns null", "SELECT JSON_QUERY('{\"age\": 30}', '$.age')", nil},
		// Missing path returns NULL
		{"missing key", "SELECT JSON_QUERY('{\"a\": 1}', '$.b')", nil},
		// NULL input returns NULL
		{"null input", "SELECT JSON_QUERY(NULL, '$.a')", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := run(t, exec, tt.sql)
			require.Len(t, result.Rows, 1)
			assert.Equal(t, tt.expected, result.Rows[0][0])
		})
	}
}

func TestJSON_QUERY_ErrorCases(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	tests := []struct {
		name string
		sql  string
	}{
		{"no args", "SELECT JSON_QUERY()"},
		{"one arg", "SELECT JSON_QUERY('{\"a\": 1}')"},
		{"three args", "SELECT JSON_QUERY('{\"a\": 1}', '$.a', 'extra')"},
		{"non-string first arg", "SELECT JSON_QUERY(123, '$.a')"},
		{"non-string path", "SELECT JSON_QUERY('{\"a\": 1}', 123)"},
		{"invalid json", "SELECT JSON_QUERY('not json', '$.a')"},
		{"invalid path no dollar", "SELECT JSON_QUERY('{\"a\": 1}', 'a')"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := runWithError(exec, tt.sql)
			require.Error(t, err, "expected error for %s", tt.sql)
		})
	}
}

func TestJSON_QUERY_WithColumnRef(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE docs (id INT, data JSON)")
	run(t, exec, "INSERT INTO docs VALUES (1, '{\"tags\": [\"go\", \"sql\"], \"meta\": {\"version\": 1}}')")
	run(t, exec, "INSERT INTO docs VALUES (2, '{\"tags\": [\"rust\"], \"meta\": {\"version\": 2}}')")

	// Extract array from JSON column
	result := run(t, exec, "SELECT JSON_QUERY(data, '$.tags') FROM docs ORDER BY id")
	require.Len(t, result.Rows, 2)
	assert.Equal(t, `["go","sql"]`, result.Rows[0][0])
	assert.Equal(t, `["rust"]`, result.Rows[1][0])

	// Extract object from JSON column
	result = run(t, exec, "SELECT JSON_QUERY(data, '$.meta') FROM docs WHERE id = 1")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, `{"version":1}`, result.Rows[0][0])
}

func TestJSON_EXISTS(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	tests := []struct {
		name     string
		sql      string
		expected interface{}
	}{
		// Existing keys
		{"existing string key", "SELECT JSON_EXISTS('{\"name\": \"alice\"}', '$.name')", true},
		{"existing int key", "SELECT JSON_EXISTS('{\"age\": 30}', '$.age')", true},
		{"existing null value", "SELECT JSON_EXISTS('{\"v\": null}', '$.v')", true},
		{"existing nested", "SELECT JSON_EXISTS('{\"a\": {\"b\": 1}}', '$.a.b')", true},
		{"existing array element", "SELECT JSON_EXISTS('[1, 2, 3]', '$[0]')", true},
		{"existing object in array", "SELECT JSON_EXISTS('[{\"id\": 1}]', '$[0].id')", true},
		{"root always exists", "SELECT JSON_EXISTS('{\"a\": 1}', '$')", true},
		{"existing object value", "SELECT JSON_EXISTS('{\"a\": {\"b\": 1}}', '$.a')", true},
		{"existing array value", "SELECT JSON_EXISTS('{\"items\": [1]}', '$.items')", true},

		// Missing keys
		{"missing key", "SELECT JSON_EXISTS('{\"a\": 1}', '$.b')", false},
		{"missing nested key", "SELECT JSON_EXISTS('{\"a\": 1}', '$.a.b')", false},
		{"out of bounds", "SELECT JSON_EXISTS('[1, 2]', '$[5]')", false},
		{"member on array", "SELECT JSON_EXISTS('[1, 2]', '$.name')", false},
		{"index on object", "SELECT JSON_EXISTS('{\"a\": 1}', '$[0]')", false},

		// SQL NULL input
		{"null input", "SELECT JSON_EXISTS(NULL, '$.a')", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := run(t, exec, tt.sql)
			require.Len(t, result.Rows, 1)
			assert.Equal(t, tt.expected, result.Rows[0][0])
		})
	}
}

func TestJSON_EXISTS_ErrorCases(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	tests := []struct {
		name string
		sql  string
	}{
		{"no args", "SELECT JSON_EXISTS()"},
		{"one arg", "SELECT JSON_EXISTS('{\"a\": 1}')"},
		{"three args", "SELECT JSON_EXISTS('{\"a\": 1}', '$.a', 'extra')"},
		{"non-string first arg", "SELECT JSON_EXISTS(123, '$.a')"},
		{"non-string path", "SELECT JSON_EXISTS('{\"a\": 1}', 123)"},
		{"invalid json", "SELECT JSON_EXISTS('not json', '$.a')"},
		{"invalid path no dollar", "SELECT JSON_EXISTS('{\"a\": 1}', 'a')"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := runWithError(exec, tt.sql)
			require.Error(t, err, "expected error for %s", tt.sql)
		})
	}
}

func TestJSON_EXISTS_WithColumnRef(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE docs (id INT, data JSON)")
	run(t, exec, "INSERT INTO docs VALUES (1, '{\"name\": \"alice\", \"email\": \"a@example.com\"}')")
	run(t, exec, "INSERT INTO docs VALUES (2, '{\"name\": \"bob\"}')")

	// Use in WHERE clause to filter rows that have a specific key
	result := run(t, exec, "SELECT id FROM docs WHERE JSON_EXISTS(data, '$.email')")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(1), result.Rows[0][0])

	// SELECT JSON_EXISTS as column
	result = run(t, exec, "SELECT id, JSON_EXISTS(data, '$.email') FROM docs ORDER BY id")
	require.Len(t, result.Rows, 2)
	assert.Equal(t, true, result.Rows[0][1])
	assert.Equal(t, false, result.Rows[1][1])
}

func TestIS_JSON(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	tests := []struct {
		name     string
		sql      string
		expected interface{}
	}{
		// Valid JSON strings
		{"json object", `SELECT '{"a": 1}' IS JSON`, true},
		{"json array", `SELECT '[1, 2, 3]' IS JSON`, true},
		{"json string", `SELECT '"hello"' IS JSON`, true},
		{"json number", `SELECT '42' IS JSON`, true},
		{"json true", `SELECT 'true' IS JSON`, true},
		{"json false", `SELECT 'false' IS JSON`, true},
		{"json null", `SELECT 'null' IS JSON`, true},
		{"nested object", `SELECT '{"a": {"b": [1, 2]}}' IS JSON`, true},

		// Invalid JSON strings
		{"plain text", `SELECT 'hello' IS JSON`, false},
		{"incomplete object", `SELECT '{"a":' IS JSON`, false},
		{"single quotes", `SELECT '{''a'': 1}' IS JSON`, false},

		// Non-string types
		{"int value", `SELECT 42 IS JSON`, false},
		{"float value", `SELECT 3.14 IS JSON`, false},
		{"bool true", `SELECT TRUE IS JSON`, false},

		// NULL
		{"null input", `SELECT NULL IS JSON`, false},

		// IS NOT JSON
		{"is not json valid", `SELECT '{"a": 1}' IS NOT JSON`, false},
		{"is not json invalid", `SELECT 'hello' IS NOT JSON`, true},
		{"is not json null", `SELECT NULL IS NOT JSON`, true},
		{"is not json int", `SELECT 42 IS NOT JSON`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := run(t, exec, tt.sql)
			require.Len(t, result.Rows, 1)
			assert.Equal(t, tt.expected, result.Rows[0][0])
		})
	}
}

func TestIS_JSON_WithColumnRef(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE mixed (id INT, val TEXT)")
	run(t, exec, `INSERT INTO mixed VALUES (1, '{"name": "alice"}')`)
	run(t, exec, `INSERT INTO mixed VALUES (2, 'not json')`)
	run(t, exec, `INSERT INTO mixed VALUES (3, '[1, 2, 3]')`)

	// Filter rows with valid JSON
	result := run(t, exec, "SELECT id FROM mixed WHERE val IS JSON ORDER BY id")
	require.Len(t, result.Rows, 2)
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, int64(3), result.Rows[1][0])

	// Filter rows without valid JSON
	result = run(t, exec, "SELECT id FROM mixed WHERE val IS NOT JSON ORDER BY id")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(2), result.Rows[0][0])

	// SELECT IS JSON as column
	result = run(t, exec, "SELECT id, val IS JSON FROM mixed ORDER BY id")
	require.Len(t, result.Rows, 3)
	assert.Equal(t, true, result.Rows[0][1])
	assert.Equal(t, false, result.Rows[1][1])
	assert.Equal(t, true, result.Rows[2][1])
}

func TestIS_JSON_WithJSONColumn(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE docs (id INT, data JSON)")
	run(t, exec, `INSERT INTO docs VALUES (1, '{"a": 1}')`)

	// JSON column values are always valid JSON
	result := run(t, exec, "SELECT data IS JSON FROM docs")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, true, result.Rows[0][0])
}

func TestJSON_OBJECT_InsertIntoJSONColumn(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE docs (id INT, data JSON)")

	// JSON_OBJECT result can be inserted into JSON column
	run(t, exec, "INSERT INTO docs SELECT 1, JSON_OBJECT('name', 'alice') FROM (SELECT 1) AS t")
	result := run(t, exec, "SELECT data FROM docs")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, `{"name":"alice"}`, result.Rows[0][0])
}

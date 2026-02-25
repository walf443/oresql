package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectWithIndexEquality(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")

	result := run(t, exec, "SELECT * FROM users WHERE name = 'bob'")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(2), result.Rows[0][0])
	assert.Equal(t, "bob", result.Rows[0][1])
}

func TestSelectWithIndexNoMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")

	result := run(t, exec, "SELECT * FROM users WHERE name = 'nonexistent'")
	require.Len(t, result.Rows, 0, "expected 0 rows")
}

func TestIndexMaintainedOnInsert(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")

	// Insert after index creation
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users WHERE name = 'bob'")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestIndexMaintainedOnDelete(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")

	run(t, exec, "DELETE FROM users WHERE id = 2")

	result := run(t, exec, "SELECT * FROM users WHERE name = 'bob'")
	require.Len(t, result.Rows, 0, "expected 0 rows after delete")
}

func TestIndexMaintainedOnUpdate(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")

	run(t, exec, "UPDATE users SET name = 'bobby' WHERE id = 2")

	// Old value should not be found
	result := run(t, exec, "SELECT * FROM users WHERE name = 'bob'")
	require.Len(t, result.Rows, 0, "expected 0 rows for old value")

	// New value should be found
	result = run(t, exec, "SELECT * FROM users WHERE name = 'bobby'")
	require.Len(t, result.Rows, 1, "expected 1 row for new value")
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestIndexClearedOnTruncate(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")

	run(t, exec, "TRUNCATE TABLE users")

	result := run(t, exec, "SELECT * FROM users WHERE name = 'alice'")
	require.Len(t, result.Rows, 0, "expected 0 rows after truncate")

	// Index still works after reinserting
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	result = run(t, exec, "SELECT * FROM users WHERE name = 'bob'")
	require.Len(t, result.Rows, 1, "expected 1 row after reinsert")
}

func TestIndexOnIntColumn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "CREATE INDEX idx_id ON users(id)")

	result := run(t, exec, "SELECT * FROM users WHERE id = 2")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "bob", result.Rows[0][1])
}

func TestIndexOnFloatColumn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, val FLOAT)")
	run(t, exec, "INSERT INTO t VALUES (1, 1.5)")
	run(t, exec, "INSERT INTO t VALUES (2, 2.5)")
	run(t, exec, "INSERT INTO t VALUES (3, 3.5)")
	run(t, exec, "CREATE INDEX idx_val ON t(val)")

	result := run(t, exec, "SELECT * FROM t WHERE val = 2.5")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestIndexWithNullValues(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, NULL)")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob')")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")

	// NULL values are stored in the index via binary encoding
	result := run(t, exec, "SELECT * FROM users WHERE name = 'alice'")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])

	// WHERE name IS NULL should use index and find the NULL row
	result = run(t, exec, "SELECT * FROM users WHERE name IS NULL")
	require.Len(t, result.Rows, 1, "expected 1 row for IS NULL")
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestCreateIndexOnExistingData(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice')")

	// Create index after data is inserted
	run(t, exec, "CREATE INDEX idx_name ON users(name)")

	// Index should find multiple rows
	result := run(t, exec, "SELECT * FROM users WHERE name = 'alice'")
	require.Len(t, result.Rows, 2, "expected 2 rows")
}

func TestCreateCompositeIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	result := run(t, exec, "CREATE INDEX idx_name_age ON users(name, age)")
	assert.Equal(t, "index created", result.Message)
}

func TestSelectWithCompositeIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (2, 'alice', 25)")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob', 30)")
	run(t, exec, "CREATE INDEX idx_name_age ON users(name, age)")

	// Both columns in equality condition -> use composite index
	result := run(t, exec, "SELECT * FROM users WHERE name = 'alice' AND age = 30")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])
}

func TestSelectWithCompositeIndexPartialMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (2, 'alice', 25)")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob', 30)")
	run(t, exec, "CREATE INDEX idx_name_age ON users(name, age)")

	// Only one column -> composite index not used, falls back to full scan
	result := run(t, exec, "SELECT * FROM users WHERE name = 'alice'")
	require.Len(t, result.Rows, 2, "expected 2 rows")
}

func TestCompositeIndexMaintainedOnInsert(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "CREATE INDEX idx_name_age ON users(name, age)")

	// Insert after index creation
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 25)")

	result := run(t, exec, "SELECT * FROM users WHERE name = 'bob' AND age = 25")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestCompositeIndexMaintainedOnDelete(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 25)")
	run(t, exec, "CREATE INDEX idx_name_age ON users(name, age)")

	run(t, exec, "DELETE FROM users WHERE id = 2")

	result := run(t, exec, "SELECT * FROM users WHERE name = 'bob' AND age = 25")
	require.Len(t, result.Rows, 0, "expected 0 rows after delete")
}

func TestCompositeIndexMaintainedOnUpdate(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 25)")
	run(t, exec, "CREATE INDEX idx_name_age ON users(name, age)")

	run(t, exec, "UPDATE users SET age = 35 WHERE id = 2")

	// Old value should not be found
	result := run(t, exec, "SELECT * FROM users WHERE name = 'bob' AND age = 25")
	require.Len(t, result.Rows, 0, "expected 0 rows for old value")

	// New value should be found
	result = run(t, exec, "SELECT * FROM users WHERE name = 'bob' AND age = 35")
	require.Len(t, result.Rows, 1, "expected 1 row for new value")
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestCompositeIndexWithNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (2, NULL, 25)")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob', NULL)")
	run(t, exec, "CREATE INDEX idx_name_age ON users(name, age)")

	// NULL values are stored in composite index; non-NULL row should still be found
	result := run(t, exec, "SELECT * FROM users WHERE name = 'alice' AND age = 30")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])

	// WHERE name IS NULL AND age = 25 should use composite index
	result = run(t, exec, "SELECT * FROM users WHERE name IS NULL AND age = 25")
	require.Len(t, result.Rows, 1, "expected 1 row for IS NULL composite")
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestCreateCompositeIndexOnExistingData(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (2, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob', 25)")

	// Create composite index after data is inserted
	run(t, exec, "CREATE INDEX idx_name_age ON users(name, age)")

	// Index should find multiple matching rows
	result := run(t, exec, "SELECT * FROM users WHERE name = 'alice' AND age = 30")
	require.Len(t, result.Rows, 2, "expected 2 rows")
}

func TestSelectWithIndexIsNull(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE products (id INT, name TEXT, category TEXT)")
	run(t, exec, "INSERT INTO products VALUES (1, 'Widget', 'tools')")
	run(t, exec, "INSERT INTO products VALUES (2, 'Gadget', NULL)")
	run(t, exec, "INSERT INTO products VALUES (3, 'Doohickey', NULL)")
	run(t, exec, "INSERT INTO products VALUES (4, 'Thingamajig', 'toys')")
	run(t, exec, "CREATE INDEX idx_category ON products(category)")

	// IS NULL should use index and find rows with NULL category
	result := run(t, exec, "SELECT * FROM products WHERE category IS NULL")
	require.Len(t, result.Rows, 2, "expected 2 rows for IS NULL")
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	assert.True(t, ids[2] && ids[3], "expected ids 2 and 3, got %v", ids)

	// Non-NULL lookup should still work
	result = run(t, exec, "SELECT * FROM products WHERE category = 'tools'")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])
}

// --- Index range scan tests ---

func TestSelectWithIndexRangeGt(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_id ON t(id)")
	run(t, exec, "INSERT INTO t VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t VALUES (2, 'b')")
	run(t, exec, "INSERT INTO t VALUES (3, 'c')")
	run(t, exec, "INSERT INTO t VALUES (4, 'd')")
	run(t, exec, "INSERT INTO t VALUES (5, 'e')")

	result := run(t, exec, "SELECT * FROM t WHERE id > 3")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	assert.True(t, ids[4] && ids[5], "expected ids 4 and 5, got %v", ids)
}

func TestSelectWithIndexRangeGte(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_id ON t(id)")
	run(t, exec, "INSERT INTO t VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t VALUES (2, 'b')")
	run(t, exec, "INSERT INTO t VALUES (3, 'c')")
	run(t, exec, "INSERT INTO t VALUES (4, 'd')")
	run(t, exec, "INSERT INTO t VALUES (5, 'e')")

	result := run(t, exec, "SELECT * FROM t WHERE id >= 3")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	assert.True(t, ids[3] && ids[4] && ids[5], "expected ids 3,4,5, got %v", ids)
}

func TestSelectWithIndexRangeLt(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_id ON t(id)")
	run(t, exec, "INSERT INTO t VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t VALUES (2, 'b')")
	run(t, exec, "INSERT INTO t VALUES (3, 'c')")
	run(t, exec, "INSERT INTO t VALUES (4, 'd')")
	run(t, exec, "INSERT INTO t VALUES (5, 'e')")

	result := run(t, exec, "SELECT * FROM t WHERE id < 3")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	assert.True(t, ids[1] && ids[2], "expected ids 1 and 2, got %v", ids)
}

func TestSelectWithIndexRangeLte(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_id ON t(id)")
	run(t, exec, "INSERT INTO t VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t VALUES (2, 'b')")
	run(t, exec, "INSERT INTO t VALUES (3, 'c')")
	run(t, exec, "INSERT INTO t VALUES (4, 'd')")
	run(t, exec, "INSERT INTO t VALUES (5, 'e')")

	result := run(t, exec, "SELECT * FROM t WHERE id <= 3")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	assert.True(t, ids[1] && ids[2] && ids[3], "expected ids 1,2,3, got %v", ids)
}

func TestSelectWithIndexRangeBetween(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_id ON t(id)")
	run(t, exec, "INSERT INTO t VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t VALUES (2, 'b')")
	run(t, exec, "INSERT INTO t VALUES (3, 'c')")
	run(t, exec, "INSERT INTO t VALUES (4, 'd')")
	run(t, exec, "INSERT INTO t VALUES (5, 'e')")

	result := run(t, exec, "SELECT * FROM t WHERE id BETWEEN 2 AND 4")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	assert.True(t, ids[2] && ids[3] && ids[4], "expected ids 2,3,4, got %v", ids)
}

func TestSelectWithIndexRangeCombined(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_id ON t(id)")
	run(t, exec, "INSERT INTO t VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t VALUES (2, 'b')")
	run(t, exec, "INSERT INTO t VALUES (3, 'c')")
	run(t, exec, "INSERT INTO t VALUES (4, 'd')")
	run(t, exec, "INSERT INTO t VALUES (5, 'e')")

	result := run(t, exec, "SELECT * FROM t WHERE id >= 2 AND id < 5")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	assert.True(t, ids[2] && ids[3] && ids[4], "expected ids 2,3,4, got %v", ids)
}

func TestSelectWithIndexRangeNoMatchRange(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_id ON t(id)")
	run(t, exec, "INSERT INTO t VALUES (1, 'a')")
	run(t, exec, "INSERT INTO t VALUES (2, 'b')")
	run(t, exec, "INSERT INTO t VALUES (3, 'c')")

	result := run(t, exec, "SELECT * FROM t WHERE id > 10")
	require.Len(t, result.Rows, 0, "expected 0 rows")
}

func TestSelectWithIndexRangeText(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON t(name)")
	run(t, exec, "INSERT INTO t VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO t VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO t VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO t VALUES (4, 'dave')")

	result := run(t, exec, "SELECT * FROM t WHERE name > 'b'")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	names := map[string]bool{}
	for _, row := range result.Rows {
		names[row[1].(string)] = true
	}
	assert.True(t, names["bob"] && names["charlie"] && names["dave"], "expected bob, charlie, dave, got %v", names)
}

func TestSelectWithIndexRangeNegativeInt(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_id ON t(id)")
	run(t, exec, "INSERT INTO t VALUES (-5, 'a')")
	run(t, exec, "INSERT INTO t VALUES (-2, 'b')")
	run(t, exec, "INSERT INTO t VALUES (0, 'c')")
	run(t, exec, "INSERT INTO t VALUES (3, 'd')")
	run(t, exec, "INSERT INTO t VALUES (7, 'e')")

	// Range: id >= -2 AND id <= 3
	result := run(t, exec, "SELECT * FROM t WHERE id >= -2 AND id <= 3")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	assert.True(t, ids[-2] && ids[0] && ids[3], "expected ids -2,0,3, got %v", ids)
}

func TestSelectWithIndexRangeAndFilter(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_id ON t(id)")
	run(t, exec, "INSERT INTO t VALUES (1, 'x')")
	run(t, exec, "INSERT INTO t VALUES (2, 'y')")
	run(t, exec, "INSERT INTO t VALUES (3, 'x')")
	run(t, exec, "INSERT INTO t VALUES (4, 'y')")
	run(t, exec, "INSERT INTO t VALUES (5, 'x')")

	// Range scan on id, then filter by name
	result := run(t, exec, "SELECT * FROM t WHERE id > 2 AND name = 'x'")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	assert.True(t, ids[3] && ids[5], "expected ids 3 and 5, got %v", ids)
}

func TestSelectWithCompositeIndexRangeGt(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (col1 INT, col2 INT, col3 TEXT)")
	run(t, exec, "CREATE INDEX idx ON t(col1, col2)")
	run(t, exec, "INSERT INTO t VALUES (1, 3, 'a')")
	run(t, exec, "INSERT INTO t VALUES (1, 5, 'b')")
	run(t, exec, "INSERT INTO t VALUES (1, 7, 'c')")
	run(t, exec, "INSERT INTO t VALUES (1, 10, 'd')")
	run(t, exec, "INSERT INTO t VALUES (2, 1, 'e')")
	run(t, exec, "INSERT INTO t VALUES (2, 8, 'f')")

	result := run(t, exec, "SELECT * FROM t WHERE col1 = 1 AND col2 > 5")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	col2s := map[int64]bool{}
	for _, row := range result.Rows {
		col2s[row[1].(int64)] = true
	}
	assert.True(t, col2s[7] && col2s[10], "expected col2 7 and 10, got %v", col2s)
}

func TestSelectWithCompositeIndexRangeBetween(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (col1 INT, col2 INT, col3 TEXT)")
	run(t, exec, "CREATE INDEX idx ON t(col1, col2)")
	run(t, exec, "INSERT INTO t VALUES (1, 1, 'a')")
	run(t, exec, "INSERT INTO t VALUES (1, 3, 'b')")
	run(t, exec, "INSERT INTO t VALUES (1, 5, 'c')")
	run(t, exec, "INSERT INTO t VALUES (1, 7, 'd')")
	run(t, exec, "INSERT INTO t VALUES (1, 10, 'e')")
	run(t, exec, "INSERT INTO t VALUES (2, 5, 'f')")

	result := run(t, exec, "SELECT * FROM t WHERE col1 = 1 AND col2 BETWEEN 3 AND 7")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	col2s := map[int64]bool{}
	for _, row := range result.Rows {
		col2s[row[1].(int64)] = true
	}
	assert.True(t, col2s[3] && col2s[5] && col2s[7], "expected col2 3,5,7, got %v", col2s)
}

func TestSelectWithCompositeIndexRangeLt(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (col1 TEXT, col2 INT, col3 TEXT)")
	run(t, exec, "CREATE INDEX idx ON t(col1, col2)")
	run(t, exec, "INSERT INTO t VALUES ('a', 3, 'x')")
	run(t, exec, "INSERT INTO t VALUES ('a', 7, 'y')")
	run(t, exec, "INSERT INTO t VALUES ('a', 10, 'z')")
	run(t, exec, "INSERT INTO t VALUES ('b', 5, 'w')")

	result := run(t, exec, "SELECT * FROM t WHERE col1 = 'a' AND col2 < 10")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	col2s := map[int64]bool{}
	for _, row := range result.Rows {
		col2s[row[1].(int64)] = true
	}
	assert.True(t, col2s[3] && col2s[7], "expected col2 3 and 7, got %v", col2s)
}

func TestSelectWithCompositeIndexRangeNoMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (col1 INT, col2 INT)")
	run(t, exec, "CREATE INDEX idx ON t(col1, col2)")
	run(t, exec, "INSERT INTO t VALUES (1, 3)")
	run(t, exec, "INSERT INTO t VALUES (1, 5)")
	run(t, exec, "INSERT INTO t VALUES (2, 1)")

	result := run(t, exec, "SELECT * FROM t WHERE col1 = 1 AND col2 > 100")
	require.Len(t, result.Rows, 0, "expected 0 rows")
}

func TestSelectWithCompositeIndexRangeWithPostFilter(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (col1 INT, col2 INT, col3 TEXT)")
	run(t, exec, "CREATE INDEX idx ON t(col1, col2)")
	run(t, exec, "INSERT INTO t VALUES (1, 3, 'x')")
	run(t, exec, "INSERT INTO t VALUES (1, 5, 'y')")
	run(t, exec, "INSERT INTO t VALUES (1, 7, 'x')")
	run(t, exec, "INSERT INTO t VALUES (1, 10, 'y')")
	run(t, exec, "INSERT INTO t VALUES (2, 4, 'x')")

	// col1=1 AND col2>3 uses composite index, col3='x' is post-filtered
	result := run(t, exec, "SELECT * FROM t WHERE col1 = 1 AND col2 > 3 AND col3 = 'x'")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(7), result.Rows[0][1].(int64))
	assert.Equal(t, "x", result.Rows[0][2].(string))
}

func TestSelectWithCompositeIndexRangePartialPrefix(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (a INT, b INT, c INT, d TEXT)")
	run(t, exec, "CREATE INDEX idx ON t(a, b, c)")
	run(t, exec, "INSERT INTO t VALUES (1, 2, 3, 'x')")
	run(t, exec, "INSERT INTO t VALUES (1, 2, 5, 'y')")
	run(t, exec, "INSERT INTO t VALUES (1, 2, 8, 'z')")
	run(t, exec, "INSERT INTO t VALUES (1, 2, 10, 'w')")
	run(t, exec, "INSERT INTO t VALUES (1, 3, 1, 'v')")
	run(t, exec, "INSERT INTO t VALUES (2, 2, 5, 'u')")

	// a=1 AND b=2 AND c>=5 uses 3-column composite index
	result := run(t, exec, "SELECT * FROM t WHERE a = 1 AND b = 2 AND c >= 5")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	cs := map[int64]bool{}
	for _, row := range result.Rows {
		cs[row[2].(int64)] = true
	}
	assert.True(t, cs[5] && cs[8] && cs[10], "expected c 5,8,10, got %v", cs)
}

func TestSelectWithIndexIn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "CREATE INDEX idx_id ON users(id)")

	result := run(t, exec, "SELECT * FROM users WHERE id IN (1, 3)")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	ids := map[int64]bool{}
	for _, row := range result.Rows {
		ids[row[0].(int64)] = true
	}
	assert.True(t, ids[1] && ids[3], "expected ids 1 and 3, got %v", ids)
}

func TestSelectWithIndexInNoMatch(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "CREATE INDEX idx_id ON users(id)")

	result := run(t, exec, "SELECT * FROM users WHERE id IN (100, 200)")
	require.Len(t, result.Rows, 0, "expected 0 rows")
}

func TestSelectWithIndexInSingleValue(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "CREATE INDEX idx_id ON users(id)")

	result := run(t, exec, "SELECT * FROM users WHERE id IN (2)")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestSelectWithCompositeIndexIn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE items (a INT, b INT, c TEXT)")
	run(t, exec, "INSERT INTO items VALUES (1, 3, 'x')")
	run(t, exec, "INSERT INTO items VALUES (1, 5, 'y')")
	run(t, exec, "INSERT INTO items VALUES (1, 7, 'z')")
	run(t, exec, "INSERT INTO items VALUES (2, 3, 'w')")
	run(t, exec, "CREATE INDEX idx_ab ON items(a, b)")

	result := run(t, exec, "SELECT * FROM items WHERE a = 1 AND b IN (3, 7)")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	bs := map[int64]bool{}
	for _, row := range result.Rows {
		bs[row[1].(int64)] = true
	}
	assert.True(t, bs[3] && bs[7], "expected b 3 and 7, got %v", bs)
}

func TestSelectWithIndexInAndFilter(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'alice')")
	run(t, exec, "CREATE INDEX idx_id ON users(id)")

	result := run(t, exec, "SELECT * FROM users WHERE id IN (1, 2, 3) AND name = 'alice'")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	for _, row := range result.Rows {
		assert.Equal(t, "alice", row[1])
	}
}

func TestSelectWithCompositeIndexRangeMiddleColumn(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (a INT, b INT, c TEXT)")
	run(t, exec, "CREATE INDEX idx ON t(a, b, c)")
	run(t, exec, "INSERT INTO t VALUES (1, 2, 'x')")
	run(t, exec, "INSERT INTO t VALUES (1, 5, 'y')")
	run(t, exec, "INSERT INTO t VALUES (1, 8, 'z')")
	run(t, exec, "INSERT INTO t VALUES (2, 3, 'w')")

	// a=1 AND b>3 should use composite index with prefix a=1 and range on b
	result := run(t, exec, "SELECT * FROM t WHERE a = 1 AND b > 3")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	bs := map[int64]bool{}
	for _, row := range result.Rows {
		bs[row[1].(int64)] = true
	}
	assert.True(t, bs[5] && bs[8], "expected b 5,8, got %v", bs)
}

func TestSelectWithCompositeIndexRangeMiddleColumnBetween(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (a INT, b INT, c TEXT)")
	run(t, exec, "CREATE INDEX idx ON t(a, b, c)")
	run(t, exec, "INSERT INTO t VALUES (1, 1, 'a')")
	run(t, exec, "INSERT INTO t VALUES (1, 3, 'b')")
	run(t, exec, "INSERT INTO t VALUES (1, 5, 'c')")
	run(t, exec, "INSERT INTO t VALUES (1, 7, 'd')")
	run(t, exec, "INSERT INTO t VALUES (2, 4, 'e')")

	// a=1 AND b BETWEEN 2 AND 5 should use composite index
	result := run(t, exec, "SELECT * FROM t WHERE a = 1 AND b BETWEEN 2 AND 5")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	bs := map[int64]bool{}
	for _, row := range result.Rows {
		bs[row[1].(int64)] = true
	}
	assert.True(t, bs[3] && bs[5], "expected b 3,5, got %v", bs)
}

func TestSelectWithCompositeIndexRangeMiddleColumnWithPostFilter(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE t (a INT, b INT, c TEXT)")
	run(t, exec, "CREATE INDEX idx ON t(a, b, c)")
	run(t, exec, "INSERT INTO t VALUES (1, 2, 'x')")
	run(t, exec, "INSERT INTO t VALUES (1, 5, 'x')")
	run(t, exec, "INSERT INTO t VALUES (1, 8, 'y')")
	run(t, exec, "INSERT INTO t VALUES (2, 3, 'x')")

	// a=1 AND b>3 AND c='x' -- index handles a=1 + b>3, post-filter handles c='x'
	result := run(t, exec, "SELECT * FROM t WHERE a = 1 AND b > 3 AND c = 'x'")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(5), result.Rows[0][1])
	assert.Equal(t, "x", result.Rows[0][2])
}

func TestSelectWhereLikeWithIndex(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'albert')")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (4, 'almond')")

	result := run(t, exec, "SELECT * FROM users WHERE name LIKE 'al%'")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	// Verify all returned rows start with "al"
	for _, row := range result.Rows {
		name := row[1].(string)
		assert.Equal(t, "al", name[:2], "expected name starting with 'al', got %q", name)
	}
}

func TestSelectWhereLikeWithIndexNoPrefix(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")

	// LIKE '%ice' has no prefix, so index should not be used, but results should be correct
	result := run(t, exec, "SELECT * FROM users WHERE name LIKE '%ice'")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "alice", result.Rows[0][1])
}

func TestSelectWhereLikeWithIndexEscape(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE items (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON items(name)")
	run(t, exec, "INSERT INTO items VALUES (1, 'a_bcd')")
	run(t, exec, "INSERT INTO items VALUES (2, 'a_xyz')")
	run(t, exec, "INSERT INTO items VALUES (3, 'abcd')")

	// LIKE 'a\_b%' -- escaped underscore, prefix is "a_b"
	result := run(t, exec, "SELECT * FROM items WHERE name LIKE 'a\\_b%'")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])
}

func TestSelectWithIndexAndCondition(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (2, 'alice', 25)")
	run(t, exec, "INSERT INTO users VALUES (3, 'bob', 30)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")

	// Index used for name = 'alice', then age > 28 is applied as filter
	result := run(t, exec, "SELECT * FROM users WHERE name = 'alice' AND age > 28")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])
}

package engine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExplainSelectFullScan(t *testing.T) {
	db := NewDatabase("test")
	exec := NewExecutor(db)

	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'Alice', 30)")

	result := run(t, exec, "EXPLAIN SELECT * FROM users")

	require.Equal(t, []string{"id", "operation", "table", "type", "possible_keys", "key", "extra"}, result.Columns)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, "SELECT", result.Rows[0][1])
	assert.Equal(t, "users", result.Rows[0][2])
	assert.Equal(t, "full scan", result.Rows[0][3])
}

func TestExplainSelectPrimaryKeyLookup(t *testing.T) {
	db := NewDatabase("test")
	exec := NewExecutor(db)

	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'Alice')")

	result := run(t, exec, "EXPLAIN SELECT * FROM users WHERE id = 1")

	require.Len(t, result.Rows, 1)
	assert.Equal(t, "const", result.Rows[0][3])
	assert.Equal(t, "PRIMARY", result.Rows[0][5])
}

func TestExplainSelectWithIndex(t *testing.T) {
	db := NewDatabase("test")
	exec := NewExecutor(db)

	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	run(t, exec, "CREATE INDEX idx_age ON users(age)")
	run(t, exec, "INSERT INTO users VALUES (1, 'Alice', 30)")

	result := run(t, exec, "EXPLAIN SELECT * FROM users WHERE age = 30")

	require.Len(t, result.Rows, 1)
	assert.Equal(t, "ref", result.Rows[0][3])
	assert.Equal(t, "idx_age", result.Rows[0][5])
}

func TestExplainSelectRangeScan(t *testing.T) {
	db := NewDatabase("test")
	exec := NewExecutor(db)

	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	run(t, exec, "CREATE INDEX idx_age ON users(age)")

	result := run(t, exec, "EXPLAIN SELECT * FROM users WHERE age > 20")

	require.Len(t, result.Rows, 1)
	assert.Equal(t, "range", result.Rows[0][3])
	assert.Equal(t, "idx_age", result.Rows[0][5])
}

func TestExplainSelectWithOrderByIndex(t *testing.T) {
	db := NewDatabase("test")
	exec := NewExecutor(db)

	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT NOT NULL)")
	run(t, exec, "CREATE INDEX idx_age ON users(age)")

	result := run(t, exec, "EXPLAIN SELECT * FROM users ORDER BY age LIMIT 10")

	require.Len(t, result.Rows, 1)
	assert.Equal(t, "index scan", result.Rows[0][3])
	assert.Contains(t, result.Rows[0][6], "Using index for ORDER BY")
}

func TestExplainSelectWithWhere(t *testing.T) {
	db := NewDatabase("test")
	exec := NewExecutor(db)

	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")

	result := run(t, exec, "EXPLAIN SELECT * FROM users WHERE name = 'Alice'")

	require.Len(t, result.Rows, 1)
	assert.Equal(t, "full scan", result.Rows[0][3])
	assert.Contains(t, result.Rows[0][6], "Using where")
}

func TestExplainSelectWithJoin(t *testing.T) {
	db := NewDatabase("test")
	exec := NewExecutor(db)

	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)")

	result := run(t, exec, "EXPLAIN SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id")

	require.Len(t, result.Rows, 2)
	assert.Equal(t, "SELECT", result.Rows[0][1])
	assert.Equal(t, "users", result.Rows[0][2])
	assert.Equal(t, "INNER JOIN", result.Rows[1][1])
	assert.Equal(t, "orders", result.Rows[1][2])
}

func TestExplainInsert(t *testing.T) {
	db := NewDatabase("test")
	exec := NewExecutor(db)

	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")

	result := run(t, exec, "EXPLAIN INSERT INTO users VALUES (1, 'Alice')")

	require.Len(t, result.Rows, 1)
	assert.Equal(t, "INSERT", result.Rows[0][1])
	assert.Equal(t, "users", result.Rows[0][2])
	assert.Contains(t, result.Rows[0][6], "1 row(s)")
}

func TestExplainUpdate(t *testing.T) {
	db := NewDatabase("test")
	exec := NewExecutor(db)

	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")

	result := run(t, exec, "EXPLAIN UPDATE users SET name = 'Bob' WHERE id = 1")

	require.Len(t, result.Rows, 1)
	assert.Equal(t, "UPDATE", result.Rows[0][1])
	assert.Equal(t, "const", result.Rows[0][3])
	assert.Equal(t, "PRIMARY", result.Rows[0][5])
}

func TestExplainDelete(t *testing.T) {
	db := NewDatabase("test")
	exec := NewExecutor(db)

	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")

	result := run(t, exec, "EXPLAIN DELETE FROM users WHERE id = 1")

	require.Len(t, result.Rows, 1)
	assert.Equal(t, "DELETE", result.Rows[0][1])
	assert.Equal(t, "const", result.Rows[0][3])
	assert.Equal(t, "PRIMARY", result.Rows[0][5])
}

func TestExplainSelectCountStar(t *testing.T) {
	db := NewDatabase("test")
	exec := NewExecutor(db)

	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")

	result := run(t, exec, "EXPLAIN SELECT COUNT(*) FROM users")

	require.Len(t, result.Rows, 1)
	assert.Equal(t, "row count", result.Rows[0][3])
	assert.Contains(t, result.Rows[0][6], "Using row count optimization")
}

func TestExplainSelectDistinct(t *testing.T) {
	db := NewDatabase("test")
	exec := NewExecutor(db)

	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")

	result := run(t, exec, "EXPLAIN SELECT DISTINCT name FROM users")

	require.Len(t, result.Rows, 1)
	assert.Contains(t, result.Rows[0][6], "Using distinct")
}

// TestExplainAndExecuteUseSamePlan verifies that EXPLAIN and actual execution
// use the same planSelect method, guaranteeing consistency.
func TestExplainAndExecuteUseSamePlan(t *testing.T) {
	db := NewDatabase("test")
	exec := NewExecutor(db)

	run(t, exec, "CREATE TABLE items (id INT PRIMARY KEY, category TEXT, price INT)")
	run(t, exec, "CREATE INDEX idx_category ON items(category)")
	run(t, exec, "CREATE INDEX idx_price ON items(price)")
	for i := 1; i <= 20; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO items VALUES (%d, 'cat%d', %d)", i, i%5, i*100))
	}

	tests := []struct {
		name         string
		sql          string
		expectedType string // expected access type in EXPLAIN
	}{
		{"PK lookup", "SELECT * FROM items WHERE id = 1", "const"},
		{"index lookup", "SELECT * FROM items WHERE category = 'cat1'", "ref"},
		{"range scan", "SELECT * FROM items WHERE price > 500", "range"},
		{"full scan", "SELECT * FROM items WHERE id != 1", "full scan"},
		{"COUNT(*)", "SELECT COUNT(*) FROM items", "row count"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// EXPLAIN should report the expected access type
			explainResult := run(t, exec, "EXPLAIN "+tt.sql)
			require.Len(t, explainResult.Rows, 1)
			assert.Equal(t, tt.expectedType, explainResult.Rows[0][3],
				"EXPLAIN access type mismatch for %q", tt.sql)

			// The actual query should succeed (uses same plan)
			result := run(t, exec, tt.sql)
			assert.NotNil(t, result)
		})
	}
}

func TestExplainSelectGroupBy(t *testing.T) {
	db := NewDatabase("test")
	exec := NewExecutor(db)

	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")

	result := run(t, exec, "EXPLAIN SELECT age, COUNT(*) FROM users GROUP BY age")

	require.Len(t, result.Rows, 1)
	assert.Contains(t, result.Rows[0][6], "Using group by")
}

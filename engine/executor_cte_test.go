package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCTEBasic(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	setup := []string{
		"CREATE TABLE users (id INT, name TEXT, age INT)",
		"INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)",
	}
	for _, sql := range setup {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}

	tests := []struct {
		name     string
		sql      string
		wantRows int
		wantCols []string
		wantData [][]interface{}
	}{
		{
			name:     "basic CTE",
			sql:      "WITH t AS (SELECT id, name FROM users) SELECT * FROM t",
			wantRows: 3,
			wantCols: []string{"id", "name"},
			wantData: [][]interface{}{{int64(1), "Alice"}, {int64(2), "Bob"}, {int64(3), "Charlie"}},
		},
		{
			name:     "CTE with WHERE in body",
			sql:      "WITH t AS (SELECT id, name, age FROM users) SELECT * FROM t WHERE age > 28",
			wantRows: 2,
			wantCols: []string{"id", "name", "age"},
			wantData: [][]interface{}{{int64(1), "Alice", int64(30)}, {int64(3), "Charlie", int64(35)}},
		},
		{
			name:     "CTE with WHERE in CTE query",
			sql:      "WITH t AS (SELECT id, name FROM users WHERE id > 1) SELECT * FROM t",
			wantRows: 2,
			wantCols: []string{"id", "name"},
			wantData: [][]interface{}{{int64(2), "Bob"}, {int64(3), "Charlie"}},
		},
		{
			name:     "CTE with ORDER BY in body",
			sql:      "WITH t AS (SELECT id, name FROM users) SELECT * FROM t ORDER BY id DESC",
			wantRows: 3,
			wantCols: []string{"id", "name"},
			wantData: [][]interface{}{{int64(3), "Charlie"}, {int64(2), "Bob"}, {int64(1), "Alice"}},
		},
		{
			name:     "CTE with LIMIT",
			sql:      "WITH t AS (SELECT id, name FROM users) SELECT * FROM t ORDER BY id LIMIT 2",
			wantRows: 2,
			wantCols: []string{"id", "name"},
			wantData: [][]interface{}{{int64(1), "Alice"}, {int64(2), "Bob"}},
		},
		{
			name:     "CTE with aggregate",
			sql:      "WITH t AS (SELECT id, age FROM users) SELECT COUNT(*), SUM(age) FROM t",
			wantRows: 1,
			wantCols: []string{"COUNT(*)", "SUM(age)"},
			wantData: [][]interface{}{{int64(3), int64(90)}},
		},
		{
			name:     "CTE with specific columns",
			sql:      "WITH t AS (SELECT id, name FROM users) SELECT name FROM t WHERE id = 1",
			wantRows: 1,
			wantCols: []string{"name"},
			wantData: [][]interface{}{{"Alice"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := e.ExecuteSQL(tt.sql)
			require.NoError(t, err, "ExecuteSQL(%q)", tt.sql)
			assert.Len(t, result.Rows, tt.wantRows, "row count")
			if tt.wantCols != nil {
				require.Len(t, result.Columns, len(tt.wantCols), "column count")
				for i, col := range tt.wantCols {
					assert.Equal(t, col, result.Columns[i], "column[%d]", i)
				}
			}
			if tt.wantData != nil {
				for i, wantRow := range tt.wantData {
					if i >= len(result.Rows) {
						break
					}
					for j, wantVal := range wantRow {
						if j < len(result.Rows[i]) {
							assert.Equal(t, wantVal, result.Rows[i][j], "row[%d][%d]", i, j)
						}
					}
				}
			}
		})
	}
}

func TestCTEMultiple(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	setup := []string{
		"CREATE TABLE orders (id INT, user_id INT, amount INT)",
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
		"INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)",
	}
	for _, sql := range setup {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}

	tests := []struct {
		name     string
		sql      string
		wantRows int
		wantCols []string
		wantData [][]interface{}
	}{
		{
			name: "multiple CTEs",
			sql: `WITH u AS (SELECT id, name FROM users),
			           o AS (SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id)
			      SELECT u.name, o.total FROM u JOIN o ON u.id = o.user_id ORDER BY u.name`,
			wantRows: 2,
			wantCols: []string{"name", "total"},
			wantData: [][]interface{}{{"Alice", int64(300)}, {"Bob", int64(150)}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := e.ExecuteSQL(tt.sql)
			require.NoError(t, err, "ExecuteSQL(%q)", tt.sql)
			assert.Len(t, result.Rows, tt.wantRows, "row count")
			if tt.wantCols != nil {
				require.Len(t, result.Columns, len(tt.wantCols), "column count")
				for i, col := range tt.wantCols {
					assert.Equal(t, col, result.Columns[i], "column[%d]", i)
				}
			}
			if tt.wantData != nil {
				for i, wantRow := range tt.wantData {
					if i >= len(result.Rows) {
						break
					}
					for j, wantVal := range wantRow {
						if j < len(result.Rows[i]) {
							assert.Equal(t, wantVal, result.Rows[i][j], "row[%d][%d]", i, j)
						}
					}
				}
			}
		})
	}
}

func TestCTEWithJoin(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	setup := []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"CREATE TABLE orders (id INT, user_id INT, amount INT)",
		"INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
		"INSERT INTO orders VALUES (1, 1, 100), (2, 2, 200)",
	}
	for _, sql := range setup {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}

	tests := []struct {
		name     string
		sql      string
		wantRows int
		wantCols []string
		wantData [][]interface{}
	}{
		{
			name:     "CTE joined with real table",
			sql:      "WITH o AS (SELECT user_id, amount FROM orders) SELECT u.name, o.amount FROM users AS u JOIN o ON u.id = o.user_id ORDER BY u.name",
			wantRows: 2,
			wantCols: []string{"name", "amount"},
			wantData: [][]interface{}{{"Alice", int64(100)}, {"Bob", int64(200)}},
		},
		{
			name:     "real table joined with CTE",
			sql:      "WITH u AS (SELECT id, name FROM users) SELECT u.name, orders.amount FROM u JOIN orders ON u.id = orders.user_id ORDER BY u.name",
			wantRows: 2,
			wantCols: []string{"name", "amount"},
			wantData: [][]interface{}{{"Alice", int64(100)}, {"Bob", int64(200)}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := e.ExecuteSQL(tt.sql)
			require.NoError(t, err, "ExecuteSQL(%q)", tt.sql)
			assert.Len(t, result.Rows, tt.wantRows, "row count")
			if tt.wantCols != nil {
				require.Len(t, result.Columns, len(tt.wantCols), "column count")
				for i, col := range tt.wantCols {
					assert.Equal(t, col, result.Columns[i], "column[%d]", i)
				}
			}
			if tt.wantData != nil {
				for i, wantRow := range tt.wantData {
					if i >= len(result.Rows) {
						break
					}
					for j, wantVal := range wantRow {
						if j < len(result.Rows[i]) {
							assert.Equal(t, wantVal, result.Rows[i][j], "row[%d][%d]", i, j)
						}
					}
				}
			}
		})
	}
}

func TestCTEWithUnionBody(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	setup := []string{
		"CREATE TABLE t1 (id INT, val TEXT)",
		"CREATE TABLE t2 (id INT, val TEXT)",
		"INSERT INTO t1 VALUES (1, 'a'), (2, 'b')",
		"INSERT INTO t2 VALUES (3, 'c'), (4, 'd')",
	}
	for _, sql := range setup {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}

	tests := []struct {
		name     string
		sql      string
		wantRows int
		wantCols []string
		wantData [][]interface{}
	}{
		{
			name:     "CTE with UNION body",
			sql:      "WITH combined AS (SELECT id, val FROM t1 UNION ALL SELECT id, val FROM t2) SELECT * FROM combined ORDER BY id",
			wantRows: 4,
			wantCols: []string{"id", "val"},
			wantData: [][]interface{}{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}, {int64(4), "d"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := e.ExecuteSQL(tt.sql)
			require.NoError(t, err, "ExecuteSQL(%q)", tt.sql)
			assert.Len(t, result.Rows, tt.wantRows, "row count")
			if tt.wantCols != nil {
				require.Len(t, result.Columns, len(tt.wantCols), "column count")
				for i, col := range tt.wantCols {
					assert.Equal(t, col, result.Columns[i], "column[%d]", i)
				}
			}
			if tt.wantData != nil {
				for i, wantRow := range tt.wantData {
					if i >= len(result.Rows) {
						break
					}
					for j, wantVal := range wantRow {
						if j < len(result.Rows[i]) {
							assert.Equal(t, wantVal, result.Rows[i][j], "row[%d][%d]", i, j)
						}
					}
				}
			}
		})
	}
}

func TestCTEMultipleReferences(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	setup := []string{
		"CREATE TABLE nums (n INT)",
		"INSERT INTO nums VALUES (1), (2), (3)",
	}
	for _, sql := range setup {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}

	// CTE referenced twice in a JOIN
	result, err := e.ExecuteSQL("WITH t AS (SELECT n FROM nums) SELECT a.n, b.n FROM t AS a JOIN t AS b ON a.n = b.n ORDER BY a.n")
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3, "row count")
	assert.Equal(t, []string{"n", "n"}, result.Columns)
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, int64(1), result.Rows[0][1])
}

func TestCTEShadowsTable(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	setup := []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
	}
	for _, sql := range setup {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}

	// CTE named 'users' shadows the real 'users' table
	result, err := e.ExecuteSQL("WITH users AS (SELECT 99 AS id, 'CTE' AS name) SELECT * FROM users")
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1, "row count")
	assert.Equal(t, int64(99), result.Rows[0][0])
	assert.Equal(t, "CTE", result.Rows[0][1])
}

func TestCTEError(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	// Parse error: missing AS
	_, err := e.ExecuteSQL("WITH t (SELECT 1) SELECT * FROM t")
	assert.Error(t, err, "expected error for missing AS")

	// Parse error: missing parentheses
	_, err = e.ExecuteSQL("WITH t AS SELECT 1 SELECT * FROM t")
	assert.Error(t, err, "expected error for missing parentheses")
}

func TestRecursiveCTESequence(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	// Generate sequence 1..5
	result, err := e.ExecuteSQL(`
		WITH RECURSIVE seq AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 5
		)
		SELECT * FROM seq
	`)
	require.NoError(t, err)
	assert.Equal(t, []string{"n"}, result.Columns)
	require.Len(t, result.Rows, 5)
	for i, row := range result.Rows {
		assert.Equal(t, int64(i+1), row[0], "row[%d]", i)
	}
}

func TestRecursiveCTEHierarchy(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	setup := []string{
		"CREATE TABLE employees (id INT, name TEXT, manager_id INT)",
		"INSERT INTO employees VALUES (1, 'CEO', 0)",
		"INSERT INTO employees VALUES (2, 'VP', 1)",
		"INSERT INTO employees VALUES (3, 'Director', 2)",
		"INSERT INTO employees VALUES (4, 'Manager', 3)",
	}
	for _, sql := range setup {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup: %s", sql)
	}

	// Traverse hierarchy from CEO down
	result, err := e.ExecuteSQL(`
		WITH RECURSIVE org AS (
			SELECT id, name, manager_id FROM employees WHERE id = 1
			UNION ALL
			SELECT e.id, e.name, e.manager_id FROM employees AS e JOIN org ON e.manager_id = org.id
		)
		SELECT * FROM org ORDER BY id
	`)
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name", "manager_id"}, result.Columns)
	require.Len(t, result.Rows, 4)
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, "CEO", result.Rows[0][1])
	assert.Equal(t, int64(4), result.Rows[3][0])
	assert.Equal(t, "Manager", result.Rows[3][1])
}

func TestRecursiveCTEUnionDistinct(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	// UNION (without ALL) should deduplicate.
	// Anchor: {1}
	// Iter 1: n=1, n<5 → {2}. New: {2}
	// Iter 2: n=2, n<5 → {3}. New: {3}
	// Iter 3: n=3, n<5 → {4}. New: {4}
	// Iter 4: n=4, n<5 → {5}. New: {5}
	// Iter 5: n=5, n<5 is false → empty. Stop.
	// UNION ALL would produce the same here; test that UNION path works.
	result, err := e.ExecuteSQL(`
		WITH RECURSIVE seq AS (
			SELECT 1 AS n
			UNION
			SELECT n + 1 FROM seq WHERE n < 5
		)
		SELECT * FROM seq ORDER BY n
	`)
	require.NoError(t, err)
	require.Len(t, result.Rows, 5)
	for i, row := range result.Rows {
		assert.Equal(t, int64(i+1), row[0], "row[%d]", i)
	}

	// Verify UNION deduplication actually works: anchor produces duplicate values
	// that the recursive term also generates.
	result2, err := e.ExecuteSQL(`
		WITH RECURSIVE dup AS (
			SELECT 1 AS n
			UNION
			SELECT CASE WHEN n < 3 THEN n + 1 ELSE 1 END FROM dup WHERE n < 4
		)
		SELECT * FROM dup ORDER BY n
	`)
	require.NoError(t, err)
	// Anchor: {1}, Iter1: {2}, Iter2: {3}, Iter3: {1} (deduped) → stop
	require.Len(t, result2.Rows, 3)
	assert.Equal(t, int64(1), result2.Rows[0][0])
	assert.Equal(t, int64(2), result2.Rows[1][0])
	assert.Equal(t, int64(3), result2.Rows[2][0])
}

func TestRecursiveCTEMaxDepthError(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	// Infinite recursion: no termination condition
	_, err := e.ExecuteSQL(`
		WITH RECURSIVE inf AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 FROM inf
		)
		SELECT * FROM inf
	`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeded maximum depth")
}

func TestRecursiveCTENonUnionError(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	// Recursive CTE without UNION should error
	_, err := e.ExecuteSQL(`
		WITH RECURSIVE t AS (
			SELECT 1 AS n
		)
		SELECT * FROM t
	`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must use UNION")
}

func TestRecursiveCTEWithOuterJoin(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	setup := []string{
		"CREATE TABLE items (id INT, label TEXT)",
		"INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
	}
	for _, sql := range setup {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup: %s", sql)
	}

	// Recursive CTE joined with a real table
	result, err := e.ExecuteSQL(`
		WITH RECURSIVE seq AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 3
		)
		SELECT seq.n, items.label FROM seq JOIN items ON seq.n = items.id ORDER BY seq.n
	`)
	require.NoError(t, err)
	require.Len(t, result.Rows, 3)
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, "a", result.Rows[0][1])
	assert.Equal(t, int64(2), result.Rows[1][0])
	assert.Equal(t, "b", result.Rows[1][1])
	assert.Equal(t, int64(3), result.Rows[2][0])
	assert.Equal(t, "c", result.Rows[2][1])
}

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

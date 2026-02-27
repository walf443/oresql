package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInSubquery(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	// Setup: create tables
	setup := []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
		"CREATE TABLE orders (id INT, user_id INT, status TEXT)",
		"INSERT INTO orders VALUES (1, 1, 'active'), (2, 1, 'completed'), (3, 2, 'active')",
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
			name:     "IN subquery with matching rows",
			sql:      "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'active')",
			wantRows: 2,
			wantCols: []string{"id", "name"},
			wantData: [][]interface{}{{int64(1), "Alice"}, {int64(2), "Bob"}},
		},
		{
			name:     "IN subquery with no matching rows",
			sql:      "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'cancelled')",
			wantRows: 0,
			wantCols: []string{"id", "name"},
		},
		{
			name:     "NOT IN subquery",
			sql:      "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders)",
			wantRows: 1,
			wantCols: []string{"id", "name"},
			wantData: [][]interface{}{{int64(3), "Charlie"}},
		},
		{
			name:     "IN subquery combined with AND",
			sql:      "SELECT * FROM users WHERE id = 1 AND id IN (SELECT user_id FROM orders WHERE status = 'active')",
			wantRows: 1,
			wantCols: []string{"id", "name"},
			wantData: [][]interface{}{{int64(1), "Alice"}},
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

func TestScalarSubquery(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	// Setup: create tables
	setup := []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
		"CREATE TABLE orders (id INT, user_id INT, amount INT)",
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
		wantErr  bool
	}{
		{
			name:     "scalar subquery in SELECT list",
			sql:      "SELECT (SELECT MAX(id) FROM orders) AS max_id",
			wantRows: 1,
			wantCols: []string{"max_id"},
			wantData: [][]interface{}{{int64(3)}},
		},
		{
			name:     "scalar subquery in WHERE clause",
			sql:      "SELECT * FROM users WHERE id = (SELECT MAX(user_id) FROM orders)",
			wantRows: 1,
			wantCols: []string{"id", "name"},
			wantData: [][]interface{}{{int64(2), "Bob"}},
		},
		{
			name:     "scalar subquery returning empty result (NULL)",
			sql:      "SELECT * FROM users WHERE id = (SELECT user_id FROM orders WHERE amount = 999)",
			wantRows: 0,
			wantCols: []string{"id", "name"},
		},
		{
			name:    "scalar subquery returning multiple rows (error)",
			sql:     "SELECT (SELECT user_id FROM orders)",
			wantErr: true,
		},
		{
			name:     "scalar subquery with arithmetic",
			sql:      "SELECT (SELECT MAX(amount) FROM orders) + 10 AS total",
			wantRows: 1,
			wantCols: []string{"total"},
			wantData: [][]interface{}{{int64(210)}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := e.ExecuteSQL(tt.sql)
			if tt.wantErr {
				require.Error(t, err, "expected error")
				return
			}
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

func TestFromSubqueryBasic(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	setup := []string{
		"CREATE TABLE t1 (id INT, name TEXT)",
		"INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
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
			name:     "basic FROM subquery",
			sql:      "SELECT * FROM (SELECT id, name FROM t1) AS sub",
			wantRows: 3,
			wantCols: []string{"id", "name"},
			wantData: [][]interface{}{{int64(1), "Alice"}, {int64(2), "Bob"}, {int64(3), "Charlie"}},
		},
		{
			name:     "FROM subquery with inner WHERE",
			sql:      "SELECT * FROM (SELECT id, name FROM t1 WHERE id > 1) AS sub",
			wantRows: 2,
			wantCols: []string{"id", "name"},
			wantData: [][]interface{}{{int64(2), "Bob"}, {int64(3), "Charlie"}},
		},
		{
			name:     "FROM subquery with alias-qualified column",
			sql:      "SELECT sub.id FROM (SELECT id, name FROM t1) AS sub",
			wantRows: 3,
			wantCols: []string{"id"},
			wantData: [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		},
		{
			name:     "FROM subquery with outer ORDER BY",
			sql:      "SELECT * FROM (SELECT id, name FROM t1) AS sub ORDER BY sub.id DESC",
			wantRows: 3,
			wantCols: []string{"id", "name"},
			wantData: [][]interface{}{{int64(3), "Charlie"}, {int64(2), "Bob"}, {int64(1), "Alice"}},
		},
		{
			name:     "FROM subquery with column types",
			sql:      "SELECT sub.id, sub.name FROM (SELECT id, name FROM t1 WHERE id = 1) AS sub",
			wantRows: 1,
			wantCols: []string{"id", "name"},
			wantData: [][]interface{}{{int64(1), "Alice"}},
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

func TestFromSubqueryWithUnion(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	setup := []string{
		"CREATE TABLE t1 (id INT)",
		"CREATE TABLE t2 (id INT)",
		"INSERT INTO t1 VALUES (1), (2)",
		"INSERT INTO t2 VALUES (3), (4)",
	}
	for _, sql := range setup {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}

	result, err := e.ExecuteSQL("SELECT * FROM (SELECT id FROM t1 UNION SELECT id FROM t2) AS sub ORDER BY sub.id")
	require.NoError(t, err, "ExecuteSQL error")
	require.Len(t, result.Rows, 4, "expected 4 rows")
	for i, want := range []int64{1, 2, 3, 4} {
		assert.Equal(t, want, result.Rows[i][0], "row[%d][0]", i)
	}
}

func TestFromSubqueryWithJoin(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	setup := []string{
		"CREATE TABLE t1 (id INT, val TEXT)",
		"CREATE TABLE t2 (id INT, name TEXT)",
		"INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')",
		"INSERT INTO t2 VALUES (1, 'Alice'), (2, 'Bob')",
	}
	for _, sql := range setup {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}

	result, err := e.ExecuteSQL("SELECT a.id, b.name FROM (SELECT id FROM t1) AS a JOIN t2 AS b ON a.id = b.id ORDER BY a.id")
	require.NoError(t, err, "ExecuteSQL error")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(1), result.Rows[0][0], "row[0][0]")
	assert.Equal(t, "Alice", result.Rows[0][1], "row[0][1]")
	assert.Equal(t, int64(2), result.Rows[1][0], "row[1][0]")
	assert.Equal(t, "Bob", result.Rows[1][1], "row[1][1]")
}

func TestFromSubqueryColumnTypes(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	setup := []string{
		"CREATE TABLE t1 (id INT, name TEXT)",
		"INSERT INTO t1 VALUES (1, 'Alice')",
	}
	for _, sql := range setup {
		_, err := e.ExecuteSQL(sql)
		require.NoError(t, err, "setup failed: %s", sql)
	}

	result, err := e.ExecuteSQL("SELECT * FROM (SELECT id, name FROM t1) AS sub")
	require.NoError(t, err, "ExecuteSQL error")
	require.Len(t, result.ColumnTypes, 2, "expected 2 column types")
	assert.Equal(t, "INT", result.ColumnTypes[0], "column type[0]")
	assert.Equal(t, "TEXT", result.ColumnTypes[1], "column type[1]")
}

func TestExistsSubquery(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	// Setup: create tables
	setup := []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
		"CREATE TABLE orders (id INT, user_id INT, status TEXT)",
		"INSERT INTO orders VALUES (1, 1, 'active'), (2, 1, 'completed'), (3, 2, 'active')",
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
			name:     "EXISTS with matching rows",
			sql:      "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE status = 'active')",
			wantRows: 3, // subquery returns rows, so all users match
			wantCols: []string{"id", "name"},
		},
		{
			name:     "EXISTS with no matching rows",
			sql:      "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE status = 'cancelled')",
			wantRows: 0, // subquery returns no rows, so no users match
			wantCols: []string{"id", "name"},
		},
		{
			name:     "NOT EXISTS with matching rows",
			sql:      "SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE status = 'active')",
			wantRows: 0, // subquery returns rows, so NOT EXISTS is false
			wantCols: []string{"id", "name"},
		},
		{
			name:     "NOT EXISTS with no matching rows",
			sql:      "SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE status = 'cancelled')",
			wantRows: 3, // subquery returns no rows, so NOT EXISTS is true
			wantCols: []string{"id", "name"},
		},
		{
			name:     "EXISTS combined with AND",
			sql:      "SELECT * FROM users WHERE id = 1 AND EXISTS (SELECT 1 FROM orders WHERE status = 'active')",
			wantRows: 1,
			wantCols: []string{"id", "name"},
			wantData: [][]interface{}{{int64(1), "Alice"}},
		},
		{
			name:     "EXISTS combined with OR",
			sql:      "SELECT * FROM users WHERE id = 1 OR EXISTS (SELECT 1 FROM orders WHERE status = 'cancelled')",
			wantRows: 1,
			wantCols: []string{"id", "name"},
			wantData: [][]interface{}{{int64(1), "Alice"}},
		},
		{
			name:     "EXISTS inside CASE WHEN (true)",
			sql:      "SELECT id, CASE WHEN EXISTS (SELECT 1 FROM orders WHERE status = 'active') THEN 'has_orders' ELSE 'no_orders' END AS label FROM users WHERE id = 1",
			wantRows: 1,
			wantCols: []string{"id", "label"},
			wantData: [][]interface{}{{int64(1), "has_orders"}},
		},
		{
			name:     "EXISTS inside CASE WHEN (false)",
			sql:      "SELECT id, CASE WHEN EXISTS (SELECT 1 FROM orders WHERE status = 'cancelled') THEN 'has_orders' ELSE 'no_orders' END AS label FROM users WHERE id = 1",
			wantRows: 1,
			wantCols: []string{"id", "label"},
			wantData: [][]interface{}{{int64(1), "no_orders"}},
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

func TestCorrelatedExists(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	setup := []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
		"CREATE TABLE orders (id INT, user_id INT, status TEXT)",
		"INSERT INTO orders VALUES (1, 1, 'active'), (2, 1, 'completed'), (3, 2, 'active')",
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
			name:     "correlated EXISTS",
			sql:      "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
			wantRows: 2,
			wantCols: []string{"id", "name"},
			wantData: [][]interface{}{{int64(1), "Alice"}, {int64(2), "Bob"}},
		},
		{
			name:     "correlated NOT EXISTS",
			sql:      "SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
			wantRows: 1,
			wantCols: []string{"id", "name"},
			wantData: [][]interface{}{{int64(3), "Charlie"}},
		},
		{
			name:     "correlated EXISTS with additional filter",
			sql:      "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'active')",
			wantRows: 2,
			wantCols: []string{"id", "name"},
			wantData: [][]interface{}{{int64(1), "Alice"}, {int64(2), "Bob"}},
		},
		{
			name:     "correlated EXISTS with completed status only",
			sql:      "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'completed')",
			wantRows: 1,
			wantCols: []string{"id", "name"},
			wantData: [][]interface{}{{int64(1), "Alice"}},
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

func TestCorrelatedScalar(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	setup := []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
		"CREATE TABLE orders (id INT, user_id INT, amount INT)",
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
			name:     "correlated scalar COUNT",
			sql:      "SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count FROM users u",
			wantRows: 3,
			wantCols: []string{"name", "order_count"},
			wantData: [][]interface{}{{"Alice", int64(2)}, {"Bob", int64(1)}, {"Charlie", int64(0)}},
		},
		{
			name:     "correlated scalar SUM",
			sql:      "SELECT u.name, (SELECT SUM(amount) FROM orders o WHERE o.user_id = u.id) AS total FROM users u",
			wantRows: 3,
			wantCols: []string{"name", "total"},
			wantData: [][]interface{}{{"Alice", int64(300)}, {"Bob", int64(150)}, {"Charlie", nil}},
		},
		{
			name:     "correlated scalar MAX",
			sql:      "SELECT u.name, (SELECT MAX(amount) FROM orders o WHERE o.user_id = u.id) AS max_amount FROM users u",
			wantRows: 3,
			wantCols: []string{"name", "max_amount"},
			wantData: [][]interface{}{{"Alice", int64(200)}, {"Bob", int64(150)}, {"Charlie", nil}},
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

func TestCorrelatedComparison(t *testing.T) {
	e := NewExecutor(NewDatabase("test"))

	setup := []string{
		"CREATE TABLE employees (id INT, name TEXT, dept TEXT, salary INT)",
		"INSERT INTO employees VALUES (1, 'Alice', 'eng', 100), (2, 'Bob', 'eng', 120), (3, 'Charlie', 'sales', 90), (4, 'Diana', 'sales', 110)",
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
			name:     "correlated comparison with AVG",
			sql:      "SELECT * FROM employees e1 WHERE e1.salary > (SELECT AVG(salary) FROM employees e2 WHERE e2.dept = e1.dept)",
			wantRows: 2,
			wantCols: []string{"id", "name", "dept", "salary"},
			wantData: [][]interface{}{{int64(2), "Bob", "eng", int64(120)}, {int64(4), "Diana", "sales", int64(110)}},
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

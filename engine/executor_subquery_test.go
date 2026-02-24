package engine

import (
	"testing"
)

func TestInSubquery(t *testing.T) {
	e := NewExecutor()

	// Setup: create tables
	setup := []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
		"CREATE TABLE orders (id INT, user_id INT, status TEXT)",
		"INSERT INTO orders VALUES (1, 1, 'active'), (2, 1, 'completed'), (3, 2, 'active')",
	}
	for _, sql := range setup {
		if _, err := e.ExecuteSQL(sql); err != nil {
			t.Fatalf("setup failed: %s: %v", sql, err)
		}
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
			if err != nil {
				t.Fatalf("ExecuteSQL(%q) error: %v", tt.sql, err)
			}
			if len(result.Rows) != tt.wantRows {
				t.Errorf("got %d rows, want %d", len(result.Rows), tt.wantRows)
			}
			if tt.wantCols != nil {
				if len(result.Columns) != len(tt.wantCols) {
					t.Errorf("got %d columns, want %d", len(result.Columns), len(tt.wantCols))
				}
				for i, col := range tt.wantCols {
					if i < len(result.Columns) && result.Columns[i] != col {
						t.Errorf("column[%d] = %q, want %q", i, result.Columns[i], col)
					}
				}
			}
			if tt.wantData != nil {
				for i, wantRow := range tt.wantData {
					if i >= len(result.Rows) {
						break
					}
					for j, wantVal := range wantRow {
						if j < len(result.Rows[i]) && result.Rows[i][j] != wantVal {
							t.Errorf("row[%d][%d] = %v, want %v", i, j, result.Rows[i][j], wantVal)
						}
					}
				}
			}
		})
	}
}

func TestScalarSubquery(t *testing.T) {
	e := NewExecutor()

	// Setup: create tables
	setup := []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
		"CREATE TABLE orders (id INT, user_id INT, amount INT)",
		"INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)",
	}
	for _, sql := range setup {
		if _, err := e.ExecuteSQL(sql); err != nil {
			t.Fatalf("setup failed: %s: %v", sql, err)
		}
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
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("ExecuteSQL(%q) error: %v", tt.sql, err)
			}
			if len(result.Rows) != tt.wantRows {
				t.Errorf("got %d rows, want %d", len(result.Rows), tt.wantRows)
			}
			if tt.wantCols != nil {
				if len(result.Columns) != len(tt.wantCols) {
					t.Errorf("got %d columns, want %d", len(result.Columns), len(tt.wantCols))
				}
				for i, col := range tt.wantCols {
					if i < len(result.Columns) && result.Columns[i] != col {
						t.Errorf("column[%d] = %q, want %q", i, result.Columns[i], col)
					}
				}
			}
			if tt.wantData != nil {
				for i, wantRow := range tt.wantData {
					if i >= len(result.Rows) {
						break
					}
					for j, wantVal := range wantRow {
						if j < len(result.Rows[i]) && result.Rows[i][j] != wantVal {
							t.Errorf("row[%d][%d] = %v, want %v", i, j, result.Rows[i][j], wantVal)
						}
					}
				}
			}
		})
	}
}

func TestFromSubqueryBasic(t *testing.T) {
	e := NewExecutor()

	setup := []string{
		"CREATE TABLE t1 (id INT, name TEXT)",
		"INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
	}
	for _, sql := range setup {
		if _, err := e.ExecuteSQL(sql); err != nil {
			t.Fatalf("setup failed: %s: %v", sql, err)
		}
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
			if err != nil {
				t.Fatalf("ExecuteSQL(%q) error: %v", tt.sql, err)
			}
			if len(result.Rows) != tt.wantRows {
				t.Errorf("got %d rows, want %d", len(result.Rows), tt.wantRows)
			}
			if tt.wantCols != nil {
				if len(result.Columns) != len(tt.wantCols) {
					t.Errorf("got %d columns, want %d", len(result.Columns), len(tt.wantCols))
				}
				for i, col := range tt.wantCols {
					if i < len(result.Columns) && result.Columns[i] != col {
						t.Errorf("column[%d] = %q, want %q", i, result.Columns[i], col)
					}
				}
			}
			if tt.wantData != nil {
				for i, wantRow := range tt.wantData {
					if i >= len(result.Rows) {
						break
					}
					for j, wantVal := range wantRow {
						if j < len(result.Rows[i]) && result.Rows[i][j] != wantVal {
							t.Errorf("row[%d][%d] = %v, want %v", i, j, result.Rows[i][j], wantVal)
						}
					}
				}
			}
		})
	}
}

func TestFromSubqueryWithUnion(t *testing.T) {
	e := NewExecutor()

	setup := []string{
		"CREATE TABLE t1 (id INT)",
		"CREATE TABLE t2 (id INT)",
		"INSERT INTO t1 VALUES (1), (2)",
		"INSERT INTO t2 VALUES (3), (4)",
	}
	for _, sql := range setup {
		if _, err := e.ExecuteSQL(sql); err != nil {
			t.Fatalf("setup failed: %s: %v", sql, err)
		}
	}

	result, err := e.ExecuteSQL("SELECT * FROM (SELECT id FROM t1 UNION SELECT id FROM t2) AS sub ORDER BY sub.id")
	if err != nil {
		t.Fatalf("ExecuteSQL error: %v", err)
	}
	if len(result.Rows) != 4 {
		t.Fatalf("got %d rows, want 4", len(result.Rows))
	}
	for i, want := range []int64{1, 2, 3, 4} {
		if result.Rows[i][0] != want {
			t.Errorf("row[%d][0] = %v, want %v", i, result.Rows[i][0], want)
		}
	}
}

func TestFromSubqueryWithJoin(t *testing.T) {
	e := NewExecutor()

	setup := []string{
		"CREATE TABLE t1 (id INT, val TEXT)",
		"CREATE TABLE t2 (id INT, name TEXT)",
		"INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')",
		"INSERT INTO t2 VALUES (1, 'Alice'), (2, 'Bob')",
	}
	for _, sql := range setup {
		if _, err := e.ExecuteSQL(sql); err != nil {
			t.Fatalf("setup failed: %s: %v", sql, err)
		}
	}

	result, err := e.ExecuteSQL("SELECT a.id, b.name FROM (SELECT id FROM t1) AS a JOIN t2 AS b ON a.id = b.id ORDER BY a.id")
	if err != nil {
		t.Fatalf("ExecuteSQL error: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(result.Rows))
	}
	if result.Rows[0][0] != int64(1) || result.Rows[0][1] != "Alice" {
		t.Errorf("row[0] = %v, want [1, Alice]", result.Rows[0])
	}
	if result.Rows[1][0] != int64(2) || result.Rows[1][1] != "Bob" {
		t.Errorf("row[1] = %v, want [2, Bob]", result.Rows[1])
	}
}

func TestFromSubqueryColumnTypes(t *testing.T) {
	e := NewExecutor()

	setup := []string{
		"CREATE TABLE t1 (id INT, name TEXT)",
		"INSERT INTO t1 VALUES (1, 'Alice')",
	}
	for _, sql := range setup {
		if _, err := e.ExecuteSQL(sql); err != nil {
			t.Fatalf("setup failed: %s: %v", sql, err)
		}
	}

	result, err := e.ExecuteSQL("SELECT * FROM (SELECT id, name FROM t1) AS sub")
	if err != nil {
		t.Fatalf("ExecuteSQL error: %v", err)
	}
	if len(result.ColumnTypes) != 2 {
		t.Fatalf("got %d column types, want 2", len(result.ColumnTypes))
	}
	if result.ColumnTypes[0] != "INT" {
		t.Errorf("column type[0] = %q, want %q", result.ColumnTypes[0], "INT")
	}
	if result.ColumnTypes[1] != "TEXT" {
		t.Errorf("column type[1] = %q, want %q", result.ColumnTypes[1], "TEXT")
	}
}

func TestExistsSubquery(t *testing.T) {
	e := NewExecutor()

	// Setup: create tables
	setup := []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
		"CREATE TABLE orders (id INT, user_id INT, status TEXT)",
		"INSERT INTO orders VALUES (1, 1, 'active'), (2, 1, 'completed'), (3, 2, 'active')",
	}
	for _, sql := range setup {
		if _, err := e.ExecuteSQL(sql); err != nil {
			t.Fatalf("setup failed: %s: %v", sql, err)
		}
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
			if err != nil {
				t.Fatalf("ExecuteSQL(%q) error: %v", tt.sql, err)
			}
			if len(result.Rows) != tt.wantRows {
				t.Errorf("got %d rows, want %d", len(result.Rows), tt.wantRows)
			}
			if tt.wantCols != nil {
				if len(result.Columns) != len(tt.wantCols) {
					t.Errorf("got %d columns, want %d", len(result.Columns), len(tt.wantCols))
				}
				for i, col := range tt.wantCols {
					if i < len(result.Columns) && result.Columns[i] != col {
						t.Errorf("column[%d] = %q, want %q", i, result.Columns[i], col)
					}
				}
			}
			if tt.wantData != nil {
				for i, wantRow := range tt.wantData {
					if i >= len(result.Rows) {
						break
					}
					for j, wantVal := range wantRow {
						if j < len(result.Rows[i]) && result.Rows[i][j] != wantVal {
							t.Errorf("row[%d][%d] = %v, want %v", i, j, result.Rows[i][j], wantVal)
						}
					}
				}
			}
		})
	}
}

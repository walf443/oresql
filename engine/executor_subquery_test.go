package engine

import (
	"testing"
)

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

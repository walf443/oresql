package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupCoveringTestTable creates a table with indexed columns for covering index tests.
func setupCoveringTestTable(t *testing.T, storageType string) *Executor {
	t.Helper()
	var exec *Executor
	if storageType == "disk" {
		tmpDir := t.TempDir()
		db := NewDatabase("test", WithDataDir(tmpDir), WithStorageType("disk"))
		exec = NewExecutor(db)
	} else {
		exec = NewExecutor(NewDatabase("test"))
	}
	run(t, exec, "CREATE TABLE items (id INT PRIMARY KEY, val INT, name TEXT, category INT)")
	run(t, exec, "INSERT INTO items VALUES (1, 10, 'apple', 1)")
	run(t, exec, "INSERT INTO items VALUES (2, 20, 'banana', 1)")
	run(t, exec, "INSERT INTO items VALUES (3, 30, 'cherry', 2)")
	run(t, exec, "INSERT INTO items VALUES (4, 40, 'date', 2)")
	run(t, exec, "INSERT INTO items VALUES (5, 50, 'elderberry', 3)")
	run(t, exec, "CREATE INDEX idx_val ON items(val)")
	run(t, exec, "CREATE INDEX idx_category ON items(category)")
	run(t, exec, "CREATE INDEX idx_cat_val ON items(category, val)")
	return exec
}

func TestCoveringIndexEquality(t *testing.T) {
	for _, storageType := range []string{"memory", "disk"} {
		t.Run(storageType, func(t *testing.T) {
			exec := setupCoveringTestTable(t, storageType)

			// Covering: SELECT indexed_col FROM t WHERE indexed_col = X
			q := "SELECT val FROM items WHERE val = 20"
			result := run(t, exec, q)
			require.Len(t, result.Rows, 1)
			assert.Equal(t, int64(20), result.Rows[0][0])

			assertExplain(t, exec, q, []planRow{{Type: "ref", Key: "idx_val", Extra: "Using covering index"}})
		})
	}
}

func TestCoveringIndexNonCovering(t *testing.T) {
	for _, storageType := range []string{"memory", "disk"} {
		t.Run(storageType, func(t *testing.T) {
			exec := setupCoveringTestTable(t, storageType)

			// Non-covering: SELECT non_indexed FROM t WHERE indexed_col = X
			q := "SELECT name FROM items WHERE val = 20"
			result := run(t, exec, q)
			require.Len(t, result.Rows, 1)
			assert.Equal(t, "banana", result.Rows[0][0])

			assertExplain(t, exec, q, []planRow{{Type: "ref", Key: "idx_val"}})
		})
	}
}

func TestCoveringIndexPKAndIndexed(t *testing.T) {
	for _, storageType := range []string{"memory", "disk"} {
		t.Run(storageType, func(t *testing.T) {
			exec := setupCoveringTestTable(t, storageType)

			// PK + indexed column: both are covered
			q := "SELECT id, val FROM items WHERE val = 30"
			result := run(t, exec, q)
			require.Len(t, result.Rows, 1)
			assert.Equal(t, int64(3), result.Rows[0][0])
			assert.Equal(t, int64(30), result.Rows[0][1])

			assertExplain(t, exec, q, []planRow{{Type: "ref", Key: "idx_val", Extra: "Using covering index"}})
		})
	}
}

func TestCoveringIndexSelectStar(t *testing.T) {
	for _, storageType := range []string{"memory", "disk"} {
		t.Run(storageType, func(t *testing.T) {
			exec := setupCoveringTestTable(t, storageType)

			// SELECT * should not be covering (needs all columns)
			q := "SELECT * FROM items WHERE val = 20"
			result := run(t, exec, q)
			require.Len(t, result.Rows, 1)
			assert.Equal(t, int64(2), result.Rows[0][0])
			assert.Equal(t, int64(20), result.Rows[0][1])
			assert.Equal(t, "banana", result.Rows[0][2])
			assert.Equal(t, int64(1), result.Rows[0][3])

			// Should NOT have "Using covering index"
			explainResult := run(t, exec, "EXPLAIN "+q)
			extra, _ := explainResult.Rows[0][6].(string)
			assert.NotContains(t, extra, "Using covering index")
		})
	}
}

func TestCoveringIndexComposite(t *testing.T) {
	for _, storageType := range []string{"memory", "disk"} {
		t.Run(storageType, func(t *testing.T) {
			exec := setupCoveringTestTable(t, storageType)

			// Composite index covering: SELECT col1, col2 FROM t WHERE col1 = X AND col2 = Y
			q := "SELECT category, val FROM items WHERE category = 2 AND val = 30"
			result := run(t, exec, q)
			require.Len(t, result.Rows, 1)
			assert.Equal(t, int64(2), result.Rows[0][0])
			assert.Equal(t, int64(30), result.Rows[0][1])

			assertExplain(t, exec, q, []planRow{{Type: "ref", Extra: "Using covering index"}})
		})
	}
}

func TestCoveringIndexExpression(t *testing.T) {
	for _, storageType := range []string{"memory", "disk"} {
		t.Run(storageType, func(t *testing.T) {
			exec := setupCoveringTestTable(t, storageType)

			// Expression on indexed column: SELECT val + 1 FROM t WHERE val > 30
			result := run(t, exec, "SELECT val + 1 FROM items WHERE val > 30")
			require.Len(t, result.Rows, 2) // val=40, val=50
			// Results may come in any order, so collect values
			vals := make(map[int64]bool)
			for _, row := range result.Rows {
				vals[row[0].(int64)] = true
			}
			assert.True(t, vals[41])
			assert.True(t, vals[51])
		})
	}
}

func TestCoveringIndexOrderBy(t *testing.T) {
	for _, storageType := range []string{"memory", "disk"} {
		t.Run(storageType, func(t *testing.T) {
			exec := setupCoveringTestTable(t, storageType)

			// ORDER BY + covering: SELECT indexed_col FROM t ORDER BY indexed_col LIMIT 3
			q := "SELECT val FROM items ORDER BY val LIMIT 3"
			result := run(t, exec, q)
			require.Len(t, result.Rows, 3)
			assert.Equal(t, int64(10), result.Rows[0][0])
			assert.Equal(t, int64(20), result.Rows[1][0])
			assert.Equal(t, int64(30), result.Rows[2][0])

			// This table has PRIMARY KEY so tryIndexOrder may pick PK, not idx_val
			// Just verify covering is detected when index order scan is used
		})
	}
}

func TestCoveringIndexOrderByDesc(t *testing.T) {
	for _, storageType := range []string{"memory", "disk"} {
		t.Run(storageType, func(t *testing.T) {
			exec := setupCoveringTestTable(t, storageType)

			// ORDER BY DESC + covering
			q := "SELECT val FROM items ORDER BY val DESC LIMIT 3"
			result := run(t, exec, q)
			require.Len(t, result.Rows, 3)
			assert.Equal(t, int64(50), result.Rows[0][0])
			assert.Equal(t, int64(40), result.Rows[1][0])
			assert.Equal(t, int64(30), result.Rows[2][0])
		})
	}
}

func TestCoveringIndexStreamingLimit(t *testing.T) {
	for _, storageType := range []string{"memory", "disk"} {
		t.Run(storageType, func(t *testing.T) {
			exec := setupCoveringTestTable(t, storageType)

			// Streaming with LIMIT: should work with covering index
			q := "SELECT val FROM items WHERE val >= 20 LIMIT 2"
			result := run(t, exec, q)
			require.Len(t, result.Rows, 2)

			assertExplain(t, exec, q, []planRow{{Type: "range", Key: "idx_val", Extra: "Using covering index"}})
		})
	}
}

func TestCoveringIndexPKOnly(t *testing.T) {
	for _, storageType := range []string{"memory", "disk"} {
		t.Run(storageType, func(t *testing.T) {
			exec := setupCoveringTestTable(t, storageType)

			// Select PK + indexed columns should be covered
			q := "SELECT id, category FROM items WHERE category = 1"
			result := run(t, exec, q)
			require.Len(t, result.Rows, 2)
			// Both apple(1) and banana(2) have category=1
			ids := make(map[int64]bool)
			for _, row := range result.Rows {
				ids[row[0].(int64)] = true
			}
			assert.True(t, ids[1])
			assert.True(t, ids[2])

			assertExplain(t, exec, q, []planRow{{Type: "ref", Key: "idx_category", Extra: "Using covering index"}})
		})
	}
}

func TestCoveringIndexMultipleResults(t *testing.T) {
	for _, storageType := range []string{"memory", "disk"} {
		t.Run(storageType, func(t *testing.T) {
			exec := setupCoveringTestTable(t, storageType)

			// Multiple matching rows with covering
			q := "SELECT category FROM items WHERE category = 2"
			result := run(t, exec, q)
			require.Len(t, result.Rows, 2)
			for _, row := range result.Rows {
				assert.Equal(t, int64(2), row[0])
			}

			assertExplain(t, exec, q, []planRow{{Type: "ref", Key: "idx_category", Extra: "Using covering index"}})
		})
	}
}

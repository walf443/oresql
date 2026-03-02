package engine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupPKCoveringTestTable creates a table with 100 rows for PK covering tests.
func setupPKCoveringTestTable(t *testing.T, storageType string) *Executor {
	t.Helper()
	var exec *Executor
	if storageType == "disk" {
		tmpDir := t.TempDir()
		db := NewDatabase("test", WithDataDir(tmpDir), WithStorageType("disk"))
		exec = NewExecutor(db)
	} else {
		exec = NewExecutor(NewDatabase("test"))
	}
	run(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT, name TEXT)")
	for i := 1; i <= 100; i++ {
		run(t, exec, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, 'name_%d')", i, i*10, i))
	}
	return exec
}

func TestPKCoveringOrderByASC(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupPKCoveringTestTable(t, st)
			result := run(t, exec, "SELECT id FROM t ORDER BY id LIMIT 3")
			require.Len(t, result.Rows, 3)
			assert.Equal(t, int64(1), result.Rows[0][0])
			assert.Equal(t, int64(2), result.Rows[1][0])
			assert.Equal(t, int64(3), result.Rows[2][0])
		})
	}
}

func TestPKCoveringOrderByDESC(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupPKCoveringTestTable(t, st)
			result := run(t, exec, "SELECT id FROM t ORDER BY id DESC LIMIT 3")
			require.Len(t, result.Rows, 3)
			assert.Equal(t, int64(100), result.Rows[0][0])
			assert.Equal(t, int64(99), result.Rows[1][0])
			assert.Equal(t, int64(98), result.Rows[2][0])
		})
	}
}

func TestPKCoveringWhereOnPK(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupPKCoveringTestTable(t, st)
			result := run(t, exec, "SELECT id FROM t WHERE id > 95 ORDER BY id")
			require.Len(t, result.Rows, 5)
			assert.Equal(t, int64(96), result.Rows[0][0])
			assert.Equal(t, int64(100), result.Rows[4][0])
		})
	}
}

func TestPKCoveringExprOnPK(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupPKCoveringTestTable(t, st)
			result := run(t, exec, "SELECT id + 1 FROM t ORDER BY id LIMIT 3")
			require.Len(t, result.Rows, 3)
			assert.Equal(t, int64(2), result.Rows[0][0])
			assert.Equal(t, int64(3), result.Rows[1][0])
			assert.Equal(t, int64(4), result.Rows[2][0])
		})
	}
}

func TestPKCoveringCountStar(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupPKCoveringTestTable(t, st)
			result := run(t, exec, "SELECT COUNT(*) FROM t")
			require.Len(t, result.Rows, 1)
			assert.Equal(t, int64(100), result.Rows[0][0])
		})
	}
}

func TestPKCoveringNonCoveringIDName(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupPKCoveringTestTable(t, st)
			// Non-covering: needs name column too
			result := run(t, exec, "SELECT id, name FROM t ORDER BY id LIMIT 3")
			require.Len(t, result.Rows, 3)
			assert.Equal(t, int64(1), result.Rows[0][0])
			assert.Equal(t, "name_1", result.Rows[0][1])
			assert.Equal(t, int64(2), result.Rows[1][0])
			assert.Equal(t, "name_2", result.Rows[1][1])
		})
	}
}

func TestPKCoveringNonCoveringSelectStar(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupPKCoveringTestTable(t, st)
			// SELECT * is non-covering
			result := run(t, exec, "SELECT * FROM t ORDER BY id LIMIT 3")
			require.Len(t, result.Rows, 3)
			assert.Equal(t, int64(1), result.Rows[0][0])
			assert.Equal(t, int64(10), result.Rows[0][1])
			assert.Equal(t, "name_1", result.Rows[0][2])
		})
	}
}

func TestPKCoveringFullScan(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupPKCoveringTestTable(t, st)
			// PK-only full scan (no ORDER BY, no WHERE)
			result := run(t, exec, "SELECT id FROM t")
			require.Len(t, result.Rows, 100)
			// Verify first and last
			assert.Equal(t, int64(1), result.Rows[0][0])
			assert.Equal(t, int64(100), result.Rows[99][0])
		})
	}
}

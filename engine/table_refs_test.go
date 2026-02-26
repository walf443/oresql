package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/lexer"
	"github.com/walf443/oresql/parser"
	"github.com/walf443/oresql/storage"
)

func parseLockStmt(t *testing.T, sql string) ast.Statement {
	t.Helper()
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	assert.NoError(t, err)
	return stmt
}

func TestCollectLockRefs(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		wantLocks    map[string]storage.TableLockMode
		catalogWrite bool
	}{
		{
			name:      "simple SELECT",
			sql:       "SELECT * FROM t",
			wantLocks: map[string]storage.TableLockMode{"t": storage.TableLockRead},
		},
		{
			name: "SELECT with JOIN",
			sql:  "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id",
			wantLocks: map[string]storage.TableLockMode{
				"t1": storage.TableLockRead,
				"t2": storage.TableLockRead,
			},
		},
		{
			name: "INSERT with VALUES",
			sql:  "INSERT INTO t VALUES (1, 'a')",
			wantLocks: map[string]storage.TableLockMode{
				"t": storage.TableLockWrite,
			},
		},
		{
			name: "INSERT SELECT",
			sql:  "INSERT INTO t1 SELECT * FROM t2",
			wantLocks: map[string]storage.TableLockMode{
				"t1": storage.TableLockWrite,
				"t2": storage.TableLockRead,
			},
		},
		{
			name: "UPDATE with subquery in WHERE",
			sql:  "UPDATE t SET x = 1 WHERE id IN (SELECT id FROM t2)",
			wantLocks: map[string]storage.TableLockMode{
				"t":  storage.TableLockWrite,
				"t2": storage.TableLockRead,
			},
		},
		{
			name: "DELETE",
			sql:  "DELETE FROM t WHERE id = 1",
			wantLocks: map[string]storage.TableLockMode{
				"t": storage.TableLockWrite,
			},
		},
		{
			name:         "CREATE TABLE",
			sql:          "CREATE TABLE t (id INT, name TEXT)",
			wantLocks:    map[string]storage.TableLockMode{},
			catalogWrite: true,
		},
		{
			name: "DROP TABLE",
			sql:  "DROP TABLE t",
			wantLocks: map[string]storage.TableLockMode{
				"t": storage.TableLockWrite,
			},
			catalogWrite: true,
		},
		{
			name: "TRUNCATE TABLE",
			sql:  "TRUNCATE TABLE t",
			wantLocks: map[string]storage.TableLockMode{
				"t": storage.TableLockWrite,
			},
		},
		{
			name: "CREATE INDEX",
			sql:  "CREATE INDEX idx ON t (col)",
			wantLocks: map[string]storage.TableLockMode{
				"t": storage.TableLockWrite,
			},
		},
		{
			name: "ALTER TABLE ADD COLUMN",
			sql:  "ALTER TABLE t ADD COLUMN c INT",
			wantLocks: map[string]storage.TableLockMode{
				"t": storage.TableLockWrite,
			},
			catalogWrite: true,
		},
		{
			name: "ALTER TABLE DROP COLUMN",
			sql:  "ALTER TABLE t DROP COLUMN c",
			wantLocks: map[string]storage.TableLockMode{
				"t": storage.TableLockWrite,
			},
			catalogWrite: true,
		},
		{
			name: "SELECT with EXISTS subquery",
			sql:  "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)",
			wantLocks: map[string]storage.TableLockMode{
				"t1": storage.TableLockRead,
				"t2": storage.TableLockRead,
			},
		},
		{
			name: "UNION",
			sql:  "SELECT * FROM t1 UNION SELECT * FROM t2",
			wantLocks: map[string]storage.TableLockMode{
				"t1": storage.TableLockRead,
				"t2": storage.TableLockRead,
			},
		},
		{
			name: "self-join INSERT promotes to Write",
			sql:  "INSERT INTO t SELECT * FROM t",
			wantLocks: map[string]storage.TableLockMode{
				"t": storage.TableLockWrite,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseLockStmt(t, tt.sql)
			refs, catalogWrite := collectLockRefs(stmt)
			locks := mergeLockRefs(refs)

			assert.Equal(t, tt.catalogWrite, catalogWrite, "catalogWrite mismatch")
			assert.Equal(t, len(tt.wantLocks), len(locks), "lock count mismatch")

			for _, lock := range locks {
				wantMode, ok := tt.wantLocks[lock.TableName]
				assert.True(t, ok, "unexpected table lock: %s", lock.TableName)
				assert.Equal(t, wantMode, lock.Mode, "mode mismatch for table %s", lock.TableName)
			}
		})
	}
}

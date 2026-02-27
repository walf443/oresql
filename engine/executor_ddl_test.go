package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateInsertSelect(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	result := run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	assert.Equal(t, "table created", result.Message)

	result = run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	assert.Equal(t, "1 row inserted", result.Message)

	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	// SELECT *
	result = run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	require.Len(t, result.Columns, 2, "expected 2 columns")
	assert.Equal(t, []string{"id", "name"}, result.Columns)

	// SELECT specific columns
	result = run(t, exec, "SELECT name FROM users")
	require.Len(t, result.Columns, 1, "expected 1 column")
	assert.Equal(t, "name", result.Columns[0])
	require.Len(t, result.Rows, 2, "expected 2 rows")
}

func TestErrorDuplicateTable(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT)")
	runExpectError(t, exec, "CREATE TABLE users (id INT)")
}

func TestTruncateTable(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "TRUNCATE TABLE users")
	assert.Equal(t, "table truncated", result.Message)

	// Table still exists but has no rows
	result = run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 0, "expected 0 rows")

	// Can insert again after truncate
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	result = run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(3), result.Rows[0][0])
}

func TestTruncateTableNotExists(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	runExpectError(t, exec, "TRUNCATE TABLE nonexistent")
}

func TestDropTable(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	result := run(t, exec, "DROP TABLE users")
	assert.Equal(t, "table dropped", result.Message)

	// SELECT after DROP TABLE should error
	runExpectError(t, exec, "SELECT * FROM users")
}

func TestDropTableNotExists(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	runExpectError(t, exec, "DROP TABLE nonexistent")
}

func TestDropTableRecreate(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "DROP TABLE users")

	// Re-create the table
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestCreateIndex(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	result := run(t, exec, "CREATE INDEX idx_name ON users(name)")
	assert.Equal(t, "index created", result.Message)
}

func TestCreateIndexTableNotExists(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	runExpectError(t, exec, "CREATE INDEX idx_name ON nonexistent(name)")
}

func TestCreateIndexColumnNotExists(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	runExpectError(t, exec, "CREATE INDEX idx_foo ON users(foo)")
}

func TestCreateIndexDuplicateName(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")
	runExpectError(t, exec, "CREATE INDEX idx_name ON users(name)")
}

func TestDropIndex(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")
	result := run(t, exec, "DROP INDEX idx_name")
	assert.Equal(t, "index dropped", result.Message)
}

func TestDropIndexNotExists(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	runExpectError(t, exec, "DROP INDEX nonexistent")
}

func TestDropTableRemovesIndexes(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE INDEX idx_name ON users(name)")
	run(t, exec, "DROP TABLE users")

	// Re-create table and try to create index with same name
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	result := run(t, exec, "CREATE INDEX idx_name ON users(name)")
	assert.Equal(t, "index created", result.Message)
}

func TestPrimaryKeyCreateInsertSelect(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie')")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	// SELECT should return rows in PK order
	result := run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, "alice", result.Rows[0][1])
	assert.Equal(t, int64(2), result.Rows[1][0])
	assert.Equal(t, int64(3), result.Rows[2][0])
}

func TestPrimaryKeyDuplicateInsertError(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	runExpectError(t, exec, "INSERT INTO users VALUES (1, 'bob')")
}

func TestPrimaryKeyDeleteAndReinsert(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "DELETE FROM users WHERE id = 1")

	// Should be able to reinsert with the same PK
	run(t, exec, "INSERT INTO users VALUES (1, 'bob')")
	result := run(t, exec, "SELECT name FROM users WHERE id = 1")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "bob", result.Rows[0][0])
}

func TestPrimaryKeyImpliesNotNull(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	runExpectError(t, exec, "INSERT INTO users VALUES (NULL, 'alice')")
}

func TestTextPrimaryKeyAllowed(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	result := run(t, exec, "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)")
	assert.Equal(t, "table created", result.Message)
}

func TestErrorMultiplePrimaryKeys(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	runExpectError(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, code INT PRIMARY KEY)")
}

func TestPrimaryKeyUpdate(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	run(t, exec, "UPDATE users SET name = 'ALICE' WHERE id = 1")
	result := run(t, exec, "SELECT name FROM users WHERE id = 1")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "ALICE", result.Rows[0][0])
}

func TestPrimaryKeyTruncateAndReinsert(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "TRUNCATE TABLE users")

	// Should be able to insert with same PK after truncate
	run(t, exec, "INSERT INTO users VALUES (1, 'bob')")
	result := run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "bob", result.Rows[0][1])
}

func TestCompositePrimaryKey(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE enrollment (student_id INT, course_id INT, grade TEXT, PRIMARY KEY (student_id, course_id))")
	run(t, exec, "INSERT INTO enrollment VALUES (1, 100, 'A')")
	run(t, exec, "INSERT INTO enrollment VALUES (1, 200, 'B')")
	run(t, exec, "INSERT INTO enrollment VALUES (2, 100, 'C')")

	result := run(t, exec, "SELECT * FROM enrollment")
	require.Len(t, result.Rows, 3, "expected 3 rows")
}

func TestCompositePrimaryKeyDuplicate(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE enrollment (student_id INT, course_id INT, grade TEXT, PRIMARY KEY (student_id, course_id))")
	run(t, exec, "INSERT INTO enrollment VALUES (1, 100, 'A')")

	// Same composite key should fail
	runExpectError(t, exec, "INSERT INTO enrollment VALUES (1, 100, 'B')")
}

func TestCompositePrimaryKeyNull(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE enrollment (student_id INT, course_id INT, grade TEXT, PRIMARY KEY (student_id, course_id))")

	// NULL in PK column should fail
	runExpectError(t, exec, "INSERT INTO enrollment VALUES (NULL, 100, 'A')")
	runExpectError(t, exec, "INSERT INTO enrollment VALUES (1, NULL, 'A')")
}

func TestCompositePrimaryKeyWithTextColumns(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE tags (category TEXT, tag TEXT, value INT, PRIMARY KEY (category, tag))")
	run(t, exec, "INSERT INTO tags VALUES ('color', 'red', 1)")
	run(t, exec, "INSERT INTO tags VALUES ('color', 'blue', 2)")
	run(t, exec, "INSERT INTO tags VALUES ('size', 'red', 3)")

	result := run(t, exec, "SELECT * FROM tags")
	require.Len(t, result.Rows, 3, "expected 3 rows")

	// Duplicate composite key with TEXT should fail
	runExpectError(t, exec, "INSERT INTO tags VALUES ('color', 'red', 99)")
}

func TestCompositePrimaryKeyUpdate(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE enrollment (student_id INT, course_id INT, grade TEXT, PRIMARY KEY (student_id, course_id))")
	run(t, exec, "INSERT INTO enrollment VALUES (1, 100, 'A')")
	run(t, exec, "INSERT INTO enrollment VALUES (2, 100, 'B')")

	// Update to duplicate composite key should fail
	runExpectError(t, exec, "UPDATE enrollment SET student_id = 1 WHERE student_id = 2 AND course_id = 100")

	// Update non-PK column should succeed
	run(t, exec, "UPDATE enrollment SET grade = 'A+' WHERE student_id = 1 AND course_id = 100")
	result := run(t, exec, "SELECT grade FROM enrollment WHERE student_id = 1 AND course_id = 100")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "A+", result.Rows[0][0])
}

func TestCompositePrimaryKeyDropColumn(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE enrollment (student_id INT, course_id INT, grade TEXT, PRIMARY KEY (student_id, course_id))")

	// Dropping PK column should fail
	runExpectError(t, exec, "ALTER TABLE enrollment DROP COLUMN student_id")
	runExpectError(t, exec, "ALTER TABLE enrollment DROP COLUMN course_id")

	// Dropping non-PK column should succeed
	run(t, exec, "ALTER TABLE enrollment DROP COLUMN grade")
}

func TestBothColumnAndTablePrimaryKey(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))

	// Both column-level and table-level PK should error
	runExpectError(t, exec, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, PRIMARY KEY (id, name))")
}

func TestTableLevelSinglePrimaryKey(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))")
	run(t, exec, "INSERT INTO t VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO t VALUES (2, 'bob')")

	// Duplicate should fail
	runExpectError(t, exec, "INSERT INTO t VALUES (1, 'charlie')")

	result := run(t, exec, "SELECT * FROM t")
	require.Len(t, result.Rows, 2, "expected 2 rows")
}

func TestCompositePrimaryKeyDeleteAndReinsert(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE enrollment (student_id INT, course_id INT, grade TEXT, PRIMARY KEY (student_id, course_id))")
	run(t, exec, "INSERT INTO enrollment VALUES (1, 100, 'A')")

	// Delete the row
	run(t, exec, "DELETE FROM enrollment WHERE student_id = 1 AND course_id = 100")

	// Reinsert with the same PK should succeed
	run(t, exec, "INSERT INTO enrollment VALUES (1, 100, 'B')")

	result := run(t, exec, "SELECT grade FROM enrollment WHERE student_id = 1 AND course_id = 100")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "B", result.Rows[0][0])
}

func TestTextColumnPrimaryKey(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE codes (code TEXT PRIMARY KEY, description TEXT)")
	run(t, exec, "INSERT INTO codes VALUES ('A01', 'first')")
	run(t, exec, "INSERT INTO codes VALUES ('B02', 'second')")

	// Duplicate TEXT PK should fail
	runExpectError(t, exec, "INSERT INTO codes VALUES ('A01', 'duplicate')")

	// NULL in TEXT PK should fail
	runExpectError(t, exec, "INSERT INTO codes VALUES (NULL, 'null key')")

	result := run(t, exec, "SELECT * FROM codes")
	require.Len(t, result.Rows, 2, "expected 2 rows")
}

func TestFloatColumnPrimaryKey(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE measurements (sensor_id FLOAT PRIMARY KEY, value TEXT)")
	run(t, exec, "INSERT INTO measurements VALUES (1.5, 'a')")
	run(t, exec, "INSERT INTO measurements VALUES (2.5, 'b')")

	// Duplicate FLOAT PK should fail
	runExpectError(t, exec, "INSERT INTO measurements VALUES (1.5, 'duplicate')")

	result := run(t, exec, "SELECT * FROM measurements")
	require.Len(t, result.Rows, 2, "expected 2 rows")
}

func TestAlterTableAddColumn(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	result := run(t, exec, "ALTER TABLE users ADD COLUMN age INT")
	assert.Equal(t, "table altered", result.Message)

	// New column should be usable in INSERT
	run(t, exec, "INSERT INTO users VALUES (3, 'charlie', 30)")

	// Existing rows should have NULL for the new column
	result = run(t, exec, "SELECT id, name, age FROM users ORDER BY id")
	require.Len(t, result.Rows, 3, "expected 3 rows")
	assert.Nil(t, result.Rows[0][2])
	assert.Equal(t, int64(30), result.Rows[2][2])

	// COLUMN keyword should be optional
	result = run(t, exec, "ALTER TABLE users ADD email TEXT")
	assert.Equal(t, "table altered", result.Message)
}

func TestAlterTableAddColumnWithDefault(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob')")

	run(t, exec, "ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'active'")

	// Existing rows should have the default value
	result := run(t, exec, "SELECT id, status FROM users ORDER BY id")
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, "active", result.Rows[0][1])
	assert.Equal(t, "active", result.Rows[1][1])
}

func TestAlterTableAddColumnNotNull(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	// NOT NULL + DEFAULT should work: existing rows get the default
	run(t, exec, "ALTER TABLE users ADD COLUMN age INT NOT NULL DEFAULT 0")

	result := run(t, exec, "SELECT id, age FROM users")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(0), result.Rows[0][1])
}

func TestAlterTableAddColumnNotNullNoDefault(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	// NOT NULL without DEFAULT should error when rows exist
	runExpectError(t, exec, "ALTER TABLE users ADD COLUMN age INT NOT NULL")
}

func TestAlterTableDropColumn(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 25)")

	result := run(t, exec, "ALTER TABLE users DROP COLUMN age")
	assert.Equal(t, "table altered", result.Message)

	// Column should be gone
	result = run(t, exec, "SELECT * FROM users ORDER BY id")
	require.Len(t, result.Columns, 2, "expected 2 columns")
	assert.Equal(t, []string{"id", "name"}, result.Columns)
	require.Len(t, result.Rows, 2, "expected 2 rows")
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, "alice", result.Rows[0][1])
	assert.Equal(t, int64(2), result.Rows[1][0])
	assert.Equal(t, "bob", result.Rows[1][1])

	// COLUMN keyword should be optional
	result = run(t, exec, "ALTER TABLE users DROP name")
	assert.Equal(t, "table altered", result.Message)
}

func TestAlterTableDropColumnWithIndex(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "CREATE INDEX idx_age ON users(age)")

	// Dropping a column with a single-column index should auto-delete the index
	run(t, exec, "ALTER TABLE users DROP COLUMN age")

	// Verify the index is gone by creating a new one with the same name
	run(t, exec, "ALTER TABLE users ADD COLUMN age INT")
	run(t, exec, "CREATE INDEX idx_age ON users(age)")
}

func TestAlterTableDropColumnCompositeIndex(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, age INT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 30)")
	run(t, exec, "CREATE INDEX idx_name_age ON users(name, age)")

	// Dropping a column used in a composite index should error
	runExpectError(t, exec, "ALTER TABLE users DROP COLUMN age")
	runExpectError(t, exec, "ALTER TABLE users DROP COLUMN name")
}

func TestAlterTableDropColumnAdjustsIndex(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (a INT, b INT, c INT)")
	run(t, exec, "INSERT INTO t VALUES (1, 10, 100)")
	run(t, exec, "INSERT INTO t VALUES (2, 20, 200)")
	run(t, exec, "CREATE INDEX idx_c ON t(c)")

	// Drop column b (index 1) — index on c should be adjusted
	run(t, exec, "ALTER TABLE t DROP COLUMN b")

	// Index on c should still work for lookups
	result := run(t, exec, "SELECT a, c FROM t WHERE c = 100")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, int64(1), result.Rows[0][0])
}

func TestAlterTableDropPrimaryKey(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	// Cannot drop PK column
	runExpectError(t, exec, "ALTER TABLE users DROP COLUMN id")
}

func TestAlterTableDropLastColumn(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE t (a INT)")

	// Cannot drop the last column
	runExpectError(t, exec, "ALTER TABLE t DROP COLUMN a")
}

func TestAlterTableDuplicateColumnName(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")

	// Adding duplicate column name should error
	runExpectError(t, exec, "ALTER TABLE users ADD COLUMN name TEXT")
}

func TestAlterTableAddColumnUnique(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")

	run(t, exec, "ALTER TABLE users ADD COLUMN email TEXT UNIQUE")

	// Both existing rows have NULL for email (allowed by UNIQUE)
	run(t, exec, "INSERT INTO users VALUES (2, 'bob', 'bob@example.com')")

	// Duplicate email should fail
	runExpectError(t, exec, "INSERT INTO users VALUES (3, 'charlie', 'bob@example.com')")
}

func TestUniqueColumnConstraint(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, email TEXT UNIQUE)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice@example.com')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob@example.com')")

	// Duplicate email should fail
	runExpectError(t, exec, "INSERT INTO users VALUES (3, 'alice@example.com')")

	// Verify existing data is intact
	result := run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 2, "expected 2 rows")
}

func TestUniqueColumnInsertNull(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, email TEXT UNIQUE)")

	// Multiple NULLs should be allowed per SQL standard
	run(t, exec, "INSERT INTO users VALUES (1, NULL)")
	run(t, exec, "INSERT INTO users VALUES (2, NULL)")

	result := run(t, exec, "SELECT * FROM users")
	require.Len(t, result.Rows, 2, "expected 2 rows")
}

func TestCreateUniqueIndex(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, email TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice@example.com')")
	run(t, exec, "CREATE UNIQUE INDEX idx_email ON users(email)")

	// Duplicate email should fail
	runExpectError(t, exec, "INSERT INTO users VALUES (2, 'alice@example.com')")

	// Different email should succeed
	run(t, exec, "INSERT INTO users VALUES (3, 'bob@example.com')")
}

func TestCreateUniqueCompositeIndex(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE orders (user_id INT, product_id INT, qty INT)")
	run(t, exec, "CREATE UNIQUE INDEX idx_user_product ON orders(user_id, product_id)")

	run(t, exec, "INSERT INTO orders VALUES (1, 100, 1)")
	run(t, exec, "INSERT INTO orders VALUES (1, 200, 2)")
	run(t, exec, "INSERT INTO orders VALUES (2, 100, 3)")

	// Duplicate (user_id, product_id) should fail
	runExpectError(t, exec, "INSERT INTO orders VALUES (1, 100, 5)")

	result := run(t, exec, "SELECT * FROM orders")
	require.Len(t, result.Rows, 3, "expected 3 rows")
}

func TestCreateUniqueIndexOnExistingDuplicates(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, email TEXT)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice@example.com')")
	run(t, exec, "INSERT INTO users VALUES (2, 'alice@example.com')")

	// Creating unique index on data with duplicates should fail
	runExpectError(t, exec, "CREATE UNIQUE INDEX idx_email ON users(email)")
}

func TestUniqueUpdateViolation(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, email TEXT UNIQUE)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice@example.com')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob@example.com')")

	// Update that causes duplicate should fail
	runExpectError(t, exec, "UPDATE users SET email = 'alice@example.com' WHERE id = 2")

	// Verify data unchanged
	result := run(t, exec, "SELECT email FROM users WHERE id = 2")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "bob@example.com", result.Rows[0][0])
}

func TestUniqueUpdateSameRow(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, email TEXT UNIQUE)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice@example.com')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob@example.com')")

	// Updating a row to the same value should succeed
	run(t, exec, "UPDATE users SET email = 'alice@example.com' WHERE id = 1")

	result := run(t, exec, "SELECT email FROM users WHERE id = 1")
	require.Len(t, result.Rows, 1, "expected 1 row")
	assert.Equal(t, "alice@example.com", result.Rows[0][0])
}

func TestUniqueColumnWithNotNull(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	run(t, exec, "CREATE TABLE users (id INT, email TEXT NOT NULL UNIQUE)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice@example.com')")
	run(t, exec, "INSERT INTO users VALUES (2, 'bob@example.com')")

	// Duplicate should fail
	runExpectError(t, exec, "INSERT INTO users VALUES (3, 'alice@example.com')")

	// NULL should fail (due to NOT NULL, not UNIQUE)
	runExpectError(t, exec, "INSERT INTO users VALUES (4, NULL)")
}

func TestCreateTableWithDefault(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	result := run(t, exec, "CREATE TABLE t (id INT DEFAULT 0, name TEXT DEFAULT 'unknown')")
	assert.Equal(t, "table created", result.Message)
}

func TestErrorCreateTableNotNullDefaultNull(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	runExpectError(t, exec, "CREATE TABLE t (id INT NOT NULL DEFAULT NULL)")
}

func TestErrorCreateTableDefaultTypeMismatch(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
	runExpectError(t, exec, "CREATE TABLE t (id INT DEFAULT 'hello')")
}

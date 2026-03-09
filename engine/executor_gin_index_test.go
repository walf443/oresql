package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupGinTestExecutor(t *testing.T, storageType string) *Executor {
	t.Helper()
	if storageType == "disk" {
		tmpDir := t.TempDir()
		db := NewDatabase("test", WithDataDir(tmpDir), WithStorageType("disk"))
		return NewExecutor(db)
	}
	return NewExecutor(NewDatabase("test"))
}

func TestGinIndexBasicSearch(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE articles (id INT PRIMARY KEY, title TEXT, body TEXT)")
			run(t, exec, "INSERT INTO articles VALUES (1, 'hello world', 'this is a test article')")
			run(t, exec, "INSERT INTO articles VALUES (2, 'goodbye world', 'another test document')")
			run(t, exec, "INSERT INTO articles VALUES (3, 'hello again', 'testing hello search')")
			run(t, exec, "CREATE INDEX idx_body_gin ON articles(body) USING GIN")

			result := run(t, exec, "SELECT id, title FROM articles WHERE body @@ 'test'")
			require.Len(t, result.Rows, 2)
			assert.Equal(t, int64(1), result.Rows[0][0])
			assert.Equal(t, int64(2), result.Rows[1][0])
		})
	}
}

func TestGinIndexNoMatch(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE docs (id INT PRIMARY KEY, content TEXT)")
			run(t, exec, "INSERT INTO docs VALUES (1, 'the quick brown fox')")
			run(t, exec, "INSERT INTO docs VALUES (2, 'lazy dog sleeping')")
			run(t, exec, "CREATE INDEX idx_content_gin ON docs(content) USING GIN")

			result := run(t, exec, "SELECT id FROM docs WHERE content @@ 'cat'")
			require.Len(t, result.Rows, 0)
		})
	}
}

func TestGinIndexCaseInsensitive(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)")
			run(t, exec, "INSERT INTO notes VALUES (1, 'Hello World')")
			run(t, exec, "INSERT INTO notes VALUES (2, 'HELLO EVERYONE')")
			run(t, exec, "CREATE INDEX idx_notes_gin ON notes(body) USING GIN")

			result := run(t, exec, "SELECT id FROM notes WHERE body @@ 'hello'")
			require.Len(t, result.Rows, 2)
			assert.Equal(t, int64(1), result.Rows[0][0])
			assert.Equal(t, int64(2), result.Rows[1][0])
		})
	}
}

func TestGinIndexMaintainedOnInsert(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE logs (id INT PRIMARY KEY, message TEXT)")
			run(t, exec, "INSERT INTO logs VALUES (1, 'error occurred')")
			run(t, exec, "CREATE INDEX idx_logs_gin ON logs(message) USING GIN")
			run(t, exec, "INSERT INTO logs VALUES (2, 'another error here')")
			run(t, exec, "INSERT INTO logs VALUES (3, 'all good')")

			result := run(t, exec, "SELECT id FROM logs WHERE message @@ 'error'")
			require.Len(t, result.Rows, 2)
			assert.Equal(t, int64(1), result.Rows[0][0])
			assert.Equal(t, int64(2), result.Rows[1][0])
		})
	}
}

func TestGinIndexMaintainedOnDelete(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE items (id INT PRIMARY KEY, description TEXT)")
			run(t, exec, "INSERT INTO items VALUES (1, 'red apple')")
			run(t, exec, "INSERT INTO items VALUES (2, 'green apple')")
			run(t, exec, "INSERT INTO items VALUES (3, 'red cherry')")
			run(t, exec, "CREATE INDEX idx_items_gin ON items(description) USING GIN")
			run(t, exec, "DELETE FROM items WHERE id = 1")

			result := run(t, exec, "SELECT id FROM items WHERE description @@ 'red'")
			require.Len(t, result.Rows, 1)
			assert.Equal(t, int64(3), result.Rows[0][0])
		})
	}
}

func TestGinIndexMaintainedOnUpdate(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE products (id INT PRIMARY KEY, name TEXT)")
			run(t, exec, "INSERT INTO products VALUES (1, 'blue shirt')")
			run(t, exec, "INSERT INTO products VALUES (2, 'red shirt')")
			run(t, exec, "CREATE INDEX idx_products_gin ON products(name) USING GIN")
			run(t, exec, "UPDATE products SET name = 'green shirt' WHERE id = 1")

			result := run(t, exec, "SELECT id FROM products WHERE name @@ 'blue'")
			require.Len(t, result.Rows, 0)

			result = run(t, exec, "SELECT id FROM products WHERE name @@ 'green'")
			require.Len(t, result.Rows, 1)
			assert.Equal(t, int64(1), result.Rows[0][0])
		})
	}
}

func TestGinIndexWithNullValues(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE data (id INT PRIMARY KEY, info TEXT)")
			run(t, exec, "INSERT INTO data VALUES (1, NULL)")
			run(t, exec, "INSERT INTO data VALUES (2, 'some info here')")
			run(t, exec, "CREATE INDEX idx_data_gin ON data(info) USING GIN")

			result := run(t, exec, "SELECT id FROM data WHERE info @@ 'info'")
			require.Len(t, result.Rows, 1)
			assert.Equal(t, int64(2), result.Rows[0][0])
		})
	}
}

func TestGinIndexOnIntColumnFails(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE nums (id INT PRIMARY KEY, val INT)")
			runExpectError(t, exec, "CREATE INDEX idx_nums_gin ON nums(val) USING GIN")
		})
	}
}

func TestGinIndexExplain(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE articles (id INT PRIMARY KEY, body TEXT)")
			run(t, exec, "INSERT INTO articles VALUES (1, 'hello world')")
			run(t, exec, "CREATE INDEX idx_body_gin ON articles(body) USING GIN")

			assertExplain(t, exec, "SELECT id FROM articles WHERE body @@ 'hello'", []planRow{
				{Table: "articles", Type: "fulltext", Key: "idx_body_gin"},
			})
		})
	}
}

func TestGinIndexPersistence(t *testing.T) {
	tmpDir := t.TempDir()

	// Create database, insert data, create GIN index
	db := NewDatabase("test", WithDataDir(tmpDir), WithStorageType("disk"))
	exec := NewExecutor(db)
	run(t, exec, "CREATE TABLE articles (id INT PRIMARY KEY, body TEXT)")
	run(t, exec, "INSERT INTO articles VALUES (1, 'hello world')")
	run(t, exec, "INSERT INTO articles VALUES (2, 'goodbye world')")
	run(t, exec, "CREATE INDEX idx_body_gin ON articles(body) USING GIN")

	// Verify search works
	result := run(t, exec, "SELECT id FROM articles WHERE body @@ 'hello'")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(1), result.Rows[0][0])

	// Reopen database
	db2 := NewDatabase("test", WithDataDir(tmpDir), WithStorageType("disk"))
	exec2 := NewExecutor(db2)

	// Verify GIN index still works after reopening
	result2 := run(t, exec2, "SELECT id FROM articles WHERE body @@ 'hello'")
	require.Len(t, result2.Rows, 1)
	assert.Equal(t, int64(1), result2.Rows[0][0])

	result3 := run(t, exec2, "SELECT id FROM articles WHERE body @@ 'world'")
	require.Len(t, result3.Rows, 2)
}

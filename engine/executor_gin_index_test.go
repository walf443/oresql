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

func TestGinIndexLikePrefix(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE articles (id INT PRIMARY KEY, body TEXT)")
			run(t, exec, "INSERT INTO articles VALUES (1, 'testing the system')")
			run(t, exec, "INSERT INTO articles VALUES (2, 'a simple test case')")
			run(t, exec, "INSERT INTO articles VALUES (3, 'contest winner')")
			run(t, exec, "INSERT INTO articles VALUES (4, 'no match here')")
			run(t, exec, "CREATE INDEX idx_body_gin ON articles(body) USING GIN")

			// LIKE 'test%' matches row 1 ("testing the system") — starts with "test"
			// GIN narrows candidates via token prefix, then LIKE filters exactly
			result := run(t, exec, "SELECT id FROM articles WHERE body LIKE 'test%'")
			require.Len(t, result.Rows, 1)
			assert.Equal(t, int64(1), result.Rows[0][0])

			// EXPLAIN should show GIN index usage
			assertExplain(t, exec, "SELECT id FROM articles WHERE body LIKE 'test%'", []planRow{
				{Table: "articles", Type: "fulltext", Key: "idx_body_gin"},
			})
		})
	}
}

func TestGinIndexLikePrefixNoMatch(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE docs (id INT PRIMARY KEY, content TEXT)")
			run(t, exec, "INSERT INTO docs VALUES (1, 'hello world')")
			run(t, exec, "CREATE INDEX idx_gin ON docs(content) USING GIN")

			result := run(t, exec, "SELECT id FROM docs WHERE content LIKE 'xyz%'")
			require.Len(t, result.Rows, 0)
		})
	}
}

func TestGinIndexLikeContainsNotUsesGin(t *testing.T) {
	// LIKE '%word%' (contains) should NOT use GIN
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE docs (id INT PRIMARY KEY, content TEXT)")
			run(t, exec, "INSERT INTO docs VALUES (1, 'hello world')")
			run(t, exec, "CREATE INDEX idx_gin ON docs(content) USING GIN")

			assertExplain(t, exec, "SELECT id FROM docs WHERE content LIKE '%hello%'", []planRow{
				{Table: "docs", Type: "full scan"},
			})
		})
	}
}

func TestGinIndexBigramTokenizer(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE articles (id INT PRIMARY KEY, body TEXT)")
			run(t, exec, "INSERT INTO articles VALUES (1, '東京都は日本の首都です')")
			run(t, exec, "INSERT INTO articles VALUES (2, '京都は古い都市です')")
			run(t, exec, "INSERT INTO articles VALUES (3, '大阪は楽しい街です')")
			run(t, exec, "CREATE INDEX idx_body_gin ON articles(body) USING GIN WITH (tokenizer = 'bigram')")

			// @@ '東京' should match row 1 (contains bigram "東京")
			result := run(t, exec, "SELECT id FROM articles WHERE body @@ '東京'")
			require.Len(t, result.Rows, 1)
			assert.Equal(t, int64(1), result.Rows[0][0])

			// @@ '京都' should match rows 1 ("東京都" contains "京都") and 2 ("京都")
			result = run(t, exec, "SELECT id FROM articles WHERE body @@ '京都'")
			require.Len(t, result.Rows, 2)
			assert.Equal(t, int64(1), result.Rows[0][0])
			assert.Equal(t, int64(2), result.Rows[1][0])

			// @@ '大阪' should match row 3 only
			result = run(t, exec, "SELECT id FROM articles WHERE body @@ '大阪'")
			require.Len(t, result.Rows, 1)
			assert.Equal(t, int64(3), result.Rows[0][0])

			// @@ '東京都' (3 chars) should be split into bigrams ["東京","京都"]
			// and intersect: row 1 has both, row 2 has "京都" but not "東京"
			result = run(t, exec, "SELECT id FROM articles WHERE body @@ '東京都'")
			require.Len(t, result.Rows, 1)
			assert.Equal(t, int64(1), result.Rows[0][0])

			// @@ '日本の首都' (5 chars) → bigrams ["日本","本の","の首","首都"]
			// only row 1 has all of them
			result = run(t, exec, "SELECT id FROM articles WHERE body @@ '日本の首都'")
			require.Len(t, result.Rows, 1)
			assert.Equal(t, int64(1), result.Rows[0][0])

			// @@ '古い都市' → bigrams ["古い","い都","都市"]
			// only row 2 has all of them
			result = run(t, exec, "SELECT id FROM articles WHERE body @@ '古い都市'")
			require.Len(t, result.Rows, 1)
			assert.Equal(t, int64(2), result.Rows[0][0])

			// @@ '福岡' should match nothing
			result = run(t, exec, "SELECT id FROM articles WHERE body @@ '福岡'")
			require.Len(t, result.Rows, 0)
		})
	}
}

func TestGinIndexBigramLikePrefix(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE docs (id INT PRIMARY KEY, content TEXT)")
			run(t, exec, "INSERT INTO docs VALUES (1, '東京タワー')")
			run(t, exec, "INSERT INTO docs VALUES (2, '東京スカイツリー')")
			run(t, exec, "INSERT INTO docs VALUES (3, '大阪城')")
			run(t, exec, "CREATE INDEX idx_gin ON docs(content) USING GIN WITH (tokenizer = 'bigram')")

			// LIKE '東京%' should use GIN and match rows 1, 2
			result := run(t, exec, "SELECT id FROM docs WHERE content LIKE '東京%'")
			require.Len(t, result.Rows, 2)
			assert.Equal(t, int64(1), result.Rows[0][0])
			assert.Equal(t, int64(2), result.Rows[1][0])

			assertExplain(t, exec, "SELECT id FROM docs WHERE content LIKE '東京%'", []planRow{
				{Table: "docs", Type: "fulltext", Key: "idx_gin"},
			})
		})
	}
}

func TestGinIndexBigramLikeAllPatterns(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE docs (id INT PRIMARY KEY, content TEXT)")
			run(t, exec, "INSERT INTO docs VALUES (1, '東京タワー')")
			run(t, exec, "INSERT INTO docs VALUES (2, '東京スカイツリー')")
			run(t, exec, "INSERT INTO docs VALUES (3, '大阪城')")
			run(t, exec, "INSERT INTO docs VALUES (4, '京都タワー')")
			run(t, exec, "CREATE INDEX idx_gin ON docs(content) USING GIN WITH (tokenizer = 'bigram')")

			// LIKE '%タワー' (suffix) should use GIN, match rows 1, 4
			result := run(t, exec, "SELECT id FROM docs WHERE content LIKE '%タワー'")
			require.Len(t, result.Rows, 2)
			assert.Equal(t, int64(1), result.Rows[0][0])
			assert.Equal(t, int64(4), result.Rows[1][0])
			assertExplain(t, exec, "SELECT id FROM docs WHERE content LIKE '%タワー'", []planRow{
				{Table: "docs", Type: "fulltext", Key: "idx_gin"},
			})

			// LIKE '%東京%' (contains) should use GIN, match rows 1, 2
			result = run(t, exec, "SELECT id FROM docs WHERE content LIKE '%東京%'")
			require.Len(t, result.Rows, 2)
			assert.Equal(t, int64(1), result.Rows[0][0])
			assert.Equal(t, int64(2), result.Rows[1][0])
			assertExplain(t, exec, "SELECT id FROM docs WHERE content LIKE '%東京%'", []planRow{
				{Table: "docs", Type: "fulltext", Key: "idx_gin"},
			})

			// LIKE '大阪城' (exact) should use GIN, match row 3
			result = run(t, exec, "SELECT id FROM docs WHERE content LIKE '大阪城'")
			require.Len(t, result.Rows, 1)
			assert.Equal(t, int64(3), result.Rows[0][0])
			assertExplain(t, exec, "SELECT id FROM docs WHERE content LIKE '大阪城'", []planRow{
				{Table: "docs", Type: "fulltext", Key: "idx_gin"},
			})

			// LIKE '%京%' (single char) should NOT use GIN (< 2 chars literal)
			assertExplain(t, exec, "SELECT id FROM docs WHERE content LIKE '%京%'", []planRow{
				{Table: "docs", Type: "full scan"},
			})
		})
	}
}

func TestGinIndexBigramPersistence(t *testing.T) {
	tmpDir := t.TempDir()

	db := NewDatabase("test", WithDataDir(tmpDir), WithStorageType("disk"))
	exec := NewExecutor(db)
	run(t, exec, "CREATE TABLE articles (id INT PRIMARY KEY, body TEXT)")
	run(t, exec, "INSERT INTO articles VALUES (1, '東京都')")
	run(t, exec, "INSERT INTO articles VALUES (2, '京都市')")
	run(t, exec, "CREATE INDEX idx_gin ON articles(body) USING GIN WITH (tokenizer = 'bigram')")

	result := run(t, exec, "SELECT id FROM articles WHERE body @@ '東京'")
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(1), result.Rows[0][0])

	// Reopen database
	db2 := NewDatabase("test", WithDataDir(tmpDir), WithStorageType("disk"))
	exec2 := NewExecutor(db2)

	// Verify bigram GIN index still works after reopening
	result2 := run(t, exec2, "SELECT id FROM articles WHERE body @@ '東京'")
	require.Len(t, result2.Rows, 1)
	assert.Equal(t, int64(1), result2.Rows[0][0])

	result3 := run(t, exec2, "SELECT id FROM articles WHERE body @@ '京都'")
	require.Len(t, result3.Rows, 2)
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

func TestGinIndexWordEquality(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE docs (id INT PRIMARY KEY, content TEXT)")
			run(t, exec, "INSERT INTO docs VALUES (1, 'the quick brown fox')")
			run(t, exec, "INSERT INTO docs VALUES (2, 'lazy dog sleeping')")
			run(t, exec, "INSERT INTO docs VALUES (3, 'the quick brown fox')")
			run(t, exec, "CREATE INDEX idx_content_gin ON docs(content) USING GIN")

			// Exact match with word tokenizer should NOT use GIN index
			// (word tokenizer GIN is for full-text search, not exact match)
			explain := run(t, exec, "EXPLAIN SELECT id FROM docs WHERE content = 'the quick brown fox'")
			found := false
			for _, row := range explain.Rows {
				for _, col := range row {
					if s, ok := col.(string); ok && s == "idx_content_gin" {
						found = true
					}
				}
			}
			assert.False(t, found, "word tokenizer GIN should not be used for equality")

			// But result should still be correct via full scan
			result := run(t, exec, "SELECT id FROM docs WHERE content = 'the quick brown fox'")
			require.Len(t, result.Rows, 2)
			assert.Equal(t, int64(1), result.Rows[0][0])
			assert.Equal(t, int64(3), result.Rows[1][0])
		})
	}
}

func TestGinIndexWordIN(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE docs (id INT PRIMARY KEY, content TEXT)")
			run(t, exec, "INSERT INTO docs VALUES (1, 'the quick brown fox')")
			run(t, exec, "INSERT INTO docs VALUES (2, 'lazy dog sleeping')")
			run(t, exec, "INSERT INTO docs VALUES (3, 'hello world')")
			run(t, exec, "CREATE INDEX idx_content_gin ON docs(content) USING GIN")

			// IN with word tokenizer should NOT use GIN index
			explain := run(t, exec, "EXPLAIN SELECT id FROM docs WHERE content IN ('the quick brown fox', 'hello world')")
			found := false
			for _, row := range explain.Rows {
				for _, col := range row {
					if s, ok := col.(string); ok && s == "idx_content_gin" {
						found = true
					}
				}
			}
			assert.False(t, found, "word tokenizer GIN should not be used for IN")

			// But result should still be correct via full scan
			result := run(t, exec, "SELECT id FROM docs WHERE content IN ('the quick brown fox', 'hello world')")
			require.Len(t, result.Rows, 2)
			assert.Equal(t, int64(1), result.Rows[0][0])
			assert.Equal(t, int64(3), result.Rows[1][0])
		})
	}
}

func TestGinIndexBTreePriority(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE articles (id INT PRIMARY KEY, body TEXT)")
			run(t, exec, "INSERT INTO articles VALUES (1, '東京都は日本の首都です')")
			run(t, exec, "INSERT INTO articles VALUES (2, '京都は古い都市です')")
			run(t, exec, "INSERT INTO articles VALUES (3, '大阪は楽しい街です')")
			run(t, exec, "CREATE INDEX idx_body_btree ON articles(body)")
			run(t, exec, "CREATE INDEX idx_body_gin ON articles(body) USING GIN WITH (tokenizer = 'bigram')")

			// Equality: B-tree should be used, not GIN
			explain := run(t, exec, "EXPLAIN SELECT id FROM articles WHERE body = '東京都は日本の首都です'")
			btreeUsed := false
			ginUsed := false
			for _, row := range explain.Rows {
				for _, col := range row {
					if s, ok := col.(string); ok {
						if s == "idx_body_btree" {
							btreeUsed = true
						}
						if s == "idx_body_gin" {
							ginUsed = true
						}
					}
				}
			}
			assert.True(t, btreeUsed, "B-tree index should be used for equality")
			assert.False(t, ginUsed, "GIN index should not be used when B-tree is available")

			// IN: B-tree should be used (access type "range"), not GIN ("fulltext")
			explain2 := run(t, exec, "EXPLAIN SELECT id FROM articles WHERE body IN ('東京都は日本の首都です', '大阪は楽しい街です')")
			ginUsed = false
			hasRange := false
			for _, row := range explain2.Rows {
				for _, col := range row {
					if s, ok := col.(string); ok {
						if s == "idx_body_gin" {
							ginUsed = true
						}
						if s == "range" {
							hasRange = true
						}
					}
				}
			}
			assert.True(t, hasRange, "B-tree index should be used for IN (access type 'range')")
			assert.False(t, ginUsed, "GIN index should not be used for IN when B-tree is available")

			// Result correctness
			result := run(t, exec, "SELECT id FROM articles WHERE body = '東京都は日本の首都です'")
			require.Len(t, result.Rows, 1)
			assert.Equal(t, int64(1), result.Rows[0][0])
		})
	}
}

func TestGinIndexBigramEquality(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE articles (id INT PRIMARY KEY, body TEXT)")
			run(t, exec, "INSERT INTO articles VALUES (1, '東京都は日本の首都です')")
			run(t, exec, "INSERT INTO articles VALUES (2, '京都は古い都市です')")
			run(t, exec, "INSERT INTO articles VALUES (3, '大阪は楽しい街です')")
			run(t, exec, "CREATE INDEX idx_body_gin ON articles(body) USING GIN WITH (tokenizer = 'bigram')")

			// Exact match using GIN index
			result := run(t, exec, "SELECT id FROM articles WHERE body = '東京都は日本の首都です'")
			require.Len(t, result.Rows, 1)
			assert.Equal(t, int64(1), result.Rows[0][0])

			// Verify GIN index is used via EXPLAIN
			explain := run(t, exec, "EXPLAIN SELECT id FROM articles WHERE body = '東京都は日本の首都です'")
			found := false
			for _, row := range explain.Rows {
				for _, col := range row {
					if s, ok := col.(string); ok && s == "idx_body_gin" {
						found = true
					}
				}
			}
			assert.True(t, found, "expected GIN index idx_body_gin to be used")

			// No match
			result2 := run(t, exec, "SELECT id FROM articles WHERE body = '存在しないテキスト'")
			require.Len(t, result2.Rows, 0)
		})
	}
}

func TestGinIndexBigramIN(t *testing.T) {
	for _, st := range []string{"memory", "disk"} {
		t.Run(st, func(t *testing.T) {
			exec := setupGinTestExecutor(t, st)
			run(t, exec, "CREATE TABLE articles (id INT PRIMARY KEY, body TEXT)")
			run(t, exec, "INSERT INTO articles VALUES (1, '東京都は日本の首都です')")
			run(t, exec, "INSERT INTO articles VALUES (2, '京都は古い都市です')")
			run(t, exec, "INSERT INTO articles VALUES (3, '大阪は楽しい街です')")
			run(t, exec, "CREATE INDEX idx_body_gin ON articles(body) USING GIN WITH (tokenizer = 'bigram')")

			// IN search using GIN index
			result := run(t, exec, "SELECT id FROM articles WHERE body IN ('東京都は日本の首都です', '大阪は楽しい街です')")
			require.Len(t, result.Rows, 2)
			assert.Equal(t, int64(1), result.Rows[0][0])
			assert.Equal(t, int64(3), result.Rows[1][0])

			// Verify GIN index is used via EXPLAIN
			explain := run(t, exec, "EXPLAIN SELECT id FROM articles WHERE body IN ('東京都は日本の首都です', '大阪は楽しい街です')")
			found := false
			for _, row := range explain.Rows {
				for _, col := range row {
					if s, ok := col.(string); ok && s == "idx_body_gin" {
						found = true
					}
				}
			}
			assert.True(t, found, "expected GIN index idx_body_gin to be used")
		})
	}
}

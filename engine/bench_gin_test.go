package engine

import (
	"fmt"
	"strings"
	"testing"
)

// setupGinBenchTable creates a table with N rows of Japanese text and optionally
// creates a GIN index with bigram tokenizer.
// Schema: articles (id INT PRIMARY KEY, body TEXT)
func setupGinBenchTable(b *testing.B, n int, withGinIndex bool) *Executor {
	b.Helper()
	exec := NewExecutor(NewDatabase("test"))
	if err := execSQL(exec, "CREATE TABLE articles (id INT PRIMARY KEY, body TEXT)"); err != nil {
		b.Fatal(err)
	}

	bodies := []string{
		"東京都は日本の首都です",
		"京都は古い都市です",
		"大阪は楽しい街です",
		"東京タワーは観光名所です",
		"東京スカイツリーは新しい名所です",
		"横浜は港町として有名です",
		"名古屋は中部地方の中心です",
		"福岡は九州の玄関口です",
		"札幌は北海道の中心都市です",
		"神戸は港町で有名です",
	}

	batchSize := 1000
	for start := 0; start < n; start += batchSize {
		end := start + batchSize
		if end > n {
			end = n
		}
		var buf strings.Builder
		buf.WriteString("INSERT INTO articles VALUES ")
		for i := start; i < end; i++ {
			if i > start {
				buf.WriteString(", ")
			}
			body := bodies[i%len(bodies)]
			fmt.Fprintf(&buf, "(%d, '%s')", i, body)
		}
		if err := execSQL(exec, buf.String()); err != nil {
			b.Fatal(err)
		}
	}

	if withGinIndex {
		if err := execSQL(exec, "CREATE INDEX idx_body_gin ON articles(body) USING GIN WITH (tokenizer = 'bigram')"); err != nil {
			b.Fatal(err)
		}
	}

	return exec
}

// --- GIN bigram LIKE prefix (LIKE 'word%') ---

func BenchmarkGinLikePrefixNoIndex_10000(b *testing.B) {
	exec := setupGinBenchTable(b, 10000, false)
	sql := "SELECT id FROM articles WHERE body LIKE '東京%'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGinLikePrefixWithIndex_10000(b *testing.B) {
	exec := setupGinBenchTable(b, 10000, true)
	sql := "SELECT id FROM articles WHERE body LIKE '東京%'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- GIN bigram LIKE contains (LIKE '%word%') ---

func BenchmarkGinLikeContainsNoIndex_10000(b *testing.B) {
	exec := setupGinBenchTable(b, 10000, false)
	sql := "SELECT id FROM articles WHERE body LIKE '%タワー%'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGinLikeContainsWithIndex_10000(b *testing.B) {
	exec := setupGinBenchTable(b, 10000, true)
	sql := "SELECT id FROM articles WHERE body LIKE '%タワー%'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- GIN bigram LIKE suffix (LIKE '%word') ---

func BenchmarkGinLikeSuffixNoIndex_10000(b *testing.B) {
	exec := setupGinBenchTable(b, 10000, false)
	sql := "SELECT id FROM articles WHERE body LIKE '%観光名所です'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGinLikeSuffixWithIndex_10000(b *testing.B) {
	exec := setupGinBenchTable(b, 10000, true)
	sql := "SELECT id FROM articles WHERE body LIKE '%観光名所です'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- GIN AND intersection (body @@ X AND body @@ Y) ---

func BenchmarkGinAndNoIndex_10000(b *testing.B) {
	exec := setupGinBenchTable(b, 10000, false)
	sql := "SELECT id FROM articles WHERE body LIKE '%東京%' AND body LIKE '%名所%'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGinAndWithIndex_10000(b *testing.B) {
	exec := setupGinBenchTable(b, 10000, true)
	sql := "SELECT id FROM articles WHERE body @@ '東京' AND body @@ '名所'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- GIN OR union (body @@ X OR body @@ Y) ---

func BenchmarkGinOrNoIndex_10000(b *testing.B) {
	exec := setupGinBenchTable(b, 10000, false)
	sql := "SELECT id FROM articles WHERE body LIKE '%東京%' OR body LIKE '%大阪%'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGinOrWithIndex_10000(b *testing.B) {
	exec := setupGinBenchTable(b, 10000, true)
	sql := "SELECT id FROM articles WHERE body @@ '東京' OR body @@ '大阪'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- GIN OR with many terms ---

func BenchmarkGinOrManyTermsNoIndex_10000(b *testing.B) {
	exec := setupGinBenchTable(b, 10000, false)
	sql := "SELECT id FROM articles WHERE body LIKE '%東京%' OR body LIKE '%大阪%' OR body LIKE '%福岡%' OR body LIKE '%札幌%'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGinOrManyTermsWithIndex_10000(b *testing.B) {
	exec := setupGinBenchTable(b, 10000, true)
	sql := "SELECT id FROM articles WHERE body @@ '東京' OR body @@ '大阪' OR body @@ '福岡' OR body @@ '札幌'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

// --- GIN AND+OR combined ---

func BenchmarkGinAndOrNoIndex_10000(b *testing.B) {
	exec := setupGinBenchTable(b, 10000, false)
	sql := "SELECT id FROM articles WHERE (body LIKE '%東京%' OR body LIKE '%大阪%') AND body LIKE '%名所%'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGinAndOrWithIndex_10000(b *testing.B) {
	exec := setupGinBenchTable(b, 10000, true)
	sql := "SELECT id FROM articles WHERE (body @@ '東京' OR body @@ '大阪') AND body @@ '名所'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
}

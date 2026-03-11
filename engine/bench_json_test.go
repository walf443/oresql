package engine

import (
	"fmt"
	"testing"

	"github.com/walf443/oresql/lexer"
	"github.com/walf443/oresql/parser"
)

func setupJSONBench(b *testing.B, colType string, n int) *Executor {
	b.Helper()
	exec := NewExecutor(NewDatabase("test"))
	sql := fmt.Sprintf("CREATE TABLE docs (id INT PRIMARY KEY, data %s)", colType)
	if err := execSQL(exec, sql); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < n; i++ {
		insert := fmt.Sprintf(`INSERT INTO docs VALUES (%d, '{"name":"user_%d","age":%d,"tags":["go","sql","db"],"address":{"city":"Tokyo","zip":"100-0001"}}')`, i, i, 20+i%50)
		if err := execSQL(exec, insert); err != nil {
			b.Fatal(err)
		}
	}
	return exec
}

func benchQuery(b *testing.B, exec *Executor, sql string) {
	b.Helper()
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := exec.Execute(stmt)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONValue_JSON_vs_JSONB(b *testing.B) {
	const rows = 100

	b.Run("JSON", func(b *testing.B) {
		exec := setupJSONBench(b, "JSON", rows)
		benchQuery(b, exec, "SELECT JSON_VALUE(data, '$.name') FROM docs")
	})

	b.Run("JSONB", func(b *testing.B) {
		exec := setupJSONBench(b, "JSONB", rows)
		benchQuery(b, exec, "SELECT JSON_VALUE(data, '$.name') FROM docs")
	})
}

func BenchmarkJSONValue_Nested_JSON_vs_JSONB(b *testing.B) {
	const rows = 100

	b.Run("JSON", func(b *testing.B) {
		exec := setupJSONBench(b, "JSON", rows)
		benchQuery(b, exec, "SELECT JSON_VALUE(data, '$.address.city') FROM docs")
	})

	b.Run("JSONB", func(b *testing.B) {
		exec := setupJSONBench(b, "JSONB", rows)
		benchQuery(b, exec, "SELECT JSON_VALUE(data, '$.address.city') FROM docs")
	})
}

func BenchmarkJSONQuery_JSON_vs_JSONB(b *testing.B) {
	const rows = 100

	b.Run("JSON", func(b *testing.B) {
		exec := setupJSONBench(b, "JSON", rows)
		benchQuery(b, exec, "SELECT JSON_QUERY(data, '$.tags') FROM docs")
	})

	b.Run("JSONB", func(b *testing.B) {
		exec := setupJSONBench(b, "JSONB", rows)
		benchQuery(b, exec, "SELECT JSON_QUERY(data, '$.tags') FROM docs")
	})
}

func BenchmarkJSONExists_JSON_vs_JSONB(b *testing.B) {
	const rows = 100

	b.Run("JSON", func(b *testing.B) {
		exec := setupJSONBench(b, "JSON", rows)
		benchQuery(b, exec, "SELECT JSON_EXISTS(data, '$.name') FROM docs")
	})

	b.Run("JSONB", func(b *testing.B) {
		exec := setupJSONBench(b, "JSONB", rows)
		benchQuery(b, exec, "SELECT JSON_EXISTS(data, '$.name') FROM docs")
	})
}

func BenchmarkJSONExists_WHERE_JSON_vs_JSONB(b *testing.B) {
	const rows = 100

	b.Run("JSON", func(b *testing.B) {
		exec := setupJSONBench(b, "JSON", rows)
		benchQuery(b, exec, "SELECT id FROM docs WHERE JSON_EXISTS(data, '$.address.city')")
	})

	b.Run("JSONB", func(b *testing.B) {
		exec := setupJSONBench(b, "JSONB", rows)
		benchQuery(b, exec, "SELECT id FROM docs WHERE JSON_EXISTS(data, '$.address.city')")
	})
}

// --- JSON が有利なワークロード ---

func BenchmarkInsert_JSON_vs_JSONB(b *testing.B) {
	jsonData := `'{"name":"user","age":30,"tags":["go","sql","db"],"address":{"city":"Tokyo","zip":"100-0001"}}'`

	b.Run("JSON", func(b *testing.B) {
		exec := NewExecutor(NewDatabase("test"))
		execSQL(exec, "CREATE TABLE docs (id INT PRIMARY KEY, data JSON)")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sql := fmt.Sprintf("INSERT INTO docs VALUES (%d, %s)", i, jsonData)
			l := lexer.New(sql)
			p := parser.New(l)
			stmt, _ := p.Parse()
			exec.Execute(stmt)
		}
	})

	b.Run("JSONB", func(b *testing.B) {
		exec := NewExecutor(NewDatabase("test"))
		execSQL(exec, "CREATE TABLE docs (id INT PRIMARY KEY, data JSONB)")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sql := fmt.Sprintf("INSERT INTO docs VALUES (%d, %s)", i, jsonData)
			l := lexer.New(sql)
			p := parser.New(l)
			stmt, _ := p.Parse()
			exec.Execute(stmt)
		}
	})
}

func BenchmarkSelectAll_JSON_vs_JSONB(b *testing.B) {
	const rows = 100

	b.Run("JSON", func(b *testing.B) {
		exec := setupJSONBench(b, "JSON", rows)
		benchQuery(b, exec, "SELECT data FROM docs")
	})

	b.Run("JSONB", func(b *testing.B) {
		exec := setupJSONBench(b, "JSONB", rows)
		benchQuery(b, exec, "SELECT data FROM docs")
	})
}

func BenchmarkSelectById_JSON_vs_JSONB(b *testing.B) {
	const rows = 100

	b.Run("JSON", func(b *testing.B) {
		exec := setupJSONBench(b, "JSON", rows)
		benchQuery(b, exec, "SELECT data FROM docs WHERE id = 50")
	})

	b.Run("JSONB", func(b *testing.B) {
		exec := setupJSONBench(b, "JSONB", rows)
		benchQuery(b, exec, "SELECT data FROM docs WHERE id = 50")
	})
}

func BenchmarkSelectIdOnly_JSON_vs_JSONB(b *testing.B) {
	const rows = 100

	b.Run("JSON", func(b *testing.B) {
		exec := setupJSONBench(b, "JSON", rows)
		benchQuery(b, exec, "SELECT id FROM docs")
	})

	b.Run("JSONB", func(b *testing.B) {
		exec := setupJSONBench(b, "JSONB", rows)
		benchQuery(b, exec, "SELECT id FROM docs")
	})
}

// --- GIN Index benchmarks ---

func setupJSONBGinBench(b *testing.B, n int, withIndex bool) *Executor {
	b.Helper()
	exec := NewExecutor(NewDatabase("test"))
	if err := execSQL(exec, "CREATE TABLE docs (id INT PRIMARY KEY, data JSONB)"); err != nil {
		b.Fatal(err)
	}
	statuses := []string{"active", "inactive", "pending", "suspended", "deleted"}
	for i := 0; i < n; i++ {
		sql := fmt.Sprintf(`INSERT INTO docs VALUES (%d, '{"status":"%s","name":"user_%d","age":%d}')`,
			i, statuses[i%len(statuses)], i, 20+i%50)
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
	if withIndex {
		if err := execSQL(exec, "CREATE INDEX idx_data_gin ON docs(data) USING GIN"); err != nil {
			b.Fatal(err)
		}
	}
	return exec
}

func setupJSONGinBench(b *testing.B, n int) *Executor {
	b.Helper()
	exec := NewExecutor(NewDatabase("test"))
	if err := execSQL(exec, "CREATE TABLE docs (id INT PRIMARY KEY, data JSON)"); err != nil {
		b.Fatal(err)
	}
	statuses := []string{"active", "inactive", "pending", "suspended", "deleted"}
	for i := 0; i < n; i++ {
		sql := fmt.Sprintf(`INSERT INTO docs VALUES (%d, '{"status":"%s","name":"user_%d","age":%d}')`,
			i, statuses[i%len(statuses)], i, 20+i%50)
		if err := execSQL(exec, sql); err != nil {
			b.Fatal(err)
		}
	}
	return exec
}

func BenchmarkJSONBGinIndex_WhereEquality(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("rows=%d/JSON", n), func(b *testing.B) {
			exec := setupJSONGinBench(b, n)
			benchQuery(b, exec, `SELECT id FROM docs WHERE JSON_VALUE(data, '$.status') = 'active'`)
		})
		b.Run(fmt.Sprintf("rows=%d/JSONB_NoIndex", n), func(b *testing.B) {
			exec := setupJSONBGinBench(b, n, false)
			benchQuery(b, exec, `SELECT id FROM docs WHERE JSON_VALUE(data, '$.status') = 'active'`)
		})
		b.Run(fmt.Sprintf("rows=%d/JSONB_GinIndex", n), func(b *testing.B) {
			exec := setupJSONBGinBench(b, n, true)
			benchQuery(b, exec, `SELECT id FROM docs WHERE JSON_VALUE(data, '$.status') = 'active'`)
		})
	}
}

func BenchmarkJSONBGinIndex_WhereIN(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("rows=%d/JSON", n), func(b *testing.B) {
			exec := setupJSONGinBench(b, n)
			benchQuery(b, exec, `SELECT id FROM docs WHERE JSON_VALUE(data, '$.status') IN ('active', 'pending')`)
		})
		b.Run(fmt.Sprintf("rows=%d/JSONB_NoIndex", n), func(b *testing.B) {
			exec := setupJSONBGinBench(b, n, false)
			benchQuery(b, exec, `SELECT id FROM docs WHERE JSON_VALUE(data, '$.status') IN ('active', 'pending')`)
		})
		b.Run(fmt.Sprintf("rows=%d/JSONB_GinIndex", n), func(b *testing.B) {
			exec := setupJSONBGinBench(b, n, true)
			benchQuery(b, exec, `SELECT id FROM docs WHERE JSON_VALUE(data, '$.status') IN ('active', 'pending')`)
		})
	}
}

func BenchmarkJSONBGinIndex_Insert(b *testing.B) {
	jsonData := `'{"status":"active","name":"user","age":30}'`

	b.Run("NoIndex", func(b *testing.B) {
		exec := NewExecutor(NewDatabase("test"))
		execSQL(exec, "CREATE TABLE docs (id INT PRIMARY KEY, data JSONB)")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sql := fmt.Sprintf("INSERT INTO docs VALUES (%d, %s)", i, jsonData)
			l := lexer.New(sql)
			p := parser.New(l)
			stmt, _ := p.Parse()
			exec.Execute(stmt)
		}
	})

	b.Run("GinIndex", func(b *testing.B) {
		exec := NewExecutor(NewDatabase("test"))
		execSQL(exec, "CREATE TABLE docs (id INT PRIMARY KEY, data JSONB)")
		execSQL(exec, "CREATE INDEX idx_data_gin ON docs(data) USING GIN")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sql := fmt.Sprintf("INSERT INTO docs VALUES (%d, %s)", i, jsonData)
			l := lexer.New(sql)
			p := parser.New(l)
			stmt, _ := p.Parse()
			exec.Execute(stmt)
		}
	})
}

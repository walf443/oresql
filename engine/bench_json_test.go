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

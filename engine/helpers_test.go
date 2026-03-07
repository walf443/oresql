package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/walf443/oresql/lexer"
	"github.com/walf443/oresql/parser"
)

func run(t *testing.T, exec *Executor, sql string) *Result {
	t.Helper()
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	require.NoError(t, err, "parse error for %q", sql)
	result, err := exec.Execute(stmt)
	require.NoError(t, err, "execute error for %q", sql)
	return result
}

// planRow describes the expected values for one row of an EXPLAIN result.
// Empty string fields are not asserted.
type planRow struct {
	Table string // expected table name (column 2)
	Type  string // expected access type (column 3)
	Key   string // expected key used (column 5)
	Extra string // substring expected in extra (column 6)
}

// assertExplain runs EXPLAIN on the given SQL and asserts each row's table, access type and key.
func assertExplain(t *testing.T, exec *Executor, sql string, expected []planRow) {
	t.Helper()
	result := run(t, exec, "EXPLAIN "+sql)
	require.Len(t, result.Rows, len(expected), "EXPLAIN %s: row count", sql)
	for i, exp := range expected {
		if exp.Table != "" {
			assert.Equal(t, exp.Table, result.Rows[i][2], "EXPLAIN %s: row %d table", sql, i)
		}
		if exp.Type != "" {
			assert.Equal(t, exp.Type, result.Rows[i][3], "EXPLAIN %s: row %d access type", sql, i)
		}
		if exp.Key != "" {
			assert.Equal(t, exp.Key, result.Rows[i][5], "EXPLAIN %s: row %d key used", sql, i)
		}
		if exp.Extra != "" {
			extra, _ := result.Rows[i][6].(string)
			assert.Contains(t, extra, exp.Extra, "EXPLAIN %s: row %d extra", sql, i)
		}
	}
}

func runWithError(exec *Executor, sql string) (*Result, error) {
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		return nil, err
	}
	return exec.Execute(stmt)
}

func runExpectError(t *testing.T, exec *Executor, sql string) {
	t.Helper()
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		return // parse error is also acceptable
	}
	_, err = exec.Execute(stmt)
	require.Error(t, err, "expected error for %q", sql)
}

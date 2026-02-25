package engine

import (
	"testing"

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

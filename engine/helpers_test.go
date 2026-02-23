package engine

import (
	"testing"

	"github.com/walf443/oresql/lexer"
	"github.com/walf443/oresql/parser"
)

func run(t *testing.T, exec *Executor, sql string) *Result {
	t.Helper()
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error for %q: %s", sql, err)
	}
	result, err := exec.Execute(stmt)
	if err != nil {
		t.Fatalf("execute error for %q: %s", sql, err)
	}
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
	if err == nil {
		t.Fatalf("expected error for %q, got nil", sql)
	}
}

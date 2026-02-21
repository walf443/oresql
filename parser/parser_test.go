package parser

import (
	"testing"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/lexer"
)

func parse(t *testing.T, input string) ast.Statement {
	t.Helper()
	l := lexer.New(input)
	p := New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %s", err)
	}
	return stmt
}

func TestParseCreateTable(t *testing.T) {
	stmt := parse(t, "CREATE TABLE users (id INT, name TEXT)")
	ct, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		t.Fatalf("expected CreateTableStmt, got %T", stmt)
	}
	if ct.TableName != "users" {
		t.Errorf("table name: expected %q, got %q", "users", ct.TableName)
	}
	if len(ct.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(ct.Columns))
	}
	if ct.Columns[0].Name != "id" || ct.Columns[0].DataType != "INT" {
		t.Errorf("column 0: expected (id, INT), got (%s, %s)", ct.Columns[0].Name, ct.Columns[0].DataType)
	}
	if ct.Columns[1].Name != "name" || ct.Columns[1].DataType != "TEXT" {
		t.Errorf("column 1: expected (name, TEXT), got (%s, %s)", ct.Columns[1].Name, ct.Columns[1].DataType)
	}
}

func TestParseInsert(t *testing.T) {
	stmt := parse(t, "INSERT INTO users VALUES (1, 'alice')")
	ins, ok := stmt.(*ast.InsertStmt)
	if !ok {
		t.Fatalf("expected InsertStmt, got %T", stmt)
	}
	if ins.TableName != "users" {
		t.Errorf("table name: expected %q, got %q", "users", ins.TableName)
	}
	if len(ins.Values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(ins.Values))
	}
	intVal, ok := ins.Values[0].(*ast.IntLitExpr)
	if !ok {
		t.Fatalf("value 0: expected IntLitExpr, got %T", ins.Values[0])
	}
	if intVal.Value != 1 {
		t.Errorf("value 0: expected 1, got %d", intVal.Value)
	}
	strVal, ok := ins.Values[1].(*ast.StringLitExpr)
	if !ok {
		t.Fatalf("value 1: expected StringLitExpr, got %T", ins.Values[1])
	}
	if strVal.Value != "alice" {
		t.Errorf("value 1: expected %q, got %q", "alice", strVal.Value)
	}
}

func TestParseSelectStar(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users")
	sel, ok := stmt.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", stmt)
	}
	if sel.TableName != "users" {
		t.Errorf("table name: expected %q, got %q", "users", sel.TableName)
	}
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column expr, got %d", len(sel.Columns))
	}
	if _, ok := sel.Columns[0].(*ast.StarExpr); !ok {
		t.Errorf("expected StarExpr, got %T", sel.Columns[0])
	}
	if sel.Where != nil {
		t.Errorf("expected no WHERE, got %v", sel.Where)
	}
}

func TestParseSelectColumns(t *testing.T) {
	stmt := parse(t, "SELECT id, name FROM users")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(sel.Columns))
	}
	col0 := sel.Columns[0].(*ast.IdentExpr)
	col1 := sel.Columns[1].(*ast.IdentExpr)
	if col0.Name != "id" {
		t.Errorf("column 0: expected %q, got %q", "id", col0.Name)
	}
	if col1.Name != "name" {
		t.Errorf("column 1: expected %q, got %q", "name", col1.Name)
	}
}

func TestParseSelectWhere(t *testing.T) {
	stmt := parse(t, "SELECT name FROM users WHERE id = 1 AND name = 'alice'")
	sel := stmt.(*ast.SelectStmt)

	if sel.Where == nil {
		t.Fatal("expected WHERE clause")
	}

	logical, ok := sel.Where.(*ast.LogicalExpr)
	if !ok {
		t.Fatalf("expected LogicalExpr, got %T", sel.Where)
	}
	if logical.Op != "AND" {
		t.Errorf("expected AND, got %s", logical.Op)
	}

	// Left: id = 1
	left := logical.Left.(*ast.BinaryExpr)
	if left.Op != "=" {
		t.Errorf("left op: expected =, got %s", left.Op)
	}
	if left.Left.(*ast.IdentExpr).Name != "id" {
		t.Errorf("left.left: expected id")
	}
	if left.Right.(*ast.IntLitExpr).Value != 1 {
		t.Errorf("left.right: expected 1")
	}

	// Right: name = 'alice'
	right := logical.Right.(*ast.BinaryExpr)
	if right.Op != "=" {
		t.Errorf("right op: expected =, got %s", right.Op)
	}
	if right.Left.(*ast.IdentExpr).Name != "name" {
		t.Errorf("right.left: expected name")
	}
	if right.Right.(*ast.StringLitExpr).Value != "alice" {
		t.Errorf("right.right: expected alice")
	}
}

func TestParseSelectWhereOr(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users WHERE id = 1 OR id = 2")
	sel := stmt.(*ast.SelectStmt)
	logical := sel.Where.(*ast.LogicalExpr)
	if logical.Op != "OR" {
		t.Errorf("expected OR, got %s", logical.Op)
	}
}

func TestParseSelectWithSemicolon(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users;")
	if _, ok := stmt.(*ast.SelectStmt); !ok {
		t.Fatalf("expected SelectStmt, got %T", stmt)
	}
}

func TestParseError(t *testing.T) {
	inputs := []string{
		"CREATE",
		"CREATE users",
		"INSERT users",
		"SELECT FROM",
		"FOOBAR",
	}
	for _, input := range inputs {
		l := lexer.New(input)
		p := New(l)
		_, err := p.Parse()
		if err == nil {
			t.Errorf("expected error for %q, got nil", input)
		}
	}
}

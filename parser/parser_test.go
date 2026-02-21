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
	if len(ins.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(ins.Rows))
	}
	if len(ins.Rows[0]) != 2 {
		t.Fatalf("expected 2 values, got %d", len(ins.Rows[0]))
	}
	intVal, ok := ins.Rows[0][0].(*ast.IntLitExpr)
	if !ok {
		t.Fatalf("value 0: expected IntLitExpr, got %T", ins.Rows[0][0])
	}
	if intVal.Value != 1 {
		t.Errorf("value 0: expected 1, got %d", intVal.Value)
	}
	strVal, ok := ins.Rows[0][1].(*ast.StringLitExpr)
	if !ok {
		t.Fatalf("value 1: expected StringLitExpr, got %T", ins.Rows[0][1])
	}
	if strVal.Value != "alice" {
		t.Errorf("value 1: expected %q, got %q", "alice", strVal.Value)
	}
}

func TestParseInsertMultipleRows(t *testing.T) {
	stmt := parse(t, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")
	ins, ok := stmt.(*ast.InsertStmt)
	if !ok {
		t.Fatalf("expected InsertStmt, got %T", stmt)
	}
	if ins.TableName != "users" {
		t.Errorf("table name: expected %q, got %q", "users", ins.TableName)
	}
	if len(ins.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(ins.Rows))
	}
	// Check each row has 2 values
	for i, row := range ins.Rows {
		if len(row) != 2 {
			t.Errorf("row %d: expected 2 values, got %d", i, len(row))
		}
	}
	// Spot check values
	if ins.Rows[0][0].(*ast.IntLitExpr).Value != 1 {
		t.Errorf("row 0 value 0: expected 1")
	}
	if ins.Rows[1][1].(*ast.StringLitExpr).Value != "bob" {
		t.Errorf("row 1 value 1: expected 'bob'")
	}
	if ins.Rows[2][0].(*ast.IntLitExpr).Value != 3 {
		t.Errorf("row 2 value 0: expected 3")
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

func TestParseSelectQualifiedColumns(t *testing.T) {
	stmt := parse(t, "SELECT users.id, users.name FROM users")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(sel.Columns))
	}
	col0 := sel.Columns[0].(*ast.IdentExpr)
	if col0.Table != "users" || col0.Name != "id" {
		t.Errorf("column 0: expected users.id, got %s.%s", col0.Table, col0.Name)
	}
	col1 := sel.Columns[1].(*ast.IdentExpr)
	if col1.Table != "users" || col1.Name != "name" {
		t.Errorf("column 1: expected users.name, got %s.%s", col1.Table, col1.Name)
	}
}

func TestParseSelectQualifiedWhere(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users WHERE users.id = 1")
	sel := stmt.(*ast.SelectStmt)
	bin := sel.Where.(*ast.BinaryExpr)
	ident := bin.Left.(*ast.IdentExpr)
	if ident.Table != "users" || ident.Name != "id" {
		t.Errorf("expected users.id, got %s.%s", ident.Table, ident.Name)
	}
}

func TestParseSelectMixedColumns(t *testing.T) {
	stmt := parse(t, "SELECT users.id, name FROM users")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(sel.Columns))
	}
	col0 := sel.Columns[0].(*ast.IdentExpr)
	if col0.Table != "users" || col0.Name != "id" {
		t.Errorf("column 0: expected users.id, got %s.%s", col0.Table, col0.Name)
	}
	col1 := sel.Columns[1].(*ast.IdentExpr)
	if col1.Table != "" || col1.Name != "name" {
		t.Errorf("column 1: expected name (unqualified), got %s.%s", col1.Table, col1.Name)
	}
}

func TestParseSelectCountStar(t *testing.T) {
	stmt := parse(t, "SELECT COUNT(*) FROM users")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
	call, ok := sel.Columns[0].(*ast.CallExpr)
	if !ok {
		t.Fatalf("expected CallExpr, got %T", sel.Columns[0])
	}
	if call.Name != "COUNT" {
		t.Errorf("expected function name COUNT, got %s", call.Name)
	}
	if len(call.Args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(call.Args))
	}
	if _, ok := call.Args[0].(*ast.StarExpr); !ok {
		t.Errorf("expected StarExpr arg, got %T", call.Args[0])
	}
}

func TestParseSelectCountStarLowerCase(t *testing.T) {
	stmt := parse(t, "select count(*) from users")
	sel := stmt.(*ast.SelectStmt)
	call := sel.Columns[0].(*ast.CallExpr)
	if call.Name != "COUNT" {
		t.Errorf("expected function name COUNT, got %s", call.Name)
	}
}

func TestParseSelectLiteral(t *testing.T) {
	stmt := parse(t, "SELECT 1")
	sel := stmt.(*ast.SelectStmt)
	if sel.TableName != "" {
		t.Errorf("expected empty table name, got %q", sel.TableName)
	}
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
	lit, ok := sel.Columns[0].(*ast.IntLitExpr)
	if !ok {
		t.Fatalf("expected IntLitExpr, got %T", sel.Columns[0])
	}
	if lit.Value != 1 {
		t.Errorf("expected value 1, got %d", lit.Value)
	}
}

func TestParseIsNull(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users WHERE name IS NULL")
	sel := stmt.(*ast.SelectStmt)
	isNull, ok := sel.Where.(*ast.IsNullExpr)
	if !ok {
		t.Fatalf("expected IsNullExpr, got %T", sel.Where)
	}
	ident := isNull.Expr.(*ast.IdentExpr)
	if ident.Name != "name" {
		t.Errorf("expected column name 'name', got %q", ident.Name)
	}
	if isNull.Not {
		t.Errorf("expected Not=false, got true")
	}
}

func TestParseIsNotNull(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users WHERE name IS NOT NULL")
	sel := stmt.(*ast.SelectStmt)
	isNull, ok := sel.Where.(*ast.IsNullExpr)
	if !ok {
		t.Fatalf("expected IsNullExpr, got %T", sel.Where)
	}
	ident := isNull.Expr.(*ast.IdentExpr)
	if ident.Name != "name" {
		t.Errorf("expected column name 'name', got %q", ident.Name)
	}
	if !isNull.Not {
		t.Errorf("expected Not=true, got false")
	}
}

func TestParseInsertNull(t *testing.T) {
	stmt := parse(t, "INSERT INTO users VALUES (1, NULL)")
	ins := stmt.(*ast.InsertStmt)
	if len(ins.Rows[0]) != 2 {
		t.Fatalf("expected 2 values, got %d", len(ins.Rows[0]))
	}
	if _, ok := ins.Rows[0][1].(*ast.NullLitExpr); !ok {
		t.Errorf("expected NullLitExpr, got %T", ins.Rows[0][1])
	}
}

func TestParseSelectAlias(t *testing.T) {
	stmt := parse(t, "SELECT id AS user_id FROM users")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
	alias, ok := sel.Columns[0].(*ast.AliasExpr)
	if !ok {
		t.Fatalf("expected AliasExpr, got %T", sel.Columns[0])
	}
	ident, ok := alias.Expr.(*ast.IdentExpr)
	if !ok {
		t.Fatalf("expected IdentExpr inside AliasExpr, got %T", alias.Expr)
	}
	if ident.Name != "id" {
		t.Errorf("expected column name 'id', got %q", ident.Name)
	}
	if alias.Alias != "user_id" {
		t.Errorf("expected alias 'user_id', got %q", alias.Alias)
	}
}

func TestParseSelectCountAlias(t *testing.T) {
	stmt := parse(t, "SELECT COUNT(*) AS total FROM users")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
	alias, ok := sel.Columns[0].(*ast.AliasExpr)
	if !ok {
		t.Fatalf("expected AliasExpr, got %T", sel.Columns[0])
	}
	call, ok := alias.Expr.(*ast.CallExpr)
	if !ok {
		t.Fatalf("expected CallExpr inside AliasExpr, got %T", alias.Expr)
	}
	if call.Name != "COUNT" {
		t.Errorf("expected function name COUNT, got %s", call.Name)
	}
	if alias.Alias != "total" {
		t.Errorf("expected alias 'total', got %q", alias.Alias)
	}
}

func TestParseSelectLiteralAlias(t *testing.T) {
	stmt := parse(t, "SELECT 1 AS one")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
	alias, ok := sel.Columns[0].(*ast.AliasExpr)
	if !ok {
		t.Fatalf("expected AliasExpr, got %T", sel.Columns[0])
	}
	lit, ok := alias.Expr.(*ast.IntLitExpr)
	if !ok {
		t.Fatalf("expected IntLitExpr inside AliasExpr, got %T", alias.Expr)
	}
	if lit.Value != 1 {
		t.Errorf("expected value 1, got %d", lit.Value)
	}
	if alias.Alias != "one" {
		t.Errorf("expected alias 'one', got %q", alias.Alias)
	}
}

func TestParseSelectQuotedIdent(t *testing.T) {
	stmt := parse(t, "SELECT `count` FROM t")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
	ident, ok := sel.Columns[0].(*ast.IdentExpr)
	if !ok {
		t.Fatalf("expected IdentExpr, got %T", sel.Columns[0])
	}
	if ident.Name != "count" {
		t.Errorf("expected column name 'count', got %q", ident.Name)
	}
}

func TestParseCreateTableQuotedIdent(t *testing.T) {
	stmt := parse(t, "CREATE TABLE t (`count` INT)")
	ct := stmt.(*ast.CreateTableStmt)
	if len(ct.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(ct.Columns))
	}
	if ct.Columns[0].Name != "count" {
		t.Errorf("expected column name 'count', got %q", ct.Columns[0].Name)
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

package parser

import (
	"fmt"
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

func TestParseSelectArithmetic(t *testing.T) {
	stmt := parse(t, "SELECT 1 * 2")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
	arith, ok := sel.Columns[0].(*ast.ArithmeticExpr)
	if !ok {
		t.Fatalf("expected ArithmeticExpr, got %T", sel.Columns[0])
	}
	if arith.Op != "*" {
		t.Errorf("expected op '*', got %q", arith.Op)
	}
	if arith.Left.(*ast.IntLitExpr).Value != 1 {
		t.Errorf("expected left=1")
	}
	if arith.Right.(*ast.IntLitExpr).Value != 2 {
		t.Errorf("expected right=2")
	}
}

func TestParseSelectArithmeticPrecedence(t *testing.T) {
	// 1 + 2 * 3 should parse as 1 + (2 * 3)
	stmt := parse(t, "SELECT 1 + 2 * 3")
	sel := stmt.(*ast.SelectStmt)
	arith, ok := sel.Columns[0].(*ast.ArithmeticExpr)
	if !ok {
		t.Fatalf("expected ArithmeticExpr, got %T", sel.Columns[0])
	}
	if arith.Op != "+" {
		t.Errorf("expected top-level op '+', got %q", arith.Op)
	}
	if arith.Left.(*ast.IntLitExpr).Value != 1 {
		t.Errorf("expected left=1")
	}
	right, ok := arith.Right.(*ast.ArithmeticExpr)
	if !ok {
		t.Fatalf("expected right to be ArithmeticExpr, got %T", arith.Right)
	}
	if right.Op != "*" {
		t.Errorf("expected right op '*', got %q", right.Op)
	}
}

func TestParseSelectUnaryMinus(t *testing.T) {
	stmt := parse(t, "SELECT -1")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
	arith, ok := sel.Columns[0].(*ast.ArithmeticExpr)
	if !ok {
		t.Fatalf("expected ArithmeticExpr, got %T", sel.Columns[0])
	}
	if arith.Op != "-" {
		t.Errorf("expected op '-', got %q", arith.Op)
	}
	if arith.Left.(*ast.IntLitExpr).Value != 0 {
		t.Errorf("expected left=0")
	}
	if arith.Right.(*ast.IntLitExpr).Value != 1 {
		t.Errorf("expected right=1")
	}
}

func TestParseCreateTableNotNull(t *testing.T) {
	stmt := parse(t, "CREATE TABLE t (id INT NOT NULL, name TEXT)")
	ct, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		t.Fatalf("expected CreateTableStmt, got %T", stmt)
	}
	if len(ct.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(ct.Columns))
	}
	if ct.Columns[0].Name != "id" || ct.Columns[0].DataType != "INT" || !ct.Columns[0].NotNull {
		t.Errorf("column 0: expected (id, INT, NOT NULL), got (%s, %s, NotNull=%v)", ct.Columns[0].Name, ct.Columns[0].DataType, ct.Columns[0].NotNull)
	}
	if ct.Columns[1].Name != "name" || ct.Columns[1].DataType != "TEXT" || ct.Columns[1].NotNull {
		t.Errorf("column 1: expected (name, TEXT, nullable), got (%s, %s, NotNull=%v)", ct.Columns[1].Name, ct.Columns[1].DataType, ct.Columns[1].NotNull)
	}
}

func TestParseUpdate(t *testing.T) {
	stmt := parse(t, "UPDATE users SET name = 'bob' WHERE id = 1")
	upd, ok := stmt.(*ast.UpdateStmt)
	if !ok {
		t.Fatalf("expected UpdateStmt, got %T", stmt)
	}
	if upd.TableName != "users" {
		t.Errorf("table name: expected %q, got %q", "users", upd.TableName)
	}
	if len(upd.Sets) != 1 {
		t.Fatalf("expected 1 set clause, got %d", len(upd.Sets))
	}
	if upd.Sets[0].Column != "name" {
		t.Errorf("set column: expected %q, got %q", "name", upd.Sets[0].Column)
	}
	strVal, ok := upd.Sets[0].Value.(*ast.StringLitExpr)
	if !ok {
		t.Fatalf("set value: expected StringLitExpr, got %T", upd.Sets[0].Value)
	}
	if strVal.Value != "bob" {
		t.Errorf("set value: expected %q, got %q", "bob", strVal.Value)
	}
	if upd.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	bin := upd.Where.(*ast.BinaryExpr)
	if bin.Left.(*ast.IdentExpr).Name != "id" {
		t.Errorf("where left: expected 'id'")
	}
	if bin.Right.(*ast.IntLitExpr).Value != 1 {
		t.Errorf("where right: expected 1")
	}
}

func TestParseUpdateMultipleSet(t *testing.T) {
	stmt := parse(t, "UPDATE users SET name = 'bob', age = 30 WHERE id = 1")
	upd := stmt.(*ast.UpdateStmt)
	if len(upd.Sets) != 2 {
		t.Fatalf("expected 2 set clauses, got %d", len(upd.Sets))
	}
	if upd.Sets[0].Column != "name" {
		t.Errorf("set 0 column: expected %q, got %q", "name", upd.Sets[0].Column)
	}
	if upd.Sets[0].Value.(*ast.StringLitExpr).Value != "bob" {
		t.Errorf("set 0 value: expected 'bob'")
	}
	if upd.Sets[1].Column != "age" {
		t.Errorf("set 1 column: expected %q, got %q", "age", upd.Sets[1].Column)
	}
	if upd.Sets[1].Value.(*ast.IntLitExpr).Value != 30 {
		t.Errorf("set 1 value: expected 30")
	}
	if upd.Where == nil {
		t.Fatal("expected WHERE clause")
	}
}

func TestParseUpdateNoWhere(t *testing.T) {
	stmt := parse(t, "UPDATE users SET name = 'bob'")
	upd := stmt.(*ast.UpdateStmt)
	if upd.TableName != "users" {
		t.Errorf("table name: expected %q, got %q", "users", upd.TableName)
	}
	if len(upd.Sets) != 1 {
		t.Fatalf("expected 1 set clause, got %d", len(upd.Sets))
	}
	if upd.Where != nil {
		t.Errorf("expected no WHERE, got %v", upd.Where)
	}
}

func TestParseDelete(t *testing.T) {
	stmt := parse(t, "DELETE FROM users WHERE id = 1")
	del, ok := stmt.(*ast.DeleteStmt)
	if !ok {
		t.Fatalf("expected DeleteStmt, got %T", stmt)
	}
	if del.TableName != "users" {
		t.Errorf("table name: expected %q, got %q", "users", del.TableName)
	}
	if del.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	bin := del.Where.(*ast.BinaryExpr)
	if bin.Left.(*ast.IdentExpr).Name != "id" {
		t.Errorf("where left: expected 'id'")
	}
	if bin.Right.(*ast.IntLitExpr).Value != 1 {
		t.Errorf("where right: expected 1")
	}
}

func TestParseDeleteNoWhere(t *testing.T) {
	stmt := parse(t, "DELETE FROM users")
	del := stmt.(*ast.DeleteStmt)
	if del.TableName != "users" {
		t.Errorf("table name: expected %q, got %q", "users", del.TableName)
	}
	if del.Where != nil {
		t.Errorf("expected no WHERE, got %v", del.Where)
	}
}

func TestParseUpdateOrderByLimit(t *testing.T) {
	stmt := parse(t, "UPDATE users SET name = 'bob' WHERE id > 1 ORDER BY id LIMIT 2")
	upd, ok := stmt.(*ast.UpdateStmt)
	if !ok {
		t.Fatalf("expected UpdateStmt, got %T", stmt)
	}
	if upd.TableName != "users" {
		t.Errorf("table name: expected %q, got %q", "users", upd.TableName)
	}
	if len(upd.Sets) != 1 {
		t.Fatalf("expected 1 set clause, got %d", len(upd.Sets))
	}
	if upd.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if len(upd.OrderBy) != 1 {
		t.Fatalf("expected 1 order by clause, got %d", len(upd.OrderBy))
	}
	ident, ok := upd.OrderBy[0].Expr.(*ast.IdentExpr)
	if !ok {
		t.Fatalf("expected IdentExpr in ORDER BY, got %T", upd.OrderBy[0].Expr)
	}
	if ident.Name != "id" {
		t.Errorf("order by column: expected 'id', got %q", ident.Name)
	}
	if upd.OrderBy[0].Desc {
		t.Error("expected ASC, got DESC")
	}
	if upd.Limit == nil {
		t.Fatal("expected LIMIT clause")
	}
	if *upd.Limit != 2 {
		t.Errorf("limit: expected 2, got %d", *upd.Limit)
	}
}

func TestParseUpdateOrderByOnly(t *testing.T) {
	stmt := parse(t, "UPDATE users SET name = 'bob' ORDER BY id DESC")
	upd := stmt.(*ast.UpdateStmt)
	if upd.Where != nil {
		t.Errorf("expected no WHERE, got %v", upd.Where)
	}
	if len(upd.OrderBy) != 1 {
		t.Fatalf("expected 1 order by clause, got %d", len(upd.OrderBy))
	}
	if !upd.OrderBy[0].Desc {
		t.Error("expected DESC")
	}
	if upd.Limit != nil {
		t.Errorf("expected no LIMIT, got %v", *upd.Limit)
	}
}

func TestParseUpdateLimitOnly(t *testing.T) {
	stmt := parse(t, "UPDATE users SET name = 'bob' LIMIT 5")
	upd := stmt.(*ast.UpdateStmt)
	if upd.Where != nil {
		t.Errorf("expected no WHERE, got %v", upd.Where)
	}
	if len(upd.OrderBy) != 0 {
		t.Errorf("expected no ORDER BY, got %d clauses", len(upd.OrderBy))
	}
	if upd.Limit == nil {
		t.Fatal("expected LIMIT clause")
	}
	if *upd.Limit != 5 {
		t.Errorf("limit: expected 5, got %d", *upd.Limit)
	}
}

func TestParseDeleteOrderByLimit(t *testing.T) {
	stmt := parse(t, "DELETE FROM users WHERE id > 1 ORDER BY id LIMIT 2")
	del, ok := stmt.(*ast.DeleteStmt)
	if !ok {
		t.Fatalf("expected DeleteStmt, got %T", stmt)
	}
	if del.TableName != "users" {
		t.Errorf("table name: expected %q, got %q", "users", del.TableName)
	}
	if del.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if len(del.OrderBy) != 1 {
		t.Fatalf("expected 1 order by clause, got %d", len(del.OrderBy))
	}
	ident, ok := del.OrderBy[0].Expr.(*ast.IdentExpr)
	if !ok {
		t.Fatalf("expected IdentExpr in ORDER BY, got %T", del.OrderBy[0].Expr)
	}
	if ident.Name != "id" {
		t.Errorf("order by column: expected 'id', got %q", ident.Name)
	}
	if del.Limit == nil {
		t.Fatal("expected LIMIT clause")
	}
	if *del.Limit != 2 {
		t.Errorf("limit: expected 2, got %d", *del.Limit)
	}
}

func TestParseDeleteLimitOnly(t *testing.T) {
	stmt := parse(t, "DELETE FROM users LIMIT 3")
	del := stmt.(*ast.DeleteStmt)
	if del.Where != nil {
		t.Errorf("expected no WHERE, got %v", del.Where)
	}
	if len(del.OrderBy) != 0 {
		t.Errorf("expected no ORDER BY, got %d clauses", len(del.OrderBy))
	}
	if del.Limit == nil {
		t.Fatal("expected LIMIT clause")
	}
	if *del.Limit != 3 {
		t.Errorf("limit: expected 3, got %d", *del.Limit)
	}
}

func TestParseSelectOrderBySingleColumn(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users ORDER BY id")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.OrderBy) != 1 {
		t.Fatalf("expected 1 order by clause, got %d", len(sel.OrderBy))
	}
	ident, ok := sel.OrderBy[0].Expr.(*ast.IdentExpr)
	if !ok {
		t.Fatalf("expected IdentExpr, got %T", sel.OrderBy[0].Expr)
	}
	if ident.Name != "id" {
		t.Errorf("expected column 'id', got %q", ident.Name)
	}
	if sel.OrderBy[0].Desc {
		t.Errorf("expected ASC (Desc=false), got DESC")
	}
}

func TestParseSelectOrderByDesc(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users ORDER BY id DESC")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.OrderBy) != 1 {
		t.Fatalf("expected 1 order by clause, got %d", len(sel.OrderBy))
	}
	if !sel.OrderBy[0].Desc {
		t.Errorf("expected DESC (Desc=true), got ASC")
	}
}

func TestParseSelectOrderByAsc(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users ORDER BY id ASC")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.OrderBy) != 1 {
		t.Fatalf("expected 1 order by clause, got %d", len(sel.OrderBy))
	}
	if sel.OrderBy[0].Desc {
		t.Errorf("expected ASC (Desc=false), got DESC")
	}
}

func TestParseSelectOrderByMultipleColumns(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users ORDER BY name ASC, id DESC")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.OrderBy) != 2 {
		t.Fatalf("expected 2 order by clauses, got %d", len(sel.OrderBy))
	}
	ident0 := sel.OrderBy[0].Expr.(*ast.IdentExpr)
	if ident0.Name != "name" {
		t.Errorf("order by 0: expected 'name', got %q", ident0.Name)
	}
	if sel.OrderBy[0].Desc {
		t.Errorf("order by 0: expected ASC")
	}
	ident1 := sel.OrderBy[1].Expr.(*ast.IdentExpr)
	if ident1.Name != "id" {
		t.Errorf("order by 1: expected 'id', got %q", ident1.Name)
	}
	if !sel.OrderBy[1].Desc {
		t.Errorf("order by 1: expected DESC")
	}
}

func TestParseSelectWhereOrderBy(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users WHERE id > 1 ORDER BY name")
	sel := stmt.(*ast.SelectStmt)
	if sel.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if len(sel.OrderBy) != 1 {
		t.Fatalf("expected 1 order by clause, got %d", len(sel.OrderBy))
	}
	ident := sel.OrderBy[0].Expr.(*ast.IdentExpr)
	if ident.Name != "name" {
		t.Errorf("expected order by 'name', got %q", ident.Name)
	}
}

func TestParseSelectLimitOnly(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users LIMIT 10")
	sel := stmt.(*ast.SelectStmt)
	if sel.Limit == nil {
		t.Fatal("expected LIMIT clause")
	}
	if *sel.Limit != 10 {
		t.Errorf("expected LIMIT 10, got %d", *sel.Limit)
	}
	if sel.Offset != nil {
		t.Errorf("expected no OFFSET, got %d", *sel.Offset)
	}
}

func TestParseSelectOffsetOnly(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users OFFSET 5")
	sel := stmt.(*ast.SelectStmt)
	if sel.Offset == nil {
		t.Fatal("expected OFFSET clause")
	}
	if *sel.Offset != 5 {
		t.Errorf("expected OFFSET 5, got %d", *sel.Offset)
	}
	if sel.Limit != nil {
		t.Errorf("expected no LIMIT, got %d", *sel.Limit)
	}
}

func TestParseSelectLimitOffset(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users LIMIT 10 OFFSET 5")
	sel := stmt.(*ast.SelectStmt)
	if sel.Limit == nil {
		t.Fatal("expected LIMIT clause")
	}
	if *sel.Limit != 10 {
		t.Errorf("expected LIMIT 10, got %d", *sel.Limit)
	}
	if sel.Offset == nil {
		t.Fatal("expected OFFSET clause")
	}
	if *sel.Offset != 5 {
		t.Errorf("expected OFFSET 5, got %d", *sel.Offset)
	}
}

func TestParseSelectOrderByLimitOffset(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users ORDER BY id ASC LIMIT 2 OFFSET 1")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.OrderBy) != 1 {
		t.Fatalf("expected 1 order by clause, got %d", len(sel.OrderBy))
	}
	if sel.Limit == nil || *sel.Limit != 2 {
		t.Errorf("expected LIMIT 2")
	}
	if sel.Offset == nil || *sel.Offset != 1 {
		t.Errorf("expected OFFSET 1")
	}
}

func TestParseTruncateTable(t *testing.T) {
	stmt := parse(t, "TRUNCATE TABLE users")
	tt, ok := stmt.(*ast.TruncateTableStmt)
	if !ok {
		t.Fatalf("expected TruncateTableStmt, got %T", stmt)
	}
	if tt.TableName != "users" {
		t.Errorf("table name: expected %q, got %q", "users", tt.TableName)
	}
}

func TestParseDropTable(t *testing.T) {
	stmt := parse(t, "DROP TABLE users")
	dt, ok := stmt.(*ast.DropTableStmt)
	if !ok {
		t.Fatalf("expected DropTableStmt, got %T", stmt)
	}
	if dt.TableName != "users" {
		t.Errorf("table name: expected %q, got %q", "users", dt.TableName)
	}
}

func TestParseSelectGroupBy(t *testing.T) {
	stmt := parse(t, "SELECT name, COUNT(*) FROM users GROUP BY name")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(sel.Columns))
	}
	ident, ok := sel.Columns[0].(*ast.IdentExpr)
	if !ok {
		t.Fatalf("expected IdentExpr, got %T", sel.Columns[0])
	}
	if ident.Name != "name" {
		t.Errorf("expected column 'name', got %q", ident.Name)
	}
	call, ok := sel.Columns[1].(*ast.CallExpr)
	if !ok {
		t.Fatalf("expected CallExpr, got %T", sel.Columns[1])
	}
	if call.Name != "COUNT" {
		t.Errorf("expected function name COUNT, got %s", call.Name)
	}
	if len(sel.GroupBy) != 1 {
		t.Fatalf("expected 1 GROUP BY expr, got %d", len(sel.GroupBy))
	}
	gbIdent, ok := sel.GroupBy[0].(*ast.IdentExpr)
	if !ok {
		t.Fatalf("expected IdentExpr in GROUP BY, got %T", sel.GroupBy[0])
	}
	if gbIdent.Name != "name" {
		t.Errorf("expected GROUP BY 'name', got %q", gbIdent.Name)
	}
	if sel.Having != nil {
		t.Errorf("expected no HAVING, got %v", sel.Having)
	}
}

func TestParseSelectGroupByHaving(t *testing.T) {
	stmt := parse(t, "SELECT name FROM users GROUP BY name HAVING COUNT(*) > 1")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.GroupBy) != 1 {
		t.Fatalf("expected 1 GROUP BY expr, got %d", len(sel.GroupBy))
	}
	if sel.Having == nil {
		t.Fatal("expected HAVING clause")
	}
	bin, ok := sel.Having.(*ast.BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr in HAVING, got %T", sel.Having)
	}
	if bin.Op != ">" {
		t.Errorf("expected op '>', got %q", bin.Op)
	}
	call, ok := bin.Left.(*ast.CallExpr)
	if !ok {
		t.Fatalf("expected CallExpr on left of HAVING, got %T", bin.Left)
	}
	if call.Name != "COUNT" {
		t.Errorf("expected COUNT, got %s", call.Name)
	}
	if bin.Right.(*ast.IntLitExpr).Value != 1 {
		t.Errorf("expected right=1")
	}
}

func TestParseSelectGroupByMultiple(t *testing.T) {
	stmt := parse(t, "SELECT col1, col2, COUNT(*) FROM t GROUP BY col1, col2")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.GroupBy) != 2 {
		t.Fatalf("expected 2 GROUP BY exprs, got %d", len(sel.GroupBy))
	}
	if sel.GroupBy[0].(*ast.IdentExpr).Name != "col1" {
		t.Errorf("expected GROUP BY 'col1'")
	}
	if sel.GroupBy[1].(*ast.IdentExpr).Name != "col2" {
		t.Errorf("expected GROUP BY 'col2'")
	}
}

func TestParseSelectGroupByOrderBy(t *testing.T) {
	stmt := parse(t, "SELECT name, COUNT(*) FROM users GROUP BY name ORDER BY name ASC")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.GroupBy) != 1 {
		t.Fatalf("expected 1 GROUP BY expr, got %d", len(sel.GroupBy))
	}
	if len(sel.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY clause, got %d", len(sel.OrderBy))
	}
}

func TestParseSelectSumAmount(t *testing.T) {
	stmt := parse(t, "SELECT SUM(amount) FROM orders")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
	call, ok := sel.Columns[0].(*ast.CallExpr)
	if !ok {
		t.Fatalf("expected CallExpr, got %T", sel.Columns[0])
	}
	if call.Name != "SUM" {
		t.Errorf("expected function name SUM, got %s", call.Name)
	}
	if len(call.Args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(call.Args))
	}
	ident, ok := call.Args[0].(*ast.IdentExpr)
	if !ok {
		t.Fatalf("expected IdentExpr arg, got %T", call.Args[0])
	}
	if ident.Name != "amount" {
		t.Errorf("expected arg 'amount', got %q", ident.Name)
	}
}

func TestParseSelectMinMaxId(t *testing.T) {
	stmt := parse(t, "SELECT MIN(id), MAX(id) FROM users")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(sel.Columns))
	}
	minCall, ok := sel.Columns[0].(*ast.CallExpr)
	if !ok {
		t.Fatalf("expected CallExpr for MIN, got %T", sel.Columns[0])
	}
	if minCall.Name != "MIN" {
		t.Errorf("expected function name MIN, got %s", minCall.Name)
	}
	maxCall, ok := sel.Columns[1].(*ast.CallExpr)
	if !ok {
		t.Fatalf("expected CallExpr for MAX, got %T", sel.Columns[1])
	}
	if maxCall.Name != "MAX" {
		t.Errorf("expected function name MAX, got %s", maxCall.Name)
	}
}

func TestParseSelectAvgAge(t *testing.T) {
	stmt := parse(t, "SELECT AVG(age) FROM users")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
	call, ok := sel.Columns[0].(*ast.CallExpr)
	if !ok {
		t.Fatalf("expected CallExpr, got %T", sel.Columns[0])
	}
	if call.Name != "AVG" {
		t.Errorf("expected function name AVG, got %s", call.Name)
	}
}

func TestParseSelectGroupBySumHaving(t *testing.T) {
	stmt := parse(t, "SELECT name, SUM(amount) FROM orders GROUP BY name HAVING SUM(amount) > 100")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(sel.Columns))
	}
	call, ok := sel.Columns[1].(*ast.CallExpr)
	if !ok {
		t.Fatalf("expected CallExpr, got %T", sel.Columns[1])
	}
	if call.Name != "SUM" {
		t.Errorf("expected function name SUM, got %s", call.Name)
	}
	if sel.Having == nil {
		t.Fatal("expected HAVING clause")
	}
	bin, ok := sel.Having.(*ast.BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr in HAVING, got %T", sel.Having)
	}
	havingCall, ok := bin.Left.(*ast.CallExpr)
	if !ok {
		t.Fatalf("expected CallExpr on left of HAVING, got %T", bin.Left)
	}
	if havingCall.Name != "SUM" {
		t.Errorf("expected SUM in HAVING, got %s", havingCall.Name)
	}
}

func TestParseSelectFloatLiteral(t *testing.T) {
	stmt := parse(t, "SELECT 3.14")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
	lit, ok := sel.Columns[0].(*ast.FloatLitExpr)
	if !ok {
		t.Fatalf("expected FloatLitExpr, got %T", sel.Columns[0])
	}
	if lit.Value != 3.14 {
		t.Errorf("expected value 3.14, got %f", lit.Value)
	}
}

func TestParseCreateTableFloat(t *testing.T) {
	stmt := parse(t, "CREATE TABLE t (val FLOAT)")
	ct, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		t.Fatalf("expected CreateTableStmt, got %T", stmt)
	}
	if len(ct.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(ct.Columns))
	}
	if ct.Columns[0].Name != "val" || ct.Columns[0].DataType != "FLOAT" {
		t.Errorf("column 0: expected (val, FLOAT), got (%s, %s)", ct.Columns[0].Name, ct.Columns[0].DataType)
	}
}

func TestParseSelectDistinct(t *testing.T) {
	stmt := parse(t, "SELECT DISTINCT name FROM users")
	sel := stmt.(*ast.SelectStmt)
	if !sel.Distinct {
		t.Errorf("expected Distinct=true, got false")
	}
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
	ident, ok := sel.Columns[0].(*ast.IdentExpr)
	if !ok {
		t.Fatalf("expected IdentExpr, got %T", sel.Columns[0])
	}
	if ident.Name != "name" {
		t.Errorf("expected column 'name', got %q", ident.Name)
	}
}

func TestParseSelectWithoutDistinct(t *testing.T) {
	stmt := parse(t, "SELECT name FROM users")
	sel := stmt.(*ast.SelectStmt)
	if sel.Distinct {
		t.Errorf("expected Distinct=false, got true")
	}
}

func TestParseCreateTableWithDefault(t *testing.T) {
	stmt := parse(t, "CREATE TABLE t (id INT DEFAULT 0, name TEXT DEFAULT 'unknown')")
	ct, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		t.Fatalf("expected CreateTableStmt, got %T", stmt)
	}
	if len(ct.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(ct.Columns))
	}
	if ct.Columns[0].Name != "id" || ct.Columns[0].DataType != "INT" {
		t.Errorf("column 0: expected (id, INT), got (%s, %s)", ct.Columns[0].Name, ct.Columns[0].DataType)
	}
	if ct.Columns[0].Default == nil {
		t.Fatal("column 0: expected DEFAULT expr, got nil")
	}
	intLit, ok := ct.Columns[0].Default.(*ast.IntLitExpr)
	if !ok {
		t.Fatalf("column 0 default: expected IntLitExpr, got %T", ct.Columns[0].Default)
	}
	if intLit.Value != 0 {
		t.Errorf("column 0 default: expected 0, got %d", intLit.Value)
	}
	if ct.Columns[1].Default == nil {
		t.Fatal("column 1: expected DEFAULT expr, got nil")
	}
	strLit, ok := ct.Columns[1].Default.(*ast.StringLitExpr)
	if !ok {
		t.Fatalf("column 1 default: expected StringLitExpr, got %T", ct.Columns[1].Default)
	}
	if strLit.Value != "unknown" {
		t.Errorf("column 1 default: expected 'unknown', got %q", strLit.Value)
	}
}

func TestParseCreateTableWithDefaultNull(t *testing.T) {
	stmt := parse(t, "CREATE TABLE t (id INT, name TEXT DEFAULT NULL)")
	ct := stmt.(*ast.CreateTableStmt)
	if ct.Columns[0].Default != nil {
		t.Errorf("column 0: expected no DEFAULT, got %v", ct.Columns[0].Default)
	}
	if ct.Columns[1].Default == nil {
		t.Fatal("column 1: expected DEFAULT NULL, got nil")
	}
	if _, ok := ct.Columns[1].Default.(*ast.NullLitExpr); !ok {
		t.Errorf("column 1 default: expected NullLitExpr, got %T", ct.Columns[1].Default)
	}
}

func TestParseCreateTableNotNullDefault(t *testing.T) {
	stmt := parse(t, "CREATE TABLE t (id INT NOT NULL DEFAULT 1)")
	ct := stmt.(*ast.CreateTableStmt)
	if !ct.Columns[0].NotNull {
		t.Errorf("column 0: expected NOT NULL")
	}
	if ct.Columns[0].Default == nil {
		t.Fatal("column 0: expected DEFAULT expr, got nil")
	}
	intLit, ok := ct.Columns[0].Default.(*ast.IntLitExpr)
	if !ok {
		t.Fatalf("column 0 default: expected IntLitExpr, got %T", ct.Columns[0].Default)
	}
	if intLit.Value != 1 {
		t.Errorf("column 0 default: expected 1, got %d", intLit.Value)
	}
}

func TestParseInsertWithColumns(t *testing.T) {
	stmt := parse(t, "INSERT INTO users (id, name) VALUES (1, 'alice')")
	ins, ok := stmt.(*ast.InsertStmt)
	if !ok {
		t.Fatalf("expected InsertStmt, got %T", stmt)
	}
	if ins.TableName != "users" {
		t.Errorf("table name: expected %q, got %q", "users", ins.TableName)
	}
	if len(ins.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(ins.Columns))
	}
	if ins.Columns[0] != "id" {
		t.Errorf("column 0: expected 'id', got %q", ins.Columns[0])
	}
	if ins.Columns[1] != "name" {
		t.Errorf("column 1: expected 'name', got %q", ins.Columns[1])
	}
	if len(ins.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(ins.Rows))
	}
	if len(ins.Rows[0]) != 2 {
		t.Fatalf("expected 2 values, got %d", len(ins.Rows[0]))
	}
}

func TestParseInsertWithPartialColumns(t *testing.T) {
	stmt := parse(t, "INSERT INTO users (name) VALUES ('alice')")
	ins := stmt.(*ast.InsertStmt)
	if len(ins.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(ins.Columns))
	}
	if ins.Columns[0] != "name" {
		t.Errorf("column 0: expected 'name', got %q", ins.Columns[0])
	}
	if len(ins.Rows[0]) != 1 {
		t.Fatalf("expected 1 value, got %d", len(ins.Rows[0]))
	}
}

func TestParseInsertWithoutColumns(t *testing.T) {
	stmt := parse(t, "INSERT INTO users VALUES (1, 'alice')")
	ins := stmt.(*ast.InsertStmt)
	if ins.Columns != nil {
		t.Errorf("expected nil columns, got %v", ins.Columns)
	}
}

func TestParseSelectWhereIn(t *testing.T) {
	stmt := parse(t, "SELECT * FROM t WHERE id IN (1, 2, 3)")
	sel := stmt.(*ast.SelectStmt)
	inExpr, ok := sel.Where.(*ast.InExpr)
	if !ok {
		t.Fatalf("expected InExpr, got %T", sel.Where)
	}
	if inExpr.Not {
		t.Errorf("expected Not=false, got true")
	}
	ident := inExpr.Left.(*ast.IdentExpr)
	if ident.Name != "id" {
		t.Errorf("expected column 'id', got %q", ident.Name)
	}
	if len(inExpr.Values) != 3 {
		t.Fatalf("expected 3 values, got %d", len(inExpr.Values))
	}
	if inExpr.Values[0].(*ast.IntLitExpr).Value != 1 {
		t.Errorf("expected value 0 = 1")
	}
	if inExpr.Values[1].(*ast.IntLitExpr).Value != 2 {
		t.Errorf("expected value 1 = 2")
	}
	if inExpr.Values[2].(*ast.IntLitExpr).Value != 3 {
		t.Errorf("expected value 2 = 3")
	}
}

func TestParseSelectWhereNotIn(t *testing.T) {
	stmt := parse(t, "SELECT * FROM t WHERE name NOT IN ('a', 'b')")
	sel := stmt.(*ast.SelectStmt)
	inExpr, ok := sel.Where.(*ast.InExpr)
	if !ok {
		t.Fatalf("expected InExpr, got %T", sel.Where)
	}
	if !inExpr.Not {
		t.Errorf("expected Not=true, got false")
	}
	ident := inExpr.Left.(*ast.IdentExpr)
	if ident.Name != "name" {
		t.Errorf("expected column 'name', got %q", ident.Name)
	}
	if len(inExpr.Values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(inExpr.Values))
	}
	if inExpr.Values[0].(*ast.StringLitExpr).Value != "a" {
		t.Errorf("expected value 0 = 'a'")
	}
	if inExpr.Values[1].(*ast.StringLitExpr).Value != "b" {
		t.Errorf("expected value 1 = 'b'")
	}
}

func TestParseCreateTablePrimaryKey(t *testing.T) {
	stmt := parse(t, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
	ct, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		t.Fatalf("expected CreateTableStmt, got %T", stmt)
	}
	if len(ct.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(ct.Columns))
	}
	if ct.Columns[0].Name != "id" || ct.Columns[0].DataType != "INT" {
		t.Errorf("column 0: expected (id, INT), got (%s, %s)", ct.Columns[0].Name, ct.Columns[0].DataType)
	}
	if !ct.Columns[0].PrimaryKey {
		t.Error("column 0: expected PrimaryKey=true")
	}
	if !ct.Columns[0].NotNull {
		t.Error("column 0: expected NotNull=true (implied by PRIMARY KEY)")
	}
	if ct.Columns[1].PrimaryKey {
		t.Error("column 1: expected PrimaryKey=false")
	}
}

func TestParseCreateTableNotNullPrimaryKey(t *testing.T) {
	stmt := parse(t, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name TEXT)")
	ct, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		t.Fatalf("expected CreateTableStmt, got %T", stmt)
	}
	if len(ct.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(ct.Columns))
	}
	if !ct.Columns[0].PrimaryKey {
		t.Error("column 0: expected PrimaryKey=true")
	}
	if !ct.Columns[0].NotNull {
		t.Error("column 0: expected NotNull=true")
	}
}

func TestParseSelectWhereBetween(t *testing.T) {
	stmt := parse(t, "SELECT * FROM t WHERE id BETWEEN 1 AND 10")
	sel := stmt.(*ast.SelectStmt)
	betweenExpr, ok := sel.Where.(*ast.BetweenExpr)
	if !ok {
		t.Fatalf("expected BetweenExpr, got %T", sel.Where)
	}
	if betweenExpr.Not {
		t.Errorf("expected Not=false, got true")
	}
	ident := betweenExpr.Left.(*ast.IdentExpr)
	if ident.Name != "id" {
		t.Errorf("expected column 'id', got %q", ident.Name)
	}
	low := betweenExpr.Low.(*ast.IntLitExpr)
	if low.Value != 1 {
		t.Errorf("expected low=1, got %d", low.Value)
	}
	high := betweenExpr.High.(*ast.IntLitExpr)
	if high.Value != 10 {
		t.Errorf("expected high=10, got %d", high.Value)
	}
}

func TestParseSelectWhereNotBetween(t *testing.T) {
	stmt := parse(t, "SELECT * FROM t WHERE id NOT BETWEEN 1 AND 10")
	sel := stmt.(*ast.SelectStmt)
	betweenExpr, ok := sel.Where.(*ast.BetweenExpr)
	if !ok {
		t.Fatalf("expected BetweenExpr, got %T", sel.Where)
	}
	if !betweenExpr.Not {
		t.Errorf("expected Not=true, got false")
	}
	ident := betweenExpr.Left.(*ast.IdentExpr)
	if ident.Name != "id" {
		t.Errorf("expected column 'id', got %q", ident.Name)
	}
	low := betweenExpr.Low.(*ast.IntLitExpr)
	if low.Value != 1 {
		t.Errorf("expected low=1, got %d", low.Value)
	}
	high := betweenExpr.High.(*ast.IntLitExpr)
	if high.Value != 10 {
		t.Errorf("expected high=10, got %d", high.Value)
	}
}

func TestParseSelectWhereLike(t *testing.T) {
	stmt := parse(t, "SELECT * FROM t WHERE name LIKE '%alice%'")
	sel := stmt.(*ast.SelectStmt)
	likeExpr, ok := sel.Where.(*ast.LikeExpr)
	if !ok {
		t.Fatalf("expected LikeExpr, got %T", sel.Where)
	}
	if likeExpr.Not {
		t.Errorf("expected Not=false, got true")
	}
	ident := likeExpr.Left.(*ast.IdentExpr)
	if ident.Name != "name" {
		t.Errorf("expected column 'name', got %q", ident.Name)
	}
	pattern := likeExpr.Pattern.(*ast.StringLitExpr)
	if pattern.Value != "%alice%" {
		t.Errorf("expected pattern '%%alice%%', got %q", pattern.Value)
	}
}

func TestParseSelectWhereNotLike(t *testing.T) {
	stmt := parse(t, "SELECT * FROM t WHERE name NOT LIKE 'bob%'")
	sel := stmt.(*ast.SelectStmt)
	likeExpr, ok := sel.Where.(*ast.LikeExpr)
	if !ok {
		t.Fatalf("expected LikeExpr, got %T", sel.Where)
	}
	if !likeExpr.Not {
		t.Errorf("expected Not=true, got false")
	}
	ident := likeExpr.Left.(*ast.IdentExpr)
	if ident.Name != "name" {
		t.Errorf("expected column 'name', got %q", ident.Name)
	}
	pattern := likeExpr.Pattern.(*ast.StringLitExpr)
	if pattern.Value != "bob%" {
		t.Errorf("expected pattern 'bob%%', got %q", pattern.Value)
	}
}

func TestParseCreateIndex(t *testing.T) {
	stmt := parse(t, "CREATE INDEX idx_name ON users(name)")
	ci, ok := stmt.(*ast.CreateIndexStmt)
	if !ok {
		t.Fatalf("expected CreateIndexStmt, got %T", stmt)
	}
	if ci.IndexName != "idx_name" {
		t.Errorf("index name: expected %q, got %q", "idx_name", ci.IndexName)
	}
	if ci.TableName != "users" {
		t.Errorf("table name: expected %q, got %q", "users", ci.TableName)
	}
	if len(ci.ColumnNames) != 1 || ci.ColumnNames[0] != "name" {
		t.Errorf("column names: expected [\"name\"], got %v", ci.ColumnNames)
	}
}

func TestParseCreateCompositeIndex(t *testing.T) {
	stmt := parse(t, "CREATE INDEX idx_name_age ON users(name, age)")
	ci, ok := stmt.(*ast.CreateIndexStmt)
	if !ok {
		t.Fatalf("expected CreateIndexStmt, got %T", stmt)
	}
	if ci.IndexName != "idx_name_age" {
		t.Errorf("index name: expected %q, got %q", "idx_name_age", ci.IndexName)
	}
	if ci.TableName != "users" {
		t.Errorf("table name: expected %q, got %q", "users", ci.TableName)
	}
	if len(ci.ColumnNames) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(ci.ColumnNames))
	}
	if ci.ColumnNames[0] != "name" {
		t.Errorf("column 0: expected %q, got %q", "name", ci.ColumnNames[0])
	}
	if ci.ColumnNames[1] != "age" {
		t.Errorf("column 1: expected %q, got %q", "age", ci.ColumnNames[1])
	}
}

func TestParseDropIndex(t *testing.T) {
	stmt := parse(t, "DROP INDEX idx_name")
	di, ok := stmt.(*ast.DropIndexStmt)
	if !ok {
		t.Fatalf("expected DropIndexStmt, got %T", stmt)
	}
	if di.IndexName != "idx_name" {
		t.Errorf("index name: expected %q, got %q", "idx_name", di.IndexName)
	}
}

func TestParseLeftJoin(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id")
	sel, ok := stmt.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", stmt)
	}
	if len(sel.Joins) != 1 {
		t.Fatalf("expected 1 join, got %d", len(sel.Joins))
	}
	if sel.Joins[0].JoinType != ast.JoinLeft {
		t.Errorf("expected JoinType=%q, got %q", ast.JoinLeft, sel.Joins[0].JoinType)
	}
	if sel.Joins[0].TableName != "orders" {
		t.Errorf("expected table name %q, got %q", "orders", sel.Joins[0].TableName)
	}
}

func TestParseLeftOuterJoin(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id")
	sel, ok := stmt.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", stmt)
	}
	if len(sel.Joins) != 1 {
		t.Fatalf("expected 1 join, got %d", len(sel.Joins))
	}
	if sel.Joins[0].JoinType != ast.JoinLeft {
		t.Errorf("expected JoinType=%q, got %q", ast.JoinLeft, sel.Joins[0].JoinType)
	}
}

func TestParseInnerJoinType(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"bare JOIN", "SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id"},
		{"INNER JOIN", "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.t1_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parse(t, tt.input)
			sel, ok := stmt.(*ast.SelectStmt)
			if !ok {
				t.Fatalf("expected SelectStmt, got %T", stmt)
			}
			if len(sel.Joins) != 1 {
				t.Fatalf("expected 1 join, got %d", len(sel.Joins))
			}
			if sel.Joins[0].JoinType != ast.JoinInner {
				t.Errorf("expected JoinType=%q, got %q", ast.JoinInner, sel.Joins[0].JoinType)
			}
		})
	}
}

func TestParseCaseSearched(t *testing.T) {
	stmt := parse(t, "SELECT CASE WHEN id > 0 THEN 'positive' ELSE 'non-positive' END FROM t")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
	caseExpr, ok := sel.Columns[0].(*ast.CaseExpr)
	if !ok {
		t.Fatalf("expected CaseExpr, got %T", sel.Columns[0])
	}
	if caseExpr.Operand != nil {
		t.Errorf("expected nil Operand for Searched CASE, got %T", caseExpr.Operand)
	}
	if len(caseExpr.Whens) != 1 {
		t.Fatalf("expected 1 WHEN clause, got %d", len(caseExpr.Whens))
	}
	// WHEN condition should be a BinaryExpr (id > 0)
	_, ok = caseExpr.Whens[0].When.(*ast.BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr in WHEN, got %T", caseExpr.Whens[0].When)
	}
	// THEN value
	thenVal, ok := caseExpr.Whens[0].Then.(*ast.StringLitExpr)
	if !ok {
		t.Fatalf("expected StringLitExpr in THEN, got %T", caseExpr.Whens[0].Then)
	}
	if thenVal.Value != "positive" {
		t.Errorf("expected THEN 'positive', got %q", thenVal.Value)
	}
	// ELSE value
	if caseExpr.Else == nil {
		t.Fatal("expected ELSE clause")
	}
	elseVal, ok := caseExpr.Else.(*ast.StringLitExpr)
	if !ok {
		t.Fatalf("expected StringLitExpr in ELSE, got %T", caseExpr.Else)
	}
	if elseVal.Value != "non-positive" {
		t.Errorf("expected ELSE 'non-positive', got %q", elseVal.Value)
	}
}

func TestParseCaseSimple(t *testing.T) {
	stmt := parse(t, "SELECT CASE status WHEN 1 THEN 'active' WHEN 0 THEN 'inactive' END FROM t")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
	caseExpr, ok := sel.Columns[0].(*ast.CaseExpr)
	if !ok {
		t.Fatalf("expected CaseExpr, got %T", sel.Columns[0])
	}
	// Operand should be an IdentExpr
	operand, ok := caseExpr.Operand.(*ast.IdentExpr)
	if !ok {
		t.Fatalf("expected IdentExpr as Operand, got %T", caseExpr.Operand)
	}
	if operand.Name != "status" {
		t.Errorf("expected Operand 'status', got %q", operand.Name)
	}
	if len(caseExpr.Whens) != 2 {
		t.Fatalf("expected 2 WHEN clauses, got %d", len(caseExpr.Whens))
	}
	// First WHEN: 1 THEN 'active'
	when1, ok := caseExpr.Whens[0].When.(*ast.IntLitExpr)
	if !ok {
		t.Fatalf("expected IntLitExpr in WHEN 0, got %T", caseExpr.Whens[0].When)
	}
	if when1.Value != 1 {
		t.Errorf("expected WHEN value 1, got %d", when1.Value)
	}
	then1, ok := caseExpr.Whens[0].Then.(*ast.StringLitExpr)
	if !ok {
		t.Fatalf("expected StringLitExpr in THEN 0, got %T", caseExpr.Whens[0].Then)
	}
	if then1.Value != "active" {
		t.Errorf("expected THEN 'active', got %q", then1.Value)
	}
	// Second WHEN: 0 THEN 'inactive'
	when2, ok := caseExpr.Whens[1].When.(*ast.IntLitExpr)
	if !ok {
		t.Fatalf("expected IntLitExpr in WHEN 1, got %T", caseExpr.Whens[1].When)
	}
	if when2.Value != 0 {
		t.Errorf("expected WHEN value 0, got %d", when2.Value)
	}
	// No ELSE
	if caseExpr.Else != nil {
		t.Errorf("expected nil ELSE, got %T", caseExpr.Else)
	}
}

func TestParseCaseNoElse(t *testing.T) {
	stmt := parse(t, "SELECT CASE WHEN id = 1 THEN 'one' END FROM t")
	sel := stmt.(*ast.SelectStmt)
	caseExpr, ok := sel.Columns[0].(*ast.CaseExpr)
	if !ok {
		t.Fatalf("expected CaseExpr, got %T", sel.Columns[0])
	}
	if caseExpr.Operand != nil {
		t.Errorf("expected nil Operand, got %T", caseExpr.Operand)
	}
	if len(caseExpr.Whens) != 1 {
		t.Fatalf("expected 1 WHEN clause, got %d", len(caseExpr.Whens))
	}
	if caseExpr.Else != nil {
		t.Errorf("expected nil ELSE, got %T", caseExpr.Else)
	}
}

func TestParseCoalesce(t *testing.T) {
	stmt := parse(t, "SELECT COALESCE(a, b, c) FROM t")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
	call, ok := sel.Columns[0].(*ast.CallExpr)
	if !ok {
		t.Fatalf("expected CallExpr, got %T", sel.Columns[0])
	}
	if call.Name != "COALESCE" {
		t.Errorf("expected function name COALESCE, got %q", call.Name)
	}
	if len(call.Args) != 3 {
		t.Fatalf("expected 3 args, got %d", len(call.Args))
	}
	for i, name := range []string{"a", "b", "c"} {
		ident, ok := call.Args[i].(*ast.IdentExpr)
		if !ok {
			t.Fatalf("arg %d: expected IdentExpr, got %T", i, call.Args[i])
		}
		if ident.Name != name {
			t.Errorf("arg %d: expected %q, got %q", i, name, ident.Name)
		}
	}
}

func TestParseNullif(t *testing.T) {
	stmt := parse(t, "SELECT NULLIF(a, b) FROM t")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
	call, ok := sel.Columns[0].(*ast.CallExpr)
	if !ok {
		t.Fatalf("expected CallExpr, got %T", sel.Columns[0])
	}
	if call.Name != "NULLIF" {
		t.Errorf("expected function name NULLIF, got %q", call.Name)
	}
	if len(call.Args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(call.Args))
	}
}

func TestParseNumericFunctions(t *testing.T) {
	tests := []struct {
		input    string
		funcName string
		argCount int
	}{
		{"SELECT ABS(x) FROM t", "ABS", 1},
		{"SELECT ROUND(x, 2) FROM t", "ROUND", 2},
		{"SELECT ROUND(x) FROM t", "ROUND", 1},
		{"SELECT MOD(a, b) FROM t", "MOD", 2},
		{"SELECT CEIL(x) FROM t", "CEIL", 1},
		{"SELECT FLOOR(x) FROM t", "FLOOR", 1},
		{"SELECT POWER(x, y) FROM t", "POWER", 2},
	}
	for _, tt := range tests {
		t.Run(tt.funcName, func(t *testing.T) {
			stmt := parse(t, tt.input)
			sel := stmt.(*ast.SelectStmt)
			if len(sel.Columns) != 1 {
				t.Fatalf("expected 1 column, got %d", len(sel.Columns))
			}
			call, ok := sel.Columns[0].(*ast.CallExpr)
			if !ok {
				t.Fatalf("expected CallExpr, got %T", sel.Columns[0])
			}
			if call.Name != tt.funcName {
				t.Errorf("expected function name %q, got %q", tt.funcName, call.Name)
			}
			if len(call.Args) != tt.argCount {
				t.Errorf("expected %d args, got %d", tt.argCount, len(call.Args))
			}
		})
	}
}

func TestParseStringFunctions(t *testing.T) {
	tests := []struct {
		input    string
		funcName string
		argCount int
	}{
		{"SELECT LENGTH('hello') FROM t", "LENGTH", 1},
		{"SELECT UPPER(name) FROM t", "UPPER", 1},
		{"SELECT LOWER(name) FROM t", "LOWER", 1},
		{"SELECT SUBSTRING(name, 1, 3) FROM t", "SUBSTRING", 3},
		{"SELECT SUBSTRING(name, 2) FROM t", "SUBSTRING", 2},
		{"SELECT TRIM(name) FROM t", "TRIM", 1},
		{"SELECT CONCAT(a, b) FROM t", "CONCAT", 2},
		{"SELECT CONCAT(a, b, c) FROM t", "CONCAT", 3},
	}
	for _, tt := range tests {
		t.Run(tt.funcName+"_"+fmt.Sprintf("%d", tt.argCount), func(t *testing.T) {
			stmt := parse(t, tt.input)
			sel := stmt.(*ast.SelectStmt)
			if len(sel.Columns) != 1 {
				t.Fatalf("expected 1 column, got %d", len(sel.Columns))
			}
			call, ok := sel.Columns[0].(*ast.CallExpr)
			if !ok {
				t.Fatalf("expected CallExpr, got %T", sel.Columns[0])
			}
			if call.Name != tt.funcName {
				t.Errorf("expected function name %q, got %q", tt.funcName, call.Name)
			}
			if len(call.Args) != tt.argCount {
				t.Errorf("expected %d args, got %d", tt.argCount, len(call.Args))
			}
		})
	}
}

func TestParseUnion(t *testing.T) {
	stmt := parse(t, "SELECT a FROM t1 UNION SELECT b FROM t2")
	u, ok := stmt.(*ast.UnionStmt)
	if !ok {
		t.Fatalf("expected UnionStmt, got %T", stmt)
	}
	if u.All {
		t.Error("expected All=false for UNION")
	}
	left, ok := u.Left.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected left to be SelectStmt, got %T", u.Left)
	}
	if left.TableName != "t1" {
		t.Errorf("left table: expected %q, got %q", "t1", left.TableName)
	}
	if u.Right.TableName != "t2" {
		t.Errorf("right table: expected %q, got %q", "t2", u.Right.TableName)
	}
}

func TestParseUnionAll(t *testing.T) {
	stmt := parse(t, "SELECT a FROM t1 UNION ALL SELECT b FROM t2")
	u, ok := stmt.(*ast.UnionStmt)
	if !ok {
		t.Fatalf("expected UnionStmt, got %T", stmt)
	}
	if !u.All {
		t.Error("expected All=true for UNION ALL")
	}
}

func TestParseUnionChain(t *testing.T) {
	stmt := parse(t, "SELECT a FROM t1 UNION SELECT b FROM t2 UNION SELECT c FROM t3")
	u, ok := stmt.(*ast.UnionStmt)
	if !ok {
		t.Fatalf("expected UnionStmt, got %T", stmt)
	}
	// Right is t3
	if u.Right.TableName != "t3" {
		t.Errorf("right table: expected %q, got %q", "t3", u.Right.TableName)
	}
	// Left is another UnionStmt
	inner, ok := u.Left.(*ast.UnionStmt)
	if !ok {
		t.Fatalf("expected left to be UnionStmt, got %T", u.Left)
	}
	innerLeft, ok := inner.Left.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected inner left to be SelectStmt, got %T", inner.Left)
	}
	if innerLeft.TableName != "t1" {
		t.Errorf("inner left table: expected %q, got %q", "t1", innerLeft.TableName)
	}
	if inner.Right.TableName != "t2" {
		t.Errorf("inner right table: expected %q, got %q", "t2", inner.Right.TableName)
	}
}

func TestParseUnionOrderByLimit(t *testing.T) {
	stmt := parse(t, "SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a LIMIT 5 OFFSET 2")
	u, ok := stmt.(*ast.UnionStmt)
	if !ok {
		t.Fatalf("expected UnionStmt, got %T", stmt)
	}
	if len(u.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY clause, got %d", len(u.OrderBy))
	}
	ident := u.OrderBy[0].Expr.(*ast.IdentExpr)
	if ident.Name != "a" {
		t.Errorf("expected ORDER BY 'a', got %q", ident.Name)
	}
	if u.Limit == nil || *u.Limit != 5 {
		t.Errorf("expected LIMIT 5")
	}
	if u.Offset == nil || *u.Offset != 2 {
		t.Errorf("expected OFFSET 2")
	}
}

func TestParseSelectWithoutUnionUnchanged(t *testing.T) {
	// Without UNION, parseSelect should still return *SelectStmt
	stmt := parse(t, "SELECT a FROM t1 ORDER BY a LIMIT 10")
	sel, ok := stmt.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt without UNION, got %T", stmt)
	}
	if sel.TableName != "t1" {
		t.Errorf("table name: expected %q, got %q", "t1", sel.TableName)
	}
	if len(sel.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY clause, got %d", len(sel.OrderBy))
	}
	if sel.Limit == nil || *sel.Limit != 10 {
		t.Errorf("expected LIMIT 10")
	}
}

func TestParseUnionParenthesizedLimit(t *testing.T) {
	stmt := parse(t, "(SELECT a FROM t1 LIMIT 2) UNION (SELECT a FROM t2 LIMIT 3)")
	u, ok := stmt.(*ast.UnionStmt)
	if !ok {
		t.Fatalf("expected UnionStmt, got %T", stmt)
	}
	left, ok := u.Left.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected left to be SelectStmt, got %T", u.Left)
	}
	if left.Limit == nil || *left.Limit != 2 {
		t.Errorf("left LIMIT: expected 2, got %v", left.Limit)
	}
	if u.Right.Limit == nil || *u.Right.Limit != 3 {
		t.Errorf("right LIMIT: expected 3, got %v", u.Right.Limit)
	}
}

func TestParseUnionParenthesizedOrderByLimit(t *testing.T) {
	stmt := parse(t, "(SELECT a FROM t1 ORDER BY a LIMIT 2) UNION (SELECT a FROM t2 ORDER BY a LIMIT 3) ORDER BY a LIMIT 10")
	u, ok := stmt.(*ast.UnionStmt)
	if !ok {
		t.Fatalf("expected UnionStmt, got %T", stmt)
	}
	// Left individual SELECT should have its own ORDER BY and LIMIT
	left := u.Left.(*ast.SelectStmt)
	if len(left.OrderBy) != 1 {
		t.Errorf("left ORDER BY: expected 1 clause, got %d", len(left.OrderBy))
	}
	if left.Limit == nil || *left.Limit != 2 {
		t.Errorf("left LIMIT: expected 2")
	}
	// Right individual SELECT
	if len(u.Right.OrderBy) != 1 {
		t.Errorf("right ORDER BY: expected 1 clause, got %d", len(u.Right.OrderBy))
	}
	if u.Right.Limit == nil || *u.Right.Limit != 3 {
		t.Errorf("right LIMIT: expected 3")
	}
	// Overall UNION ORDER BY / LIMIT
	if len(u.OrderBy) != 1 {
		t.Fatalf("union ORDER BY: expected 1 clause, got %d", len(u.OrderBy))
	}
	if u.Limit == nil || *u.Limit != 10 {
		t.Errorf("union LIMIT: expected 10")
	}
}

func TestParseUnionBareLimitError(t *testing.T) {
	// LIMIT before UNION without parentheses should be a syntax error
	l := lexer.New("SELECT a FROM t1 LIMIT 2 UNION SELECT a FROM t2")
	p := New(l)
	_, err := p.Parse()
	if err == nil {
		t.Fatal("expected syntax error for bare LIMIT before UNION, got nil")
	}
}

func TestParseUnionBareOrderByError(t *testing.T) {
	// ORDER BY before UNION without parentheses should be a syntax error
	l := lexer.New("SELECT a FROM t1 ORDER BY a UNION SELECT a FROM t2")
	p := New(l)
	_, err := p.Parse()
	if err == nil {
		t.Fatal("expected syntax error for bare ORDER BY before UNION, got nil")
	}
}

func TestParseParenthesizedSelectOnly(t *testing.T) {
	// Parenthesized SELECT without UNION should also work
	stmt := parse(t, "(SELECT a FROM t1 LIMIT 5)")
	sel, ok := stmt.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", stmt)
	}
	if sel.Limit == nil || *sel.Limit != 5 {
		t.Errorf("expected LIMIT 5")
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

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

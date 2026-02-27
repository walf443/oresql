package parser

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/lexer"
)

func parse(t *testing.T, input string) ast.Statement {
	t.Helper()
	l := lexer.New(input)
	p := New(l)
	stmt, err := p.Parse()
	require.NoError(t, err)
	return stmt
}

func TestParseCreateTable(t *testing.T) {
	stmt := parse(t, "CREATE TABLE users (id INT, name TEXT)")
	require.IsType(t, &ast.CreateTableStmt{}, stmt)
	ct := stmt.(*ast.CreateTableStmt)
	assert.Equal(t, "users", ct.TableName)
	require.Len(t, ct.Columns, 2, "expected 2 columns")
	assert.Equal(t, "id", ct.Columns[0].Name)
	assert.Equal(t, "INT", ct.Columns[0].DataType)
	assert.Equal(t, "name", ct.Columns[1].Name)
	assert.Equal(t, "TEXT", ct.Columns[1].DataType)
}

func TestParseInsert(t *testing.T) {
	stmt := parse(t, "INSERT INTO users VALUES (1, 'alice')")
	require.IsType(t, &ast.InsertStmt{}, stmt)
	ins := stmt.(*ast.InsertStmt)
	assert.Equal(t, "users", ins.TableName)
	require.Len(t, ins.Rows, 1, "expected 1 row")
	require.Len(t, ins.Rows[0], 2, "expected 2 values")
	require.IsType(t, &ast.IntLitExpr{}, ins.Rows[0][0])
	intVal := ins.Rows[0][0].(*ast.IntLitExpr)
	assert.Equal(t, int64(1), intVal.Value)
	require.IsType(t, &ast.StringLitExpr{}, ins.Rows[0][1])
	strVal := ins.Rows[0][1].(*ast.StringLitExpr)
	assert.Equal(t, "alice", strVal.Value)
}

func TestParseInsertMultipleRows(t *testing.T) {
	stmt := parse(t, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")
	require.IsType(t, &ast.InsertStmt{}, stmt)
	ins := stmt.(*ast.InsertStmt)
	assert.Equal(t, "users", ins.TableName)
	require.Len(t, ins.Rows, 3, "expected 3 rows")
	// Check each row has 2 values
	for i, row := range ins.Rows {
		assert.Len(t, row, 2, "row %d", i)
	}
	// Spot check values
	assert.Equal(t, int64(1), ins.Rows[0][0].(*ast.IntLitExpr).Value)
	assert.Equal(t, "bob", ins.Rows[1][1].(*ast.StringLitExpr).Value)
	assert.Equal(t, int64(3), ins.Rows[2][0].(*ast.IntLitExpr).Value)
}

func TestParseSelectStar(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	sel := stmt.(*ast.SelectStmt)
	assert.Equal(t, "users", sel.TableName)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	assert.IsType(t, &ast.StarExpr{}, sel.Columns[0])
	assert.Nil(t, sel.Where)
}

func TestParseSelectColumns(t *testing.T) {
	stmt := parse(t, "SELECT id, name FROM users")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 2, "expected 2 columns")
	col0 := sel.Columns[0].(*ast.IdentExpr)
	col1 := sel.Columns[1].(*ast.IdentExpr)
	assert.Equal(t, "id", col0.Name)
	assert.Equal(t, "name", col1.Name)
}

func TestParseSelectWhere(t *testing.T) {
	stmt := parse(t, "SELECT name FROM users WHERE id = 1 AND name = 'alice'")
	sel := stmt.(*ast.SelectStmt)

	require.NotNil(t, sel.Where)

	require.IsType(t, &ast.LogicalExpr{}, sel.Where)
	logical := sel.Where.(*ast.LogicalExpr)
	assert.Equal(t, "AND", logical.Op)

	// Left: id = 1
	left := logical.Left.(*ast.BinaryExpr)
	assert.Equal(t, "=", left.Op)
	assert.Equal(t, "id", left.Left.(*ast.IdentExpr).Name)
	assert.Equal(t, int64(1), left.Right.(*ast.IntLitExpr).Value)

	// Right: name = 'alice'
	right := logical.Right.(*ast.BinaryExpr)
	assert.Equal(t, "=", right.Op)
	assert.Equal(t, "name", right.Left.(*ast.IdentExpr).Name)
	assert.Equal(t, "alice", right.Right.(*ast.StringLitExpr).Value)
}

func TestParseSelectWhereOr(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users WHERE id = 1 OR id = 2")
	sel := stmt.(*ast.SelectStmt)
	logical := sel.Where.(*ast.LogicalExpr)
	assert.Equal(t, "OR", logical.Op)
}

func TestParseSelectWithSemicolon(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users;")
	require.IsType(t, &ast.SelectStmt{}, stmt)
}

func TestParseSelectQualifiedColumns(t *testing.T) {
	stmt := parse(t, "SELECT users.id, users.name FROM users")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 2, "expected 2 columns")
	col0 := sel.Columns[0].(*ast.IdentExpr)
	assert.Equal(t, "users", col0.Table)
	assert.Equal(t, "id", col0.Name)
	col1 := sel.Columns[1].(*ast.IdentExpr)
	assert.Equal(t, "users", col1.Table)
	assert.Equal(t, "name", col1.Name)
}

func TestParseSelectQualifiedWhere(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users WHERE users.id = 1")
	sel := stmt.(*ast.SelectStmt)
	bin := sel.Where.(*ast.BinaryExpr)
	ident := bin.Left.(*ast.IdentExpr)
	assert.Equal(t, "users", ident.Table)
	assert.Equal(t, "id", ident.Name)
}

func TestParseSelectMixedColumns(t *testing.T) {
	stmt := parse(t, "SELECT users.id, name FROM users")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 2, "expected 2 columns")
	col0 := sel.Columns[0].(*ast.IdentExpr)
	assert.Equal(t, "users", col0.Table)
	assert.Equal(t, "id", col0.Name)
	col1 := sel.Columns[1].(*ast.IdentExpr)
	assert.Equal(t, "", col1.Table)
	assert.Equal(t, "name", col1.Name)
}

func TestParseSelectCountStar(t *testing.T) {
	stmt := parse(t, "SELECT COUNT(*) FROM users")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.CallExpr{}, sel.Columns[0])
	call := sel.Columns[0].(*ast.CallExpr)
	assert.Equal(t, "COUNT", call.Name)
	require.Len(t, call.Args, 1, "expected 1 arg")
	assert.IsType(t, &ast.StarExpr{}, call.Args[0])
}

func TestParseSelectCountStarLowerCase(t *testing.T) {
	stmt := parse(t, "select count(*) from users")
	sel := stmt.(*ast.SelectStmt)
	call := sel.Columns[0].(*ast.CallExpr)
	assert.Equal(t, "COUNT", call.Name)
}

func TestParseSelectLiteral(t *testing.T) {
	stmt := parse(t, "SELECT 1")
	sel := stmt.(*ast.SelectStmt)
	assert.Equal(t, "", sel.TableName)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.IntLitExpr{}, sel.Columns[0])
	lit := sel.Columns[0].(*ast.IntLitExpr)
	assert.Equal(t, int64(1), lit.Value)
}

func TestParseIsNull(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users WHERE name IS NULL")
	sel := stmt.(*ast.SelectStmt)
	require.IsType(t, &ast.IsNullExpr{}, sel.Where)
	isNull := sel.Where.(*ast.IsNullExpr)
	ident := isNull.Expr.(*ast.IdentExpr)
	assert.Equal(t, "name", ident.Name)
	assert.False(t, isNull.Not)
}

func TestParseIsNotNull(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users WHERE name IS NOT NULL")
	sel := stmt.(*ast.SelectStmt)
	require.IsType(t, &ast.IsNullExpr{}, sel.Where)
	isNull := sel.Where.(*ast.IsNullExpr)
	ident := isNull.Expr.(*ast.IdentExpr)
	assert.Equal(t, "name", ident.Name)
	assert.True(t, isNull.Not)
}

func TestParseInsertNull(t *testing.T) {
	stmt := parse(t, "INSERT INTO users VALUES (1, NULL)")
	ins := stmt.(*ast.InsertStmt)
	require.Len(t, ins.Rows[0], 2, "expected 2 values")
	assert.IsType(t, &ast.NullLitExpr{}, ins.Rows[0][1])
}

func TestParseSelectAlias(t *testing.T) {
	stmt := parse(t, "SELECT id AS user_id FROM users")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.AliasExpr{}, sel.Columns[0])
	alias := sel.Columns[0].(*ast.AliasExpr)
	require.IsType(t, &ast.IdentExpr{}, alias.Expr)
	ident := alias.Expr.(*ast.IdentExpr)
	assert.Equal(t, "id", ident.Name)
	assert.Equal(t, "user_id", alias.Alias)
}

func TestParseSelectCountAlias(t *testing.T) {
	stmt := parse(t, "SELECT COUNT(*) AS total FROM users")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.AliasExpr{}, sel.Columns[0])
	alias := sel.Columns[0].(*ast.AliasExpr)
	require.IsType(t, &ast.CallExpr{}, alias.Expr)
	call := alias.Expr.(*ast.CallExpr)
	assert.Equal(t, "COUNT", call.Name)
	assert.Equal(t, "total", alias.Alias)
}

func TestParseSelectLiteralAlias(t *testing.T) {
	stmt := parse(t, "SELECT 1 AS one")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.AliasExpr{}, sel.Columns[0])
	alias := sel.Columns[0].(*ast.AliasExpr)
	require.IsType(t, &ast.IntLitExpr{}, alias.Expr)
	lit := alias.Expr.(*ast.IntLitExpr)
	assert.Equal(t, int64(1), lit.Value)
	assert.Equal(t, "one", alias.Alias)
}

func TestParseSelectQuotedIdent(t *testing.T) {
	stmt := parse(t, "SELECT `count` FROM t")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.IdentExpr{}, sel.Columns[0])
	ident := sel.Columns[0].(*ast.IdentExpr)
	assert.Equal(t, "count", ident.Name)
}

func TestParseCreateTableQuotedIdent(t *testing.T) {
	stmt := parse(t, "CREATE TABLE t (`count` INT)")
	ct := stmt.(*ast.CreateTableStmt)
	require.Len(t, ct.Columns, 1, "expected 1 column")
	assert.Equal(t, "count", ct.Columns[0].Name)
}

func TestParseSelectArithmetic(t *testing.T) {
	stmt := parse(t, "SELECT 1 * 2")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.ArithmeticExpr{}, sel.Columns[0])
	arith := sel.Columns[0].(*ast.ArithmeticExpr)
	assert.Equal(t, "*", arith.Op)
	assert.Equal(t, int64(1), arith.Left.(*ast.IntLitExpr).Value)
	assert.Equal(t, int64(2), arith.Right.(*ast.IntLitExpr).Value)
}

func TestParseSelectArithmeticPrecedence(t *testing.T) {
	// 1 + 2 * 3 should parse as 1 + (2 * 3)
	stmt := parse(t, "SELECT 1 + 2 * 3")
	sel := stmt.(*ast.SelectStmt)
	require.IsType(t, &ast.ArithmeticExpr{}, sel.Columns[0])
	arith := sel.Columns[0].(*ast.ArithmeticExpr)
	assert.Equal(t, "+", arith.Op)
	assert.Equal(t, int64(1), arith.Left.(*ast.IntLitExpr).Value)
	require.IsType(t, &ast.ArithmeticExpr{}, arith.Right)
	right := arith.Right.(*ast.ArithmeticExpr)
	assert.Equal(t, "*", right.Op)
}

func TestParseSelectUnaryMinus(t *testing.T) {
	stmt := parse(t, "SELECT -1")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.ArithmeticExpr{}, sel.Columns[0])
	arith := sel.Columns[0].(*ast.ArithmeticExpr)
	assert.Equal(t, "-", arith.Op)
	assert.Equal(t, int64(0), arith.Left.(*ast.IntLitExpr).Value)
	assert.Equal(t, int64(1), arith.Right.(*ast.IntLitExpr).Value)
}

func TestParseCreateTableNotNull(t *testing.T) {
	stmt := parse(t, "CREATE TABLE t (id INT NOT NULL, name TEXT)")
	require.IsType(t, &ast.CreateTableStmt{}, stmt)
	ct := stmt.(*ast.CreateTableStmt)
	require.Len(t, ct.Columns, 2, "expected 2 columns")
	assert.Equal(t, "id", ct.Columns[0].Name)
	assert.Equal(t, "INT", ct.Columns[0].DataType)
	assert.True(t, ct.Columns[0].NotNull)
	assert.Equal(t, "name", ct.Columns[1].Name)
	assert.Equal(t, "TEXT", ct.Columns[1].DataType)
	assert.False(t, ct.Columns[1].NotNull)
}

func TestParseUpdate(t *testing.T) {
	stmt := parse(t, "UPDATE users SET name = 'bob' WHERE id = 1")
	require.IsType(t, &ast.UpdateStmt{}, stmt)
	upd := stmt.(*ast.UpdateStmt)
	assert.Equal(t, "users", upd.TableName)
	require.Len(t, upd.Sets, 1, "expected 1 SET clauses")
	assert.Equal(t, "name", upd.Sets[0].Column)
	require.IsType(t, &ast.StringLitExpr{}, upd.Sets[0].Value)
	strVal := upd.Sets[0].Value.(*ast.StringLitExpr)
	assert.Equal(t, "bob", strVal.Value)
	require.NotNil(t, upd.Where)
	bin := upd.Where.(*ast.BinaryExpr)
	assert.Equal(t, "id", bin.Left.(*ast.IdentExpr).Name)
	assert.Equal(t, int64(1), bin.Right.(*ast.IntLitExpr).Value)
}

func TestParseUpdateMultipleSet(t *testing.T) {
	stmt := parse(t, "UPDATE users SET name = 'bob', age = 30 WHERE id = 1")
	upd := stmt.(*ast.UpdateStmt)
	require.Len(t, upd.Sets, 2, "expected 2 SET clauses")
	assert.Equal(t, "name", upd.Sets[0].Column)
	assert.Equal(t, "bob", upd.Sets[0].Value.(*ast.StringLitExpr).Value)
	assert.Equal(t, "age", upd.Sets[1].Column)
	assert.Equal(t, int64(30), upd.Sets[1].Value.(*ast.IntLitExpr).Value)
	require.NotNil(t, upd.Where)
}

func TestParseUpdateNoWhere(t *testing.T) {
	stmt := parse(t, "UPDATE users SET name = 'bob'")
	upd := stmt.(*ast.UpdateStmt)
	assert.Equal(t, "users", upd.TableName)
	require.Len(t, upd.Sets, 1, "expected 1 SET clauses")
	assert.Nil(t, upd.Where)
}

func TestParseDelete(t *testing.T) {
	stmt := parse(t, "DELETE FROM users WHERE id = 1")
	require.IsType(t, &ast.DeleteStmt{}, stmt)
	del := stmt.(*ast.DeleteStmt)
	assert.Equal(t, "users", del.TableName)
	require.NotNil(t, del.Where)
	bin := del.Where.(*ast.BinaryExpr)
	assert.Equal(t, "id", bin.Left.(*ast.IdentExpr).Name)
	assert.Equal(t, int64(1), bin.Right.(*ast.IntLitExpr).Value)
}

func TestParseDeleteNoWhere(t *testing.T) {
	stmt := parse(t, "DELETE FROM users")
	del := stmt.(*ast.DeleteStmt)
	assert.Equal(t, "users", del.TableName)
	assert.Nil(t, del.Where)
}

func TestParseUpdateOrderByLimit(t *testing.T) {
	stmt := parse(t, "UPDATE users SET name = 'bob' WHERE id > 1 ORDER BY id LIMIT 2")
	require.IsType(t, &ast.UpdateStmt{}, stmt)
	upd := stmt.(*ast.UpdateStmt)
	assert.Equal(t, "users", upd.TableName)
	require.Len(t, upd.Sets, 1, "expected 1 SET clauses")
	require.NotNil(t, upd.Where)
	require.Len(t, upd.OrderBy, 1, "expected 1 ORDER BY clauses")
	require.IsType(t, &ast.IdentExpr{}, upd.OrderBy[0].Expr)
	ident := upd.OrderBy[0].Expr.(*ast.IdentExpr)
	assert.Equal(t, "id", ident.Name)
	assert.False(t, upd.OrderBy[0].Desc)
	require.NotNil(t, upd.Limit)
	assert.Equal(t, int64(2), *upd.Limit)
}

func TestParseUpdateOrderByOnly(t *testing.T) {
	stmt := parse(t, "UPDATE users SET name = 'bob' ORDER BY id DESC")
	upd := stmt.(*ast.UpdateStmt)
	assert.Nil(t, upd.Where)
	require.Len(t, upd.OrderBy, 1, "expected 1 ORDER BY clauses")
	assert.True(t, upd.OrderBy[0].Desc)
	assert.Nil(t, upd.Limit)
}

func TestParseUpdateLimitOnly(t *testing.T) {
	stmt := parse(t, "UPDATE users SET name = 'bob' LIMIT 5")
	upd := stmt.(*ast.UpdateStmt)
	assert.Nil(t, upd.Where)
	assert.Len(t, upd.OrderBy, 0)
	require.NotNil(t, upd.Limit)
	assert.Equal(t, int64(5), *upd.Limit)
}

func TestParseDeleteOrderByLimit(t *testing.T) {
	stmt := parse(t, "DELETE FROM users WHERE id > 1 ORDER BY id LIMIT 2")
	require.IsType(t, &ast.DeleteStmt{}, stmt)
	del := stmt.(*ast.DeleteStmt)
	assert.Equal(t, "users", del.TableName)
	require.NotNil(t, del.Where)
	require.Len(t, del.OrderBy, 1, "expected 1 ORDER BY clauses")
	require.IsType(t, &ast.IdentExpr{}, del.OrderBy[0].Expr)
	ident := del.OrderBy[0].Expr.(*ast.IdentExpr)
	assert.Equal(t, "id", ident.Name)
	require.NotNil(t, del.Limit)
	assert.Equal(t, int64(2), *del.Limit)
}

func TestParseDeleteLimitOnly(t *testing.T) {
	stmt := parse(t, "DELETE FROM users LIMIT 3")
	del := stmt.(*ast.DeleteStmt)
	assert.Nil(t, del.Where)
	assert.Len(t, del.OrderBy, 0)
	require.NotNil(t, del.Limit)
	assert.Equal(t, int64(3), *del.Limit)
}

func TestParseSelectOrderBySingleColumn(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users ORDER BY id")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.OrderBy, 1, "expected 1 ORDER BY clauses")
	require.IsType(t, &ast.IdentExpr{}, sel.OrderBy[0].Expr)
	ident := sel.OrderBy[0].Expr.(*ast.IdentExpr)
	assert.Equal(t, "id", ident.Name)
	assert.False(t, sel.OrderBy[0].Desc)
}

func TestParseSelectOrderByDesc(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users ORDER BY id DESC")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.OrderBy, 1, "expected 1 ORDER BY clauses")
	assert.True(t, sel.OrderBy[0].Desc)
}

func TestParseSelectOrderByAsc(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users ORDER BY id ASC")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.OrderBy, 1, "expected 1 ORDER BY clauses")
	assert.False(t, sel.OrderBy[0].Desc)
}

func TestParseSelectOrderByMultipleColumns(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users ORDER BY name ASC, id DESC")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.OrderBy, 2, "expected 2 ORDER BY clauses")
	ident0 := sel.OrderBy[0].Expr.(*ast.IdentExpr)
	assert.Equal(t, "name", ident0.Name)
	assert.False(t, sel.OrderBy[0].Desc)
	ident1 := sel.OrderBy[1].Expr.(*ast.IdentExpr)
	assert.Equal(t, "id", ident1.Name)
	assert.True(t, sel.OrderBy[1].Desc)
}

func TestParseSelectWhereOrderBy(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users WHERE id > 1 ORDER BY name")
	sel := stmt.(*ast.SelectStmt)
	require.NotNil(t, sel.Where)
	require.Len(t, sel.OrderBy, 1, "expected 1 ORDER BY clauses")
	ident := sel.OrderBy[0].Expr.(*ast.IdentExpr)
	assert.Equal(t, "name", ident.Name)
}

func TestParseSelectLimitOnly(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users LIMIT 10")
	sel := stmt.(*ast.SelectStmt)
	require.NotNil(t, sel.Limit)
	assert.Equal(t, int64(10), *sel.Limit)
	assert.Nil(t, sel.Offset)
}

func TestParseSelectOffsetOnly(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users OFFSET 5")
	sel := stmt.(*ast.SelectStmt)
	require.NotNil(t, sel.Offset)
	assert.Equal(t, int64(5), *sel.Offset)
	assert.Nil(t, sel.Limit)
}

func TestParseSelectLimitOffset(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users LIMIT 10 OFFSET 5")
	sel := stmt.(*ast.SelectStmt)
	require.NotNil(t, sel.Limit)
	assert.Equal(t, int64(10), *sel.Limit)
	require.NotNil(t, sel.Offset)
	assert.Equal(t, int64(5), *sel.Offset)
}

func TestParseSelectOrderByLimitOffset(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users ORDER BY id ASC LIMIT 2 OFFSET 1")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.OrderBy, 1, "expected 1 ORDER BY clauses")
	require.NotNil(t, sel.Limit)
	assert.Equal(t, int64(2), *sel.Limit)
	require.NotNil(t, sel.Offset)
	assert.Equal(t, int64(1), *sel.Offset)
}

func TestParseTruncateTable(t *testing.T) {
	stmt := parse(t, "TRUNCATE TABLE users")
	require.IsType(t, &ast.TruncateTableStmt{}, stmt)
	tt := stmt.(*ast.TruncateTableStmt)
	assert.Equal(t, "users", tt.TableName)
}

func TestParseDropTable(t *testing.T) {
	stmt := parse(t, "DROP TABLE users")
	require.IsType(t, &ast.DropTableStmt{}, stmt)
	dt := stmt.(*ast.DropTableStmt)
	assert.Equal(t, "users", dt.TableName)
}

func TestParseSelectGroupBy(t *testing.T) {
	stmt := parse(t, "SELECT name, COUNT(*) FROM users GROUP BY name")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 2, "expected 2 columns")
	require.IsType(t, &ast.IdentExpr{}, sel.Columns[0])
	ident := sel.Columns[0].(*ast.IdentExpr)
	assert.Equal(t, "name", ident.Name)
	require.IsType(t, &ast.CallExpr{}, sel.Columns[1])
	call := sel.Columns[1].(*ast.CallExpr)
	assert.Equal(t, "COUNT", call.Name)
	require.Len(t, sel.GroupBy, 1, "expected 1 GROUP BY clauses")
	require.IsType(t, &ast.IdentExpr{}, sel.GroupBy[0])
	gbIdent := sel.GroupBy[0].(*ast.IdentExpr)
	assert.Equal(t, "name", gbIdent.Name)
	assert.Nil(t, sel.Having)
}

func TestParseSelectGroupByHaving(t *testing.T) {
	stmt := parse(t, "SELECT name FROM users GROUP BY name HAVING COUNT(*) > 1")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.GroupBy, 1, "expected 1 GROUP BY clauses")
	require.NotNil(t, sel.Having)
	require.IsType(t, &ast.BinaryExpr{}, sel.Having)
	bin := sel.Having.(*ast.BinaryExpr)
	assert.Equal(t, ">", bin.Op)
	require.IsType(t, &ast.CallExpr{}, bin.Left)
	call := bin.Left.(*ast.CallExpr)
	assert.Equal(t, "COUNT", call.Name)
	assert.Equal(t, int64(1), bin.Right.(*ast.IntLitExpr).Value)
}

func TestParseSelectGroupByMultiple(t *testing.T) {
	stmt := parse(t, "SELECT col1, col2, COUNT(*) FROM t GROUP BY col1, col2")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.GroupBy, 2, "expected 2 GROUP BY clauses")
	assert.Equal(t, "col1", sel.GroupBy[0].(*ast.IdentExpr).Name)
	assert.Equal(t, "col2", sel.GroupBy[1].(*ast.IdentExpr).Name)
}

func TestParseSelectGroupByOrderBy(t *testing.T) {
	stmt := parse(t, "SELECT name, COUNT(*) FROM users GROUP BY name ORDER BY name ASC")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.GroupBy, 1, "expected 1 GROUP BY clauses")
	require.Len(t, sel.OrderBy, 1, "expected 1 ORDER BY clauses")
}

func TestParseSelectSumAmount(t *testing.T) {
	stmt := parse(t, "SELECT SUM(amount) FROM orders")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.CallExpr{}, sel.Columns[0])
	call := sel.Columns[0].(*ast.CallExpr)
	assert.Equal(t, "SUM", call.Name)
	require.Len(t, call.Args, 1, "expected 1 arg")
	require.IsType(t, &ast.IdentExpr{}, call.Args[0])
	ident := call.Args[0].(*ast.IdentExpr)
	assert.Equal(t, "amount", ident.Name)
}

func TestParseSelectMinMaxId(t *testing.T) {
	stmt := parse(t, "SELECT MIN(id), MAX(id) FROM users")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 2, "expected 2 columns")
	require.IsType(t, &ast.CallExpr{}, sel.Columns[0])
	minCall := sel.Columns[0].(*ast.CallExpr)
	assert.Equal(t, "MIN", minCall.Name)
	require.IsType(t, &ast.CallExpr{}, sel.Columns[1])
	maxCall := sel.Columns[1].(*ast.CallExpr)
	assert.Equal(t, "MAX", maxCall.Name)
}

func TestParseSelectAvgAge(t *testing.T) {
	stmt := parse(t, "SELECT AVG(age) FROM users")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.CallExpr{}, sel.Columns[0])
	call := sel.Columns[0].(*ast.CallExpr)
	assert.Equal(t, "AVG", call.Name)
}

func TestParseSelectGroupBySumHaving(t *testing.T) {
	stmt := parse(t, "SELECT name, SUM(amount) FROM orders GROUP BY name HAVING SUM(amount) > 100")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 2, "expected 2 columns")
	require.IsType(t, &ast.CallExpr{}, sel.Columns[1])
	call := sel.Columns[1].(*ast.CallExpr)
	assert.Equal(t, "SUM", call.Name)
	require.NotNil(t, sel.Having)
	require.IsType(t, &ast.BinaryExpr{}, sel.Having)
	bin := sel.Having.(*ast.BinaryExpr)
	require.IsType(t, &ast.CallExpr{}, bin.Left)
	havingCall := bin.Left.(*ast.CallExpr)
	assert.Equal(t, "SUM", havingCall.Name)
}

func TestParseSelectFloatLiteral(t *testing.T) {
	stmt := parse(t, "SELECT 3.14")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.FloatLitExpr{}, sel.Columns[0])
	lit := sel.Columns[0].(*ast.FloatLitExpr)
	assert.Equal(t, 3.14, lit.Value)
}

func TestParseCreateTableFloat(t *testing.T) {
	stmt := parse(t, "CREATE TABLE t (val FLOAT)")
	require.IsType(t, &ast.CreateTableStmt{}, stmt)
	ct := stmt.(*ast.CreateTableStmt)
	require.Len(t, ct.Columns, 1, "expected 1 column")
	assert.Equal(t, "val", ct.Columns[0].Name)
	assert.Equal(t, "FLOAT", ct.Columns[0].DataType)
}

func TestParseSelectDistinct(t *testing.T) {
	stmt := parse(t, "SELECT DISTINCT name FROM users")
	sel := stmt.(*ast.SelectStmt)
	assert.True(t, sel.Distinct)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.IdentExpr{}, sel.Columns[0])
	ident := sel.Columns[0].(*ast.IdentExpr)
	assert.Equal(t, "name", ident.Name)
}

func TestParseSelectWithoutDistinct(t *testing.T) {
	stmt := parse(t, "SELECT name FROM users")
	sel := stmt.(*ast.SelectStmt)
	assert.False(t, sel.Distinct)
}

func TestParseCreateTableWithDefault(t *testing.T) {
	stmt := parse(t, "CREATE TABLE t (id INT DEFAULT 0, name TEXT DEFAULT 'unknown')")
	require.IsType(t, &ast.CreateTableStmt{}, stmt)
	ct := stmt.(*ast.CreateTableStmt)
	require.Len(t, ct.Columns, 2, "expected 2 columns")
	assert.Equal(t, "id", ct.Columns[0].Name)
	assert.Equal(t, "INT", ct.Columns[0].DataType)
	require.NotNil(t, ct.Columns[0].Default)
	require.IsType(t, &ast.IntLitExpr{}, ct.Columns[0].Default)
	intLit := ct.Columns[0].Default.(*ast.IntLitExpr)
	assert.Equal(t, int64(0), intLit.Value)
	require.NotNil(t, ct.Columns[1].Default)
	require.IsType(t, &ast.StringLitExpr{}, ct.Columns[1].Default)
	strLit := ct.Columns[1].Default.(*ast.StringLitExpr)
	assert.Equal(t, "unknown", strLit.Value)
}

func TestParseCreateTableWithDefaultNull(t *testing.T) {
	stmt := parse(t, "CREATE TABLE t (id INT, name TEXT DEFAULT NULL)")
	ct := stmt.(*ast.CreateTableStmt)
	assert.Nil(t, ct.Columns[0].Default)
	require.NotNil(t, ct.Columns[1].Default)
	assert.IsType(t, &ast.NullLitExpr{}, ct.Columns[1].Default)
}

func TestParseCreateTableNotNullDefault(t *testing.T) {
	stmt := parse(t, "CREATE TABLE t (id INT NOT NULL DEFAULT 1)")
	ct := stmt.(*ast.CreateTableStmt)
	assert.True(t, ct.Columns[0].NotNull)
	require.NotNil(t, ct.Columns[0].Default)
	require.IsType(t, &ast.IntLitExpr{}, ct.Columns[0].Default)
	intLit := ct.Columns[0].Default.(*ast.IntLitExpr)
	assert.Equal(t, int64(1), intLit.Value)
}

func TestParseInsertWithColumns(t *testing.T) {
	stmt := parse(t, "INSERT INTO users (id, name) VALUES (1, 'alice')")
	require.IsType(t, &ast.InsertStmt{}, stmt)
	ins := stmt.(*ast.InsertStmt)
	assert.Equal(t, "users", ins.TableName)
	require.Len(t, ins.Columns, 2, "expected 2 columns")
	assert.Equal(t, "id", ins.Columns[0])
	assert.Equal(t, "name", ins.Columns[1])
	require.Len(t, ins.Rows, 1, "expected 1 row")
	require.Len(t, ins.Rows[0], 2, "expected 2 values")
}

func TestParseInsertWithPartialColumns(t *testing.T) {
	stmt := parse(t, "INSERT INTO users (name) VALUES ('alice')")
	ins := stmt.(*ast.InsertStmt)
	require.Len(t, ins.Columns, 1, "expected 1 column")
	assert.Equal(t, "name", ins.Columns[0])
	require.Len(t, ins.Rows[0], 1, "expected 1 value")
}

func TestParseInsertWithoutColumns(t *testing.T) {
	stmt := parse(t, "INSERT INTO users VALUES (1, 'alice')")
	ins := stmt.(*ast.InsertStmt)
	assert.Nil(t, ins.Columns)
}

func TestParseSelectWhereIn(t *testing.T) {
	stmt := parse(t, "SELECT * FROM t WHERE id IN (1, 2, 3)")
	sel := stmt.(*ast.SelectStmt)
	require.IsType(t, &ast.InExpr{}, sel.Where)
	inExpr := sel.Where.(*ast.InExpr)
	assert.False(t, inExpr.Not)
	ident := inExpr.Left.(*ast.IdentExpr)
	assert.Equal(t, "id", ident.Name)
	require.Len(t, inExpr.Values, 3, "expected 3 values")
	assert.Equal(t, int64(1), inExpr.Values[0].(*ast.IntLitExpr).Value)
	assert.Equal(t, int64(2), inExpr.Values[1].(*ast.IntLitExpr).Value)
	assert.Equal(t, int64(3), inExpr.Values[2].(*ast.IntLitExpr).Value)
}

func TestParseSelectWhereNotIn(t *testing.T) {
	stmt := parse(t, "SELECT * FROM t WHERE name NOT IN ('a', 'b')")
	sel := stmt.(*ast.SelectStmt)
	require.IsType(t, &ast.InExpr{}, sel.Where)
	inExpr := sel.Where.(*ast.InExpr)
	assert.True(t, inExpr.Not)
	ident := inExpr.Left.(*ast.IdentExpr)
	assert.Equal(t, "name", ident.Name)
	require.Len(t, inExpr.Values, 2, "expected 2 values")
	assert.Equal(t, "a", inExpr.Values[0].(*ast.StringLitExpr).Value)
	assert.Equal(t, "b", inExpr.Values[1].(*ast.StringLitExpr).Value)
}

func TestParseCreateTablePrimaryKey(t *testing.T) {
	stmt := parse(t, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
	require.IsType(t, &ast.CreateTableStmt{}, stmt)
	ct := stmt.(*ast.CreateTableStmt)
	require.Len(t, ct.Columns, 2, "expected 2 columns")
	assert.Equal(t, "id", ct.Columns[0].Name)
	assert.Equal(t, "INT", ct.Columns[0].DataType)
	assert.True(t, ct.Columns[0].PrimaryKey)
	assert.True(t, ct.Columns[0].NotNull)
	assert.False(t, ct.Columns[1].PrimaryKey)
}

func TestParseCreateTableNotNullPrimaryKey(t *testing.T) {
	stmt := parse(t, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name TEXT)")
	require.IsType(t, &ast.CreateTableStmt{}, stmt)
	ct := stmt.(*ast.CreateTableStmt)
	require.Len(t, ct.Columns, 2, "expected 2 columns")
	assert.True(t, ct.Columns[0].PrimaryKey)
	assert.True(t, ct.Columns[0].NotNull)
}

func TestParseSelectWhereBetween(t *testing.T) {
	stmt := parse(t, "SELECT * FROM t WHERE id BETWEEN 1 AND 10")
	sel := stmt.(*ast.SelectStmt)
	require.IsType(t, &ast.BetweenExpr{}, sel.Where)
	betweenExpr := sel.Where.(*ast.BetweenExpr)
	assert.False(t, betweenExpr.Not)
	ident := betweenExpr.Left.(*ast.IdentExpr)
	assert.Equal(t, "id", ident.Name)
	low := betweenExpr.Low.(*ast.IntLitExpr)
	assert.Equal(t, int64(1), low.Value)
	high := betweenExpr.High.(*ast.IntLitExpr)
	assert.Equal(t, int64(10), high.Value)
}

func TestParseSelectWhereNotBetween(t *testing.T) {
	stmt := parse(t, "SELECT * FROM t WHERE id NOT BETWEEN 1 AND 10")
	sel := stmt.(*ast.SelectStmt)
	require.IsType(t, &ast.BetweenExpr{}, sel.Where)
	betweenExpr := sel.Where.(*ast.BetweenExpr)
	assert.True(t, betweenExpr.Not)
	ident := betweenExpr.Left.(*ast.IdentExpr)
	assert.Equal(t, "id", ident.Name)
	low := betweenExpr.Low.(*ast.IntLitExpr)
	assert.Equal(t, int64(1), low.Value)
	high := betweenExpr.High.(*ast.IntLitExpr)
	assert.Equal(t, int64(10), high.Value)
}

func TestParseSelectWhereLike(t *testing.T) {
	stmt := parse(t, "SELECT * FROM t WHERE name LIKE '%alice%'")
	sel := stmt.(*ast.SelectStmt)
	require.IsType(t, &ast.LikeExpr{}, sel.Where)
	likeExpr := sel.Where.(*ast.LikeExpr)
	assert.False(t, likeExpr.Not)
	ident := likeExpr.Left.(*ast.IdentExpr)
	assert.Equal(t, "name", ident.Name)
	pattern := likeExpr.Pattern.(*ast.StringLitExpr)
	assert.Equal(t, "%alice%", pattern.Value)
}

func TestParseSelectWhereNotLike(t *testing.T) {
	stmt := parse(t, "SELECT * FROM t WHERE name NOT LIKE 'bob%'")
	sel := stmt.(*ast.SelectStmt)
	require.IsType(t, &ast.LikeExpr{}, sel.Where)
	likeExpr := sel.Where.(*ast.LikeExpr)
	assert.True(t, likeExpr.Not)
	ident := likeExpr.Left.(*ast.IdentExpr)
	assert.Equal(t, "name", ident.Name)
	pattern := likeExpr.Pattern.(*ast.StringLitExpr)
	assert.Equal(t, "bob%", pattern.Value)
}

func TestParseCreateIndex(t *testing.T) {
	stmt := parse(t, "CREATE INDEX idx_name ON users(name)")
	require.IsType(t, &ast.CreateIndexStmt{}, stmt)
	ci := stmt.(*ast.CreateIndexStmt)
	assert.Equal(t, "idx_name", ci.IndexName)
	assert.Equal(t, "users", ci.TableName)
	require.Len(t, ci.ColumnNames, 1, "expected 1 item")
	assert.Equal(t, "name", ci.ColumnNames[0])
}

func TestParseCreateCompositeIndex(t *testing.T) {
	stmt := parse(t, "CREATE INDEX idx_name_age ON users(name, age)")
	require.IsType(t, &ast.CreateIndexStmt{}, stmt)
	ci := stmt.(*ast.CreateIndexStmt)
	assert.Equal(t, "idx_name_age", ci.IndexName)
	assert.Equal(t, "users", ci.TableName)
	require.Len(t, ci.ColumnNames, 2, "expected 2 items")
	assert.Equal(t, "name", ci.ColumnNames[0])
	assert.Equal(t, "age", ci.ColumnNames[1])
}

func TestParseDropIndex(t *testing.T) {
	stmt := parse(t, "DROP INDEX idx_name")
	require.IsType(t, &ast.DropIndexStmt{}, stmt)
	di := stmt.(*ast.DropIndexStmt)
	assert.Equal(t, "idx_name", di.IndexName)
}

func TestParseLeftJoin(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Joins, 1, "expected 1 join")
	assert.Equal(t, ast.JoinLeft, sel.Joins[0].JoinType)
	assert.Equal(t, "orders", sel.Joins[0].TableName)
}

func TestParseLeftOuterJoin(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Joins, 1, "expected 1 join")
	assert.Equal(t, ast.JoinLeft, sel.Joins[0].JoinType)
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
			require.IsType(t, &ast.SelectStmt{}, stmt)
			sel := stmt.(*ast.SelectStmt)
			require.Len(t, sel.Joins, 1, "expected 1 join")
			assert.Equal(t, ast.JoinInner, sel.Joins[0].JoinType)
		})
	}
}

func TestParseRightJoin(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Joins, 1, "expected 1 join")
	assert.Equal(t, ast.JoinRight, sel.Joins[0].JoinType)
	assert.Equal(t, "orders", sel.Joins[0].TableName)
}

func TestParseRightOuterJoin(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users RIGHT OUTER JOIN orders ON users.id = orders.user_id")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Joins, 1, "expected 1 join")
	assert.Equal(t, ast.JoinRight, sel.Joins[0].JoinType)
}

func TestParseCrossJoin(t *testing.T) {
	stmt := parse(t, "SELECT * FROM t1 CROSS JOIN t2")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Joins, 1, "expected 1 join")
	assert.Equal(t, ast.JoinCross, sel.Joins[0].JoinType)
	assert.Equal(t, "t2", sel.Joins[0].TableName)
	assert.Nil(t, sel.Joins[0].On)
}

func TestParseCrossJoinWithOnError(t *testing.T) {
	l := lexer.New("SELECT * FROM t1 CROSS JOIN t2 ON t1.id = t2.id")
	p := New(l)
	_, err := p.Parse()
	assert.Error(t, err)
}

func TestParseCaseSearched(t *testing.T) {
	stmt := parse(t, "SELECT CASE WHEN id > 0 THEN 'positive' ELSE 'non-positive' END FROM t")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.CaseExpr{}, sel.Columns[0])
	caseExpr := sel.Columns[0].(*ast.CaseExpr)
	assert.Nil(t, caseExpr.Operand)
	require.Len(t, caseExpr.Whens, 1, "expected 1 WHEN clauses")
	// WHEN condition should be a BinaryExpr (id > 0)
	require.IsType(t, &ast.BinaryExpr{}, caseExpr.Whens[0].When)
	// THEN value
	require.IsType(t, &ast.StringLitExpr{}, caseExpr.Whens[0].Then)
	thenVal := caseExpr.Whens[0].Then.(*ast.StringLitExpr)
	assert.Equal(t, "positive", thenVal.Value)
	// ELSE value
	require.NotNil(t, caseExpr.Else)
	require.IsType(t, &ast.StringLitExpr{}, caseExpr.Else)
	elseVal := caseExpr.Else.(*ast.StringLitExpr)
	assert.Equal(t, "non-positive", elseVal.Value)
}

func TestParseCaseSimple(t *testing.T) {
	stmt := parse(t, "SELECT CASE status WHEN 1 THEN 'active' WHEN 0 THEN 'inactive' END FROM t")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.CaseExpr{}, sel.Columns[0])
	caseExpr := sel.Columns[0].(*ast.CaseExpr)
	// Operand should be an IdentExpr
	require.IsType(t, &ast.IdentExpr{}, caseExpr.Operand)
	operand := caseExpr.Operand.(*ast.IdentExpr)
	assert.Equal(t, "status", operand.Name)
	require.Len(t, caseExpr.Whens, 2, "expected 2 WHEN clauses")
	// First WHEN: 1 THEN 'active'
	require.IsType(t, &ast.IntLitExpr{}, caseExpr.Whens[0].When)
	when1 := caseExpr.Whens[0].When.(*ast.IntLitExpr)
	assert.Equal(t, int64(1), when1.Value)
	require.IsType(t, &ast.StringLitExpr{}, caseExpr.Whens[0].Then)
	then1 := caseExpr.Whens[0].Then.(*ast.StringLitExpr)
	assert.Equal(t, "active", then1.Value)
	// Second WHEN: 0 THEN 'inactive'
	require.IsType(t, &ast.IntLitExpr{}, caseExpr.Whens[1].When)
	when2 := caseExpr.Whens[1].When.(*ast.IntLitExpr)
	assert.Equal(t, int64(0), when2.Value)
	// No ELSE
	assert.Nil(t, caseExpr.Else)
}

func TestParseCaseNoElse(t *testing.T) {
	stmt := parse(t, "SELECT CASE WHEN id = 1 THEN 'one' END FROM t")
	sel := stmt.(*ast.SelectStmt)
	require.IsType(t, &ast.CaseExpr{}, sel.Columns[0])
	caseExpr := sel.Columns[0].(*ast.CaseExpr)
	assert.Nil(t, caseExpr.Operand)
	require.Len(t, caseExpr.Whens, 1, "expected 1 WHEN clauses")
	assert.Nil(t, caseExpr.Else)
}

func TestParseCast(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		targetType string
	}{
		{"CAST AS INT", "SELECT CAST(val AS INT) FROM t", "INT"},
		{"CAST AS TEXT", "SELECT CAST(val AS TEXT) FROM t", "TEXT"},
		{"CAST AS FLOAT", "SELECT CAST(val AS FLOAT) FROM t", "FLOAT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parse(t, tt.input)
			sel := stmt.(*ast.SelectStmt)
			require.Len(t, sel.Columns, 1, "expected 1 column")
			require.IsType(t, &ast.CastExpr{}, sel.Columns[0])
			cast := sel.Columns[0].(*ast.CastExpr)
			assert.Equal(t, tt.targetType, cast.TargetType)
		})
	}
}

func TestParseCoalesce(t *testing.T) {
	stmt := parse(t, "SELECT COALESCE(a, b, c) FROM t")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.CallExpr{}, sel.Columns[0])
	call := sel.Columns[0].(*ast.CallExpr)
	assert.Equal(t, "COALESCE", call.Name)
	require.Len(t, call.Args, 3, "expected 3 args")
	for i, name := range []string{"a", "b", "c"} {
		require.IsType(t, &ast.IdentExpr{}, call.Args[i])
		ident := call.Args[i].(*ast.IdentExpr)
		assert.Equal(t, name, ident.Name)
	}
}

func TestParseNullif(t *testing.T) {
	stmt := parse(t, "SELECT NULLIF(a, b) FROM t")
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.CallExpr{}, sel.Columns[0])
	call := sel.Columns[0].(*ast.CallExpr)
	assert.Equal(t, "NULLIF", call.Name)
	require.Len(t, call.Args, 2, "expected 2 args")
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
			require.Len(t, sel.Columns, 1, "expected 1 column")
			require.IsType(t, &ast.CallExpr{}, sel.Columns[0])
			call := sel.Columns[0].(*ast.CallExpr)
			assert.Equal(t, tt.funcName, call.Name)
			assert.Len(t, call.Args, tt.argCount)
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
			require.Len(t, sel.Columns, 1, "expected 1 column")
			require.IsType(t, &ast.CallExpr{}, sel.Columns[0])
			call := sel.Columns[0].(*ast.CallExpr)
			assert.Equal(t, tt.funcName, call.Name)
			assert.Len(t, call.Args, tt.argCount)
		})
	}
}

func TestParseUnion(t *testing.T) {
	stmt := parse(t, "SELECT a FROM t1 UNION SELECT b FROM t2")
	require.IsType(t, &ast.SetOpStmt{}, stmt)
	u := stmt.(*ast.SetOpStmt)
	assert.False(t, u.All)
	require.IsType(t, &ast.SelectStmt{}, u.Left)
	left := u.Left.(*ast.SelectStmt)
	assert.Equal(t, "t1", left.TableName)
	assert.Equal(t, "t2", u.Right.TableName)
}

func TestParseUnionAll(t *testing.T) {
	stmt := parse(t, "SELECT a FROM t1 UNION ALL SELECT b FROM t2")
	require.IsType(t, &ast.SetOpStmt{}, stmt)
	u := stmt.(*ast.SetOpStmt)
	assert.True(t, u.All)
}

func TestParseUnionChain(t *testing.T) {
	stmt := parse(t, "SELECT a FROM t1 UNION SELECT b FROM t2 UNION SELECT c FROM t3")
	require.IsType(t, &ast.SetOpStmt{}, stmt)
	u := stmt.(*ast.SetOpStmt)
	// Right is t3
	assert.Equal(t, "t3", u.Right.TableName)
	// Left is another SetOpStmt
	require.IsType(t, &ast.SetOpStmt{}, u.Left)
	inner := u.Left.(*ast.SetOpStmt)
	require.IsType(t, &ast.SelectStmt{}, inner.Left)
	innerLeft := inner.Left.(*ast.SelectStmt)
	assert.Equal(t, "t1", innerLeft.TableName)
	assert.Equal(t, "t2", inner.Right.TableName)
}

func TestParseUnionOrderByLimit(t *testing.T) {
	stmt := parse(t, "SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a LIMIT 5 OFFSET 2")
	require.IsType(t, &ast.SetOpStmt{}, stmt)
	u := stmt.(*ast.SetOpStmt)
	require.Len(t, u.OrderBy, 1, "expected 1 ORDER BY clauses")
	ident := u.OrderBy[0].Expr.(*ast.IdentExpr)
	assert.Equal(t, "a", ident.Name)
	require.NotNil(t, u.Limit)
	assert.Equal(t, int64(5), *u.Limit)
	require.NotNil(t, u.Offset)
	assert.Equal(t, int64(2), *u.Offset)
}

func TestParseSelectWithoutUnionUnchanged(t *testing.T) {
	// Without UNION, parseSelect should still return *SelectStmt
	stmt := parse(t, "SELECT a FROM t1 ORDER BY a LIMIT 10")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	sel := stmt.(*ast.SelectStmt)
	assert.Equal(t, "t1", sel.TableName)
	require.Len(t, sel.OrderBy, 1, "expected 1 ORDER BY clauses")
	require.NotNil(t, sel.Limit)
	assert.Equal(t, int64(10), *sel.Limit)
}

func TestParseUnionParenthesizedLimit(t *testing.T) {
	stmt := parse(t, "(SELECT a FROM t1 LIMIT 2) UNION (SELECT a FROM t2 LIMIT 3)")
	require.IsType(t, &ast.SetOpStmt{}, stmt)
	u := stmt.(*ast.SetOpStmt)
	require.IsType(t, &ast.SelectStmt{}, u.Left)
	left := u.Left.(*ast.SelectStmt)
	require.NotNil(t, left.Limit)
	assert.Equal(t, int64(2), *left.Limit)
	require.NotNil(t, u.Right.Limit)
	assert.Equal(t, int64(3), *u.Right.Limit)
}

func TestParseUnionParenthesizedOrderByLimit(t *testing.T) {
	stmt := parse(t, "(SELECT a FROM t1 ORDER BY a LIMIT 2) UNION (SELECT a FROM t2 ORDER BY a LIMIT 3) ORDER BY a LIMIT 10")
	require.IsType(t, &ast.SetOpStmt{}, stmt)
	u := stmt.(*ast.SetOpStmt)
	// Left individual SELECT should have its own ORDER BY and LIMIT
	left := u.Left.(*ast.SelectStmt)
	assert.Len(t, left.OrderBy, 1)
	require.NotNil(t, left.Limit)
	assert.Equal(t, int64(2), *left.Limit)
	// Right individual SELECT
	assert.Len(t, u.Right.OrderBy, 1)
	require.NotNil(t, u.Right.Limit)
	assert.Equal(t, int64(3), *u.Right.Limit)
	// Overall UNION ORDER BY / LIMIT
	require.Len(t, u.OrderBy, 1, "expected 1 ORDER BY clauses")
	require.NotNil(t, u.Limit)
	assert.Equal(t, int64(10), *u.Limit)
}

func TestParseUnionBareLimitError(t *testing.T) {
	// LIMIT before UNION without parentheses should be a syntax error
	l := lexer.New("SELECT a FROM t1 LIMIT 2 UNION SELECT a FROM t2")
	p := New(l)
	_, err := p.Parse()
	assert.Error(t, err)
}

func TestParseUnionBareOrderByError(t *testing.T) {
	// ORDER BY before UNION without parentheses should be a syntax error
	l := lexer.New("SELECT a FROM t1 ORDER BY a UNION SELECT a FROM t2")
	p := New(l)
	_, err := p.Parse()
	assert.Error(t, err)
}

func TestParseParenthesizedSelectOnly(t *testing.T) {
	// Parenthesized SELECT without UNION should also work
	stmt := parse(t, "(SELECT a FROM t1 LIMIT 5)")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	sel := stmt.(*ast.SelectStmt)
	require.NotNil(t, sel.Limit)
	assert.Equal(t, int64(5), *sel.Limit)
}

func TestParseIntersect(t *testing.T) {
	stmt := parse(t, "SELECT a FROM t1 INTERSECT SELECT b FROM t2")
	require.IsType(t, &ast.SetOpStmt{}, stmt)
	u := stmt.(*ast.SetOpStmt)
	assert.Equal(t, ast.SetOpIntersect, u.Op)
	assert.False(t, u.All)
	require.IsType(t, &ast.SelectStmt{}, u.Left)
	left := u.Left.(*ast.SelectStmt)
	assert.Equal(t, "t1", left.TableName)
	assert.Equal(t, "t2", u.Right.TableName)
}

func TestParseIntersectAll(t *testing.T) {
	stmt := parse(t, "SELECT a FROM t1 INTERSECT ALL SELECT b FROM t2")
	require.IsType(t, &ast.SetOpStmt{}, stmt)
	u := stmt.(*ast.SetOpStmt)
	assert.Equal(t, ast.SetOpIntersect, u.Op)
	assert.True(t, u.All)
}

func TestParseExcept(t *testing.T) {
	stmt := parse(t, "SELECT a FROM t1 EXCEPT SELECT b FROM t2")
	require.IsType(t, &ast.SetOpStmt{}, stmt)
	u := stmt.(*ast.SetOpStmt)
	assert.Equal(t, ast.SetOpExcept, u.Op)
	assert.False(t, u.All)
	require.IsType(t, &ast.SelectStmt{}, u.Left)
	left := u.Left.(*ast.SelectStmt)
	assert.Equal(t, "t1", left.TableName)
	assert.Equal(t, "t2", u.Right.TableName)
}

func TestParseExceptAll(t *testing.T) {
	stmt := parse(t, "SELECT a FROM t1 EXCEPT ALL SELECT b FROM t2")
	require.IsType(t, &ast.SetOpStmt{}, stmt)
	u := stmt.(*ast.SetOpStmt)
	assert.Equal(t, ast.SetOpExcept, u.Op)
	assert.True(t, u.All)
}

func TestParseFromSubquery(t *testing.T) {
	stmt := parse(t, "SELECT * FROM (SELECT id FROM t1) AS sub")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	sel := stmt.(*ast.SelectStmt)
	assert.Equal(t, "", sel.TableName)
	require.NotNil(t, sel.FromSubquery)
	assert.Equal(t, "sub", sel.TableAlias)
	require.IsType(t, &ast.SelectStmt{}, sel.FromSubquery)
	inner := sel.FromSubquery.(*ast.SelectStmt)
	assert.Equal(t, "t1", inner.TableName)
}

func TestParseFromSubqueryUnion(t *testing.T) {
	stmt := parse(t, "SELECT * FROM (SELECT id FROM t1 UNION SELECT id FROM t2) AS sub")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	sel := stmt.(*ast.SelectStmt)
	require.NotNil(t, sel.FromSubquery)
	assert.Equal(t, "sub", sel.TableAlias)
	require.IsType(t, &ast.SetOpStmt{}, sel.FromSubquery)
}

func TestParseFromSubqueryNoAlias(t *testing.T) {
	l := lexer.New("SELECT * FROM (SELECT id FROM t1)")
	p := New(l)
	_, err := p.Parse()
	assert.Error(t, err)
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
		assert.Error(t, err, "expected error for %q", input)
	}
}

func TestParseInsertSelect(t *testing.T) {
	stmt := parse(t, "INSERT INTO t1 SELECT id, name FROM t2")
	require.IsType(t, &ast.InsertStmt{}, stmt)
	ins := stmt.(*ast.InsertStmt)
	assert.Equal(t, "t1", ins.TableName)
	assert.Nil(t, ins.Rows)
	require.NotNil(t, ins.Select)
	require.IsType(t, &ast.SelectStmt{}, ins.Select)
	sel := ins.Select.(*ast.SelectStmt)
	assert.Equal(t, "t2", sel.TableName)
}

func TestParseInsertSelectWithColumns(t *testing.T) {
	stmt := parse(t, "INSERT INTO t1 (id) SELECT id FROM t2")
	require.IsType(t, &ast.InsertStmt{}, stmt)
	ins := stmt.(*ast.InsertStmt)
	assert.Equal(t, "t1", ins.TableName)
	require.Len(t, ins.Columns, 1, "expected 1 column")
	assert.Equal(t, "id", ins.Columns[0])
	require.NotNil(t, ins.Select)
}

func TestParseInsertSelectUnion(t *testing.T) {
	stmt := parse(t, "INSERT INTO t1 SELECT id FROM t2 UNION SELECT id FROM t3")
	require.IsType(t, &ast.InsertStmt{}, stmt)
	ins := stmt.(*ast.InsertStmt)
	assert.Equal(t, "t1", ins.TableName)
	require.NotNil(t, ins.Select)
	require.IsType(t, &ast.SetOpStmt{}, ins.Select)
}

func TestParseWindowRowNumber(t *testing.T) {
	stmt := parse(t, "SELECT name, ROW_NUMBER() OVER (ORDER BY id) FROM t")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 2, "expected 2 columns")
	require.IsType(t, &ast.WindowExpr{}, sel.Columns[1])
	win := sel.Columns[1].(*ast.WindowExpr)
	assert.Equal(t, "ROW_NUMBER", win.Name)
	assert.Len(t, win.PartitionBy, 0)
	require.Len(t, win.OrderBy, 1, "expected 1 ORDER BY clauses")
	require.IsType(t, &ast.IdentExpr{}, win.OrderBy[0].Expr)
	ident := win.OrderBy[0].Expr.(*ast.IdentExpr)
	assert.Equal(t, "id", ident.Name)
}

func TestParseWindowWithPartitionBy(t *testing.T) {
	stmt := parse(t, "SELECT RANK() OVER (PARTITION BY dept ORDER BY salary DESC) FROM emp")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.WindowExpr{}, sel.Columns[0])
	win := sel.Columns[0].(*ast.WindowExpr)
	assert.Equal(t, "RANK", win.Name)
	require.Len(t, win.PartitionBy, 1, "expected 1 PARTITION BY clauses")
	require.Len(t, win.OrderBy, 1, "expected 1 ORDER BY clauses")
	assert.True(t, win.OrderBy[0].Desc)
}

func TestParseWindowWithAlias(t *testing.T) {
	stmt := parse(t, "SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.AliasExpr{}, sel.Columns[0])
	alias := sel.Columns[0].(*ast.AliasExpr)
	assert.Equal(t, "rn", alias.Alias)
	require.IsType(t, &ast.WindowExpr{}, alias.Expr)
	win := alias.Expr.(*ast.WindowExpr)
	assert.Equal(t, "ROW_NUMBER", win.Name)
}

func TestParseWindowSumOver(t *testing.T) {
	stmt := parse(t, "SELECT name, SUM(salary) OVER (PARTITION BY dept) FROM emp")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 2, "expected 2 columns")
	require.IsType(t, &ast.WindowExpr{}, sel.Columns[1])
	win := sel.Columns[1].(*ast.WindowExpr)
	assert.Equal(t, "SUM", win.Name)
	require.Len(t, win.Args, 1, "expected 1 arg")
	require.IsType(t, &ast.IdentExpr{}, win.Args[0])
	ident := win.Args[0].(*ast.IdentExpr)
	assert.Equal(t, "salary", ident.Name)
	require.Len(t, win.PartitionBy, 1, "expected 1 PARTITION BY clauses")
	assert.Len(t, win.OrderBy, 0)
}

func TestParseWindowCountStarOver(t *testing.T) {
	stmt := parse(t, "SELECT COUNT(*) OVER () FROM t")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.WindowExpr{}, sel.Columns[0])
	win := sel.Columns[0].(*ast.WindowExpr)
	assert.Equal(t, "COUNT", win.Name)
	require.Len(t, win.Args, 1, "expected 1 arg")
	require.IsType(t, &ast.StarExpr{}, win.Args[0])
}

func TestParseWindowAvgOver(t *testing.T) {
	stmt := parse(t, "SELECT AVG(score) OVER (ORDER BY id) AS avg_score FROM t")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Columns, 1, "expected 1 column")
	require.IsType(t, &ast.AliasExpr{}, sel.Columns[0])
	alias := sel.Columns[0].(*ast.AliasExpr)
	assert.Equal(t, "avg_score", alias.Alias)
	require.IsType(t, &ast.WindowExpr{}, alias.Expr)
	win := alias.Expr.(*ast.WindowExpr)
	assert.Equal(t, "AVG", win.Name)
	require.Len(t, win.Args, 1, "expected 1 arg")
	require.Len(t, win.OrderBy, 1, "expected 1 ORDER BY clauses")
}

func TestParseNamedWindow(t *testing.T) {
	stmt := parse(t, "SELECT name, SUM(salary) OVER w FROM emp WINDOW w AS (PARTITION BY dept ORDER BY id)")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Windows, 1, "expected 1 window definitions")
	w := sel.Windows[0]
	assert.Equal(t, "w", w.Name)
	assert.Len(t, w.PartitionBy, 1)
	assert.Len(t, w.OrderBy, 1)
}

func TestParseNamedWindowMultiple(t *testing.T) {
	stmt := parse(t, "SELECT name, SUM(salary) OVER w1, RANK() OVER w2 FROM emp WINDOW w1 AS (PARTITION BY dept), w2 AS (PARTITION BY dept ORDER BY salary DESC)")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	sel := stmt.(*ast.SelectStmt)
	require.Len(t, sel.Windows, 2, "expected 2 window definitions")
	assert.Equal(t, "w1", sel.Windows[0].Name)
	assert.Len(t, sel.Windows[0].PartitionBy, 1)
	assert.Len(t, sel.Windows[0].OrderBy, 0)
	assert.Equal(t, "w2", sel.Windows[1].Name)
	assert.Len(t, sel.Windows[1].OrderBy, 1)
	assert.True(t, sel.Windows[1].OrderBy[0].Desc)
}

func TestParseOverWindowName(t *testing.T) {
	stmt := parse(t, "SELECT ROW_NUMBER() OVER w, SUM(salary) OVER w FROM emp WINDOW w AS (ORDER BY id)")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	sel := stmt.(*ast.SelectStmt)
	// Check that both columns reference window name "w"
	for i, col := range sel.Columns {
		var winExpr *ast.WindowExpr
		switch e := col.(type) {
		case *ast.WindowExpr:
			winExpr = e
		case *ast.AliasExpr:
			winExpr, _ = e.Expr.(*ast.WindowExpr)
		}
		require.NotNil(t, winExpr, "column %d: expected WindowExpr", i)
		assert.Equal(t, "w", winExpr.WindowName, "column %d", i)
	}
}

func TestParseCreateDatabase(t *testing.T) {
	tests := []struct {
		input string
		name  string
	}{
		{"CREATE DATABASE mydb", "mydb"},
		{"CREATE DATABASE MyDB", "MyDB"},
		{"create database testdb;", "testdb"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt := parse(t, tt.input)
			require.IsType(t, &ast.CreateDatabaseStmt{}, stmt)
			s := stmt.(*ast.CreateDatabaseStmt)
			assert.Equal(t, tt.name, s.DatabaseName)
		})
	}
}

func TestParseDropDatabase(t *testing.T) {
	tests := []struct {
		input string
		name  string
	}{
		{"DROP DATABASE mydb", "mydb"},
		{"drop database testdb;", "testdb"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt := parse(t, tt.input)
			require.IsType(t, &ast.DropDatabaseStmt{}, stmt)
			s := stmt.(*ast.DropDatabaseStmt)
			assert.Equal(t, tt.name, s.DatabaseName)
		})
	}
}

func TestParseUseDatabase(t *testing.T) {
	tests := []struct {
		input string
		name  string
	}{
		{"USE mydb", "mydb"},
		{"use default;", "default"},
		{"USE TestDB", "TestDB"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			stmt := parse(t, tt.input)
			require.IsType(t, &ast.UseDatabaseStmt{}, stmt)
			s := stmt.(*ast.UseDatabaseStmt)
			assert.Equal(t, tt.name, s.DatabaseName)
		})
	}
}

func TestParseShowDatabases(t *testing.T) {
	tests := []string{
		"SHOW DATABASES",
		"show databases;",
	}
	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			stmt := parse(t, input)
			require.IsType(t, &ast.ShowDatabasesStmt{}, stmt)
		})
	}
}

func TestParseCreateDatabaseError(t *testing.T) {
	l := lexer.New("CREATE DATABASE")
	p := New(l)
	_, err := p.Parse()
	require.Error(t, err)
}

func TestParseShowTables(t *testing.T) {
	tests := []string{
		"SHOW TABLES",
		"show tables;",
	}
	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			stmt := parse(t, input)
			require.IsType(t, &ast.ShowTablesStmt{}, stmt)
		})
	}
}

func TestParseUseDatabaseKeywordName(t *testing.T) {
	// Keywords should be usable as database names
	stmt := parse(t, "USE default")
	require.IsType(t, &ast.UseDatabaseStmt{}, stmt)
	s := stmt.(*ast.UseDatabaseStmt)
	assert.Equal(t, "default", s.DatabaseName)
}

func TestParseQualifiedTableSelect(t *testing.T) {
	stmt := parse(t, "SELECT * FROM mydb.users")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	s := stmt.(*ast.SelectStmt)
	assert.Equal(t, "mydb", s.DatabaseName)
	assert.Equal(t, "users", s.TableName)
}

func TestParseQualifiedTableInsert(t *testing.T) {
	stmt := parse(t, "INSERT INTO mydb.users VALUES (1, 'alice')")
	require.IsType(t, &ast.InsertStmt{}, stmt)
	s := stmt.(*ast.InsertStmt)
	assert.Equal(t, "mydb", s.DatabaseName)
	assert.Equal(t, "users", s.TableName)
}

func TestParseQualifiedTableUpdate(t *testing.T) {
	stmt := parse(t, "UPDATE mydb.users SET name = 'bob' WHERE id = 1")
	require.IsType(t, &ast.UpdateStmt{}, stmt)
	s := stmt.(*ast.UpdateStmt)
	assert.Equal(t, "mydb", s.DatabaseName)
	assert.Equal(t, "users", s.TableName)
}

func TestParseQualifiedTableDelete(t *testing.T) {
	stmt := parse(t, "DELETE FROM mydb.users WHERE id = 1")
	require.IsType(t, &ast.DeleteStmt{}, stmt)
	s := stmt.(*ast.DeleteStmt)
	assert.Equal(t, "mydb", s.DatabaseName)
	assert.Equal(t, "users", s.TableName)
}

func TestParseQualifiedTableJoin(t *testing.T) {
	stmt := parse(t, "SELECT * FROM db1.users JOIN db2.orders ON db1.users.id = db2.orders.user_id")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	s := stmt.(*ast.SelectStmt)
	assert.Equal(t, "db1", s.DatabaseName)
	assert.Equal(t, "users", s.TableName)
	require.Len(t, s.Joins, 1)
	assert.Equal(t, "db2", s.Joins[0].DatabaseName)
	assert.Equal(t, "orders", s.Joins[0].TableName)
}

func TestParseQualifiedTableCreateTable(t *testing.T) {
	stmt := parse(t, "CREATE TABLE mydb.users (id INT, name TEXT)")
	require.IsType(t, &ast.CreateTableStmt{}, stmt)
	s := stmt.(*ast.CreateTableStmt)
	assert.Equal(t, "mydb", s.DatabaseName)
	assert.Equal(t, "users", s.TableName)
}

func TestParseQualifiedTableDropTable(t *testing.T) {
	stmt := parse(t, "DROP TABLE mydb.users")
	require.IsType(t, &ast.DropTableStmt{}, stmt)
	s := stmt.(*ast.DropTableStmt)
	assert.Equal(t, "mydb", s.DatabaseName)
	assert.Equal(t, "users", s.TableName)
}

func TestParseQualifiedTableTruncate(t *testing.T) {
	stmt := parse(t, "TRUNCATE TABLE mydb.users")
	require.IsType(t, &ast.TruncateTableStmt{}, stmt)
	s := stmt.(*ast.TruncateTableStmt)
	assert.Equal(t, "mydb", s.DatabaseName)
	assert.Equal(t, "users", s.TableName)
}

func TestParseQualifiedTableCreateIndex(t *testing.T) {
	stmt := parse(t, "CREATE INDEX idx_name ON mydb.users (name)")
	require.IsType(t, &ast.CreateIndexStmt{}, stmt)
	s := stmt.(*ast.CreateIndexStmt)
	assert.Equal(t, "mydb", s.DatabaseName)
	assert.Equal(t, "users", s.TableName)
}

func TestParseQualifiedTableAlterTableAdd(t *testing.T) {
	stmt := parse(t, "ALTER TABLE mydb.users ADD COLUMN age INT")
	require.IsType(t, &ast.AlterTableAddColumnStmt{}, stmt)
	s := stmt.(*ast.AlterTableAddColumnStmt)
	assert.Equal(t, "mydb", s.DatabaseName)
	assert.Equal(t, "users", s.TableName)
}

func TestParseQualifiedTableAlterTableDrop(t *testing.T) {
	stmt := parse(t, "ALTER TABLE mydb.users DROP COLUMN age")
	require.IsType(t, &ast.AlterTableDropColumnStmt{}, stmt)
	s := stmt.(*ast.AlterTableDropColumnStmt)
	assert.Equal(t, "mydb", s.DatabaseName)
	assert.Equal(t, "users", s.TableName)
}

func TestParseUnqualifiedTableStillWorks(t *testing.T) {
	stmt := parse(t, "SELECT * FROM users")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	s := stmt.(*ast.SelectStmt)
	assert.Equal(t, "", s.DatabaseName)
	assert.Equal(t, "users", s.TableName)
}

func TestParseBoolLiteralTrue(t *testing.T) {
	stmt := parse(t, "SELECT * FROM t WHERE TRUE")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	s := stmt.(*ast.SelectStmt)
	require.IsType(t, &ast.BoolLitExpr{}, s.Where)
	assert.Equal(t, true, s.Where.(*ast.BoolLitExpr).Value)
}

func TestParseBoolLiteralFalse(t *testing.T) {
	stmt := parse(t, "SELECT * FROM t WHERE FALSE")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	s := stmt.(*ast.SelectStmt)
	require.IsType(t, &ast.BoolLitExpr{}, s.Where)
	assert.Equal(t, false, s.Where.(*ast.BoolLitExpr).Value)
}

func TestParseBoolLiteralCaseInsensitive(t *testing.T) {
	stmt := parse(t, "SELECT * FROM t WHERE true AND false")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	s := stmt.(*ast.SelectStmt)
	require.IsType(t, &ast.LogicalExpr{}, s.Where)
	l := s.Where.(*ast.LogicalExpr)
	assert.Equal(t, "AND", l.Op)
	require.IsType(t, &ast.BoolLitExpr{}, l.Left)
	assert.Equal(t, true, l.Left.(*ast.BoolLitExpr).Value)
	require.IsType(t, &ast.BoolLitExpr{}, l.Right)
	assert.Equal(t, false, l.Right.(*ast.BoolLitExpr).Value)
}

func TestParseBoolLiteralInSelect(t *testing.T) {
	stmt := parse(t, "SELECT TRUE, FALSE")
	require.IsType(t, &ast.SelectStmt{}, stmt)
	s := stmt.(*ast.SelectStmt)
	require.Len(t, s.Columns, 2)
	require.IsType(t, &ast.BoolLitExpr{}, s.Columns[0])
	assert.Equal(t, true, s.Columns[0].(*ast.BoolLitExpr).Value)
	require.IsType(t, &ast.BoolLitExpr{}, s.Columns[1])
	assert.Equal(t, false, s.Columns[1].(*ast.BoolLitExpr).Value)
}

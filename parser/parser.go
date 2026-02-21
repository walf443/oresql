package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/lexer"
	"github.com/walf443/oresql/token"
)

type Parser struct {
	l         *lexer.Lexer
	curToken  token.Token
	peekToken token.Token
}

func New(l *lexer.Lexer) *Parser {
	p := &Parser{l: l}
	// Read two tokens to fill curToken and peekToken.
	p.nextToken()
	p.nextToken()
	return p
}

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.l.NextToken()
}

// Parse parses a single SQL statement.
func (p *Parser) Parse() (ast.Statement, error) {
	var stmt ast.Statement
	var err error

	switch p.curToken.Type {
	case token.CREATE:
		if p.peekToken.Type == token.INDEX {
			stmt, err = p.parseCreateIndex()
		} else {
			stmt, err = p.parseCreateTable()
		}
	case token.INSERT:
		stmt, err = p.parseInsert()
	case token.SELECT:
		stmt, err = p.parseSelect()
	case token.UPDATE:
		stmt, err = p.parseUpdate()
	case token.DELETE:
		stmt, err = p.parseDelete()
	case token.DROP:
		if p.peekToken.Type == token.INDEX {
			stmt, err = p.parseDropIndex()
		} else {
			stmt, err = p.parseDropTable()
		}
	case token.TRUNCATE:
		stmt, err = p.parseTruncateTable()
	case token.ALTER:
		stmt, err = p.parseAlterTable()
	default:
		return nil, fmt.Errorf("unexpected token %s (%q)", p.curToken.Type, p.curToken.Literal)
	}

	if err != nil {
		return nil, err
	}

	// Consume optional semicolon.
	if p.curToken.Type == token.SEMICOLON {
		p.nextToken()
	}

	return stmt, nil
}

// isIdent returns true if the current token is an identifier (plain or backtick-quoted).
func (p *Parser) isIdent() bool {
	return p.curToken.Type == token.IDENT || p.curToken.Type == token.QUOTED_IDENT
}

func (p *Parser) expectToken(t token.TokenType) error {
	if p.curToken.Type != t {
		return fmt.Errorf("expected %s, got %s (%q)", t, p.curToken.Type, p.curToken.Literal)
	}
	p.nextToken()
	return nil
}

// parseCreateTable parses: CREATE TABLE <name> (<column_def>, ...)
func (p *Parser) parseCreateTable() (*ast.CreateTableStmt, error) {
	if err := p.expectToken(token.CREATE); err != nil {
		return nil, err
	}
	if err := p.expectToken(token.TABLE); err != nil {
		return nil, err
	}

	if !p.isIdent() {
		return nil, fmt.Errorf("expected table name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	tableName := p.curToken.Literal
	p.nextToken()

	if err := p.expectToken(token.LPAREN); err != nil {
		return nil, err
	}

	columns, err := p.parseColumnDefList()
	if err != nil {
		return nil, err
	}

	if err := p.expectToken(token.RPAREN); err != nil {
		return nil, err
	}

	return &ast.CreateTableStmt{
		TableName: tableName,
		Columns:   columns,
	}, nil
}

func (p *Parser) parseColumnDefList() ([]ast.ColumnDef, error) {
	var columns []ast.ColumnDef

	col, err := p.parseColumnDef()
	if err != nil {
		return nil, err
	}
	columns = append(columns, col)

	for p.curToken.Type == token.COMMA {
		p.nextToken() // skip comma
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}

	return columns, nil
}

func (p *Parser) parseColumnDef() (ast.ColumnDef, error) {
	if !p.isIdent() {
		return ast.ColumnDef{}, fmt.Errorf("expected column name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	name := p.curToken.Literal
	p.nextToken()

	if p.curToken.Type != token.INT && p.curToken.Type != token.FLOAT && p.curToken.Type != token.TEXT {
		return ast.ColumnDef{}, fmt.Errorf("expected data type (INT, FLOAT or TEXT), got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	dataType := p.curToken.Type.String()
	p.nextToken()

	notNull := false
	if p.curToken.Type == token.NOT {
		p.nextToken() // skip NOT
		if p.curToken.Type != token.NULL {
			return ast.ColumnDef{}, fmt.Errorf("expected NULL after NOT, got %s (%q)", p.curToken.Type, p.curToken.Literal)
		}
		p.nextToken() // skip NULL
		notNull = true
	}

	primaryKey := false
	if p.curToken.Type == token.PRIMARY {
		p.nextToken() // skip PRIMARY
		if err := p.expectToken(token.KEY); err != nil {
			return ast.ColumnDef{}, err
		}
		primaryKey = true
		notNull = true // PRIMARY KEY implies NOT NULL
	}

	var defaultExpr ast.Expr
	if p.curToken.Type == token.DEFAULT {
		p.nextToken() // skip DEFAULT
		var err error
		defaultExpr, err = p.parsePrimary()
		if err != nil {
			return ast.ColumnDef{}, fmt.Errorf("invalid DEFAULT value: %w", err)
		}
	}

	return ast.ColumnDef{Name: name, DataType: dataType, NotNull: notNull, PrimaryKey: primaryKey, Default: defaultExpr}, nil
}

// parseInsert parses: INSERT INTO <table> [(<columns>)] VALUES (<expr>, ...) [, (<expr>, ...) ...]
func (p *Parser) parseInsert() (*ast.InsertStmt, error) {
	if err := p.expectToken(token.INSERT); err != nil {
		return nil, err
	}
	if err := p.expectToken(token.INTO); err != nil {
		return nil, err
	}

	if !p.isIdent() {
		return nil, fmt.Errorf("expected table name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	tableName := p.curToken.Literal
	p.nextToken()

	var columns []string
	if p.curToken.Type == token.LPAREN {
		p.nextToken() // skip (
		var err error
		columns, err = p.parseIdentList()
		if err != nil {
			return nil, err
		}
		if err := p.expectToken(token.RPAREN); err != nil {
			return nil, err
		}
	}

	if err := p.expectToken(token.VALUES); err != nil {
		return nil, err
	}

	row, err := p.parseValueRow()
	if err != nil {
		return nil, err
	}
	rows := [][]ast.Expr{row}

	for p.curToken.Type == token.COMMA {
		p.nextToken() // skip comma
		row, err := p.parseValueRow()
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}

	return &ast.InsertStmt{
		TableName: tableName,
		Columns:   columns,
		Rows:      rows,
	}, nil
}

// parseIdentList parses: ident [, ident ...]
func (p *Parser) parseIdentList() ([]string, error) {
	var idents []string

	if !p.isIdent() {
		return nil, fmt.Errorf("expected identifier, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	idents = append(idents, p.curToken.Literal)
	p.nextToken()

	for p.curToken.Type == token.COMMA {
		p.nextToken() // skip comma
		if !p.isIdent() {
			return nil, fmt.Errorf("expected identifier, got %s (%q)", p.curToken.Type, p.curToken.Literal)
		}
		idents = append(idents, p.curToken.Literal)
		p.nextToken()
	}

	return idents, nil
}

// parseValueRow parses a single parenthesized value list: (<expr>, ...)
func (p *Parser) parseValueRow() ([]ast.Expr, error) {
	if err := p.expectToken(token.LPAREN); err != nil {
		return nil, err
	}

	values, err := p.parseExprList()
	if err != nil {
		return nil, err
	}

	if err := p.expectToken(token.RPAREN); err != nil {
		return nil, err
	}

	return values, nil
}

func (p *Parser) parseExprList() ([]ast.Expr, error) {
	var exprs []ast.Expr

	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	exprs = append(exprs, expr)

	for p.curToken.Type == token.COMMA {
		p.nextToken() // skip comma
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
	}

	return exprs, nil
}

// parseUpdate parses: UPDATE <table> SET <col> = <expr> [, <col> = <expr> ...] [WHERE <expr>]
func (p *Parser) parseUpdate() (*ast.UpdateStmt, error) {
	if err := p.expectToken(token.UPDATE); err != nil {
		return nil, err
	}

	if !p.isIdent() {
		return nil, fmt.Errorf("expected table name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	tableName := p.curToken.Literal
	p.nextToken()

	if err := p.expectToken(token.SET); err != nil {
		return nil, err
	}

	sets, err := p.parseSetList()
	if err != nil {
		return nil, err
	}

	var where ast.Expr
	if p.curToken.Type == token.WHERE {
		p.nextToken() // skip WHERE
		where, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}

	return &ast.UpdateStmt{
		TableName: tableName,
		Sets:      sets,
		Where:     where,
	}, nil
}

// parseSetList parses: <col> = <expr> [, <col> = <expr> ...]
func (p *Parser) parseSetList() ([]ast.SetClause, error) {
	var sets []ast.SetClause

	set, err := p.parseSetClause()
	if err != nil {
		return nil, err
	}
	sets = append(sets, set)

	for p.curToken.Type == token.COMMA {
		p.nextToken() // skip comma
		set, err := p.parseSetClause()
		if err != nil {
			return nil, err
		}
		sets = append(sets, set)
	}

	return sets, nil
}

// parseSetClause parses: <col> = <expr>
func (p *Parser) parseSetClause() (ast.SetClause, error) {
	if !p.isIdent() {
		return ast.SetClause{}, fmt.Errorf("expected column name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	column := p.curToken.Literal
	p.nextToken()

	if err := p.expectToken(token.EQ); err != nil {
		return ast.SetClause{}, err
	}

	value, err := p.parseExpr()
	if err != nil {
		return ast.SetClause{}, err
	}

	return ast.SetClause{Column: column, Value: value}, nil
}

// parseDelete parses: DELETE FROM <table> [WHERE <expr>]
func (p *Parser) parseDelete() (*ast.DeleteStmt, error) {
	if err := p.expectToken(token.DELETE); err != nil {
		return nil, err
	}
	if err := p.expectToken(token.FROM); err != nil {
		return nil, err
	}

	if !p.isIdent() {
		return nil, fmt.Errorf("expected table name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	tableName := p.curToken.Literal
	p.nextToken()

	var where ast.Expr
	var err error
	if p.curToken.Type == token.WHERE {
		p.nextToken() // skip WHERE
		where, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}

	return &ast.DeleteStmt{
		TableName: tableName,
		Where:     where,
	}, nil
}

// parseDropTable parses: DROP TABLE <name>
func (p *Parser) parseDropTable() (*ast.DropTableStmt, error) {
	if err := p.expectToken(token.DROP); err != nil {
		return nil, err
	}
	if err := p.expectToken(token.TABLE); err != nil {
		return nil, err
	}

	if !p.isIdent() {
		return nil, fmt.Errorf("expected table name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	tableName := p.curToken.Literal
	p.nextToken()

	return &ast.DropTableStmt{TableName: tableName}, nil
}

// parseTruncateTable parses: TRUNCATE TABLE <name>
func (p *Parser) parseTruncateTable() (*ast.TruncateTableStmt, error) {
	if err := p.expectToken(token.TRUNCATE); err != nil {
		return nil, err
	}
	if err := p.expectToken(token.TABLE); err != nil {
		return nil, err
	}

	if !p.isIdent() {
		return nil, fmt.Errorf("expected table name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	tableName := p.curToken.Literal
	p.nextToken()

	return &ast.TruncateTableStmt{TableName: tableName}, nil
}

// parseSelect parses: SELECT <columns> FROM <table> [WHERE <expr>]
func (p *Parser) parseSelect() (*ast.SelectStmt, error) {
	if err := p.expectToken(token.SELECT); err != nil {
		return nil, err
	}

	distinct := false
	if p.curToken.Type == token.DISTINCT {
		distinct = true
		p.nextToken()
	}

	columns, err := p.parseSelectList()
	if err != nil {
		return nil, err
	}

	var tableName string
	var where ast.Expr

	if p.curToken.Type == token.FROM {
		p.nextToken() // skip FROM

		if !p.isIdent() {
			return nil, fmt.Errorf("expected table name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
		}
		tableName = p.curToken.Literal
		p.nextToken()

		if p.curToken.Type == token.WHERE {
			p.nextToken() // skip WHERE
			where, err = p.parseExpr()
			if err != nil {
				return nil, err
			}
		}
	}

	var groupBy []ast.Expr
	if p.curToken.Type == token.GROUP {
		p.nextToken() // skip GROUP
		if err := p.expectToken(token.BY); err != nil {
			return nil, err
		}
		groupBy, err = p.parseGroupByList()
		if err != nil {
			return nil, err
		}
	}

	var having ast.Expr
	if p.curToken.Type == token.HAVING {
		p.nextToken() // skip HAVING
		having, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}

	var orderBy []ast.OrderByClause
	if p.curToken.Type == token.ORDER {
		p.nextToken() // skip ORDER
		if err := p.expectToken(token.BY); err != nil {
			return nil, err
		}
		orderBy, err = p.parseOrderByList()
		if err != nil {
			return nil, err
		}
	}

	var limit *int64
	if p.curToken.Type == token.LIMIT {
		p.nextToken() // skip LIMIT
		if p.curToken.Type != token.INT_LIT {
			return nil, fmt.Errorf("expected integer after LIMIT, got %s (%q)", p.curToken.Type, p.curToken.Literal)
		}
		val, err := strconv.ParseInt(p.curToken.Literal, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer for LIMIT: %s", p.curToken.Literal)
		}
		limit = &val
		p.nextToken()
	}

	var offset *int64
	if p.curToken.Type == token.OFFSET {
		p.nextToken() // skip OFFSET
		if p.curToken.Type != token.INT_LIT {
			return nil, fmt.Errorf("expected integer after OFFSET, got %s (%q)", p.curToken.Type, p.curToken.Literal)
		}
		val, err := strconv.ParseInt(p.curToken.Literal, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer for OFFSET: %s", p.curToken.Literal)
		}
		offset = &val
		p.nextToken()
	}

	return &ast.SelectStmt{
		Distinct:  distinct,
		Columns:   columns,
		TableName: tableName,
		Where:     where,
		GroupBy:   groupBy,
		Having:    having,
		OrderBy:   orderBy,
		Limit:     limit,
		Offset:    offset,
	}, nil
}

// parseGroupByList parses: <expr> [, <expr> ...]
func (p *Parser) parseGroupByList() ([]ast.Expr, error) {
	var exprs []ast.Expr

	expr, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}
	exprs = append(exprs, expr)

	for p.curToken.Type == token.COMMA {
		p.nextToken() // skip comma
		expr, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
	}

	return exprs, nil
}

// parseOrderByList parses: <expr> [ASC|DESC] [, <expr> [ASC|DESC] ...]
func (p *Parser) parseOrderByList() ([]ast.OrderByClause, error) {
	var clauses []ast.OrderByClause

	clause, err := p.parseOrderByItem()
	if err != nil {
		return nil, err
	}
	clauses = append(clauses, clause)

	for p.curToken.Type == token.COMMA {
		p.nextToken() // skip comma
		clause, err := p.parseOrderByItem()
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, clause)
	}

	return clauses, nil
}

// parseOrderByItem parses: <expr> [ASC|DESC]
func (p *Parser) parseOrderByItem() (ast.OrderByClause, error) {
	expr, err := p.parseAdditive()
	if err != nil {
		return ast.OrderByClause{}, err
	}

	desc := false
	if p.curToken.Type == token.ASC {
		p.nextToken() // skip ASC
	} else if p.curToken.Type == token.DESC {
		desc = true
		p.nextToken() // skip DESC
	}

	return ast.OrderByClause{Expr: expr, Desc: desc}, nil
}

func (p *Parser) parseSelectList() ([]ast.Expr, error) {
	if p.curToken.Type == token.ASTERISK {
		p.nextToken()
		return []ast.Expr{&ast.StarExpr{}}, nil
	}

	var columns []ast.Expr

	col, err := p.parseSelectItem()
	if err != nil {
		return nil, err
	}
	columns = append(columns, col)

	for p.curToken.Type == token.COMMA {
		p.nextToken() // skip comma
		col, err := p.parseSelectItem()
		if err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}

	return columns, nil
}

// parseSelectItem parses a single item in a SELECT list: column, table.column, function call, or literal.
// Optionally followed by AS alias.
func (p *Parser) parseSelectItem() (ast.Expr, error) {
	var expr ast.Expr
	var err error

	if p.curToken.Type == token.COUNT || p.curToken.Type == token.SUM || p.curToken.Type == token.AVG || p.curToken.Type == token.MIN || p.curToken.Type == token.MAX {
		expr, err = p.parseCallExpr()
	} else {
		expr, err = p.parseAdditive()
	}
	if err != nil {
		return nil, err
	}

	if p.curToken.Type == token.AS {
		p.nextToken() // skip AS
		if !p.isIdent() {
			return nil, fmt.Errorf("expected alias name after AS, got %s (%q)", p.curToken.Type, p.curToken.Literal)
		}
		alias := p.curToken.Literal
		p.nextToken()
		return &ast.AliasExpr{Expr: expr, Alias: alias}, nil
	}

	return expr, nil
}

// parseCallExpr parses a function call: NAME(args...).
func (p *Parser) parseCallExpr() (ast.Expr, error) {
	name := p.curToken.Literal
	p.nextToken() // skip function name

	if err := p.expectToken(token.LPAREN); err != nil {
		return nil, err
	}

	var args []ast.Expr
	if p.curToken.Type == token.ASTERISK {
		args = append(args, &ast.StarExpr{})
		p.nextToken()
	} else if p.curToken.Type != token.RPAREN {
		exprList, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		args = exprList
	}

	if err := p.expectToken(token.RPAREN); err != nil {
		return nil, err
	}

	return &ast.CallExpr{Name: strings.ToUpper(name), Args: args}, nil
}

// parseColumnIdent parses a column reference: ident or ident.ident
func (p *Parser) parseColumnIdent() (ast.Expr, error) {
	if !p.isIdent() {
		return nil, fmt.Errorf("expected column name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	name := p.curToken.Literal
	p.nextToken()

	if p.curToken.Type == token.DOT {
		p.nextToken() // skip dot
		if !p.isIdent() {
			return nil, fmt.Errorf("expected column name after '.', got %s (%q)", p.curToken.Type, p.curToken.Literal)
		}
		colName := p.curToken.Literal
		p.nextToken()
		return &ast.IdentExpr{Table: name, Name: colName}, nil
	}

	return &ast.IdentExpr{Name: name}, nil
}

// Expression parsing with precedence: OR < AND < comparison < primary

func (p *Parser) parseExpr() (ast.Expr, error) {
	return p.parseOrExpr()
}

func (p *Parser) parseOrExpr() (ast.Expr, error) {
	left, err := p.parseAndExpr()
	if err != nil {
		return nil, err
	}

	for p.curToken.Type == token.OR {
		p.nextToken() // skip OR
		right, err := p.parseAndExpr()
		if err != nil {
			return nil, err
		}
		left = &ast.LogicalExpr{Left: left, Op: "OR", Right: right}
	}

	return left, nil
}

func (p *Parser) parseAndExpr() (ast.Expr, error) {
	left, err := p.parseComparison()
	if err != nil {
		return nil, err
	}

	for p.curToken.Type == token.AND {
		p.nextToken() // skip AND
		right, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		left = &ast.LogicalExpr{Left: left, Op: "AND", Right: right}
	}

	return left, nil
}

func (p *Parser) parseComparison() (ast.Expr, error) {
	left, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}

	// Handle [NOT] IN (expr_list)
	if p.curToken.Type == token.IN {
		p.nextToken() // skip IN
		if err := p.expectToken(token.LPAREN); err != nil {
			return nil, err
		}
		values, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		if err := p.expectToken(token.RPAREN); err != nil {
			return nil, err
		}
		return &ast.InExpr{Left: left, Values: values, Not: false}, nil
	}
	if p.curToken.Type == token.NOT && p.peekToken.Type == token.IN {
		p.nextToken() // skip NOT
		p.nextToken() // skip IN
		if err := p.expectToken(token.LPAREN); err != nil {
			return nil, err
		}
		values, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		if err := p.expectToken(token.RPAREN); err != nil {
			return nil, err
		}
		return &ast.InExpr{Left: left, Values: values, Not: true}, nil
	}

	// Handle [NOT] BETWEEN expr AND expr
	if p.curToken.Type == token.BETWEEN {
		p.nextToken() // skip BETWEEN
		low, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		if err := p.expectToken(token.AND); err != nil {
			return nil, err
		}
		high, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		return &ast.BetweenExpr{Left: left, Low: low, High: high, Not: false}, nil
	}
	if p.curToken.Type == token.NOT && p.peekToken.Type == token.BETWEEN {
		p.nextToken() // skip NOT
		p.nextToken() // skip BETWEEN
		low, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		if err := p.expectToken(token.AND); err != nil {
			return nil, err
		}
		high, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		return &ast.BetweenExpr{Left: left, Low: low, High: high, Not: true}, nil
	}

	// Handle [NOT] LIKE <pattern>
	if p.curToken.Type == token.LIKE {
		p.nextToken() // skip LIKE
		pattern, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		return &ast.LikeExpr{Left: left, Pattern: pattern, Not: false}, nil
	}
	if p.curToken.Type == token.NOT && p.peekToken.Type == token.LIKE {
		p.nextToken() // skip NOT
		p.nextToken() // skip LIKE
		pattern, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		return &ast.LikeExpr{Left: left, Pattern: pattern, Not: true}, nil
	}

	// Handle IS [NOT] NULL
	if p.curToken.Type == token.IS {
		p.nextToken() // skip IS
		not := false
		if p.curToken.Type == token.NOT {
			not = true
			p.nextToken() // skip NOT
		}
		if p.curToken.Type != token.NULL {
			return nil, fmt.Errorf("expected NULL after IS, got %s (%q)", p.curToken.Type, p.curToken.Literal)
		}
		p.nextToken() // skip NULL
		return &ast.IsNullExpr{Expr: left, Not: not}, nil
	}

	op := ""
	switch p.curToken.Type {
	case token.EQ:
		op = "="
	case token.NEQ:
		op = "!="
	case token.LT:
		op = "<"
	case token.GT:
		op = ">"
	case token.LT_EQ:
		op = "<="
	case token.GT_EQ:
		op = ">="
	default:
		return left, nil
	}

	p.nextToken() // skip operator
	right, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}

	return &ast.BinaryExpr{Left: left, Op: op, Right: right}, nil
}

func (p *Parser) parseAdditive() (ast.Expr, error) {
	left, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}

	for p.curToken.Type == token.PLUS || p.curToken.Type == token.MINUS {
		op := p.curToken.Literal
		p.nextToken() // skip operator
		right, err := p.parseMultiplicative()
		if err != nil {
			return nil, err
		}
		left = &ast.ArithmeticExpr{Left: left, Op: op, Right: right}
	}

	return left, nil
}

func (p *Parser) parseMultiplicative() (ast.Expr, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	for p.curToken.Type == token.ASTERISK || p.curToken.Type == token.SLASH {
		op := p.curToken.Literal
		p.nextToken() // skip operator
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		left = &ast.ArithmeticExpr{Left: left, Op: op, Right: right}
	}

	return left, nil
}

func (p *Parser) parsePrimary() (ast.Expr, error) {
	switch p.curToken.Type {
	case token.IDENT, token.QUOTED_IDENT:
		name := p.curToken.Literal
		p.nextToken()
		if p.curToken.Type == token.DOT {
			p.nextToken() // skip dot
			if !p.isIdent() {
				return nil, fmt.Errorf("expected column name after '.', got %s (%q)", p.curToken.Type, p.curToken.Literal)
			}
			colName := p.curToken.Literal
			p.nextToken()
			return &ast.IdentExpr{Table: name, Name: colName}, nil
		}
		return &ast.IdentExpr{Name: name}, nil
	case token.INT_LIT:
		val, err := strconv.ParseInt(p.curToken.Literal, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer: %s", p.curToken.Literal)
		}
		expr := &ast.IntLitExpr{Value: val}
		p.nextToken()
		return expr, nil
	case token.FLOAT_LIT:
		val, err := strconv.ParseFloat(p.curToken.Literal, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float: %s", p.curToken.Literal)
		}
		expr := &ast.FloatLitExpr{Value: val}
		p.nextToken()
		return expr, nil
	case token.STRING_LIT:
		expr := &ast.StringLitExpr{Value: p.curToken.Literal}
		p.nextToken()
		return expr, nil
	case token.NULL:
		p.nextToken()
		return &ast.NullLitExpr{}, nil
	case token.MINUS:
		p.nextToken() // skip -
		operand, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &ast.ArithmeticExpr{Left: &ast.IntLitExpr{Value: 0}, Op: "-", Right: operand}, nil
	case token.PLUS:
		p.nextToken() // skip +
		return p.parsePrimary()
	case token.COUNT, token.SUM, token.AVG, token.MIN, token.MAX:
		return p.parseCallExpr()
	case token.LPAREN:
		p.nextToken() // skip (
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectToken(token.RPAREN); err != nil {
			return nil, err
		}
		return expr, nil
	default:
		return nil, fmt.Errorf("unexpected token in expression: %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
}

// parseCreateIndex parses: CREATE INDEX <name> ON <table>(<column>)
func (p *Parser) parseCreateIndex() (ast.Statement, error) {
	if err := p.expectToken(token.CREATE); err != nil {
		return nil, err
	}
	if err := p.expectToken(token.INDEX); err != nil {
		return nil, err
	}

	if !p.isIdent() {
		return nil, fmt.Errorf("expected index name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	indexName := p.curToken.Literal
	p.nextToken()

	if err := p.expectToken(token.ON); err != nil {
		return nil, err
	}

	if !p.isIdent() {
		return nil, fmt.Errorf("expected table name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	tableName := p.curToken.Literal
	p.nextToken()

	if err := p.expectToken(token.LPAREN); err != nil {
		return nil, err
	}

	columnNames, err := p.parseIdentList()
	if err != nil {
		return nil, err
	}

	if err := p.expectToken(token.RPAREN); err != nil {
		return nil, err
	}

	return &ast.CreateIndexStmt{
		IndexName:   indexName,
		TableName:   tableName,
		ColumnNames: columnNames,
	}, nil
}

// parseDropIndex parses: DROP INDEX <name>
func (p *Parser) parseDropIndex() (ast.Statement, error) {
	if err := p.expectToken(token.DROP); err != nil {
		return nil, err
	}
	if err := p.expectToken(token.INDEX); err != nil {
		return nil, err
	}

	if !p.isIdent() {
		return nil, fmt.Errorf("expected index name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	indexName := p.curToken.Literal
	p.nextToken()

	return &ast.DropIndexStmt{IndexName: indexName}, nil
}

// parseAlterTable parses: ALTER TABLE <name> ADD [COLUMN] <column_def>
//
//	ALTER TABLE <name> DROP [COLUMN] <column_name>
func (p *Parser) parseAlterTable() (ast.Statement, error) {
	if err := p.expectToken(token.ALTER); err != nil {
		return nil, err
	}
	if err := p.expectToken(token.TABLE); err != nil {
		return nil, err
	}

	if !p.isIdent() {
		return nil, fmt.Errorf("expected table name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	tableName := p.curToken.Literal
	p.nextToken()

	switch p.curToken.Type {
	case token.ADD:
		p.nextToken() // skip ADD
		// COLUMN keyword is optional
		if p.curToken.Type == token.COLUMN {
			p.nextToken()
		}
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		return &ast.AlterTableAddColumnStmt{TableName: tableName, Column: col}, nil
	case token.DROP:
		p.nextToken() // skip DROP
		// COLUMN keyword is optional
		if p.curToken.Type == token.COLUMN {
			p.nextToken()
		}
		if !p.isIdent() {
			return nil, fmt.Errorf("expected column name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
		}
		colName := p.curToken.Literal
		p.nextToken()
		return &ast.AlterTableDropColumnStmt{TableName: tableName, ColumnName: colName}, nil
	default:
		return nil, fmt.Errorf("expected ADD or DROP after ALTER TABLE, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
}

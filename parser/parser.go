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
		stmt, err = p.parseCreateTable()
	case token.INSERT:
		stmt, err = p.parseInsert()
	case token.SELECT:
		stmt, err = p.parseSelect()
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

	if p.curToken.Type != token.IDENT {
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
	if p.curToken.Type != token.IDENT {
		return ast.ColumnDef{}, fmt.Errorf("expected column name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	name := p.curToken.Literal
	p.nextToken()

	if p.curToken.Type != token.INT && p.curToken.Type != token.TEXT {
		return ast.ColumnDef{}, fmt.Errorf("expected data type (INT or TEXT), got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	dataType := p.curToken.Type.String()
	p.nextToken()

	return ast.ColumnDef{Name: name, DataType: dataType}, nil
}

// parseInsert parses: INSERT INTO <table> VALUES (<expr>, ...)
func (p *Parser) parseInsert() (*ast.InsertStmt, error) {
	if err := p.expectToken(token.INSERT); err != nil {
		return nil, err
	}
	if err := p.expectToken(token.INTO); err != nil {
		return nil, err
	}

	if p.curToken.Type != token.IDENT {
		return nil, fmt.Errorf("expected table name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	tableName := p.curToken.Literal
	p.nextToken()

	if err := p.expectToken(token.VALUES); err != nil {
		return nil, err
	}
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

	return &ast.InsertStmt{
		TableName: tableName,
		Values:    values,
	}, nil
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

// parseSelect parses: SELECT <columns> FROM <table> [WHERE <expr>]
func (p *Parser) parseSelect() (*ast.SelectStmt, error) {
	if err := p.expectToken(token.SELECT); err != nil {
		return nil, err
	}

	columns, err := p.parseSelectList()
	if err != nil {
		return nil, err
	}

	if err := p.expectToken(token.FROM); err != nil {
		return nil, err
	}

	if p.curToken.Type != token.IDENT {
		return nil, fmt.Errorf("expected table name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	tableName := p.curToken.Literal
	p.nextToken()

	var where ast.Expr
	if p.curToken.Type == token.WHERE {
		p.nextToken() // skip WHERE
		where, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}

	return &ast.SelectStmt{
		Columns:   columns,
		TableName: tableName,
		Where:     where,
	}, nil
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

// parseSelectItem parses a single item in a SELECT list: column, table.column, or function call.
func (p *Parser) parseSelectItem() (ast.Expr, error) {
	if p.curToken.Type == token.COUNT {
		return p.parseCallExpr()
	}
	return p.parseColumnIdent()
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
	if p.curToken.Type != token.IDENT {
		return nil, fmt.Errorf("expected column name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	name := p.curToken.Literal
	p.nextToken()

	if p.curToken.Type == token.DOT {
		p.nextToken() // skip dot
		if p.curToken.Type != token.IDENT {
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
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
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
	right, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	return &ast.BinaryExpr{Left: left, Op: op, Right: right}, nil
}

func (p *Parser) parsePrimary() (ast.Expr, error) {
	switch p.curToken.Type {
	case token.IDENT:
		name := p.curToken.Literal
		p.nextToken()
		if p.curToken.Type == token.DOT {
			p.nextToken() // skip dot
			if p.curToken.Type != token.IDENT {
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
	case token.STRING_LIT:
		expr := &ast.StringLitExpr{Value: p.curToken.Literal}
		p.nextToken()
		return expr, nil
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

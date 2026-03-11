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
		if p.peekToken.Type == token.DATABASE {
			stmt, err = p.parseCreateDatabase()
		} else if p.peekToken.Type == token.INDEX || p.peekToken.Type == token.UNIQUE {
			stmt, err = p.parseCreateIndex()
		} else {
			stmt, err = p.parseCreateTable()
		}
	case token.INSERT:
		stmt, err = p.parseInsert()
	case token.SELECT:
		stmt, err = p.parseSelect()
	case token.LPAREN:
		if p.peekToken.Type == token.SELECT {
			stmt, err = p.parseSelect()
		} else {
			return nil, fmt.Errorf("unexpected token %s (%q)", p.curToken.Type, p.curToken.Literal)
		}
	case token.UPDATE:
		stmt, err = p.parseUpdate()
	case token.DELETE:
		stmt, err = p.parseDelete()
	case token.DROP:
		if p.peekToken.Type == token.DATABASE {
			stmt, err = p.parseDropDatabase()
		} else if p.peekToken.Type == token.INDEX {
			stmt, err = p.parseDropIndex()
		} else {
			stmt, err = p.parseDropTable()
		}
	case token.USE:
		stmt, err = p.parseUseDatabase()
	case token.SHOW:
		if p.peekToken.Type == token.TABLES {
			stmt, err = p.parseShowTables()
		} else {
			stmt, err = p.parseShowDatabases()
		}
	case token.TRUNCATE:
		stmt, err = p.parseTruncateTable()
	case token.ALTER:
		stmt, err = p.parseAlterTable()
	case token.WITH:
		stmt, err = p.parseWith()
	case token.EXPLAIN:
		stmt, err = p.parseExplain()
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

	dbName, tableName, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}

	if err := p.expectToken(token.LPAREN); err != nil {
		return nil, err
	}

	columns, err := p.parseColumnDefList()
	if err != nil {
		return nil, err
	}

	// Parse optional table-level PRIMARY KEY (col1, col2, ...)
	var primaryKey []string
	if p.curToken.Type == token.COMMA && p.peekToken.Type == token.PRIMARY {
		p.nextToken() // skip comma
		p.nextToken() // skip PRIMARY
		if err := p.expectToken(token.KEY); err != nil {
			return nil, err
		}
		if err := p.expectToken(token.LPAREN); err != nil {
			return nil, err
		}
		primaryKey, err = p.parseIdentList()
		if err != nil {
			return nil, err
		}
		if err := p.expectToken(token.RPAREN); err != nil {
			return nil, err
		}
	}

	if err := p.expectToken(token.RPAREN); err != nil {
		return nil, err
	}

	return &ast.CreateTableStmt{
		DatabaseName: dbName,
		TableName:    tableName,
		Columns:      columns,
		PrimaryKey:   primaryKey,
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
		if p.peekToken.Type == token.PRIMARY {
			break
		}
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

	if p.curToken.Type != token.INT && p.curToken.Type != token.FLOAT && p.curToken.Type != token.TEXT && p.curToken.Type != token.JSON && p.curToken.Type != token.JSONB {
		return ast.ColumnDef{}, fmt.Errorf("expected data type (INT, FLOAT, TEXT, JSON or JSONB), got %s (%q)", p.curToken.Type, p.curToken.Literal)
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

	unique := false
	if p.curToken.Type == token.UNIQUE {
		p.nextToken() // skip UNIQUE
		unique = true
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

	return ast.ColumnDef{Name: name, DataType: dataType, NotNull: notNull, Unique: unique, PrimaryKey: primaryKey, Default: defaultExpr}, nil
}

// parseInsert parses: INSERT INTO <table> [(<columns>)] VALUES (<expr>, ...) [, (<expr>, ...) ...]
// or: INSERT INTO <table> [(<columns>)] SELECT ...
func (p *Parser) parseInsert() (*ast.InsertStmt, error) {
	if err := p.expectToken(token.INSERT); err != nil {
		return nil, err
	}
	if err := p.expectToken(token.INTO); err != nil {
		return nil, err
	}

	dbName, tableName, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}

	// Parse optional column list: (col1, col2, ...)
	// Distinguish from (SELECT ...) by peeking after LPAREN
	var columns []string
	if p.curToken.Type == token.LPAREN && p.peekToken.Type != token.SELECT {
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

	// SELECT branch: INSERT INTO t1 SELECT ... or INSERT INTO t1 (SELECT ...)
	if p.curToken.Type == token.SELECT ||
		(p.curToken.Type == token.LPAREN && p.peekToken.Type == token.SELECT) {
		selectStmt, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		return &ast.InsertStmt{
			DatabaseName: dbName,
			TableName:    tableName,
			Columns:      columns,
			Select:       selectStmt,
		}, nil
	}

	// VALUES branch
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
		DatabaseName: dbName,
		TableName:    tableName,
		Columns:      columns,
		Rows:         rows,
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

	dbName, tableName, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}

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

	return &ast.UpdateStmt{
		DatabaseName: dbName,
		TableName:    tableName,
		Sets:         sets,
		Where:        where,
		OrderBy:      orderBy,
		Limit:        limit,
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

	dbName, tableName, err := p.parseTableRef()
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

	return &ast.DeleteStmt{
		DatabaseName: dbName,
		TableName:    tableName,
		Where:        where,
		OrderBy:      orderBy,
		Limit:        limit,
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

	dbName, tableName, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}

	return &ast.DropTableStmt{DatabaseName: dbName, TableName: tableName}, nil
}

// isNameToken returns true if the current token can be used as a name
// (identifier or keyword — allows USE default, CREATE DATABASE test, etc.).
func (p *Parser) isNameToken() bool {
	return p.isIdent() || token.IsKeyword(p.curToken.Type)
}

// parseTableRef parses an optionally database-qualified table name: [db.]table
// Returns (dbName, tableName, error). dbName is empty if not qualified.
// Keywords are allowed as database names when followed by DOT (e.g., default.users).
func (p *Parser) parseTableRef() (string, string, error) {
	// Accept keyword as database name if followed by DOT
	if !p.isIdent() && !(p.isNameToken() && p.peekToken.Type == token.DOT) {
		return "", "", fmt.Errorf("expected table name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	name := p.curToken.Literal
	p.nextToken()

	if p.curToken.Type == token.DOT {
		p.nextToken() // skip DOT
		if !p.isNameToken() {
			return "", "", fmt.Errorf("expected table name after '.', got %s (%q)", p.curToken.Type, p.curToken.Literal)
		}
		tableName := p.curToken.Literal
		p.nextToken()
		return name, tableName, nil
	}

	return "", name, nil
}

// parseCreateDatabase parses: CREATE DATABASE <name>
func (p *Parser) parseCreateDatabase() (*ast.CreateDatabaseStmt, error) {
	if err := p.expectToken(token.CREATE); err != nil {
		return nil, err
	}
	if err := p.expectToken(token.DATABASE); err != nil {
		return nil, err
	}

	if !p.isNameToken() {
		return nil, fmt.Errorf("expected database name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	name := p.curToken.Literal
	p.nextToken()

	return &ast.CreateDatabaseStmt{DatabaseName: name}, nil
}

// parseDropDatabase parses: DROP DATABASE <name>
func (p *Parser) parseDropDatabase() (*ast.DropDatabaseStmt, error) {
	if err := p.expectToken(token.DROP); err != nil {
		return nil, err
	}
	if err := p.expectToken(token.DATABASE); err != nil {
		return nil, err
	}

	if !p.isNameToken() {
		return nil, fmt.Errorf("expected database name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	name := p.curToken.Literal
	p.nextToken()

	return &ast.DropDatabaseStmt{DatabaseName: name}, nil
}

// parseUseDatabase parses: USE <name>
func (p *Parser) parseUseDatabase() (*ast.UseDatabaseStmt, error) {
	if err := p.expectToken(token.USE); err != nil {
		return nil, err
	}

	if !p.isNameToken() {
		return nil, fmt.Errorf("expected database name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	name := p.curToken.Literal
	p.nextToken()

	return &ast.UseDatabaseStmt{DatabaseName: name}, nil
}

// parseShowDatabases parses: SHOW DATABASES
func (p *Parser) parseShowDatabases() (*ast.ShowDatabasesStmt, error) {
	if err := p.expectToken(token.SHOW); err != nil {
		return nil, err
	}
	if err := p.expectToken(token.DATABASES); err != nil {
		return nil, err
	}

	return &ast.ShowDatabasesStmt{}, nil
}

// parseShowTables parses: SHOW TABLES
func (p *Parser) parseShowTables() (*ast.ShowTablesStmt, error) {
	if err := p.expectToken(token.SHOW); err != nil {
		return nil, err
	}
	if err := p.expectToken(token.TABLES); err != nil {
		return nil, err
	}

	return &ast.ShowTablesStmt{}, nil
}

// parseTruncateTable parses: TRUNCATE TABLE <name>
func (p *Parser) parseTruncateTable() (*ast.TruncateTableStmt, error) {
	if err := p.expectToken(token.TRUNCATE); err != nil {
		return nil, err
	}
	if err := p.expectToken(token.TABLE); err != nil {
		return nil, err
	}

	dbName, tableName, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}

	return &ast.TruncateTableStmt{DatabaseName: dbName, TableName: tableName}, nil
}

// parseSelect parses a SELECT statement, potentially followed by UNION/INTERSECT [ALL] chains.
// Returns *ast.SelectStmt when no set operation is present (backward compatible),
// or *ast.SetOpStmt when UNION/INTERSECT is used.
func (p *Parser) parseSelect() (ast.Statement, error) {
	left, err := p.parseSelectTerm()
	if err != nil {
		return nil, err
	}

	var result ast.Statement = left

	// Parse UNION/INTERSECT/EXCEPT [ALL] chains
	for p.curToken.Type == token.UNION || p.curToken.Type == token.INTERSECT || p.curToken.Type == token.EXCEPT {
		op := ast.SetOpUnion
		if p.curToken.Type == token.INTERSECT {
			op = ast.SetOpIntersect
		} else if p.curToken.Type == token.EXCEPT {
			op = ast.SetOpExcept
		}
		p.nextToken() // skip UNION/INTERSECT/EXCEPT
		isAll := false
		if p.curToken.Type == token.ALL {
			isAll = true
			p.nextToken() // skip ALL
		}
		right, err := p.parseSelectTerm()
		if err != nil {
			return nil, err
		}
		result = &ast.SetOpStmt{Left: result, Right: right, Op: op, All: isAll}
	}

	// Parse trailing ORDER BY / LIMIT / OFFSET (applies to entire result)
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

	// Detect invalid: ORDER BY/LIMIT/OFFSET before set operation without parentheses
	if p.curToken.Type == token.UNION || p.curToken.Type == token.INTERSECT || p.curToken.Type == token.EXCEPT {
		return nil, fmt.Errorf("syntax error: use parentheses to apply ORDER BY/LIMIT/OFFSET to individual SELECT in set operation")
	}

	// Attach ORDER BY / LIMIT / OFFSET to the appropriate node
	if u, ok := result.(*ast.SetOpStmt); ok {
		u.OrderBy = orderBy
		u.Limit = limit
		u.Offset = offset
		return u, nil
	}

	// No UNION: attach to SelectStmt (only overwrite if trailing values were parsed)
	sel := result.(*ast.SelectStmt)
	if len(orderBy) > 0 {
		sel.OrderBy = orderBy
	}
	if limit != nil {
		sel.Limit = limit
	}
	if offset != nil {
		sel.Offset = offset
	}
	return sel, nil
}

// parseSelectTerm parses a single SELECT for use in UNION chains.
// Supports both bare SELECT and parenthesized (SELECT ... ORDER BY ... LIMIT ... OFFSET ...).
func (p *Parser) parseSelectTerm() (*ast.SelectStmt, error) {
	if p.curToken.Type == token.LPAREN && p.peekToken.Type == token.SELECT {
		p.nextToken() // skip (
		stmt, err := p.parseSelectCore()
		if err != nil {
			return nil, err
		}
		// Parse ORDER BY / LIMIT / OFFSET inside parentheses
		if p.curToken.Type == token.ORDER {
			p.nextToken() // skip ORDER
			if err := p.expectToken(token.BY); err != nil {
				return nil, err
			}
			orderBy, err := p.parseOrderByList()
			if err != nil {
				return nil, err
			}
			stmt.OrderBy = orderBy
		}
		if p.curToken.Type == token.LIMIT {
			p.nextToken() // skip LIMIT
			if p.curToken.Type != token.INT_LIT {
				return nil, fmt.Errorf("expected integer after LIMIT, got %s (%q)", p.curToken.Type, p.curToken.Literal)
			}
			val, err := strconv.ParseInt(p.curToken.Literal, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid integer for LIMIT: %s", p.curToken.Literal)
			}
			stmt.Limit = &val
			p.nextToken()
		}
		if p.curToken.Type == token.OFFSET {
			p.nextToken() // skip OFFSET
			if p.curToken.Type != token.INT_LIT {
				return nil, fmt.Errorf("expected integer after OFFSET, got %s (%q)", p.curToken.Type, p.curToken.Literal)
			}
			val, err := strconv.ParseInt(p.curToken.Literal, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid integer for OFFSET: %s", p.curToken.Literal)
			}
			stmt.Offset = &val
			p.nextToken()
		}
		if err := p.expectToken(token.RPAREN); err != nil {
			return nil, err
		}
		return stmt, nil
	}
	return p.parseSelectCore()
}

// parseSelectCore parses a single SELECT statement (without trailing ORDER BY/LIMIT/OFFSET
// which are handled by parseSelect for UNION support).
func (p *Parser) parseSelectCore() (*ast.SelectStmt, error) {
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

	var dbName string
	var tableName string
	var tableAlias string
	var fromSubquery ast.Statement
	var jsonTable *ast.JSONTableSource
	var joins []ast.JoinClause
	var where ast.Expr

	if p.curToken.Type == token.FROM {
		p.nextToken() // skip FROM

		if p.curToken.Type == token.LPAREN && p.peekToken.Type == token.SELECT {
			// FROM subquery: (SELECT ...) AS alias
			p.nextToken() // skip (
			subStmt, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			if err := p.expectToken(token.RPAREN); err != nil {
				return nil, err
			}
			tableAlias = p.parseTableAlias()
			if tableAlias == "" {
				return nil, fmt.Errorf("subquery in FROM must have an alias")
			}
			fromSubquery = subStmt
		} else if p.curToken.Type == token.IDENT && strings.ToUpper(p.curToken.Literal) == "JSON_TABLE" {
			// FROM JSON_TABLE(expr, path COLUMNS (...)) AS alias
			jt, err := p.parseJSONTable()
			if err != nil {
				return nil, err
			}
			jsonTable = jt
			tableAlias = jt.Alias
		} else {
			var err error
			dbName, tableName, err = p.parseTableRef()
			if err != nil {
				return nil, err
			}

			tableAlias = p.parseTableAlias()
		}

		// Parse JOIN clauses
		for p.curToken.Type == token.JOIN || p.curToken.Type == token.INNER || p.curToken.Type == token.LEFT || p.curToken.Type == token.RIGHT || p.curToken.Type == token.CROSS {
			joinType := ast.JoinInner
			if p.curToken.Type == token.LEFT {
				joinType = ast.JoinLeft
				p.nextToken() // skip LEFT
				if p.curToken.Type == token.OUTER {
					p.nextToken() // skip OUTER
				}
			} else if p.curToken.Type == token.RIGHT {
				joinType = ast.JoinRight
				p.nextToken() // skip RIGHT
				if p.curToken.Type == token.OUTER {
					p.nextToken() // skip OUTER
				}
			} else if p.curToken.Type == token.CROSS {
				joinType = ast.JoinCross
				p.nextToken() // skip CROSS
			} else if p.curToken.Type == token.INNER {
				p.nextToken() // skip INNER
			}
			if err := p.expectToken(token.JOIN); err != nil {
				return nil, err
			}
			joinDBName, joinTable, err := p.parseTableRef()
			if err != nil {
				return nil, err
			}

			joinAlias := p.parseTableAlias()

			var onExpr ast.Expr
			var usingCols []string
			if joinType == ast.JoinCross {
				if p.curToken.Type == token.ON || p.curToken.Type == token.USING {
					return nil, fmt.Errorf("CROSS JOIN does not support ON/USING clause")
				}
			} else if p.curToken.Type == token.USING {
				p.nextToken() // skip USING
				if err := p.expectToken(token.LPAREN); err != nil {
					return nil, err
				}
				for {
					if !p.isNameToken() {
						return nil, fmt.Errorf("expected column name in USING, got %s", p.curToken.Type)
					}
					usingCols = append(usingCols, p.curToken.Literal)
					p.nextToken()
					if p.curToken.Type != token.COMMA {
						break
					}
					p.nextToken() // skip comma
				}
				if err := p.expectToken(token.RPAREN); err != nil {
					return nil, err
				}
				// Determine left table name/alias
				var leftName string
				if len(joins) > 0 {
					prev := joins[len(joins)-1]
					if prev.TableAlias != "" {
						leftName = prev.TableAlias
					} else {
						leftName = prev.TableName
					}
				} else {
					if tableAlias != "" {
						leftName = tableAlias
					} else {
						leftName = tableName
					}
				}
				rightName := joinAlias
				if rightName == "" {
					rightName = joinTable
				}
				onExpr = buildUsingOnExpr(leftName, rightName, usingCols)
			} else {
				if err := p.expectToken(token.ON); err != nil {
					return nil, err
				}
				var err error
				onExpr, err = p.parseExpr()
				if err != nil {
					return nil, err
				}
			}
			joins = append(joins, ast.JoinClause{
				DatabaseName: joinDBName,
				JoinType:     joinType,
				TableName:    joinTable,
				TableAlias:   joinAlias,
				On:           onExpr,
				Using:        usingCols,
			})
		}

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

	var windows []ast.NamedWindowDef
	if p.curToken.Type == token.WINDOW {
		p.nextToken() // skip WINDOW
		windows, err = p.parseWindowDefList()
		if err != nil {
			return nil, err
		}
	}

	return &ast.SelectStmt{
		Distinct:     distinct,
		Columns:      columns,
		DatabaseName: dbName,
		TableName:    tableName,
		FromSubquery: fromSubquery,
		JSONTable:    jsonTable,
		TableAlias:   tableAlias,
		Joins:        joins,
		Where:        where,
		GroupBy:      groupBy,
		Having:       having,
		Windows:      windows,
	}, nil
}

// parseJSONTable parses: JSON_TABLE(expr, path COLUMNS (col type PATH path, ...)) AS alias
func (p *Parser) parseJSONTable() (*ast.JSONTableSource, error) {
	p.nextToken() // skip JSON_TABLE

	if err := p.expectToken(token.LPAREN); err != nil {
		return nil, err
	}

	// Parse JSON expression
	jsonExpr, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}

	if err := p.expectToken(token.COMMA); err != nil {
		return nil, err
	}

	// Parse row path (string literal)
	if p.curToken.Type != token.STRING_LIT {
		return nil, fmt.Errorf("JSON_TABLE: expected path string, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	rowPath := p.curToken.Literal
	p.nextToken()

	// Expect COLUMNS keyword (COLUMNS is not a token; it comes as IDENT)
	if !(p.curToken.Type == token.IDENT && strings.ToUpper(p.curToken.Literal) == "COLUMNS") {
		return nil, fmt.Errorf("JSON_TABLE: expected COLUMNS, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	p.nextToken() // skip COLUMNS

	if err := p.expectToken(token.LPAREN); err != nil {
		return nil, err
	}

	// Parse column definitions
	var cols []ast.JSONTableColumn
	for {
		col, err := p.parseJSONTableColumn()
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
		if p.curToken.Type != token.COMMA {
			break
		}
		p.nextToken() // skip comma
	}

	if err := p.expectToken(token.RPAREN); err != nil {
		return nil, err
	}

	// Close JSON_TABLE(...)
	if err := p.expectToken(token.RPAREN); err != nil {
		return nil, err
	}

	// Parse alias (required)
	alias := p.parseTableAlias()
	if alias == "" {
		return nil, fmt.Errorf("JSON_TABLE in FROM must have an alias")
	}

	return &ast.JSONTableSource{
		JSONExpr: jsonExpr,
		RowPath:  rowPath,
		Columns:  cols,
		Alias:    alias,
	}, nil
}

// parseJSONTableColumn parses: name TYPE PATH 'path'
func (p *Parser) parseJSONTableColumn() (ast.JSONTableColumn, error) {
	if !p.isNameToken() {
		return ast.JSONTableColumn{}, fmt.Errorf("JSON_TABLE COLUMNS: expected column name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	name := p.curToken.Literal
	p.nextToken()

	// Parse data type
	var dataType string
	switch p.curToken.Type {
	case token.INT:
		dataType = "INT"
	case token.FLOAT:
		dataType = "FLOAT"
	case token.TEXT:
		dataType = "TEXT"
	case token.JSON:
		dataType = "JSON"
	case token.JSONB:
		dataType = "JSONB"
	default:
		return ast.JSONTableColumn{}, fmt.Errorf("JSON_TABLE COLUMNS: expected data type (INT, FLOAT, TEXT, JSON, JSONB), got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	p.nextToken()

	// Expect PATH keyword (not a token, comes as IDENT)
	if !(p.curToken.Type == token.IDENT && strings.ToUpper(p.curToken.Literal) == "PATH") {
		return ast.JSONTableColumn{}, fmt.Errorf("JSON_TABLE COLUMNS: expected PATH, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	p.nextToken() // skip PATH

	// Parse path string
	if p.curToken.Type != token.STRING_LIT {
		return ast.JSONTableColumn{}, fmt.Errorf("JSON_TABLE COLUMNS: expected path string after PATH, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	path := p.curToken.Literal
	p.nextToken()

	return ast.JSONTableColumn{
		Name:     name,
		DataType: dataType,
		Path:     path,
	}, nil
}

// parseWindowDefList parses: name AS (PARTITION BY ... ORDER BY ...) [, name AS (...) ...]
func (p *Parser) parseWindowDefList() ([]ast.NamedWindowDef, error) {
	var defs []ast.NamedWindowDef

	def, err := p.parseWindowDef()
	if err != nil {
		return nil, err
	}
	defs = append(defs, def)

	for p.curToken.Type == token.COMMA {
		p.nextToken() // skip comma
		def, err := p.parseWindowDef()
		if err != nil {
			return nil, err
		}
		defs = append(defs, def)
	}

	return defs, nil
}

// parseWindowDef parses: name AS ([PARTITION BY expr, ...] [ORDER BY expr [ASC|DESC], ...])
func (p *Parser) parseWindowDef() (ast.NamedWindowDef, error) {
	if !p.isIdent() {
		return ast.NamedWindowDef{}, fmt.Errorf("expected window name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	name := p.curToken.Literal
	p.nextToken()

	if err := p.expectToken(token.AS); err != nil {
		return ast.NamedWindowDef{}, err
	}

	if err := p.expectToken(token.LPAREN); err != nil {
		return ast.NamedWindowDef{}, err
	}

	var partitionBy []ast.Expr
	if p.curToken.Type == token.PARTITION {
		p.nextToken() // skip PARTITION
		if err := p.expectToken(token.BY); err != nil {
			return ast.NamedWindowDef{}, err
		}
		var err error
		partitionBy, err = p.parseGroupByList()
		if err != nil {
			return ast.NamedWindowDef{}, err
		}
	}

	var orderBy []ast.OrderByClause
	if p.curToken.Type == token.ORDER {
		p.nextToken() // skip ORDER
		if err := p.expectToken(token.BY); err != nil {
			return ast.NamedWindowDef{}, err
		}
		var err error
		orderBy, err = p.parseOrderByList()
		if err != nil {
			return ast.NamedWindowDef{}, err
		}
	}

	if err := p.expectToken(token.RPAREN); err != nil {
		return ast.NamedWindowDef{}, err
	}

	return ast.NamedWindowDef{
		Name:        name,
		PartitionBy: partitionBy,
		OrderBy:     orderBy,
	}, nil
}

// parseTableAlias parses an optional table alias: [AS] ident
// Returns empty string if no alias is present.
// buildUsingOnExpr constructs an ON expression from USING column names.
// For a single column: leftTable.col = rightTable.col
// For multiple columns: leftTable.col1 = rightTable.col1 AND leftTable.col2 = rightTable.col2
func buildUsingOnExpr(leftTable, rightTable string, columns []string) ast.Expr {
	var exprs []ast.Expr
	for _, col := range columns {
		eq := &ast.BinaryExpr{
			Left:  &ast.IdentExpr{Table: leftTable, Name: col},
			Op:    "=",
			Right: &ast.IdentExpr{Table: rightTable, Name: col},
		}
		exprs = append(exprs, eq)
	}
	if len(exprs) == 1 {
		return exprs[0]
	}
	result := &ast.LogicalExpr{
		Left:  exprs[0],
		Op:    "AND",
		Right: exprs[1],
	}
	for i := 2; i < len(exprs); i++ {
		result = &ast.LogicalExpr{
			Left:  result,
			Op:    "AND",
			Right: exprs[i],
		}
	}
	return result
}

func (p *Parser) parseTableAlias() string {
	if p.curToken.Type == token.AS {
		p.nextToken() // skip AS
		if p.isIdent() {
			alias := p.curToken.Literal
			p.nextToken()
			return alias
		}
		return ""
	}
	// Bare alias (no AS keyword): next token is an identifier that is not a keyword
	if p.isIdent() {
		alias := p.curToken.Literal
		p.nextToken()
		return alias
	}
	return ""
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

	if p.curToken.Type == token.ROW_NUMBER || p.curToken.Type == token.RANK || p.curToken.Type == token.DENSE_RANK {
		expr, err = p.parseWindowExpr()
	} else if p.curToken.Type == token.COUNT || p.curToken.Type == token.SUM || p.curToken.Type == token.AVG || p.curToken.Type == token.MIN || p.curToken.Type == token.MAX || p.curToken.Type == token.COALESCE || p.curToken.Type == token.NULLIF || p.curToken.Type == token.ABS || p.curToken.Type == token.ROUND || p.curToken.Type == token.MOD || p.curToken.Type == token.CEIL || p.curToken.Type == token.FLOOR || p.curToken.Type == token.POWER || p.curToken.Type == token.LENGTH || p.curToken.Type == token.UPPER || p.curToken.Type == token.LOWER || p.curToken.Type == token.SUBSTRING || p.curToken.Type == token.TRIM || p.curToken.Type == token.CONCAT {
		expr, err = p.parseCallExpr()
		if err != nil {
			return nil, err
		}
		// Check if this is an aggregate window function (e.g. SUM(col) OVER (...))
		if p.curToken.Type == token.OVER {
			if call, ok := expr.(*ast.CallExpr); ok {
				expr, err = p.parseWindowOverClause(call)
			}
		}
	} else {
		expr, err = p.parseComparison()
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

	var distinct bool
	if p.curToken.Type == token.DISTINCT {
		distinct = true
		p.nextToken() // consume DISTINCT
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

	return &ast.CallExpr{Name: strings.ToUpper(name), Args: args, Distinct: distinct}, nil
}

// parseCaseExpr parses a CASE expression:
// parseCastExpr parses CAST(expr AS type).
func (p *Parser) parseCastExpr() (ast.Expr, error) {
	if err := p.expectToken(token.CAST); err != nil {
		return nil, err
	}
	if err := p.expectToken(token.LPAREN); err != nil {
		return nil, err
	}
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if err := p.expectToken(token.AS); err != nil {
		return nil, err
	}
	// Parse target type: INT, FLOAT, or TEXT
	var targetType string
	switch p.curToken.Type {
	case token.INT, token.FLOAT, token.TEXT, token.JSON, token.JSONB:
		targetType = p.curToken.Type.String()
		p.nextToken()
	default:
		return nil, fmt.Errorf("expected type name (INT, FLOAT, TEXT, JSON, JSONB) after AS, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
	if err := p.expectToken(token.RPAREN); err != nil {
		return nil, err
	}
	return &ast.CastExpr{Expr: expr, TargetType: targetType}, nil
}

// CASE [operand] WHEN expr THEN expr [WHEN expr THEN expr ...] [ELSE expr] END
func (p *Parser) parseCaseExpr() (ast.Expr, error) {
	if err := p.expectToken(token.CASE); err != nil {
		return nil, err
	}

	var operand ast.Expr
	// If next token is not WHEN, this is a Simple CASE with an operand
	if p.curToken.Type != token.WHEN {
		var err error
		operand, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}

	var whens []ast.CaseWhen
	for p.curToken.Type == token.WHEN {
		p.nextToken() // skip WHEN
		whenExpr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectToken(token.THEN); err != nil {
			return nil, err
		}
		thenExpr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		whens = append(whens, ast.CaseWhen{When: whenExpr, Then: thenExpr})
	}

	if len(whens) == 0 {
		return nil, fmt.Errorf("CASE expression requires at least one WHEN clause")
	}

	var elseExpr ast.Expr
	if p.curToken.Type == token.ELSE {
		p.nextToken() // skip ELSE
		var err error
		elseExpr, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}

	if err := p.expectToken(token.END); err != nil {
		return nil, err
	}

	return &ast.CaseExpr{
		Operand: operand,
		Whens:   whens,
		Else:    elseExpr,
	}, nil
}

// parseExistsExpr parses: EXISTS ( SELECT ... )
func (p *Parser) parseExistsExpr(not bool) (ast.Expr, error) {
	if err := p.expectToken(token.EXISTS); err != nil {
		return nil, err
	}
	if err := p.expectToken(token.LPAREN); err != nil {
		return nil, err
	}
	stmt, err := p.parseSelectCore()
	if err != nil {
		return nil, err
	}
	if err := p.expectToken(token.RPAREN); err != nil {
		return nil, err
	}
	return &ast.ExistsExpr{Subquery: stmt, Not: not}, nil
}

// parseInBody parses the body of an IN expression: ( expr_list | SELECT ... )
func (p *Parser) parseInBody(left ast.Expr, not bool) (ast.Expr, error) {
	if err := p.expectToken(token.LPAREN); err != nil {
		return nil, err
	}
	if p.curToken.Type == token.SELECT {
		stmt, err := p.parseSelectCore()
		if err != nil {
			return nil, err
		}
		if err := p.expectToken(token.RPAREN); err != nil {
			return nil, err
		}
		return &ast.InExpr{Left: left, Subquery: stmt, Not: not}, nil
	}
	values, err := p.parseExprList()
	if err != nil {
		return nil, err
	}
	if err := p.expectToken(token.RPAREN); err != nil {
		return nil, err
	}
	return &ast.InExpr{Left: left, Values: values, Not: not}, nil
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
	left, err := p.parseNotExpr()
	if err != nil {
		return nil, err
	}

	for p.curToken.Type == token.AND {
		p.nextToken() // skip AND
		right, err := p.parseNotExpr()
		if err != nil {
			return nil, err
		}
		left = &ast.LogicalExpr{Left: left, Op: "AND", Right: right}
	}

	return left, nil
}

func (p *Parser) parseNotExpr() (ast.Expr, error) {
	if p.curToken.Type == token.NOT {
		// Check that NOT is not followed by IN/BETWEEN/LIKE (those are handled in parseComparison)
		if p.peekToken.Type != token.IN && p.peekToken.Type != token.BETWEEN && p.peekToken.Type != token.LIKE {
			p.nextToken() // skip NOT
			// NOT EXISTS -> ExistsExpr with Not: true
			if p.curToken.Type == token.EXISTS {
				return p.parseExistsExpr(true)
			}
			expr, err := p.parseNotExpr()
			if err != nil {
				return nil, err
			}
			return &ast.NotExpr{Expr: expr}, nil
		}
	}
	return p.parseComparison()
}

func (p *Parser) parseComparison() (ast.Expr, error) {
	left, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}

	// Handle [NOT] IN (expr_list | SELECT ...)
	if p.curToken.Type == token.IN {
		p.nextToken() // skip IN
		return p.parseInBody(left, false)
	}
	if p.curToken.Type == token.NOT && p.peekToken.Type == token.IN {
		p.nextToken() // skip NOT
		p.nextToken() // skip IN
		return p.parseInBody(left, true)
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

	// Handle @@ (full-text match)
	if p.curToken.Type == token.MATCH_OP {
		p.nextToken() // skip @@
		if p.curToken.Type != token.STRING_LIT {
			return nil, fmt.Errorf("expected string literal after @@, got %s (%q)", p.curToken.Type, p.curToken.Literal)
		}
		pattern := p.curToken.Literal
		p.nextToken()
		return &ast.MatchExpr{Expr: left, Pattern: pattern}, nil
	}

	// Handle IS [NOT] NULL / IS [NOT] JSON
	if p.curToken.Type == token.IS {
		p.nextToken() // skip IS
		not := false
		if p.curToken.Type == token.NOT {
			not = true
			p.nextToken() // skip NOT
		}
		if p.curToken.Type == token.JSON {
			p.nextToken() // skip JSON
			return &ast.IsJSONExpr{Expr: left, Not: not}, nil
		}
		if p.curToken.Type != token.NULL {
			return nil, fmt.Errorf("expected NULL or JSON after IS, got %s (%q)", p.curToken.Type, p.curToken.Literal)
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
	case token.TRUE:
		p.nextToken()
		return &ast.BoolLitExpr{Value: true}, nil
	case token.FALSE:
		p.nextToken()
		return &ast.BoolLitExpr{Value: false}, nil
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
	case token.ROW_NUMBER, token.RANK, token.DENSE_RANK:
		return p.parseWindowExpr()
	case token.COUNT, token.SUM, token.AVG, token.MIN, token.MAX, token.COALESCE, token.NULLIF, token.ABS, token.ROUND, token.MOD, token.CEIL, token.FLOOR, token.POWER, token.LENGTH, token.UPPER, token.LOWER, token.SUBSTRING, token.TRIM, token.CONCAT, token.JSON_OBJECT, token.JSON_ARRAY, token.JSON_VALUE, token.JSON_QUERY, token.JSON_EXISTS:
		expr, err := p.parseCallExpr()
		if err != nil {
			return nil, err
		}
		if p.curToken.Type == token.OVER {
			if call, ok := expr.(*ast.CallExpr); ok {
				return p.parseWindowOverClause(call)
			}
		}
		return expr, nil
	case token.CAST:
		return p.parseCastExpr()
	case token.CASE:
		return p.parseCaseExpr()
	case token.EXISTS:
		return p.parseExistsExpr(false)
	case token.LPAREN:
		p.nextToken() // skip (
		if p.curToken.Type == token.SELECT {
			stmt, err := p.parseSelectCore()
			if err != nil {
				return nil, err
			}
			if err := p.expectToken(token.RPAREN); err != nil {
				return nil, err
			}
			return &ast.ScalarExpr{Subquery: stmt}, nil
		}
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

// parseWindowExpr parses: ROW_NUMBER() OVER ([PARTITION BY expr, ...] [ORDER BY expr [ASC|DESC], ...])
// or: ROW_NUMBER() OVER window_name
func (p *Parser) parseWindowExpr() (ast.Expr, error) {
	name := strings.ToUpper(p.curToken.Literal)
	p.nextToken() // skip function name

	if err := p.expectToken(token.LPAREN); err != nil {
		return nil, err
	}
	if err := p.expectToken(token.RPAREN); err != nil {
		return nil, err
	}

	if p.curToken.Type != token.OVER {
		return nil, fmt.Errorf("expected OVER after %s(), got %s (%q)", name, p.curToken.Type, p.curToken.Literal)
	}
	p.nextToken() // skip OVER

	// OVER window_name (named window reference)
	if p.isIdent() {
		windowName := p.curToken.Literal
		p.nextToken()
		return &ast.WindowExpr{
			Name:       name,
			WindowName: windowName,
		}, nil
	}

	if err := p.expectToken(token.LPAREN); err != nil {
		return nil, err
	}

	var partitionBy []ast.Expr
	if p.curToken.Type == token.PARTITION {
		p.nextToken() // skip PARTITION
		if err := p.expectToken(token.BY); err != nil {
			return nil, err
		}
		var err error
		partitionBy, err = p.parseGroupByList()
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
		var err error
		orderBy, err = p.parseOrderByList()
		if err != nil {
			return nil, err
		}
	}

	if err := p.expectToken(token.RPAREN); err != nil {
		return nil, err
	}

	return &ast.WindowExpr{
		Name:        name,
		PartitionBy: partitionBy,
		OrderBy:     orderBy,
	}, nil
}

// parseWindowOverClause parses: OVER ([PARTITION BY expr, ...] [ORDER BY expr [ASC|DESC], ...])
// or: OVER window_name
// after an aggregate function call has already been parsed as a CallExpr.
func (p *Parser) parseWindowOverClause(call *ast.CallExpr) (ast.Expr, error) {
	p.nextToken() // skip OVER

	// OVER window_name (named window reference)
	if p.isIdent() {
		windowName := p.curToken.Literal
		p.nextToken()
		return &ast.WindowExpr{
			Name:       call.Name,
			Args:       call.Args,
			WindowName: windowName,
		}, nil
	}

	if err := p.expectToken(token.LPAREN); err != nil {
		return nil, err
	}

	var partitionBy []ast.Expr
	if p.curToken.Type == token.PARTITION {
		p.nextToken() // skip PARTITION
		if err := p.expectToken(token.BY); err != nil {
			return nil, err
		}
		var err error
		partitionBy, err = p.parseGroupByList()
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
		var err error
		orderBy, err = p.parseOrderByList()
		if err != nil {
			return nil, err
		}
	}

	if err := p.expectToken(token.RPAREN); err != nil {
		return nil, err
	}

	return &ast.WindowExpr{
		Name:        call.Name,
		Args:        call.Args,
		PartitionBy: partitionBy,
		OrderBy:     orderBy,
	}, nil
}

// parseCreateIndex parses: CREATE [UNIQUE] INDEX <name> ON <table>(<column>)
func (p *Parser) parseCreateIndex() (ast.Statement, error) {
	if err := p.expectToken(token.CREATE); err != nil {
		return nil, err
	}
	unique := false
	if p.curToken.Type == token.UNIQUE {
		p.nextToken() // skip UNIQUE
		unique = true
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

	dbName, tableName, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}

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

	// Optional: USING GIN | USING BTREE
	indexMethod := ""
	if p.curToken.Type == token.USING {
		p.nextToken() // skip USING
		if p.curToken.Type == token.GIN {
			indexMethod = "GIN"
			p.nextToken()
		} else if p.isIdent() && strings.ToUpper(p.curToken.Literal) == "BTREE" {
			indexMethod = "BTREE"
			p.nextToken()
		} else {
			return nil, fmt.Errorf("expected GIN or BTREE after USING, got %s (%q)", p.curToken.Type, p.curToken.Literal)
		}
	}

	// Optional: WITH (tokenizer = 'bigram')
	tokenizer := ""
	if p.curToken.Type == token.WITH {
		p.nextToken() // skip WITH
		if err := p.expectToken(token.LPAREN); err != nil {
			return nil, err
		}
		if !p.isIdent() || strings.ToUpper(p.curToken.Literal) != "TOKENIZER" {
			return nil, fmt.Errorf("expected TOKENIZER in WITH clause, got %q", p.curToken.Literal)
		}
		p.nextToken() // skip TOKENIZER
		if err := p.expectToken(token.EQ); err != nil {
			return nil, err
		}
		if p.curToken.Type != token.STRING_LIT {
			return nil, fmt.Errorf("expected string value for tokenizer, got %s", p.curToken.Type)
		}
		tokenizer = p.curToken.Literal
		p.nextToken() // skip value
		if err := p.expectToken(token.RPAREN); err != nil {
			return nil, err
		}
	}

	return &ast.CreateIndexStmt{
		DatabaseName: dbName,
		IndexName:    indexName,
		TableName:    tableName,
		ColumnNames:  columnNames,
		Unique:       unique,
		IndexMethod:  indexMethod,
		Tokenizer:    tokenizer,
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

	dbName, tableName, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}

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
		return &ast.AlterTableAddColumnStmt{DatabaseName: dbName, TableName: tableName, Column: col}, nil
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
		return &ast.AlterTableDropColumnStmt{DatabaseName: dbName, TableName: tableName, ColumnName: colName}, nil
	default:
		return nil, fmt.Errorf("expected ADD or DROP after ALTER TABLE, got %s (%q)", p.curToken.Type, p.curToken.Literal)
	}
}

// parseWith parses WITH name AS (query) [, name AS (query)] ... SELECT ...
func (p *Parser) parseExplain() (ast.Statement, error) {
	p.nextToken() // consume EXPLAIN

	inner, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("EXPLAIN: %w", err)
	}
	return &ast.ExplainStmt{Statement: inner}, nil
}

func (p *Parser) parseWith() (ast.Statement, error) {
	p.nextToken() // consume WITH

	// Check for RECURSIVE keyword
	recursive := false
	if p.curToken.Type == token.RECURSIVE {
		recursive = true
		p.nextToken() // consume RECURSIVE
	}

	var ctes []ast.CTEDef
	for {
		// CTE name
		if !p.isNameToken() {
			return nil, fmt.Errorf("expected CTE name, got %s (%q)", p.curToken.Type, p.curToken.Literal)
		}
		cteName := p.curToken.Literal
		p.nextToken()

		// AS
		if err := p.expectToken(token.AS); err != nil {
			return nil, fmt.Errorf("expected AS after CTE name %q, got %s (%q)", cteName, p.curToken.Type, p.curToken.Literal)
		}

		// ( query )
		if p.curToken.Type != token.LPAREN {
			return nil, fmt.Errorf("expected '(' after AS in CTE %q, got %s (%q)", cteName, p.curToken.Type, p.curToken.Literal)
		}
		p.nextToken() // consume (

		cteQuery, err := p.parseSelect()
		if err != nil {
			return nil, fmt.Errorf("error parsing CTE %q query: %w", cteName, err)
		}

		if err := p.expectToken(token.RPAREN); err != nil {
			return nil, fmt.Errorf("expected ')' after CTE %q query, got %s (%q)", cteName, p.curToken.Type, p.curToken.Literal)
		}

		ctes = append(ctes, ast.CTEDef{Name: cteName, Query: cteQuery, Recursive: recursive})

		// Check for comma (more CTEs) or break
		if p.curToken.Type != token.COMMA {
			break
		}
		p.nextToken() // consume ,
	}

	// Parse the body SELECT
	body, err := p.parseSelect()
	if err != nil {
		return nil, fmt.Errorf("error parsing WITH body: %w", err)
	}

	return &ast.WithStmt{CTEs: ctes, Query: body}, nil
}

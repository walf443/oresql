package lexer

import (
	"testing"

	"github.com/walf443/oresql/token"
)

func TestCreateTable(t *testing.T) {
	input := `CREATE TABLE users (id INT, name TEXT);`
	expected := []token.Token{
		{token.CREATE, "CREATE"},
		{token.TABLE, "TABLE"},
		{token.IDENT, "users"},
		{token.LPAREN, "("},
		{token.IDENT, "id"},
		{token.INT, "INT"},
		{token.COMMA, ","},
		{token.IDENT, "name"},
		{token.TEXT, "TEXT"},
		{token.RPAREN, ")"},
		{token.SEMICOLON, ";"},
		{token.EOF, ""},
	}
	testTokens(t, input, expected)
}

func TestInsert(t *testing.T) {
	input := `INSERT INTO users VALUES (1, 'alice');`
	expected := []token.Token{
		{token.INSERT, "INSERT"},
		{token.INTO, "INTO"},
		{token.IDENT, "users"},
		{token.VALUES, "VALUES"},
		{token.LPAREN, "("},
		{token.INT_LIT, "1"},
		{token.COMMA, ","},
		{token.STRING_LIT, "alice"},
		{token.RPAREN, ")"},
		{token.SEMICOLON, ";"},
		{token.EOF, ""},
	}
	testTokens(t, input, expected)
}

func TestSelect(t *testing.T) {
	input := `SELECT * FROM users WHERE id = 1;`
	expected := []token.Token{
		{token.SELECT, "SELECT"},
		{token.ASTERISK, "*"},
		{token.FROM, "FROM"},
		{token.IDENT, "users"},
		{token.WHERE, "WHERE"},
		{token.IDENT, "id"},
		{token.EQ, "="},
		{token.INT_LIT, "1"},
		{token.SEMICOLON, ";"},
		{token.EOF, ""},
	}
	testTokens(t, input, expected)
}

func TestSelectWithConditions(t *testing.T) {
	input := `SELECT name FROM users WHERE id > 10 AND name = 'bob';`
	expected := []token.Token{
		{token.SELECT, "SELECT"},
		{token.IDENT, "name"},
		{token.FROM, "FROM"},
		{token.IDENT, "users"},
		{token.WHERE, "WHERE"},
		{token.IDENT, "id"},
		{token.GT, ">"},
		{token.INT_LIT, "10"},
		{token.AND, "AND"},
		{token.IDENT, "name"},
		{token.EQ, "="},
		{token.STRING_LIT, "bob"},
		{token.SEMICOLON, ";"},
		{token.EOF, ""},
	}
	testTokens(t, input, expected)
}

func TestCaseInsensitiveKeywords(t *testing.T) {
	inputs := []string{"select", "Select", "SELECT"}
	for _, input := range inputs {
		l := New(input)
		tok := l.NextToken()
		if tok.Type != token.SELECT {
			t.Errorf("input %q: expected SELECT, got %s", input, tok.Type)
		}
	}
}

func TestComparisonOperators(t *testing.T) {
	input := `<= >= <> !=`
	expected := []token.Token{
		{token.LT_EQ, "<="},
		{token.GT_EQ, ">="},
		{token.NEQ, "<>"},
		{token.NEQ, "!="},
		{token.EOF, ""},
	}
	testTokens(t, input, expected)
}

func TestEscapedQuote(t *testing.T) {
	input := `'it''s'`
	l := New(input)
	tok := l.NextToken()
	if tok.Type != token.STRING_LIT {
		t.Fatalf("expected STRING_LIT, got %s", tok.Type)
	}
	if tok.Literal != "it's" {
		t.Fatalf("expected literal %q, got %q", "it's", tok.Literal)
	}
}

func TestCountToken(t *testing.T) {
	input := `COUNT(*)`
	expected := []token.Token{
		{token.COUNT, "COUNT"},
		{token.LPAREN, "("},
		{token.ASTERISK, "*"},
		{token.RPAREN, ")"},
		{token.EOF, ""},
	}
	testTokens(t, input, expected)
}

func TestDotToken(t *testing.T) {
	input := `users.id`
	expected := []token.Token{
		{token.IDENT, "users"},
		{token.DOT, "."},
		{token.IDENT, "id"},
		{token.EOF, ""},
	}
	testTokens(t, input, expected)
}

func TestQuotedIdent(t *testing.T) {
	input := "`count`"
	l := New(input)
	tok := l.NextToken()
	if tok.Type != token.QUOTED_IDENT {
		t.Fatalf("expected QUOTED_IDENT, got %s", tok.Type)
	}
	if tok.Literal != "count" {
		t.Fatalf("expected literal %q, got %q", "count", tok.Literal)
	}
}

func TestQuotedIdentEscaped(t *testing.T) {
	input := "`back``tick`"
	l := New(input)
	tok := l.NextToken()
	if tok.Type != token.QUOTED_IDENT {
		t.Fatalf("expected QUOTED_IDENT, got %s", tok.Type)
	}
	if tok.Literal != "back`tick" {
		t.Fatalf("expected literal %q, got %q", "back`tick", tok.Literal)
	}
}

func TestArithmeticOperators(t *testing.T) {
	input := `+ - /`
	expected := []token.Token{
		{token.PLUS, "+"},
		{token.MINUS, "-"},
		{token.SLASH, "/"},
		{token.EOF, ""},
	}
	testTokens(t, input, expected)
}

func testTokens(t *testing.T, input string, expected []token.Token) {
	t.Helper()
	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.Type {
			t.Fatalf("token[%d]: expected type %s, got %s (literal=%q)", i, exp.Type, tok.Type, tok.Literal)
		}
		if tok.Literal != exp.Literal {
			t.Fatalf("token[%d]: expected literal %q, got %q", i, exp.Literal, tok.Literal)
		}
	}
}

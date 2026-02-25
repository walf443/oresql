package lexer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		assert.Equal(t, token.SELECT, tok.Type, "input %q", input)
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
	require.Equal(t, token.STRING_LIT, tok.Type)
	require.Equal(t, "it's", tok.Literal)
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
	require.Equal(t, token.QUOTED_IDENT, tok.Type)
	require.Equal(t, "count", tok.Literal)
}

func TestQuotedIdentEscaped(t *testing.T) {
	input := "`back``tick`"
	l := New(input)
	tok := l.NextToken()
	require.Equal(t, token.QUOTED_IDENT, tok.Type)
	require.Equal(t, "back`tick", tok.Literal)
}

func TestDoubleQuoteString(t *testing.T) {
	input := `"hello"`
	l := New(input)
	tok := l.NextToken()
	require.Equal(t, token.STRING_LIT, tok.Type)
	require.Equal(t, "hello", tok.Literal)
}

func TestDoubleQuoteEscaped(t *testing.T) {
	input := `"it""s"`
	l := New(input)
	tok := l.NextToken()
	require.Equal(t, token.STRING_LIT, tok.Type)
	require.Equal(t, `it"s`, tok.Literal)
}

func TestDoubleQuoteEmpty(t *testing.T) {
	input := `""`
	l := New(input)
	tok := l.NextToken()
	require.Equal(t, token.STRING_LIT, tok.Type)
	require.Equal(t, "", tok.Literal)
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

func TestFloatLiteral(t *testing.T) {
	input := `3.14`
	expected := []token.Token{
		{token.FLOAT_LIT, "3.14"},
		{token.EOF, ""},
	}
	testTokens(t, input, expected)
}

func TestFloatLiteralZeroPrefix(t *testing.T) {
	input := `0.5`
	expected := []token.Token{
		{token.FLOAT_LIT, "0.5"},
		{token.EOF, ""},
	}
	testTokens(t, input, expected)
}

func TestIntLiteralStillWorks(t *testing.T) {
	input := `42`
	expected := []token.Token{
		{token.INT_LIT, "42"},
		{token.EOF, ""},
	}
	testTokens(t, input, expected)
}

func testTokens(t *testing.T, input string, expected []token.Token) {
	t.Helper()
	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		require.Equal(t, exp.Type, tok.Type, "token[%d]: type mismatch (literal=%q)", i, tok.Literal)
		require.Equal(t, exp.Literal, tok.Literal, "token[%d]: literal mismatch", i)
	}
}

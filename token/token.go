package token

import "strings"

type TokenType int

const (
	// Special
	ILLEGAL TokenType = iota
	EOF

	// Literals
	IDENT      // column names, table names
	INT_LIT    // integer literals like 42
	STRING_LIT // string literals like 'hello'

	// Operators and delimiters
	ASTERISK  // *
	COMMA     // ,
	LPAREN    // (
	RPAREN    // )
	SEMICOLON // ;
	DOT       // .
	EQ        // =
	NEQ       // != or <>
	LT        // <
	GT        // >
	LT_EQ     // <=
	GT_EQ     // >=

	// Keywords
	CREATE
	TABLE
	INSERT
	INTO
	VALUES
	SELECT
	FROM
	WHERE
	AND
	OR
	NOT
	INT   // INT type keyword
	TEXT  // TEXT type keyword
	COUNT // COUNT aggregate function
	IS    // IS keyword
	NULL  // NULL keyword
	AS    // AS keyword
)

var tokenNames = map[TokenType]string{
	ILLEGAL:    "ILLEGAL",
	EOF:        "EOF",
	IDENT:      "IDENT",
	INT_LIT:    "INT_LIT",
	STRING_LIT: "STRING_LIT",
	ASTERISK:   "ASTERISK",
	COMMA:      "COMMA",
	LPAREN:     "LPAREN",
	RPAREN:     "RPAREN",
	SEMICOLON:  "SEMICOLON",
	DOT:        "DOT",
	EQ:         "EQ",
	NEQ:        "NEQ",
	LT:         "LT",
	GT:         "GT",
	LT_EQ:      "LT_EQ",
	GT_EQ:      "GT_EQ",
	CREATE:     "CREATE",
	TABLE:      "TABLE",
	INSERT:     "INSERT",
	INTO:       "INTO",
	VALUES:     "VALUES",
	SELECT:     "SELECT",
	FROM:       "FROM",
	WHERE:      "WHERE",
	AND:        "AND",
	OR:         "OR",
	NOT:        "NOT",
	INT:        "INT",
	TEXT:       "TEXT",
	COUNT:      "COUNT",
	IS:         "IS",
	NULL:       "NULL",
	AS:         "AS",
}

func (t TokenType) String() string {
	if name, ok := tokenNames[t]; ok {
		return name
	}
	return "UNKNOWN"
}

type Token struct {
	Type    TokenType
	Literal string
}

var keywords = map[string]TokenType{
	"CREATE": CREATE,
	"TABLE":  TABLE,
	"INSERT": INSERT,
	"INTO":   INTO,
	"VALUES": VALUES,
	"SELECT": SELECT,
	"FROM":   FROM,
	"WHERE":  WHERE,
	"AND":    AND,
	"OR":     OR,
	"NOT":    NOT,
	"INT":    INT,
	"TEXT":   TEXT,
	"COUNT":  COUNT,
	"IS":     IS,
	"NULL":   NULL,
	"AS":     AS,
}

// LookupIdent returns the keyword TokenType if the identifier is a keyword,
// otherwise returns IDENT.
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[strings.ToUpper(ident)]; ok {
		return tok
	}
	return IDENT
}

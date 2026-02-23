package token

import "strings"

type TokenType int

const (
	// Special
	ILLEGAL TokenType = iota
	EOF

	// Literals
	IDENT        // column names, table names
	QUOTED_IDENT // backtick-quoted identifiers like `count`
	INT_LIT      // integer literals like 42
	FLOAT_LIT    // float literals like 3.14
	STRING_LIT   // string literals like 'hello'

	// Operators and delimiters
	ASTERISK  // *
	PLUS      // +
	MINUS     // -
	SLASH     // /
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
	INT       // INT type keyword
	FLOAT     // FLOAT type keyword
	TEXT      // TEXT type keyword
	COUNT     // COUNT aggregate function
	SUM       // SUM aggregate function
	AVG       // AVG aggregate function
	MIN       // MIN aggregate function
	MAX       // MAX aggregate function
	IS        // IS keyword
	NULL      // NULL keyword
	AS        // AS keyword
	UPDATE    // UPDATE keyword
	SET       // SET keyword
	DELETE    // DELETE keyword
	ORDER     // ORDER keyword
	BY        // BY keyword
	ASC       // ASC keyword
	DESC      // DESC keyword
	LIMIT     // LIMIT keyword
	OFFSET    // OFFSET keyword
	DROP      // DROP keyword
	TRUNCATE  // TRUNCATE keyword
	GROUP     // GROUP keyword
	HAVING    // HAVING keyword
	DISTINCT  // DISTINCT keyword
	DEFAULT   // DEFAULT keyword
	IN        // IN keyword
	PRIMARY   // PRIMARY keyword
	KEY       // KEY keyword
	BETWEEN   // BETWEEN keyword
	LIKE      // LIKE keyword
	INDEX     // INDEX keyword
	ON        // ON keyword
	ALTER     // ALTER keyword
	ADD       // ADD keyword
	COLUMN    // COLUMN keyword
	UNIQUE    // UNIQUE keyword
	JOIN      // JOIN keyword
	INNER     // INNER keyword
	LEFT      // LEFT keyword
	OUTER     // OUTER keyword
	CASE      // CASE keyword
	WHEN      // WHEN keyword
	THEN      // THEN keyword
	ELSE      // ELSE keyword
	END       // END keyword
	COALESCE  // COALESCE function
	NULLIF    // NULLIF function
	ABS       // ABS function
	ROUND     // ROUND function
	MOD       // MOD function
	CEIL      // CEIL function
	FLOOR     // FLOOR function
	POWER     // POWER function
	LENGTH    // LENGTH function
	UPPER     // UPPER function
	LOWER     // LOWER function
	SUBSTRING // SUBSTRING function
	TRIM      // TRIM function
	CONCAT    // CONCAT function
	EXISTS    // EXISTS keyword
)

var tokenNames = map[TokenType]string{
	ILLEGAL:      "ILLEGAL",
	EOF:          "EOF",
	IDENT:        "IDENT",
	QUOTED_IDENT: "QUOTED_IDENT",
	INT_LIT:      "INT_LIT",
	FLOAT_LIT:    "FLOAT_LIT",
	STRING_LIT:   "STRING_LIT",
	ASTERISK:     "ASTERISK",
	PLUS:         "PLUS",
	MINUS:        "MINUS",
	SLASH:        "SLASH",
	COMMA:        "COMMA",
	LPAREN:       "LPAREN",
	RPAREN:       "RPAREN",
	SEMICOLON:    "SEMICOLON",
	DOT:          "DOT",
	EQ:           "EQ",
	NEQ:          "NEQ",
	LT:           "LT",
	GT:           "GT",
	LT_EQ:        "LT_EQ",
	GT_EQ:        "GT_EQ",
	CREATE:       "CREATE",
	TABLE:        "TABLE",
	INSERT:       "INSERT",
	INTO:         "INTO",
	VALUES:       "VALUES",
	SELECT:       "SELECT",
	FROM:         "FROM",
	WHERE:        "WHERE",
	AND:          "AND",
	OR:           "OR",
	NOT:          "NOT",
	INT:          "INT",
	FLOAT:        "FLOAT",
	TEXT:         "TEXT",
	COUNT:        "COUNT",
	SUM:          "SUM",
	AVG:          "AVG",
	MIN:          "MIN",
	MAX:          "MAX",
	IS:           "IS",
	NULL:         "NULL",
	AS:           "AS",
	UPDATE:       "UPDATE",
	SET:          "SET",
	DELETE:       "DELETE",
	ORDER:        "ORDER",
	BY:           "BY",
	ASC:          "ASC",
	DESC:         "DESC",
	LIMIT:        "LIMIT",
	OFFSET:       "OFFSET",
	DROP:         "DROP",
	TRUNCATE:     "TRUNCATE",
	GROUP:        "GROUP",
	HAVING:       "HAVING",
	DISTINCT:     "DISTINCT",
	DEFAULT:      "DEFAULT",
	IN:           "IN",
	PRIMARY:      "PRIMARY",
	KEY:          "KEY",
	BETWEEN:      "BETWEEN",
	LIKE:         "LIKE",
	INDEX:        "INDEX",
	ON:           "ON",
	ALTER:        "ALTER",
	ADD:          "ADD",
	COLUMN:       "COLUMN",
	UNIQUE:       "UNIQUE",
	JOIN:         "JOIN",
	INNER:        "INNER",
	LEFT:         "LEFT",
	OUTER:        "OUTER",
	CASE:         "CASE",
	WHEN:         "WHEN",
	THEN:         "THEN",
	ELSE:         "ELSE",
	END:          "END",
	COALESCE:     "COALESCE",
	NULLIF:       "NULLIF",
	ABS:          "ABS",
	ROUND:        "ROUND",
	MOD:          "MOD",
	CEIL:         "CEIL",
	FLOOR:        "FLOOR",
	POWER:        "POWER",
	LENGTH:       "LENGTH",
	UPPER:        "UPPER",
	LOWER:        "LOWER",
	SUBSTRING:    "SUBSTRING",
	TRIM:         "TRIM",
	CONCAT:       "CONCAT",
	EXISTS:       "EXISTS",
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
	"CREATE":    CREATE,
	"TABLE":     TABLE,
	"INSERT":    INSERT,
	"INTO":      INTO,
	"VALUES":    VALUES,
	"SELECT":    SELECT,
	"FROM":      FROM,
	"WHERE":     WHERE,
	"AND":       AND,
	"OR":        OR,
	"NOT":       NOT,
	"INT":       INT,
	"FLOAT":     FLOAT,
	"TEXT":      TEXT,
	"COUNT":     COUNT,
	"SUM":       SUM,
	"AVG":       AVG,
	"MIN":       MIN,
	"MAX":       MAX,
	"IS":        IS,
	"NULL":      NULL,
	"AS":        AS,
	"UPDATE":    UPDATE,
	"SET":       SET,
	"DELETE":    DELETE,
	"ORDER":     ORDER,
	"BY":        BY,
	"ASC":       ASC,
	"DESC":      DESC,
	"LIMIT":     LIMIT,
	"OFFSET":    OFFSET,
	"DROP":      DROP,
	"TRUNCATE":  TRUNCATE,
	"GROUP":     GROUP,
	"HAVING":    HAVING,
	"DISTINCT":  DISTINCT,
	"DEFAULT":   DEFAULT,
	"IN":        IN,
	"PRIMARY":   PRIMARY,
	"KEY":       KEY,
	"BETWEEN":   BETWEEN,
	"LIKE":      LIKE,
	"INDEX":     INDEX,
	"ON":        ON,
	"ALTER":     ALTER,
	"ADD":       ADD,
	"COLUMN":    COLUMN,
	"UNIQUE":    UNIQUE,
	"JOIN":      JOIN,
	"INNER":     INNER,
	"LEFT":      LEFT,
	"OUTER":     OUTER,
	"CASE":      CASE,
	"WHEN":      WHEN,
	"THEN":      THEN,
	"ELSE":      ELSE,
	"END":       END,
	"COALESCE":  COALESCE,
	"NULLIF":    NULLIF,
	"ABS":       ABS,
	"ROUND":     ROUND,
	"MOD":       MOD,
	"CEIL":      CEIL,
	"FLOOR":     FLOOR,
	"POWER":     POWER,
	"LENGTH":    LENGTH,
	"UPPER":     UPPER,
	"LOWER":     LOWER,
	"SUBSTRING": SUBSTRING,
	"TRIM":      TRIM,
	"CONCAT":    CONCAT,
	"EXISTS":    EXISTS,
}

// LookupIdent returns the keyword TokenType if the identifier is a keyword,
// otherwise returns IDENT.
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[strings.ToUpper(ident)]; ok {
		return tok
	}
	return IDENT
}

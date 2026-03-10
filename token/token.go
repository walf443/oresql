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
	MATCH_OP  // @@

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
	INT         // INT type keyword
	FLOAT       // FLOAT type keyword
	TEXT        // TEXT type keyword
	JSON        // JSON type keyword
	JSON_OBJECT // JSON_OBJECT function
	JSON_ARRAY  // JSON_ARRAY function
	COUNT       // COUNT aggregate function
	SUM         // SUM aggregate function
	AVG         // AVG aggregate function
	MIN         // MIN aggregate function
	MAX         // MAX aggregate function
	IS          // IS keyword
	NULL        // NULL keyword
	TRUE        // TRUE keyword
	FALSE       // FALSE keyword
	AS          // AS keyword
	UPDATE      // UPDATE keyword
	SET         // SET keyword
	DELETE      // DELETE keyword
	ORDER       // ORDER keyword
	BY          // BY keyword
	ASC         // ASC keyword
	DESC        // DESC keyword
	LIMIT       // LIMIT keyword
	OFFSET      // OFFSET keyword
	DROP        // DROP keyword
	TRUNCATE    // TRUNCATE keyword
	GROUP       // GROUP keyword
	HAVING      // HAVING keyword
	DISTINCT    // DISTINCT keyword
	DEFAULT     // DEFAULT keyword
	IN          // IN keyword
	PRIMARY     // PRIMARY keyword
	KEY         // KEY keyword
	BETWEEN     // BETWEEN keyword
	LIKE        // LIKE keyword
	INDEX       // INDEX keyword
	ON          // ON keyword
	ALTER       // ALTER keyword
	ADD         // ADD keyword
	COLUMN      // COLUMN keyword
	UNIQUE      // UNIQUE keyword
	JOIN        // JOIN keyword
	INNER       // INNER keyword
	LEFT        // LEFT keyword
	RIGHT       // RIGHT keyword
	CROSS       // CROSS keyword
	OUTER       // OUTER keyword
	CAST        // CAST keyword
	CASE        // CASE keyword
	WHEN        // WHEN keyword
	THEN        // THEN keyword
	ELSE        // ELSE keyword
	END         // END keyword
	COALESCE    // COALESCE function
	NULLIF      // NULLIF function
	ABS         // ABS function
	ROUND       // ROUND function
	MOD         // MOD function
	CEIL        // CEIL function
	FLOOR       // FLOOR function
	POWER       // POWER function
	LENGTH      // LENGTH function
	UPPER       // UPPER function
	LOWER       // LOWER function
	SUBSTRING   // SUBSTRING function
	TRIM        // TRIM function
	CONCAT      // CONCAT function
	EXISTS      // EXISTS keyword
	UNION       // UNION keyword
	INTERSECT   // INTERSECT keyword
	EXCEPT      // EXCEPT keyword
	ALL         // ALL keyword

	// Window function keywords
	ROW_NUMBER // ROW_NUMBER window function
	RANK       // RANK window function
	DENSE_RANK // DENSE_RANK window function
	OVER       // OVER keyword
	PARTITION  // PARTITION keyword
	WINDOW     // WINDOW keyword

	// Database management keywords
	DATABASE  // DATABASE keyword
	USE       // USE keyword
	SHOW      // SHOW keyword
	DATABASES // DATABASES keyword
	TABLES    // TABLES keyword
	USING     // USING keyword
	WITH      // WITH keyword
	RECURSIVE // RECURSIVE keyword
	EXPLAIN   // EXPLAIN keyword
	GIN       // GIN keyword
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
	MATCH_OP:     "MATCH_OP",
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
	JSON:         "JSON",
	JSON_OBJECT:  "JSON_OBJECT",
	JSON_ARRAY:   "JSON_ARRAY",
	COUNT:        "COUNT",
	SUM:          "SUM",
	AVG:          "AVG",
	MIN:          "MIN",
	MAX:          "MAX",
	IS:           "IS",
	NULL:         "NULL",
	TRUE:         "TRUE",
	FALSE:        "FALSE",
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
	RIGHT:        "RIGHT",
	CROSS:        "CROSS",
	OUTER:        "OUTER",
	CAST:         "CAST",
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
	UNION:        "UNION",
	INTERSECT:    "INTERSECT",
	EXCEPT:       "EXCEPT",
	ALL:          "ALL",
	ROW_NUMBER:   "ROW_NUMBER",
	RANK:         "RANK",
	DENSE_RANK:   "DENSE_RANK",
	OVER:         "OVER",
	PARTITION:    "PARTITION",
	WINDOW:       "WINDOW",
	DATABASE:     "DATABASE",
	USE:          "USE",
	SHOW:         "SHOW",
	DATABASES:    "DATABASES",
	TABLES:       "TABLES",
	USING:        "USING",
	WITH:         "WITH",
	RECURSIVE:    "RECURSIVE",
	EXPLAIN:      "EXPLAIN",
	GIN:          "GIN",
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
	"CREATE":      CREATE,
	"TABLE":       TABLE,
	"INSERT":      INSERT,
	"INTO":        INTO,
	"VALUES":      VALUES,
	"SELECT":      SELECT,
	"FROM":        FROM,
	"WHERE":       WHERE,
	"AND":         AND,
	"OR":          OR,
	"NOT":         NOT,
	"INT":         INT,
	"FLOAT":       FLOAT,
	"TEXT":        TEXT,
	"JSON":        JSON,
	"JSON_OBJECT": JSON_OBJECT,
	"JSON_ARRAY":  JSON_ARRAY,
	"COUNT":       COUNT,
	"SUM":         SUM,
	"AVG":         AVG,
	"MIN":         MIN,
	"MAX":         MAX,
	"IS":          IS,
	"NULL":        NULL,
	"TRUE":        TRUE,
	"FALSE":       FALSE,
	"AS":          AS,
	"UPDATE":      UPDATE,
	"SET":         SET,
	"DELETE":      DELETE,
	"ORDER":       ORDER,
	"BY":          BY,
	"ASC":         ASC,
	"DESC":        DESC,
	"LIMIT":       LIMIT,
	"OFFSET":      OFFSET,
	"DROP":        DROP,
	"TRUNCATE":    TRUNCATE,
	"GROUP":       GROUP,
	"HAVING":      HAVING,
	"DISTINCT":    DISTINCT,
	"DEFAULT":     DEFAULT,
	"IN":          IN,
	"PRIMARY":     PRIMARY,
	"KEY":         KEY,
	"BETWEEN":     BETWEEN,
	"LIKE":        LIKE,
	"INDEX":       INDEX,
	"ON":          ON,
	"ALTER":       ALTER,
	"ADD":         ADD,
	"COLUMN":      COLUMN,
	"UNIQUE":      UNIQUE,
	"JOIN":        JOIN,
	"INNER":       INNER,
	"LEFT":        LEFT,
	"RIGHT":       RIGHT,
	"CROSS":       CROSS,
	"OUTER":       OUTER,
	"CAST":        CAST,
	"CASE":        CASE,
	"WHEN":        WHEN,
	"THEN":        THEN,
	"ELSE":        ELSE,
	"END":         END,
	"COALESCE":    COALESCE,
	"NULLIF":      NULLIF,
	"ABS":         ABS,
	"ROUND":       ROUND,
	"MOD":         MOD,
	"CEIL":        CEIL,
	"FLOOR":       FLOOR,
	"POWER":       POWER,
	"LENGTH":      LENGTH,
	"UPPER":       UPPER,
	"LOWER":       LOWER,
	"SUBSTRING":   SUBSTRING,
	"TRIM":        TRIM,
	"CONCAT":      CONCAT,
	"EXISTS":      EXISTS,
	"UNION":       UNION,
	"INTERSECT":   INTERSECT,
	"EXCEPT":      EXCEPT,
	"ALL":         ALL,
	"ROW_NUMBER":  ROW_NUMBER,
	"RANK":        RANK,
	"DENSE_RANK":  DENSE_RANK,
	"OVER":        OVER,
	"PARTITION":   PARTITION,
	"WINDOW":      WINDOW,
	"DATABASE":    DATABASE,
	"USE":         USE,
	"SHOW":        SHOW,
	"DATABASES":   DATABASES,
	"TABLES":      TABLES,
	"USING":       USING,
	"WITH":        WITH,
	"RECURSIVE":   RECURSIVE,
	"EXPLAIN":     EXPLAIN,
	"GIN":         GIN,
}

// IsKeyword returns true if the given token type is a SQL keyword.
func IsKeyword(t TokenType) bool {
	_, ok := tokenNames[t]
	return ok && t >= CREATE
}

// LookupIdent returns the keyword TokenType if the identifier is a keyword,
// otherwise returns IDENT.
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[strings.ToUpper(ident)]; ok {
		return tok
	}
	return IDENT
}

package ast

// Node is the interface all AST nodes implement.
type Node interface {
	NodeType() string
}

// Statement is a top-level SQL statement.
type Statement interface {
	Node
	statementNode()
}

// Expr is an expression (used in WHERE clauses, VALUES, etc.).
type Expr interface {
	Node
	exprNode()
}

// ColumnDef represents a column definition in CREATE TABLE.
type ColumnDef struct {
	Name       string
	DataType   string // "INT" or "TEXT"
	NotNull    bool
	PrimaryKey bool
	Default    Expr // nil = no DEFAULT clause
}

// CreateTableStmt represents CREATE TABLE <name> (<columns>).
type CreateTableStmt struct {
	TableName string
	Columns   []ColumnDef
}

func (s *CreateTableStmt) NodeType() string { return "CreateTable" }
func (s *CreateTableStmt) statementNode()   {}

// InsertStmt represents INSERT INTO <table> [(<columns>)] VALUES (<values>), ...
type InsertStmt struct {
	TableName string
	Columns   []string // nil = no column list specified
	Rows      [][]Expr
}

func (s *InsertStmt) NodeType() string { return "Insert" }
func (s *InsertStmt) statementNode()   {}

// OrderByClause represents a single ORDER BY item.
type OrderByClause struct {
	Expr Expr
	Desc bool // true for DESC, false for ASC (default)
}

// SelectStmt represents SELECT [DISTINCT] <columns> FROM <table> [WHERE <condition>] [GROUP BY ...] [HAVING ...] [ORDER BY ...] [LIMIT <n>] [OFFSET <n>].
type SelectStmt struct {
	Distinct  bool
	Columns   []Expr
	TableName string
	Where     Expr            // nil if no WHERE clause
	GroupBy   []Expr          // nil if no GROUP BY clause
	Having    Expr            // nil if no HAVING clause
	OrderBy   []OrderByClause // nil if no ORDER BY clause
	Limit     *int64          // nil if no LIMIT clause
	Offset    *int64          // nil if no OFFSET clause
}

func (s *SelectStmt) NodeType() string { return "Select" }
func (s *SelectStmt) statementNode()   {}

// IdentExpr represents a column name reference, optionally qualified with a table name.
type IdentExpr struct {
	Table string // table name (empty if unqualified)
	Name  string
}

func (e *IdentExpr) NodeType() string { return "Ident" }
func (e *IdentExpr) exprNode()        {}

// IntLitExpr represents an integer literal.
type IntLitExpr struct {
	Value int64
}

func (e *IntLitExpr) NodeType() string { return "IntLit" }
func (e *IntLitExpr) exprNode()        {}

// FloatLitExpr represents a floating-point literal.
type FloatLitExpr struct {
	Value float64
}

func (e *FloatLitExpr) NodeType() string { return "FloatLit" }
func (e *FloatLitExpr) exprNode()        {}

// StringLitExpr represents a string literal.
type StringLitExpr struct {
	Value string
}

func (e *StringLitExpr) NodeType() string { return "StringLit" }
func (e *StringLitExpr) exprNode()        {}

// StarExpr represents * in SELECT *.
type StarExpr struct{}

func (e *StarExpr) NodeType() string { return "Star" }
func (e *StarExpr) exprNode()        {}

// CallExpr represents a function call like COUNT(*), SUM(col), etc.
type CallExpr struct {
	Name string // function name (e.g. "COUNT")
	Args []Expr // arguments
}

func (e *CallExpr) NodeType() string { return "Call" }
func (e *CallExpr) exprNode()        {}

// NullLitExpr represents the NULL literal.
type NullLitExpr struct{}

func (e *NullLitExpr) NodeType() string { return "NullLit" }
func (e *NullLitExpr) exprNode()        {}

// IsNullExpr represents <expr> IS [NOT] NULL.
type IsNullExpr struct {
	Expr Expr
	Not  bool // true for IS NOT NULL
}

func (e *IsNullExpr) NodeType() string { return "IsNull" }
func (e *IsNullExpr) exprNode()        {}

// AliasExpr represents an expression with an alias (e.g. id AS user_id).
type AliasExpr struct {
	Expr  Expr
	Alias string
}

func (e *AliasExpr) NodeType() string { return "Alias" }
func (e *AliasExpr) exprNode()        {}

// ArithmeticExpr represents an arithmetic operation: left <op> right.
type ArithmeticExpr struct {
	Left  Expr
	Op    string // "+", "-", "*", "/"
	Right Expr
}

func (e *ArithmeticExpr) NodeType() string { return "Arithmetic" }
func (e *ArithmeticExpr) exprNode()        {}

// SetClause represents a single column = value assignment in UPDATE.
type SetClause struct {
	Column string
	Value  Expr
}

// UpdateStmt represents UPDATE <table> SET <col> = <expr>, ... [WHERE <condition>].
type UpdateStmt struct {
	TableName string
	Sets      []SetClause
	Where     Expr // nil if no WHERE clause
}

func (s *UpdateStmt) NodeType() string { return "Update" }
func (s *UpdateStmt) statementNode()   {}

// DeleteStmt represents DELETE FROM <table> [WHERE <condition>].
type DeleteStmt struct {
	TableName string
	Where     Expr // nil if no WHERE clause
}

func (s *DeleteStmt) NodeType() string { return "Delete" }
func (s *DeleteStmt) statementNode()   {}

// DropTableStmt represents DROP TABLE <name>.
type DropTableStmt struct {
	TableName string
}

func (s *DropTableStmt) NodeType() string { return "DropTable" }
func (s *DropTableStmt) statementNode()   {}

// TruncateTableStmt represents TRUNCATE TABLE <name>.
type TruncateTableStmt struct {
	TableName string
}

func (s *TruncateTableStmt) NodeType() string { return "TruncateTable" }
func (s *TruncateTableStmt) statementNode()   {}

// InExpr represents <expr> [NOT] IN (<expr>, ...).
type InExpr struct {
	Left   Expr
	Values []Expr
	Not    bool // true for NOT IN
}

func (e *InExpr) NodeType() string { return "In" }
func (e *InExpr) exprNode()        {}

// BetweenExpr represents <expr> [NOT] BETWEEN <low> AND <high>.
type BetweenExpr struct {
	Left Expr // target expression
	Low  Expr // lower bound
	High Expr // upper bound
	Not  bool // true for NOT BETWEEN
}

func (e *BetweenExpr) NodeType() string { return "Between" }
func (e *BetweenExpr) exprNode()        {}

// BinaryExpr represents a comparison: left <op> right.
type BinaryExpr struct {
	Left  Expr
	Op    string // "=", "!=", "<", ">", "<=", ">="
	Right Expr
}

func (e *BinaryExpr) NodeType() string { return "Binary" }
func (e *BinaryExpr) exprNode()        {}

// LogicalExpr represents a logical operation: left AND/OR right.
type LogicalExpr struct {
	Left  Expr
	Op    string // "AND", "OR"
	Right Expr
}

func (e *LogicalExpr) NodeType() string { return "Logical" }
func (e *LogicalExpr) exprNode()        {}

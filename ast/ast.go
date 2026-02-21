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
	Name     string
	DataType string // "INT" or "TEXT"
	NotNull  bool
}

// CreateTableStmt represents CREATE TABLE <name> (<columns>).
type CreateTableStmt struct {
	TableName string
	Columns   []ColumnDef
}

func (s *CreateTableStmt) NodeType() string { return "CreateTable" }
func (s *CreateTableStmt) statementNode()   {}

// InsertStmt represents INSERT INTO <table> VALUES (<values>), ...
type InsertStmt struct {
	TableName string
	Rows      [][]Expr
}

func (s *InsertStmt) NodeType() string { return "Insert" }
func (s *InsertStmt) statementNode()   {}

// SelectStmt represents SELECT <columns> FROM <table> [WHERE <condition>].
type SelectStmt struct {
	Columns   []Expr
	TableName string
	Where     Expr // nil if no WHERE clause
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

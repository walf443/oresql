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
}

// CreateTableStmt represents CREATE TABLE <name> (<columns>).
type CreateTableStmt struct {
	TableName string
	Columns   []ColumnDef
}

func (s *CreateTableStmt) NodeType() string { return "CreateTable" }
func (s *CreateTableStmt) statementNode()   {}

// InsertStmt represents INSERT INTO <table> VALUES (<values>).
type InsertStmt struct {
	TableName string
	Values    []Expr
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
func (e *IdentExpr) exprNode()       {}

// IntLitExpr represents an integer literal.
type IntLitExpr struct {
	Value int64
}

func (e *IntLitExpr) NodeType() string { return "IntLit" }
func (e *IntLitExpr) exprNode()       {}

// StringLitExpr represents a string literal.
type StringLitExpr struct {
	Value string
}

func (e *StringLitExpr) NodeType() string { return "StringLit" }
func (e *StringLitExpr) exprNode()       {}

// StarExpr represents * in SELECT *.
type StarExpr struct{}

func (e *StarExpr) NodeType() string { return "Star" }
func (e *StarExpr) exprNode()       {}

// BinaryExpr represents a comparison: left <op> right.
type BinaryExpr struct {
	Left  Expr
	Op    string // "=", "!=", "<", ">", "<=", ">="
	Right Expr
}

func (e *BinaryExpr) NodeType() string { return "Binary" }
func (e *BinaryExpr) exprNode()       {}

// LogicalExpr represents a logical operation: left AND/OR right.
type LogicalExpr struct {
	Left  Expr
	Op    string // "AND", "OR"
	Right Expr
}

func (e *LogicalExpr) NodeType() string { return "Logical" }
func (e *LogicalExpr) exprNode()       {}

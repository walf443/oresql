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
	Unique     bool
	PrimaryKey bool
	Default    Expr // nil = no DEFAULT clause
}

// CreateTableStmt represents CREATE TABLE <name> (<columns>).
type CreateTableStmt struct {
	TableName  string
	Columns    []ColumnDef
	PrimaryKey []string // table-level PRIMARY KEY column names (nil if not specified)
}

func (s *CreateTableStmt) NodeType() string { return "CreateTable" }
func (s *CreateTableStmt) statementNode()   {}

// InsertStmt represents INSERT INTO <table> [(<columns>)] VALUES (<values>), ...
// or INSERT INTO <table> [(<columns>)] SELECT ...
type InsertStmt struct {
	TableName string
	Columns   []string  // nil = no column list specified
	Rows      [][]Expr  // VALUES rows (nil when Select is used)
	Select    Statement // INSERT ... SELECT (nil when VALUES is used)
}

func (s *InsertStmt) NodeType() string { return "Insert" }
func (s *InsertStmt) statementNode()   {}

// OrderByClause represents a single ORDER BY item.
type OrderByClause struct {
	Expr Expr
	Desc bool // true for DESC, false for ASC (default)
}

// Join type constants.
const (
	JoinInner = "INNER"
	JoinLeft  = "LEFT"
	JoinRight = "RIGHT"
	JoinCross = "CROSS"
)

// JoinClause represents a single JOIN in a SELECT statement.
type JoinClause struct {
	JoinType   string // JoinInner or JoinLeft
	TableName  string
	TableAlias string
	On         Expr
}

// NamedWindowDef represents a named window definition in a WINDOW clause.
type NamedWindowDef struct {
	Name        string
	PartitionBy []Expr
	OrderBy     []OrderByClause
}

// SelectStmt represents SELECT [DISTINCT] <columns> FROM <table> [WHERE <condition>] [GROUP BY ...] [HAVING ...] [WINDOW ...] [ORDER BY ...] [LIMIT <n>] [OFFSET <n>].
type SelectStmt struct {
	Distinct     bool
	Columns      []Expr
	TableName    string           // table name (empty when FromSubquery is used)
	FromSubquery Statement        // FROM subquery (nil = normal table). *SelectStmt or *SetOpStmt
	TableAlias   string           // optional alias for the FROM table or subquery
	Joins        []JoinClause     // JOIN clauses (INNER, LEFT)
	Where        Expr             // nil if no WHERE clause
	GroupBy      []Expr           // nil if no GROUP BY clause
	Having       Expr             // nil if no HAVING clause
	Windows      []NamedWindowDef // WINDOW clause definitions (nil if not specified)
	OrderBy      []OrderByClause  // nil if no ORDER BY clause
	Limit        *int64           // nil if no LIMIT clause
	Offset       *int64           // nil if no OFFSET clause
}

func (s *SelectStmt) NodeType() string { return "Select" }
func (s *SelectStmt) statementNode()   {}

// Set operation type constants.
const (
	SetOpUnion     = "UNION"
	SetOpIntersect = "INTERSECT"
	SetOpExcept    = "EXCEPT"
)

// SetOpStmt represents SELECT ... UNION|INTERSECT [ALL] SELECT ...
type SetOpStmt struct {
	Left    Statement       // *SelectStmt or *SetOpStmt (for chaining)
	Right   *SelectStmt     // right-hand SELECT
	Op      string          // SetOpUnion or SetOpIntersect
	All     bool            // true = ALL variant
	OrderBy []OrderByClause // ORDER BY on the combined result
	Limit   *int64
	Offset  *int64
}

func (s *SetOpStmt) NodeType() string { return "SetOp" }
func (s *SetOpStmt) statementNode()   {}

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

// CastExpr represents CAST(expr AS type).
type CastExpr struct {
	Expr       Expr   // expression to cast
	TargetType string // "INT", "FLOAT", or "TEXT"
}

func (e *CastExpr) NodeType() string { return "Cast" }
func (e *CastExpr) exprNode()        {}

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

// UpdateStmt represents UPDATE <table> SET <col> = <expr>, ... [WHERE <condition>] [ORDER BY ...] [LIMIT <n>].
type UpdateStmt struct {
	TableName string
	Sets      []SetClause
	Where     Expr            // nil if no WHERE clause
	OrderBy   []OrderByClause // nil if no ORDER BY clause
	Limit     *int64          // nil if no LIMIT clause
}

func (s *UpdateStmt) NodeType() string { return "Update" }
func (s *UpdateStmt) statementNode()   {}

// DeleteStmt represents DELETE FROM <table> [WHERE <condition>] [ORDER BY ...] [LIMIT <n>].
type DeleteStmt struct {
	TableName string
	Where     Expr            // nil if no WHERE clause
	OrderBy   []OrderByClause // nil if no ORDER BY clause
	Limit     *int64          // nil if no LIMIT clause
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

// InExpr represents <expr> [NOT] IN (<expr>, ...) or <expr> [NOT] IN (SELECT ...).
type InExpr struct {
	Left     Expr
	Values   []Expr      // literal IN (nil when Subquery is set)
	Subquery *SelectStmt // IN subquery (nil when Values is set)
	Not      bool        // true for NOT IN
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

// LikeExpr represents <expr> [NOT] LIKE <pattern>.
type LikeExpr struct {
	Left    Expr
	Pattern Expr
	Not     bool // true for NOT LIKE
}

func (e *LikeExpr) NodeType() string { return "Like" }
func (e *LikeExpr) exprNode()        {}

// CreateIndexStmt represents CREATE INDEX <name> ON <table>(<column>, ...).
type CreateIndexStmt struct {
	IndexName   string
	TableName   string
	ColumnNames []string
	Unique      bool
}

func (s *CreateIndexStmt) NodeType() string { return "CreateIndex" }
func (s *CreateIndexStmt) statementNode()   {}

// DropIndexStmt represents DROP INDEX <name>.
type DropIndexStmt struct {
	IndexName string
}

func (s *DropIndexStmt) NodeType() string { return "DropIndex" }
func (s *DropIndexStmt) statementNode()   {}

// AlterTableAddColumnStmt represents ALTER TABLE <name> ADD COLUMN <def>.
type AlterTableAddColumnStmt struct {
	TableName string
	Column    ColumnDef
}

func (s *AlterTableAddColumnStmt) NodeType() string { return "AlterTableAddColumn" }
func (s *AlterTableAddColumnStmt) statementNode()   {}

// AlterTableDropColumnStmt represents ALTER TABLE <name> DROP COLUMN <name>.
type AlterTableDropColumnStmt struct {
	TableName  string
	ColumnName string
}

func (s *AlterTableDropColumnStmt) NodeType() string { return "AlterTableDropColumn" }
func (s *AlterTableDropColumnStmt) statementNode()   {}

// CaseWhen represents a single WHEN ... THEN pair in a CASE expression.
type CaseWhen struct {
	When Expr
	Then Expr
}

// CaseExpr represents a CASE expression.
// Simple CASE: CASE operand WHEN val THEN result ... END
// Searched CASE: CASE WHEN cond THEN result ... END
type CaseExpr struct {
	Operand Expr       // Simple CASE target (nil for Searched CASE)
	Whens   []CaseWhen // WHEN ... THEN pairs
	Else    Expr       // ELSE expression (nil if omitted)
}

func (e *CaseExpr) NodeType() string { return "Case" }
func (e *CaseExpr) exprNode()        {}

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

// NotExpr represents NOT <expr>.
type NotExpr struct {
	Expr Expr
}

func (e *NotExpr) NodeType() string { return "Not" }
func (e *NotExpr) exprNode()        {}

// ExistsExpr represents [NOT] EXISTS (SELECT ...).
type ExistsExpr struct {
	Subquery *SelectStmt
	Not      bool
}

func (e *ExistsExpr) NodeType() string { return "Exists" }
func (e *ExistsExpr) exprNode()        {}

// WindowExpr represents a window function call: NAME([args]) OVER (PARTITION BY ... ORDER BY ...) or NAME([args]) OVER window_name.
type WindowExpr struct {
	Name        string          // "ROW_NUMBER", "RANK", "DENSE_RANK", "SUM", "COUNT", "AVG", "MIN", "MAX"
	Args        []Expr          // nil for ranking functions, e.g. []*IdentExpr for SUM(col)
	WindowName  string          // reference to named window (empty if inline OVER clause)
	PartitionBy []Expr          // nil if no PARTITION BY
	OrderBy     []OrderByClause // nil if no ORDER BY
}

func (e *WindowExpr) NodeType() string { return "Window" }
func (e *WindowExpr) exprNode()        {}

// ScalarExpr represents a scalar subquery: (SELECT ...) that returns a single value.
type ScalarExpr struct {
	Subquery *SelectStmt
}

func (e *ScalarExpr) NodeType() string { return "Scalar" }
func (e *ScalarExpr) exprNode()        {}

// CreateDatabaseStmt represents CREATE DATABASE <name>.
type CreateDatabaseStmt struct {
	DatabaseName string
}

func (s *CreateDatabaseStmt) NodeType() string { return "CreateDatabase" }
func (s *CreateDatabaseStmt) statementNode()   {}

// DropDatabaseStmt represents DROP DATABASE <name>.
type DropDatabaseStmt struct {
	DatabaseName string
}

func (s *DropDatabaseStmt) NodeType() string { return "DropDatabase" }
func (s *DropDatabaseStmt) statementNode()   {}

// UseDatabaseStmt represents USE <name>.
type UseDatabaseStmt struct {
	DatabaseName string
}

func (s *UseDatabaseStmt) NodeType() string { return "UseDatabase" }
func (s *UseDatabaseStmt) statementNode()   {}

// ShowDatabasesStmt represents SHOW DATABASES.
type ShowDatabasesStmt struct{}

func (s *ShowDatabasesStmt) NodeType() string { return "ShowDatabases" }
func (s *ShowDatabasesStmt) statementNode()   {}

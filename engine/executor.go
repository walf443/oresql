package engine

import (
	"fmt"

	"github.com/walf443/oresql/ast"
)

// Result holds the output of a query execution.
type Result struct {
	Columns []string // column names for SELECT results
	Rows    []Row    // data rows for SELECT results
	Message string   // status message for CREATE/INSERT
}

// Executor runs SQL statements.
type Executor struct {
	catalog *Catalog
	storage *Storage
}

func NewExecutor() *Executor {
	return &Executor{
		catalog: NewCatalog(),
		storage: NewStorage(),
	}
}

func (e *Executor) Execute(stmt ast.Statement) (*Result, error) {
	switch s := stmt.(type) {
	case *ast.CreateTableStmt:
		return e.executeCreateTable(s)
	case *ast.InsertStmt:
		return e.executeInsert(s)
	case *ast.SelectStmt:
		return e.executeSelect(s)
	default:
		return nil, fmt.Errorf("unknown statement type: %T", stmt)
	}
}

func (e *Executor) executeCreateTable(stmt *ast.CreateTableStmt) (*Result, error) {
	info, err := e.catalog.CreateTable(stmt.TableName, stmt.Columns)
	if err != nil {
		return nil, err
	}
	e.storage.CreateTable(info)
	return &Result{Message: "table created"}, nil
}

func (e *Executor) executeInsert(stmt *ast.InsertStmt) (*Result, error) {
	info, err := e.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	if len(stmt.Values) != len(info.Columns) {
		return nil, fmt.Errorf("expected %d values, got %d", len(info.Columns), len(stmt.Values))
	}

	row := make(Row, len(info.Columns))
	for i, valExpr := range stmt.Values {
		val, err := evalLiteral(valExpr)
		if err != nil {
			return nil, err
		}

		// Type check
		col := info.Columns[i]
		switch col.DataType {
		case "INT":
			if _, ok := val.(int64); !ok {
				return nil, fmt.Errorf("column %q expects INT, got %T", col.Name, val)
			}
		case "TEXT":
			if _, ok := val.(string); !ok {
				return nil, fmt.Errorf("column %q expects TEXT, got %T", col.Name, val)
			}
		}

		row[i] = val
	}

	if err := e.storage.Insert(stmt.TableName, row); err != nil {
		return nil, err
	}

	return &Result{Message: "1 row inserted"}, nil
}

func (e *Executor) executeSelect(stmt *ast.SelectStmt) (*Result, error) {
	info, err := e.catalog.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Resolve columns
	var colIndices []int
	var colNames []string

	if len(stmt.Columns) == 1 {
		if _, ok := stmt.Columns[0].(*ast.StarExpr); ok {
			for _, col := range info.Columns {
				colIndices = append(colIndices, col.Index)
				colNames = append(colNames, col.Name)
			}
		}
	}

	if colNames == nil {
		for _, colExpr := range stmt.Columns {
			ident, ok := colExpr.(*ast.IdentExpr)
			if !ok {
				return nil, fmt.Errorf("expected column name in SELECT, got %T", colExpr)
			}
			col, err := info.FindColumn(ident.Name)
			if err != nil {
				return nil, err
			}
			colIndices = append(colIndices, col.Index)
			colNames = append(colNames, col.Name)
		}
	}

	// Scan all rows
	allRows, err := e.storage.Scan(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Filter and project
	var resultRows []Row
	for _, row := range allRows {
		// Apply WHERE filter
		if stmt.Where != nil {
			match, err := evalWhere(stmt.Where, row, info)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}

		// Project columns
		projected := make(Row, len(colIndices))
		for i, idx := range colIndices {
			projected[i] = row[idx]
		}
		resultRows = append(resultRows, projected)
	}

	return &Result{Columns: colNames, Rows: resultRows}, nil
}

// evalLiteral evaluates a literal expression (for INSERT VALUES).
func evalLiteral(expr ast.Expr) (Value, error) {
	switch e := expr.(type) {
	case *ast.IntLitExpr:
		return e.Value, nil
	case *ast.StringLitExpr:
		return e.Value, nil
	default:
		return nil, fmt.Errorf("expected literal value, got %T", expr)
	}
}

// evalExpr evaluates an expression against a row.
func evalExpr(expr ast.Expr, row Row, info *TableInfo) (Value, error) {
	switch e := expr.(type) {
	case *ast.IdentExpr:
		col, err := info.FindColumn(e.Name)
		if err != nil {
			return nil, err
		}
		return row[col.Index], nil
	case *ast.IntLitExpr:
		return e.Value, nil
	case *ast.StringLitExpr:
		return e.Value, nil
	case *ast.BinaryExpr:
		left, err := evalExpr(e.Left, row, info)
		if err != nil {
			return nil, err
		}
		right, err := evalExpr(e.Right, row, info)
		if err != nil {
			return nil, err
		}
		return evalComparison(left, e.Op, right)
	case *ast.LogicalExpr:
		left, err := evalExpr(e.Left, row, info)
		if err != nil {
			return nil, err
		}
		leftBool, ok := left.(bool)
		if !ok {
			return nil, fmt.Errorf("expected boolean in %s expression, got %T", e.Op, left)
		}
		right, err := evalExpr(e.Right, row, info)
		if err != nil {
			return nil, err
		}
		rightBool, ok := right.(bool)
		if !ok {
			return nil, fmt.Errorf("expected boolean in %s expression, got %T", e.Op, right)
		}
		switch e.Op {
		case "AND":
			return leftBool && rightBool, nil
		case "OR":
			return leftBool || rightBool, nil
		default:
			return nil, fmt.Errorf("unknown logical operator: %s", e.Op)
		}
	default:
		return nil, fmt.Errorf("cannot evaluate expression: %T", expr)
	}
}

func evalComparison(left Value, op string, right Value) (bool, error) {
	// Both int64
	if lv, ok := left.(int64); ok {
		if rv, ok := right.(int64); ok {
			switch op {
			case "=":
				return lv == rv, nil
			case "!=":
				return lv != rv, nil
			case "<":
				return lv < rv, nil
			case ">":
				return lv > rv, nil
			case "<=":
				return lv <= rv, nil
			case ">=":
				return lv >= rv, nil
			}
		}
	}

	// Both string
	if lv, ok := left.(string); ok {
		if rv, ok := right.(string); ok {
			switch op {
			case "=":
				return lv == rv, nil
			case "!=":
				return lv != rv, nil
			case "<":
				return lv < rv, nil
			case ">":
				return lv > rv, nil
			case "<=":
				return lv <= rv, nil
			case ">=":
				return lv >= rv, nil
			}
		}
	}

	return false, fmt.Errorf("cannot compare %T and %T with %s", left, right, op)
}

// evalWhere evaluates a WHERE expression and returns a boolean.
func evalWhere(expr ast.Expr, row Row, info *TableInfo) (bool, error) {
	val, err := evalExpr(expr, row, info)
	if err != nil {
		return false, err
	}
	b, ok := val.(bool)
	if !ok {
		return false, fmt.Errorf("WHERE expression must evaluate to boolean, got %T", val)
	}
	return b, nil
}

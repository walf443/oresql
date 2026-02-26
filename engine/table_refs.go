package engine

import (
	"strings"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/storage"
)

// lockRef represents a table reference with its required lock mode.
type lockRef struct {
	TableName string
	Mode      storage.TableLockMode
}

// collectLockRefs walks the AST and returns all table references with lock modes,
// plus whether a catalog write lock is needed.
func collectLockRefs(stmt ast.Statement) (refs []lockRef, catalogWrite bool) {
	switch s := stmt.(type) {
	case *ast.CreateTableStmt:
		catalogWrite = true
		// No table lock needed since the table doesn't exist yet

	case *ast.InsertStmt:
		refs = append(refs, lockRef{TableName: s.TableName, Mode: storage.TableLockWrite})
		if s.Select != nil {
			subRefs, _ := collectLockRefs(s.Select)
			refs = append(refs, subRefs...)
		}
		// Also collect refs from value expressions (subqueries in VALUES)
		for _, row := range s.Rows {
			for _, expr := range row {
				refs = append(refs, collectLockExprRefs(expr)...)
			}
		}

	case *ast.SelectStmt:
		refs = append(refs, collectLockSelectRefs(s)...)

	case *ast.UpdateStmt:
		refs = append(refs, lockRef{TableName: s.TableName, Mode: storage.TableLockWrite})
		if s.Where != nil {
			refs = append(refs, collectLockExprRefs(s.Where)...)
		}
		for _, set := range s.Sets {
			refs = append(refs, collectLockExprRefs(set.Value)...)
		}

	case *ast.DeleteStmt:
		refs = append(refs, lockRef{TableName: s.TableName, Mode: storage.TableLockWrite})
		if s.Where != nil {
			refs = append(refs, collectLockExprRefs(s.Where)...)
		}

	case *ast.DropTableStmt:
		catalogWrite = true
		refs = append(refs, lockRef{TableName: s.TableName, Mode: storage.TableLockWrite})

	case *ast.TruncateTableStmt:
		refs = append(refs, lockRef{TableName: s.TableName, Mode: storage.TableLockWrite})

	case *ast.CreateIndexStmt:
		catalogWrite = true
		refs = append(refs, lockRef{TableName: s.TableName, Mode: storage.TableLockWrite})

	case *ast.DropIndexStmt:
		catalogWrite = true
		// Special case: table lock handled in executor since we need to resolve the table name

	case *ast.AlterTableAddColumnStmt:
		catalogWrite = true
		refs = append(refs, lockRef{TableName: s.TableName, Mode: storage.TableLockWrite})

	case *ast.AlterTableDropColumnStmt:
		catalogWrite = true
		refs = append(refs, lockRef{TableName: s.TableName, Mode: storage.TableLockWrite})

	case *ast.SetOpStmt:
		refs = append(refs, collectLockSetOpRefs(s)...)
	}

	return refs, catalogWrite
}

// collectLockSelectRefs collects table references from a SELECT statement.
func collectLockSelectRefs(s *ast.SelectStmt) []lockRef {
	var refs []lockRef

	// FROM table
	if s.TableName != "" {
		refs = append(refs, lockRef{TableName: s.TableName, Mode: storage.TableLockRead})
	}

	// FROM subquery
	if s.FromSubquery != nil {
		subRefs, _ := collectLockRefs(s.FromSubquery)
		refs = append(refs, subRefs...)
	}

	// JOIN tables
	for _, join := range s.Joins {
		refs = append(refs, lockRef{TableName: join.TableName, Mode: storage.TableLockRead})
		if join.On != nil {
			refs = append(refs, collectLockExprRefs(join.On)...)
		}
	}

	// WHERE clause
	if s.Where != nil {
		refs = append(refs, collectLockExprRefs(s.Where)...)
	}

	// SELECT expressions (may contain subqueries)
	for _, col := range s.Columns {
		refs = append(refs, collectLockExprRefs(col)...)
	}

	// GROUP BY
	for _, expr := range s.GroupBy {
		refs = append(refs, collectLockExprRefs(expr)...)
	}

	// HAVING
	if s.Having != nil {
		refs = append(refs, collectLockExprRefs(s.Having)...)
	}

	// ORDER BY
	for _, ob := range s.OrderBy {
		refs = append(refs, collectLockExprRefs(ob.Expr)...)
	}

	return refs
}

// collectLockSetOpRefs collects table references from a set operation.
func collectLockSetOpRefs(s *ast.SetOpStmt) []lockRef {
	var refs []lockRef

	// Left side
	leftRefs, _ := collectLockRefs(s.Left)
	refs = append(refs, leftRefs...)

	// Right side
	rightRefs := collectLockSelectRefs(s.Right)
	refs = append(refs, rightRefs...)

	return refs
}

// collectLockExprRefs collects table references from expressions (subqueries).
func collectLockExprRefs(expr ast.Expr) []lockRef {
	if expr == nil {
		return nil
	}

	var refs []lockRef

	switch e := expr.(type) {
	case *ast.InExpr:
		refs = append(refs, collectLockExprRefs(e.Left)...)
		if e.Subquery != nil {
			refs = append(refs, collectLockSelectRefs(e.Subquery)...)
		}
		for _, v := range e.Values {
			refs = append(refs, collectLockExprRefs(v)...)
		}

	case *ast.ExistsExpr:
		if e.Subquery != nil {
			refs = append(refs, collectLockSelectRefs(e.Subquery)...)
		}

	case *ast.ScalarExpr:
		if e.Subquery != nil {
			refs = append(refs, collectLockSelectRefs(e.Subquery)...)
		}

	case *ast.BinaryExpr:
		refs = append(refs, collectLockExprRefs(e.Left)...)
		refs = append(refs, collectLockExprRefs(e.Right)...)

	case *ast.LogicalExpr:
		refs = append(refs, collectLockExprRefs(e.Left)...)
		refs = append(refs, collectLockExprRefs(e.Right)...)

	case *ast.NotExpr:
		refs = append(refs, collectLockExprRefs(e.Expr)...)

	case *ast.IsNullExpr:
		refs = append(refs, collectLockExprRefs(e.Expr)...)

	case *ast.AliasExpr:
		refs = append(refs, collectLockExprRefs(e.Expr)...)

	case *ast.ArithmeticExpr:
		refs = append(refs, collectLockExprRefs(e.Left)...)
		refs = append(refs, collectLockExprRefs(e.Right)...)

	case *ast.CallExpr:
		for _, arg := range e.Args {
			refs = append(refs, collectLockExprRefs(arg)...)
		}

	case *ast.CastExpr:
		refs = append(refs, collectLockExprRefs(e.Expr)...)

	case *ast.CaseExpr:
		if e.Operand != nil {
			refs = append(refs, collectLockExprRefs(e.Operand)...)
		}
		for _, w := range e.Whens {
			refs = append(refs, collectLockExprRefs(w.When)...)
			refs = append(refs, collectLockExprRefs(w.Then)...)
		}
		if e.Else != nil {
			refs = append(refs, collectLockExprRefs(e.Else)...)
		}

	case *ast.BetweenExpr:
		refs = append(refs, collectLockExprRefs(e.Left)...)
		refs = append(refs, collectLockExprRefs(e.Low)...)
		refs = append(refs, collectLockExprRefs(e.High)...)

	case *ast.LikeExpr:
		refs = append(refs, collectLockExprRefs(e.Left)...)
		refs = append(refs, collectLockExprRefs(e.Pattern)...)

	case *ast.WindowExpr:
		for _, arg := range e.Args {
			refs = append(refs, collectLockExprRefs(arg)...)
		}
		for _, p := range e.PartitionBy {
			refs = append(refs, collectLockExprRefs(p)...)
		}
		for _, ob := range e.OrderBy {
			refs = append(refs, collectLockExprRefs(ob.Expr)...)
		}
	}

	return refs
}

// mergeLockRefs deduplicates table references. If a table appears with both
// Read and Write modes, it is promoted to Write.
func mergeLockRefs(refs []lockRef) []storage.TableLock {
	modes := make(map[string]storage.TableLockMode)
	for _, ref := range refs {
		lower := strings.ToLower(ref.TableName)
		if existing, ok := modes[lower]; ok {
			if ref.Mode == storage.TableLockWrite && existing == storage.TableLockRead {
				modes[lower] = storage.TableLockWrite
			}
		} else {
			modes[lower] = ref.Mode
		}
	}

	locks := make([]storage.TableLock, 0, len(modes))
	for name, mode := range modes {
		locks = append(locks, storage.TableLock{TableName: name, Mode: mode})
	}
	return locks
}

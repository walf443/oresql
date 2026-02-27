package engine

import (
	"fmt"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/lexer"
	"github.com/walf443/oresql/parser"
	"github.com/walf443/oresql/storage"
)

// Result holds the output of a query execution.
type Result struct {
	Columns     []string // column names for SELECT results
	ColumnTypes []string // column types ("INT", "TEXT", "FLOAT") for SELECT results
	Rows        []Row    // data rows for SELECT results
	Message     string   // status message for CREATE/INSERT
}

// Option configures an Executor.
type Option func(*Executor)

// WithWAL sets the WAL for the Executor.
func WithWAL(w *WAL) Option {
	return func(e *Executor) {
		e.wal = w
	}
}

// Executor runs SQL statements.
type Executor struct {
	db  *Database
	wal *WAL
}

// NewExecutor creates a new Executor for the given Database.
func NewExecutor(db *Database, opts ...Option) *Executor {
	e := &Executor{db: db}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// ExecuteSQL parses and executes a SQL string, logging mutating statements to WAL.
func (e *Executor) ExecuteSQL(sql string) (*Result, error) {
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		return nil, err
	}
	result, err := e.Execute(stmt)
	if err != nil {
		return nil, err
	}
	if e.wal != nil {
		_, isSelect := stmt.(*ast.SelectStmt)
		_, isSetOp := stmt.(*ast.SetOpStmt)
		if !isSelect && !isSetOp {
			if err := e.wal.Append(sql); err != nil {
				return nil, fmt.Errorf("WAL write error: %w", err)
			}
		}
	}
	return result, nil
}

// ReplayWAL replays the WAL file to restore state.
func (e *Executor) ReplayWAL() error {
	if e.wal == nil {
		return nil
	}
	wal := e.wal
	e.wal = nil
	defer func() { e.wal = wal }()

	return wal.Replay(func(sql string) error {
		_, err := e.ExecuteSQL(sql)
		return err
	})
}

// isDDL returns true if the statement is a DDL operation that requires
// executor-level locking. DML statements are protected by storage-internal locks.
func isDDL(stmt ast.Statement) bool {
	switch stmt.(type) {
	case *ast.CreateTableStmt, *ast.DropTableStmt, *ast.TruncateTableStmt,
		*ast.CreateIndexStmt, *ast.DropIndexStmt,
		*ast.AlterTableAddColumnStmt, *ast.AlterTableDropColumnStmt:
		return true
	default:
		return false
	}
}

func (e *Executor) Execute(stmt ast.Statement) (*Result, error) {
	// DML: storage methods handle their own locking internally
	if !isDDL(stmt) {
		return e.executeInner(stmt)
	}

	// DDL: acquire executor-level locks
	locker, ok := e.db.storage.(storage.TableLocker)
	if !ok {
		return e.executeInner(stmt)
	}

	refs, catalogWrite := collectLockRefs(stmt)

	// Special case: CreateTable — table doesn't exist yet, no table locks needed.
	// Storage methods (CreateTable, CreateIndex) handle s.mu internally.
	if _, isCreate := stmt.(*ast.CreateTableStmt); isCreate {
		return e.executeInner(stmt)
	}

	// Special case: DropIndex — AST doesn't contain table name, resolve first
	if dropIdx, isDropIdx := stmt.(*ast.DropIndexStmt); isDropIdx {
		tableName, found := locker.ResolveIndexTable(dropIdx.IndexName)
		if !found {
			return nil, fmt.Errorf("index %q does not exist", dropIdx.IndexName)
		}
		refs = append(refs, lockRef{TableName: tableName, Mode: storage.TableLockWrite})
	}

	locks := mergeLockRefs(refs)

	var result *Result
	err := locker.WithTableLocks(locks, catalogWrite, func() error {
		var execErr error
		result, execErr = e.executeInner(stmt)
		return execErr
	})
	return result, err
}

func (e *Executor) executeInner(stmt ast.Statement) (*Result, error) {
	switch s := stmt.(type) {
	case *ast.CreateTableStmt:
		return e.executeCreateTable(s)
	case *ast.InsertStmt:
		return e.executeInsert(s)
	case *ast.SelectStmt:
		return e.executeSelect(s)
	case *ast.UpdateStmt:
		return e.executeUpdate(s)
	case *ast.DeleteStmt:
		return e.executeDelete(s)
	case *ast.DropTableStmt:
		return e.executeDropTable(s)
	case *ast.TruncateTableStmt:
		return e.executeTruncateTable(s)
	case *ast.CreateIndexStmt:
		return e.executeCreateIndex(s)
	case *ast.DropIndexStmt:
		return e.executeDropIndex(s)
	case *ast.AlterTableAddColumnStmt:
		return e.executeAlterTableAddColumn(s)
	case *ast.AlterTableDropColumnStmt:
		return e.executeAlterTableDropColumn(s)
	case *ast.SetOpStmt:
		return e.executeSetOp(s)
	default:
		return nil, fmt.Errorf("unknown statement type: %T", stmt)
	}
}

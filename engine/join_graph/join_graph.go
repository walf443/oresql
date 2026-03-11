// Package join_graph provides data types for representing join graph structures
// used in query planning and optimization, with no dependency on the engine package.
package join_graph

import (
	"strings"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/storage"
)

// Node represents a single table in the join graph.
type Node struct {
	TableName  string             // lowercase real table name
	Alias      string             // alias (empty if none)
	Info       *storage.TableInfo // schema info
	Storage    storage.Engine     // storage engine for this table
	Rows       []storage.Row      // pre-materialized rows (non-nil for FROM subquery)
	LocalWhere []ast.Expr         // WHERE conditions referencing only this table
}

// EffectiveName returns the alias if present, otherwise the table name.
func (n *Node) EffectiveName() string {
	if n.Alias != "" {
		return strings.ToLower(n.Alias)
	}
	return n.TableName
}

// Edge represents an ON relationship between two tables.
type Edge struct {
	TableA, TableB string         // effective names (alias preferred)
	JoinType       string         // ast.JoinInner or ast.JoinLeft
	OnExpr         ast.Expr       // original ON expression
	EquiJoinPairs  []EquiJoinPair // equi-join pairs extracted from ON
	ResidualOn     ast.Expr       // ON conditions not covered by equi-join pairs
	IndexOnA       bool           // whether A's equi-join column has an index
	IndexOnB       bool           // whether B's equi-join column has an index
}

// Graph represents the complete join plan structure.
type Graph struct {
	Nodes      map[string]*Node    // effective name -> node
	Edges      map[string]*Edge    // "tableA\x00tableB" -> edge
	Adjacency  map[string][]string // effective name -> connected effective names
	CrossWhere []ast.Expr          // WHERE conditions spanning multiple tables
	FromTable  string              // effective name of the FROM table
	TableOrder []string            // original FROM + JOIN1 + JOIN2... order (effective names)
	UsingCols  map[string][]string // effective name -> USING column names (for SELECT * dedup)
}

// EquiJoinPair represents an equi-join condition between two tables.
type EquiJoinPair struct {
	LeftTable, LeftCol   string // lowercase table name, lowercase column name
	RightTable, RightCol string
}

// TableInfo holds table metadata for join optimization.
type TableInfo struct {
	Info          *storage.TableInfo
	TableName     string // lowercase
	Alias         string
	EffectiveName string // join graph node key (alias if present, otherwise tableName)
}

// MatchesTable returns true if the given qualifier (table name or alias) matches this table.
func (jt *TableInfo) MatchesTable(qualifier string) bool {
	if qualifier == "" {
		return false
	}
	lower := strings.ToLower(qualifier)
	return lower == jt.TableName || (jt.Alias != "" && lower == strings.ToLower(jt.Alias))
}

// EdgeKey returns a canonical key for an edge between two tables.
// The key is ordered lexicographically to ensure consistency.
func EdgeKey(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return a + "\x00" + b
}

// EffectiveNameForJoin returns the effective name for a JoinClause.
func EffectiveNameForJoin(join ast.JoinClause) string {
	if join.TableAlias != "" {
		return strings.ToLower(join.TableAlias)
	}
	return strings.ToLower(join.TableName)
}

// ResolveUnqualifiedTableN determines which table an unqualified column belongs to
// among N tables. Returns the effective name of the table, or "" if ambiguous or not found.
func ResolveUnqualifiedTableN(colName string, nodes map[string]*Node) string {
	lower := strings.ToLower(colName)
	var found string
	count := 0
	for effName, node := range nodes {
		for _, col := range node.Info.Columns {
			if strings.ToLower(col.Name) == lower {
				found = effName
				count++
				break
			}
		}
	}
	if count == 1 {
		return found
	}
	return "" // ambiguous or not found
}

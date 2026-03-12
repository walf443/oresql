package join_graph

import (
	"testing"

	"github.com/walf443/oresql/ast"
	"github.com/walf443/oresql/storage"
)

func TestNode_EffectiveName(t *testing.T) {
	tests := []struct {
		name     string
		node     Node
		expected string
	}{
		{
			name:     "with alias returns lowercase alias",
			node:     Node{TableName: "users", Alias: "U"},
			expected: "u",
		},
		{
			name:     "with lowercase alias returns alias",
			node:     Node{TableName: "users", Alias: "u"},
			expected: "u",
		},
		{
			name:     "without alias returns table name",
			node:     Node{TableName: "users", Alias: ""},
			expected: "users",
		},
		{
			name:     "alias with mixed case",
			node:     Node{TableName: "orders", Alias: "MyAlias"},
			expected: "myalias",
		},
		{
			name:     "empty alias and table name",
			node:     Node{TableName: "", Alias: ""},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.node.EffectiveName()
			if got != tt.expected {
				t.Errorf("EffectiveName() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestTableInfo_MatchesTable(t *testing.T) {
	ti := &TableInfo{
		TableName:     "users",
		Alias:         "u",
		EffectiveName: "u",
	}

	tests := []struct {
		name      string
		info      *TableInfo
		qualifier string
		expected  bool
	}{
		{
			name:      "matches table name",
			info:      ti,
			qualifier: "users",
			expected:  true,
		},
		{
			name:      "matches alias",
			info:      ti,
			qualifier: "u",
			expected:  true,
		},
		{
			name:      "case insensitive table name",
			info:      ti,
			qualifier: "USERS",
			expected:  true,
		},
		{
			name:      "case insensitive alias",
			info:      ti,
			qualifier: "U",
			expected:  true,
		},
		{
			name:      "non-matching qualifier",
			info:      ti,
			qualifier: "orders",
			expected:  false,
		},
		{
			name:      "empty qualifier",
			info:      ti,
			qualifier: "",
			expected:  false,
		},
		{
			name: "no alias only matches table name",
			info: &TableInfo{
				TableName:     "orders",
				Alias:         "",
				EffectiveName: "orders",
			},
			qualifier: "orders",
			expected:  true,
		},
		{
			name: "no alias does not match random string",
			info: &TableInfo{
				TableName:     "orders",
				Alias:         "",
				EffectiveName: "orders",
			},
			qualifier: "o",
			expected:  false,
		},
		{
			name: "mixed case alias match",
			info: &TableInfo{
				TableName:     "products",
				Alias:         "Prod",
				EffectiveName: "prod",
			},
			qualifier: "prod",
			expected:  true,
		},
		{
			name: "mixed case alias with uppercase qualifier",
			info: &TableInfo{
				TableName:     "products",
				Alias:         "Prod",
				EffectiveName: "prod",
			},
			qualifier: "PROD",
			expected:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.info.MatchesTable(tt.qualifier)
			if got != tt.expected {
				t.Errorf("MatchesTable(%q) = %v, want %v", tt.qualifier, got, tt.expected)
			}
		})
	}
}

func TestEdgeKey(t *testing.T) {
	tests := []struct {
		name     string
		a, b     string
		expected string
	}{
		{
			name:     "a < b returns a then b",
			a:        "alpha",
			b:        "beta",
			expected: "alpha\x00beta",
		},
		{
			name:     "a > b returns canonical order b then a",
			a:        "beta",
			b:        "alpha",
			expected: "alpha\x00beta",
		},
		{
			name:     "same strings",
			a:        "table",
			b:        "table",
			expected: "table\x00table",
		},
		{
			name:     "a equals b empty",
			a:        "",
			b:        "",
			expected: "\x00",
		},
		{
			name:     "one empty a < b",
			a:        "",
			b:        "z",
			expected: "\x00z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EdgeKey(tt.a, tt.b)
			if got != tt.expected {
				t.Errorf("EdgeKey(%q, %q) = %q, want %q", tt.a, tt.b, got, tt.expected)
			}
		})
	}

	// Verify symmetry: EdgeKey(a,b) == EdgeKey(b,a) for all cases
	t.Run("symmetry", func(t *testing.T) {
		pairs := [][2]string{
			{"users", "orders"},
			{"a", "b"},
			{"z", "a"},
			{"same", "same"},
		}
		for _, pair := range pairs {
			k1 := EdgeKey(pair[0], pair[1])
			k2 := EdgeKey(pair[1], pair[0])
			if k1 != k2 {
				t.Errorf("EdgeKey(%q,%q) = %q != EdgeKey(%q,%q) = %q", pair[0], pair[1], k1, pair[1], pair[0], k2)
			}
		}
	})
}

func TestEffectiveNameForJoin(t *testing.T) {
	tests := []struct {
		name     string
		join     ast.JoinClause
		expected string
	}{
		{
			name: "with alias returns lowercase alias",
			join: ast.JoinClause{
				TableName:  "Users",
				TableAlias: "U",
			},
			expected: "u",
		},
		{
			name: "without alias returns lowercase table name",
			join: ast.JoinClause{
				TableName:  "Users",
				TableAlias: "",
			},
			expected: "users",
		},
		{
			name: "mixed case alias",
			join: ast.JoinClause{
				TableName:  "OrderItems",
				TableAlias: "OI",
			},
			expected: "oi",
		},
		{
			name: "already lowercase table name",
			join: ast.JoinClause{
				TableName:  "products",
				TableAlias: "",
			},
			expected: "products",
		},
		{
			name: "already lowercase alias",
			join: ast.JoinClause{
				TableName:  "Products",
				TableAlias: "p",
			},
			expected: "p",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EffectiveNameForJoin(tt.join)
			if got != tt.expected {
				t.Errorf("EffectiveNameForJoin() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestResolveUnqualifiedTableN(t *testing.T) {
	usersInfo := &storage.TableInfo{
		Name: "users",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
			{Name: "name", DataType: "TEXT", Index: 1},
			{Name: "email", DataType: "TEXT", Index: 2},
		},
	}
	ordersInfo := &storage.TableInfo{
		Name: "orders",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
			{Name: "user_id", DataType: "INT", Index: 1},
			{Name: "total", DataType: "INT", Index: 2},
		},
	}
	productsInfo := &storage.TableInfo{
		Name: "products",
		Columns: []storage.ColumnInfo{
			{Name: "product_id", DataType: "INT", Index: 0},
			{Name: "name", DataType: "TEXT", Index: 1},
			{Name: "price", DataType: "INT", Index: 2},
		},
	}

	twoTableNodes := map[string]*Node{
		"users":  {TableName: "users", Info: usersInfo},
		"orders": {TableName: "orders", Info: ordersInfo},
	}

	threeTableNodes := map[string]*Node{
		"users":    {TableName: "users", Info: usersInfo},
		"orders":   {TableName: "orders", Info: ordersInfo},
		"products": {TableName: "products", Info: productsInfo},
	}

	tests := []struct {
		name     string
		colName  string
		nodes    map[string]*Node
		expected string
	}{
		{
			name:     "unique column in one table",
			colName:  "email",
			nodes:    twoTableNodes,
			expected: "users",
		},
		{
			name:     "unique column user_id in orders",
			colName:  "user_id",
			nodes:    twoTableNodes,
			expected: "orders",
		},
		{
			name:     "unique column total in orders",
			colName:  "total",
			nodes:    twoTableNodes,
			expected: "orders",
		},
		{
			name:     "ambiguous column id in two tables",
			colName:  "id",
			nodes:    twoTableNodes,
			expected: "",
		},
		{
			name:     "column not found",
			colName:  "nonexistent",
			nodes:    twoTableNodes,
			expected: "",
		},
		{
			name:     "case insensitive column lookup",
			colName:  "EMAIL",
			nodes:    twoTableNodes,
			expected: "users",
		},
		{
			name:     "ambiguous name in three tables (users and products)",
			colName:  "name",
			nodes:    threeTableNodes,
			expected: "",
		},
		{
			name:     "unique column product_id in three tables",
			colName:  "product_id",
			nodes:    threeTableNodes,
			expected: "products",
		},
		{
			name:     "unique column price in three tables",
			colName:  "price",
			nodes:    threeTableNodes,
			expected: "products",
		},
		{
			name:     "empty nodes map",
			colName:  "id",
			nodes:    map[string]*Node{},
			expected: "",
		},
		{
			name:    "single table unique column",
			colName: "email",
			nodes: map[string]*Node{
				"users": {TableName: "users", Info: usersInfo},
			},
			expected: "users",
		},
		{
			name:    "single table column found by case insensitive match",
			colName: "Name",
			nodes: map[string]*Node{
				"users": {TableName: "users", Info: usersInfo},
			},
			expected: "users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveUnqualifiedTableN(tt.colName, tt.nodes)
			if got != tt.expected {
				t.Errorf("ResolveUnqualifiedTableN(%q) = %q, want %q", tt.colName, got, tt.expected)
			}
		})
	}
}

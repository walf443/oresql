package engine

import (
	"testing"

	"github.com/walf443/oresql/ast"
)

func TestExtractAllEquiJoinPairs(t *testing.T) {
	usersInfo := &TableInfo{
		Name: "users",
		Columns: []ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
	}
	ordersInfo := &TableInfo{
		Name: "orders",
		Columns: []ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
			{Name: "user_id", DataType: "INT", Index: 1},
			{Name: "name", DataType: "TEXT", Index: 2},
		},
	}
	tableA := &joinTableInfo{info: usersInfo, tableName: "users", alias: "u"}
	tableB := &joinTableInfo{info: ordersInfo, tableName: "orders", alias: "o"}

	t.Run("single pair", func(t *testing.T) {
		on := &ast.BinaryExpr{
			Left:  &ast.IdentExpr{Table: "u", Name: "id"},
			Op:    "=",
			Right: &ast.IdentExpr{Table: "o", Name: "user_id"},
		}
		pairs, residual := extractAllEquiJoinPairs(on, tableA, tableB)
		if len(pairs) != 1 {
			t.Fatalf("expected 1 pair, got %d", len(pairs))
		}
		if pairs[0].leftCol != "id" || pairs[0].rightCol != "user_id" {
			t.Errorf("pair = %s.%s = %s.%s", pairs[0].leftTable, pairs[0].leftCol, pairs[0].rightTable, pairs[0].rightCol)
		}
		if residual != nil {
			t.Errorf("expected no residual, got %v", residual)
		}
	})

	t.Run("multiple pairs", func(t *testing.T) {
		// u.id = o.user_id AND u.name = o.name
		on := &ast.LogicalExpr{
			Left: &ast.BinaryExpr{
				Left:  &ast.IdentExpr{Table: "u", Name: "id"},
				Op:    "=",
				Right: &ast.IdentExpr{Table: "o", Name: "user_id"},
			},
			Op: "AND",
			Right: &ast.BinaryExpr{
				Left:  &ast.IdentExpr{Table: "u", Name: "name"},
				Op:    "=",
				Right: &ast.IdentExpr{Table: "o", Name: "name"},
			},
		}
		pairs, residual := extractAllEquiJoinPairs(on, tableA, tableB)
		if len(pairs) != 2 {
			t.Fatalf("expected 2 pairs, got %d", len(pairs))
		}
		if residual != nil {
			t.Errorf("expected no residual, got %v", residual)
		}
	})

	t.Run("mixed equi and non-equi", func(t *testing.T) {
		// u.id = o.user_id AND u.name > o.name
		on := &ast.LogicalExpr{
			Left: &ast.BinaryExpr{
				Left:  &ast.IdentExpr{Table: "u", Name: "id"},
				Op:    "=",
				Right: &ast.IdentExpr{Table: "o", Name: "user_id"},
			},
			Op: "AND",
			Right: &ast.BinaryExpr{
				Left:  &ast.IdentExpr{Table: "u", Name: "name"},
				Op:    ">",
				Right: &ast.IdentExpr{Table: "o", Name: "name"},
			},
		}
		pairs, residual := extractAllEquiJoinPairs(on, tableA, tableB)
		if len(pairs) != 1 {
			t.Fatalf("expected 1 pair, got %d", len(pairs))
		}
		if residual == nil {
			t.Error("expected residual, got nil")
		}
	})

	t.Run("no equi pairs", func(t *testing.T) {
		on := &ast.BinaryExpr{
			Left:  &ast.IdentExpr{Table: "u", Name: "id"},
			Op:    ">",
			Right: &ast.IdentExpr{Table: "o", Name: "user_id"},
		}
		pairs, residual := extractAllEquiJoinPairs(on, tableA, tableB)
		if len(pairs) != 0 {
			t.Errorf("expected 0 pairs, got %d", len(pairs))
		}
		if residual == nil {
			t.Error("expected residual, got nil")
		}
	})
}

func TestResolveUnqualifiedTableN(t *testing.T) {
	nodes := map[string]*JoinGraphNode{
		"users": {
			TableName: "users",
			Info: &TableInfo{
				Name: "users",
				Columns: []ColumnInfo{
					{Name: "id", DataType: "INT", Index: 0},
					{Name: "name", DataType: "TEXT", Index: 1},
					{Name: "status", DataType: "TEXT", Index: 2},
				},
			},
		},
		"orders": {
			TableName: "orders",
			Info: &TableInfo{
				Name: "orders",
				Columns: []ColumnInfo{
					{Name: "id", DataType: "INT", Index: 0},
					{Name: "user_id", DataType: "INT", Index: 1},
					{Name: "amount", DataType: "INT", Index: 2},
				},
			},
		},
		"items": {
			TableName: "items",
			Info: &TableInfo{
				Name: "items",
				Columns: []ColumnInfo{
					{Name: "id", DataType: "INT", Index: 0},
					{Name: "order_id", DataType: "INT", Index: 1},
					{Name: "product", DataType: "TEXT", Index: 2},
				},
			},
		},
	}

	t.Run("unique column", func(t *testing.T) {
		// "status" only in users
		got := resolveUnqualifiedTableN("status", nodes)
		if got != "users" {
			t.Errorf("got %q, want %q", got, "users")
		}
	})

	t.Run("unique column user_id", func(t *testing.T) {
		// "user_id" only in orders
		got := resolveUnqualifiedTableN("user_id", nodes)
		if got != "orders" {
			t.Errorf("got %q, want %q", got, "orders")
		}
	})

	t.Run("ambiguous column", func(t *testing.T) {
		// "id" in all three
		got := resolveUnqualifiedTableN("id", nodes)
		if got != "" {
			t.Errorf("got %q, want empty (ambiguous)", got)
		}
	})

	t.Run("not found column", func(t *testing.T) {
		got := resolveUnqualifiedTableN("nonexistent", nodes)
		if got != "" {
			t.Errorf("got %q, want empty (not found)", got)
		}
	})
}

func TestBuildJoinGraph_TwoTables(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, amount INT)")
	run(t, exec, "CREATE INDEX idx_orders_user_id ON orders(user_id)")

	stmt := &ast.SelectStmt{
		Columns:    []ast.Expr{&ast.StarExpr{}},
		TableName:  "users",
		TableAlias: "u",
		Joins: []ast.JoinClause{
			{
				TableName:  "orders",
				TableAlias: "o",
				On: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Table: "u", Name: "id"},
					Op:    "=",
					Right: &ast.IdentExpr{Table: "o", Name: "user_id"},
				},
			},
		},
	}

	graph, err := exec.buildJoinGraph(stmt)
	if err != nil {
		t.Fatalf("buildJoinGraph error: %v", err)
	}

	if len(graph.Nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(graph.Nodes))
	}
	if len(graph.Edges) != 1 {
		t.Errorf("expected 1 edge, got %d", len(graph.Edges))
	}

	// Check edge
	key := edgeKey("u", "o")
	edge, ok := graph.Edges[key]
	if !ok {
		t.Fatal("edge u-o not found")
	}
	if len(edge.EquiJoinPairs) != 1 {
		t.Errorf("expected 1 equi-join pair, got %d", len(edge.EquiJoinPairs))
	}
	if !edge.IndexOnB {
		t.Error("expected IndexOnB to be true (orders has index on user_id)")
	}
}

func TestBuildJoinGraph_ThreeTables(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, amount INT)")
	run(t, exec, "CREATE TABLE items (id INT, order_id INT, product TEXT)")

	stmt := &ast.SelectStmt{
		Columns:    []ast.Expr{&ast.StarExpr{}},
		TableName:  "users",
		TableAlias: "u",
		Joins: []ast.JoinClause{
			{
				TableName:  "orders",
				TableAlias: "o",
				On: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Table: "u", Name: "id"},
					Op:    "=",
					Right: &ast.IdentExpr{Table: "o", Name: "user_id"},
				},
			},
			{
				TableName:  "items",
				TableAlias: "i",
				On: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Table: "o", Name: "id"},
					Op:    "=",
					Right: &ast.IdentExpr{Table: "i", Name: "order_id"},
				},
			},
		},
	}

	graph, err := exec.buildJoinGraph(stmt)
	if err != nil {
		t.Fatalf("buildJoinGraph error: %v", err)
	}

	if len(graph.Nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(graph.Nodes))
	}
	if len(graph.Edges) != 2 {
		t.Errorf("expected 2 edges, got %d", len(graph.Edges))
	}
	if len(graph.TableOrder) != 3 {
		t.Errorf("expected TableOrder length 3, got %d", len(graph.TableOrder))
	}

	// Check adjacency
	if len(graph.Adjacency["o"]) != 2 {
		t.Errorf("expected orders to have 2 neighbors, got %d", len(graph.Adjacency["o"]))
	}
}

func TestBuildJoinGraph_WhereClassification(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, status TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, amount INT)")
	run(t, exec, "CREATE TABLE items (id INT, order_id INT, product TEXT)")

	stmt := &ast.SelectStmt{
		Columns:    []ast.Expr{&ast.StarExpr{}},
		TableName:  "users",
		TableAlias: "u",
		Joins: []ast.JoinClause{
			{
				TableName:  "orders",
				TableAlias: "o",
				On: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Table: "u", Name: "id"},
					Op:    "=",
					Right: &ast.IdentExpr{Table: "o", Name: "user_id"},
				},
			},
			{
				TableName:  "items",
				TableAlias: "i",
				On: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Table: "o", Name: "id"},
					Op:    "=",
					Right: &ast.IdentExpr{Table: "i", Name: "order_id"},
				},
			},
		},
		// WHERE u.status = 'active' AND o.amount > 100 AND i.product = 'widget'
		Where: &ast.LogicalExpr{
			Left: &ast.LogicalExpr{
				Left: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Table: "u", Name: "status"},
					Op:    "=",
					Right: &ast.StringLitExpr{Value: "active"},
				},
				Op: "AND",
				Right: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Table: "o", Name: "amount"},
					Op:    ">",
					Right: &ast.IntLitExpr{Value: 100},
				},
			},
			Op: "AND",
			Right: &ast.BinaryExpr{
				Left:  &ast.IdentExpr{Table: "i", Name: "product"},
				Op:    "=",
				Right: &ast.StringLitExpr{Value: "widget"},
			},
		},
	}

	graph, err := exec.buildJoinGraph(stmt)
	if err != nil {
		t.Fatalf("buildJoinGraph error: %v", err)
	}

	// Check WHERE classification
	if len(graph.Nodes["u"].LocalWhere) != 1 {
		t.Errorf("users LocalWhere = %d, want 1", len(graph.Nodes["u"].LocalWhere))
	}
	if len(graph.Nodes["o"].LocalWhere) != 1 {
		t.Errorf("orders LocalWhere = %d, want 1", len(graph.Nodes["o"].LocalWhere))
	}
	if len(graph.Nodes["i"].LocalWhere) != 1 {
		t.Errorf("items LocalWhere = %d, want 1", len(graph.Nodes["i"].LocalWhere))
	}
	if len(graph.CrossWhere) != 0 {
		t.Errorf("CrossWhere = %d, want 0", len(graph.CrossWhere))
	}
}

func TestBuildJoinGraph_IndexAnnotation(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT)")
	run(t, exec, "CREATE INDEX idx_orders_uid ON orders(user_id)")

	stmt := &ast.SelectStmt{
		Columns:    []ast.Expr{&ast.StarExpr{}},
		TableName:  "users",
		TableAlias: "u",
		Joins: []ast.JoinClause{
			{
				TableName:  "orders",
				TableAlias: "o",
				On: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Table: "u", Name: "id"},
					Op:    "=",
					Right: &ast.IdentExpr{Table: "o", Name: "user_id"},
				},
			},
		},
	}

	graph, err := exec.buildJoinGraph(stmt)
	if err != nil {
		t.Fatalf("buildJoinGraph error: %v", err)
	}

	key := edgeKey("u", "o")
	edge := graph.Edges[key]
	if edge == nil {
		t.Fatal("edge u-o not found")
	}
	// users.id has no index, orders.user_id has index
	if edge.IndexOnA {
		t.Error("expected IndexOnA = false (no index on users.id)")
	}
	if !edge.IndexOnB {
		t.Error("expected IndexOnB = true (index on orders.user_id)")
	}
}

func TestOptimizeJoinOrder_PrefersIndexed(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT)")
	run(t, exec, "CREATE INDEX idx_orders_uid ON orders(user_id)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice')")
	run(t, exec, "INSERT INTO orders VALUES (1, 1)")

	stmt := &ast.SelectStmt{
		Columns:    []ast.Expr{&ast.StarExpr{}},
		TableName:  "users",
		TableAlias: "u",
		Joins: []ast.JoinClause{
			{
				TableName:  "orders",
				TableAlias: "o",
				On: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Table: "u", Name: "id"},
					Op:    "=",
					Right: &ast.IdentExpr{Table: "o", Name: "user_id"},
				},
			},
		},
	}

	graph, err := exec.buildJoinGraph(stmt)
	if err != nil {
		t.Fatalf("buildJoinGraph error: %v", err)
	}

	order := exec.OptimizeJoinOrder(graph)
	if len(order) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(order))
	}
	// Without any WHERE, original order should be preserved (users first)
	if order[0] != "u" {
		t.Errorf("expected driving table 'u', got %q", order[0])
	}
}

func TestOptimizeJoinOrder_PrefersFiltered(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, status TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT)")
	run(t, exec, "CREATE INDEX idx_users_status ON users(status)")
	run(t, exec, "INSERT INTO users VALUES (1, 'alice', 'active')")
	run(t, exec, "INSERT INTO orders VALUES (1, 1)")

	stmt := &ast.SelectStmt{
		Columns:    []ast.Expr{&ast.StarExpr{}},
		TableName:  "orders",
		TableAlias: "o",
		Joins: []ast.JoinClause{
			{
				TableName:  "users",
				TableAlias: "u",
				On: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Table: "u", Name: "id"},
					Op:    "=",
					Right: &ast.IdentExpr{Table: "o", Name: "user_id"},
				},
			},
		},
		// WHERE u.status = 'active'
		Where: &ast.BinaryExpr{
			Left:  &ast.IdentExpr{Table: "u", Name: "status"},
			Op:    "=",
			Right: &ast.StringLitExpr{Value: "active"},
		},
	}

	graph, err := exec.buildJoinGraph(stmt)
	if err != nil {
		t.Fatalf("buildJoinGraph error: %v", err)
	}

	order := exec.OptimizeJoinOrder(graph)
	// users has WHERE with index -> should be driving table
	if order[0] != "u" {
		t.Errorf("expected driving table 'u' (has indexed WHERE), got %q", order[0])
	}
}

func TestOptimizeJoinOrder_ThreeTables(t *testing.T) {
	exec := NewExecutor()
	run(t, exec, "CREATE TABLE users (id INT, name TEXT, status TEXT)")
	run(t, exec, "CREATE TABLE orders (id INT, user_id INT, amount INT)")
	run(t, exec, "CREATE TABLE items (id INT, order_id INT, product TEXT)")
	run(t, exec, "CREATE INDEX idx_users_status ON users(status)")
	run(t, exec, "CREATE INDEX idx_orders_uid ON orders(user_id)")
	run(t, exec, "CREATE INDEX idx_items_oid ON items(order_id)")

	stmt := &ast.SelectStmt{
		Columns:    []ast.Expr{&ast.StarExpr{}},
		TableName:  "users",
		TableAlias: "u",
		Joins: []ast.JoinClause{
			{
				TableName:  "orders",
				TableAlias: "o",
				On: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Table: "u", Name: "id"},
					Op:    "=",
					Right: &ast.IdentExpr{Table: "o", Name: "user_id"},
				},
			},
			{
				TableName:  "items",
				TableAlias: "i",
				On: &ast.BinaryExpr{
					Left:  &ast.IdentExpr{Table: "o", Name: "id"},
					Op:    "=",
					Right: &ast.IdentExpr{Table: "i", Name: "order_id"},
				},
			},
		},
		// WHERE u.status = 'active'
		Where: &ast.BinaryExpr{
			Left:  &ast.IdentExpr{Table: "u", Name: "status"},
			Op:    "=",
			Right: &ast.StringLitExpr{Value: "active"},
		},
	}

	graph, err := exec.buildJoinGraph(stmt)
	if err != nil {
		t.Fatalf("buildJoinGraph error: %v", err)
	}

	order := exec.OptimizeJoinOrder(graph)
	if len(order) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(order))
	}
	// users has indexed WHERE -> driving table
	if order[0] != "u" {
		t.Errorf("expected driving table 'u', got %q", order[0])
	}
	// orders and items both have indexed equi-join columns
	// orders should come next as it connects to users
	if order[1] != "o" {
		t.Errorf("expected second table 'o', got %q", order[1])
	}
	if order[2] != "i" {
		t.Errorf("expected third table 'i', got %q", order[2])
	}
}

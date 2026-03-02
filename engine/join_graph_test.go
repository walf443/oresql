package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	tableA := &joinTableInfo{info: usersInfo, tableName: "users", alias: "u", effectiveName: "u"}
	tableB := &joinTableInfo{info: ordersInfo, tableName: "orders", alias: "o", effectiveName: "o"}

	t.Run("single pair", func(t *testing.T) {
		on := &ast.BinaryExpr{
			Left:  &ast.IdentExpr{Table: "u", Name: "id"},
			Op:    "=",
			Right: &ast.IdentExpr{Table: "o", Name: "user_id"},
		}
		pairs, residual := extractAllEquiJoinPairs(on, tableA, tableB)
		require.Len(t, pairs, 1, "expected 1 equi-join pair")
		assert.Equal(t, "id", pairs[0].leftCol)
		assert.Equal(t, "user_id", pairs[0].rightCol)
		assert.Nil(t, residual, "expected no residual")
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
		require.Len(t, pairs, 2, "expected 2 equi-join pairs")
		assert.Nil(t, residual, "expected no residual")
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
		require.Len(t, pairs, 1, "expected 1 equi-join pair")
		assert.NotNil(t, residual, "expected residual for non-equi condition")
	})

	t.Run("no equi pairs", func(t *testing.T) {
		on := &ast.BinaryExpr{
			Left:  &ast.IdentExpr{Table: "u", Name: "id"},
			Op:    ">",
			Right: &ast.IdentExpr{Table: "o", Name: "user_id"},
		}
		pairs, residual := extractAllEquiJoinPairs(on, tableA, tableB)
		assert.Len(t, pairs, 0, "expected 0 equi-join pairs")
		assert.NotNil(t, residual, "expected residual for non-equi condition")
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
		assert.Equal(t, "users", got)
	})

	t.Run("unique column user_id", func(t *testing.T) {
		// "user_id" only in orders
		got := resolveUnqualifiedTableN("user_id", nodes)
		assert.Equal(t, "orders", got)
	})

	t.Run("ambiguous column", func(t *testing.T) {
		// "id" in all three
		got := resolveUnqualifiedTableN("id", nodes)
		assert.Equal(t, "", got, "expected empty (ambiguous)")
	})

	t.Run("not found column", func(t *testing.T) {
		got := resolveUnqualifiedTableN("nonexistent", nodes)
		assert.Equal(t, "", got, "expected empty (not found)")
	})
}

func TestBuildJoinGraph_TwoTables(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
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
	require.NoError(t, err, "buildJoinGraph error")

	assert.Len(t, graph.Nodes, 2, "expected 2 nodes")
	assert.Len(t, graph.Edges, 1, "expected 1 edge")

	// Check edge
	key := edgeKey("u", "o")
	edge, ok := graph.Edges[key]
	require.True(t, ok, "edge u-o not found")
	assert.Len(t, edge.EquiJoinPairs, 1, "expected 1 equi-join pair")
	assert.True(t, edge.IndexOnB, "expected IndexOnB to be true (orders has index on user_id)")
}

func TestBuildJoinGraph_ThreeTables(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
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
	require.NoError(t, err, "buildJoinGraph error")

	assert.Len(t, graph.Nodes, 3, "expected 3 nodes")
	assert.Len(t, graph.Edges, 2, "expected 2 edges")
	assert.Len(t, graph.TableOrder, 3, "expected TableOrder length 3")

	// Check adjacency
	assert.Len(t, graph.Adjacency["o"], 2, "expected orders to have 2 neighbors")
}

func TestBuildJoinGraph_WhereClassification(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
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
	require.NoError(t, err, "buildJoinGraph error")

	// Check WHERE classification
	assert.Len(t, graph.Nodes["u"].LocalWhere, 1, "users LocalWhere count")
	assert.Len(t, graph.Nodes["o"].LocalWhere, 1, "orders LocalWhere count")
	assert.Len(t, graph.Nodes["i"].LocalWhere, 1, "items LocalWhere count")
	assert.Len(t, graph.CrossWhere, 0, "CrossWhere count")
}

func TestBuildJoinGraph_IndexAnnotation(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
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
	require.NoError(t, err, "buildJoinGraph error")

	key := edgeKey("u", "o")
	edge := graph.Edges[key]
	require.NotNil(t, edge, "edge u-o not found")
	// users.id has no index, orders.user_id has index
	assert.False(t, edge.IndexOnA, "expected IndexOnA = false (no index on users.id)")
	assert.True(t, edge.IndexOnB, "expected IndexOnB = true (index on orders.user_id)")
}

func TestOptimizeJoinOrder_PrefersIndexed(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
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
	require.NoError(t, err, "buildJoinGraph error")

	order := exec.OptimizeJoinOrder(graph)
	require.Len(t, order, 2, "expected 2 tables in join order")
	// Without any WHERE, original order should be preserved (users first)
	assert.Equal(t, "u", order[0], "expected driving table 'u'")
}

func TestOptimizeJoinOrder_PrefersFiltered(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
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
	require.NoError(t, err, "buildJoinGraph error")

	order := exec.OptimizeJoinOrder(graph)
	// users has WHERE with index -> should be driving table
	assert.Equal(t, "u", order[0], "expected driving table 'u' (has indexed WHERE)")
}

func TestOptimizeJoinOrder_ThreeTables(t *testing.T) {
	exec := NewExecutor(NewDatabase("test"))
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
	require.NoError(t, err, "buildJoinGraph error")

	order := exec.OptimizeJoinOrder(graph)
	require.Len(t, order, 3, "expected 3 tables in join order")
	// users has indexed WHERE -> driving table
	assert.Equal(t, "u", order[0], "expected driving table 'u'")
	// orders and items both have indexed equi-join columns
	// orders should come next as it connects to users
	assert.Equal(t, "o", order[1], "expected second table 'o'")
	assert.Equal(t, "i", order[2], "expected third table 'i'")
}

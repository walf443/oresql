package engine

import (
	"testing"

	"github.com/walf443/oresql/ast"
)

func TestApplyOffset(t *testing.T) {
	rows := []Row{
		{int64(1)},
		{int64(2)},
		{int64(3)},
	}

	tests := []struct {
		name   string
		offset *int64
		want   int
	}{
		{"nil offset", nil, 3},
		{"offset 0", ptr(int64(0)), 3},
		{"offset 1", ptr(int64(1)), 2},
		{"offset 3", ptr(int64(3)), 0},
		{"offset beyond", ptr(int64(10)), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := applyOffset(rows, tt.offset)
			if len(result) != tt.want {
				t.Errorf("got %d rows, want %d", len(result), tt.want)
			}
		})
	}
}

func TestApplyLimit(t *testing.T) {
	rows := []Row{
		{int64(1)},
		{int64(2)},
		{int64(3)},
	}

	tests := []struct {
		name  string
		limit *int64
		want  int
	}{
		{"nil limit", nil, 3},
		{"limit 0", ptr(int64(0)), 0},
		{"limit 2", ptr(int64(2)), 2},
		{"limit 5", ptr(int64(5)), 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := applyLimit(rows, tt.limit)
			if len(result) != tt.want {
				t.Errorf("got %d rows, want %d", len(result), tt.want)
			}
		})
	}
}

func ptr(v int64) *int64 {
	return &v
}

func TestFilterWhere(t *testing.T) {
	info := &TableInfo{
		Name: "users",
		Columns: []ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
	}
	eval := newTableEvaluator(info)
	rows := []Row{
		{int64(1), "Alice"},
		{int64(2), "Bob"},
		{int64(3), "Alice"},
	}

	// Filter by name = 'Alice'
	where := &ast.BinaryExpr{
		Left:  &ast.IdentExpr{Name: "name"},
		Op:    "=",
		Right: &ast.StringLitExpr{Value: "Alice"},
	}
	result, err := filterWhere(rows, where, eval, rowIdentity)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result))
	}

	// Nil where returns all rows
	result, err = filterWhere(rows, nil, eval, rowIdentity)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result))
	}
}

func TestSortRows(t *testing.T) {
	info := &TableInfo{
		Name: "users",
		Columns: []ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
	}
	eval := newTableEvaluator(info)
	rows := []Row{
		{int64(3), "Charlie"},
		{int64(1), "Alice"},
		{int64(2), "Bob"},
	}

	// Sort by id ASC
	orderBy := []ast.OrderByClause{
		{Expr: &ast.IdentExpr{Name: "id"}, Desc: false},
	}
	result, err := sortRows(rows, orderBy, eval, rowIdentity)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0][0] != int64(1) || result[1][0] != int64(2) || result[2][0] != int64(3) {
		t.Errorf("unexpected sort order: %v", result)
	}

	// Sort by id DESC
	orderBy = []ast.OrderByClause{
		{Expr: &ast.IdentExpr{Name: "id"}, Desc: true},
	}
	result, err = sortRows(rows, orderBy, eval, rowIdentity)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0][0] != int64(3) || result[1][0] != int64(2) || result[2][0] != int64(1) {
		t.Errorf("unexpected sort order: %v", result)
	}

	// Empty orderBy returns unchanged
	result, err = sortRows(rows, nil, eval, rowIdentity)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result))
	}
}

func TestProjectRows(t *testing.T) {
	info := &TableInfo{
		Name: "users",
		Columns: []ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
			{Name: "name", DataType: "TEXT", Index: 1},
			{Name: "age", DataType: "INT", Index: 2},
		},
	}
	eval := newTableEvaluator(info)
	rows := []Row{
		{int64(1), "Alice", int64(30)},
		{int64(2), "Bob", int64(25)},
	}

	// Project specific columns
	colExprs := []ast.Expr{
		&ast.IdentExpr{Name: "name"},
		&ast.IdentExpr{Name: "age"},
	}
	result, err := projectRows(rows, colExprs, false, eval)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}
	if result[0][0] != "Alice" || result[0][1] != int64(30) {
		t.Errorf("unexpected projection: %v", result[0])
	}

	// Star projection
	result, err = projectRows(rows, nil, true, eval)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result[0]) != 3 {
		t.Errorf("expected 3 columns, got %d", len(result[0]))
	}
}

func TestResolveSelectColumns(t *testing.T) {
	info := &TableInfo{
		Name: "users",
		Columns: []ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
	}
	eval := newTableEvaluator(info)

	// Star
	colNames, colExprs, isStar, err := resolveSelectColumns([]ast.Expr{&ast.StarExpr{}}, eval)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isStar {
		t.Error("expected isStar=true")
	}
	if len(colNames) != 2 || colNames[0] != "id" || colNames[1] != "name" {
		t.Errorf("unexpected column names: %v", colNames)
	}
	if colExprs != nil {
		t.Error("expected nil colExprs for star")
	}

	// Named columns
	columns := []ast.Expr{
		&ast.IdentExpr{Name: "name"},
		&ast.AliasExpr{Expr: &ast.IdentExpr{Name: "id"}, Alias: "user_id"},
	}
	colNames, colExprs, isStar, err = resolveSelectColumns(columns, eval)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isStar {
		t.Error("expected isStar=false")
	}
	if len(colNames) != 2 || colNames[0] != "name" || colNames[1] != "user_id" {
		t.Errorf("unexpected column names: %v", colNames)
	}
	if len(colExprs) != 2 {
		t.Errorf("expected 2 colExprs, got %d", len(colExprs))
	}
}

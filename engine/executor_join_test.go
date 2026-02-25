package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewJoinContext(t *testing.T) {
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
			{Name: "amount", DataType: "FLOAT", Index: 2},
		},
	}

	jc := newJoinContext([]struct {
		info  *TableInfo
		alias string
	}{
		{info: usersInfo, alias: ""},
		{info: ordersInfo, alias: "o"},
	})

	// Check merged columns count
	assert.Len(t, jc.MergedInfo.Columns, 5, "MergedInfo.Columns count")

	// Check column offsets
	// users columns: 0, 1
	// orders columns: 2, 3, 4
	assert.Equal(t, "id", jc.MergedInfo.Columns[0].Name)
	assert.Equal(t, 0, jc.MergedInfo.Columns[0].Index)
	assert.Equal(t, "id", jc.MergedInfo.Columns[2].Name)
	assert.Equal(t, 2, jc.MergedInfo.Columns[2].Index)
	assert.Equal(t, "amount", jc.MergedInfo.Columns[4].Name)
	assert.Equal(t, 4, jc.MergedInfo.Columns[4].Index)

	// Check alias registration
	assert.Contains(t, jc.tableMap, "o", "alias 'o' should be registered in tableMap")
	assert.Contains(t, jc.tableMap, "orders", "table name 'orders' should be registered in tableMap")
}

func TestJoinContextFindColumn(t *testing.T) {
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
		},
	}

	jc := newJoinContext([]struct {
		info  *TableInfo
		alias string
	}{
		{info: usersInfo, alias: "u"},
		{info: ordersInfo, alias: "o"},
	})

	tests := []struct {
		name      string
		tableName string
		colName   string
		wantIdx   int
		wantErr   bool
	}{
		{"table specified", "users", "name", 1, false},
		{"alias specified", "u", "name", 1, false},
		{"unqualified unique", "", "user_id", 3, false},
		{"ambiguous column", "", "id", 0, true},
		{"not found", "", "nonexistent", 0, true},
		{"unknown table", "other", "id", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col, err := jc.FindColumn(tt.tableName, tt.colName)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantIdx, col.Index)
		})
	}
}

func TestValidateTableRefWithAlias(t *testing.T) {
	tests := []struct {
		name        string
		tableRef    string
		targetTable string
		alias       string
		wantErr     bool
	}{
		{"empty ref", "", "users", "", false},
		{"table name match", "users", "users", "", false},
		{"alias match", "u", "users", "u", false},
		{"case insensitive table", "Users", "users", "", false},
		{"case insensitive alias", "U", "users", "u", false},
		{"mismatch", "orders", "users", "u", true},
		{"no alias mismatch", "u", "users", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTableRefWithAlias(tt.tableRef, tt.targetTable, tt.alias)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

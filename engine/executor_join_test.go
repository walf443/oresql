package engine

import (
	"testing"
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
	if len(jc.MergedInfo.Columns) != 5 {
		t.Errorf("MergedInfo.Columns count = %d, want 5", len(jc.MergedInfo.Columns))
	}

	// Check column offsets
	// users columns: 0, 1
	// orders columns: 2, 3, 4
	if jc.MergedInfo.Columns[0].Name != "id" || jc.MergedInfo.Columns[0].Index != 0 {
		t.Errorf("merged col[0] = {%q, %d}, want {id, 0}", jc.MergedInfo.Columns[0].Name, jc.MergedInfo.Columns[0].Index)
	}
	if jc.MergedInfo.Columns[2].Name != "id" || jc.MergedInfo.Columns[2].Index != 2 {
		t.Errorf("merged col[2] = {%q, %d}, want {id, 2}", jc.MergedInfo.Columns[2].Name, jc.MergedInfo.Columns[2].Index)
	}
	if jc.MergedInfo.Columns[4].Name != "amount" || jc.MergedInfo.Columns[4].Index != 4 {
		t.Errorf("merged col[4] = {%q, %d}, want {amount, 4}", jc.MergedInfo.Columns[4].Name, jc.MergedInfo.Columns[4].Index)
	}

	// Check alias registration
	if _, ok := jc.tableMap["o"]; !ok {
		t.Errorf("alias 'o' not registered in tableMap")
	}
	if _, ok := jc.tableMap["orders"]; !ok {
		t.Errorf("table name 'orders' not registered in tableMap")
	}
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
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if col.Index != tt.wantIdx {
				t.Errorf("FindColumn(%q, %q).Index = %d, want %d", tt.tableName, tt.colName, col.Index, tt.wantIdx)
			}
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
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

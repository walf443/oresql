package disk

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/walf443/oresql/storage"
)

func newTestStorage(t *testing.T) *DiskStorage {
	t.Helper()
	dir := t.TempDir()
	ds, err := NewDiskStorage(dir)
	require.NoError(t, err)
	return ds
}

func TestCreateTableAndInsert(t *testing.T) {
	ds := newTestStorage(t)
	defer ds.Close()

	info := &storage.TableInfo{
		Name: "users",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}
	ds.CreateTable(info)

	err := ds.Insert("users", storage.Row{int64(1), "alice"})
	require.NoError(t, err)
	err = ds.Insert("users", storage.Row{int64(2), "bob"})
	require.NoError(t, err)

	rows, err := ds.Scan("users")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, int64(1), rows[0][0])
	assert.Equal(t, "alice", rows[0][1])
	assert.Equal(t, int64(2), rows[1][0])
	assert.Equal(t, "bob", rows[1][1])
}

func TestPersistenceReload(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create and insert
	ds1, err := NewDiskStorage(dir)
	require.NoError(t, err)

	info := &storage.TableInfo{
		Name: "users",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}
	ds1.CreateTable(info)
	require.NoError(t, ds1.Insert("users", storage.Row{int64(1), "alice"}))
	require.NoError(t, ds1.Insert("users", storage.Row{int64(2), "bob"}))
	require.NoError(t, ds1.Close())

	// Phase 2: Reload
	ds2, err := NewDiskStorage(dir)
	require.NoError(t, err)
	defer ds2.Close()
	require.NoError(t, ds2.LoadAll())

	rows, err := ds2.Scan("users")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, int64(1), rows[0][0])
	assert.Equal(t, "alice", rows[0][1])
	assert.Equal(t, int64(2), rows[1][0])
	assert.Equal(t, "bob", rows[1][1])
}

func TestPersistenceWithAutoIncrement(t *testing.T) {
	dir := t.TempDir()

	// Phase 1
	ds1, err := NewDiskStorage(dir)
	require.NoError(t, err)

	info := &storage.TableInfo{
		Name: "items",
		Columns: []storage.ColumnInfo{
			{Name: "a", DataType: "TEXT", Index: 0},
			{Name: "b", DataType: "INT", Index: 1},
		},
		PrimaryKeyCol: -1,
	}
	ds1.CreateTable(info)
	require.NoError(t, ds1.Insert("items", storage.Row{"x", int64(10)}))
	require.NoError(t, ds1.Insert("items", storage.Row{"y", int64(20)}))
	require.NoError(t, ds1.Close())

	// Phase 2: Reload
	ds2, err := NewDiskStorage(dir)
	require.NoError(t, err)
	defer ds2.Close()
	require.NoError(t, ds2.LoadAll())

	rows, err := ds2.Scan("items")
	require.NoError(t, err)
	require.Len(t, rows, 2)

	// Insert more to check auto-increment continues
	require.NoError(t, ds2.Insert("items", storage.Row{"z", int64(30)}))

	rows, err = ds2.Scan("items")
	require.NoError(t, err)
	require.Len(t, rows, 3)
}

func TestDeletePersistence(t *testing.T) {
	dir := t.TempDir()

	// Phase 1
	ds1, err := NewDiskStorage(dir)
	require.NoError(t, err)

	info := &storage.TableInfo{
		Name: "users",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}
	ds1.CreateTable(info)
	require.NoError(t, ds1.Insert("users", storage.Row{int64(1), "alice"}))
	require.NoError(t, ds1.Insert("users", storage.Row{int64(2), "bob"}))
	require.NoError(t, ds1.Insert("users", storage.Row{int64(3), "charlie"}))
	require.NoError(t, ds1.DeleteByKeys("users", []int64{2}))
	require.NoError(t, ds1.Close())

	// Phase 2
	ds2, err := NewDiskStorage(dir)
	require.NoError(t, err)
	defer ds2.Close()
	require.NoError(t, ds2.LoadAll())

	rows, err := ds2.Scan("users")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, int64(1), rows[0][0])
	assert.Equal(t, int64(3), rows[1][0])
}

func TestUpdatePersistence(t *testing.T) {
	dir := t.TempDir()

	// Phase 1
	ds1, err := NewDiskStorage(dir)
	require.NoError(t, err)

	info := &storage.TableInfo{
		Name: "users",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}
	ds1.CreateTable(info)
	require.NoError(t, ds1.Insert("users", storage.Row{int64(1), "alice"}))
	require.NoError(t, ds1.UpdateRow("users", 1, storage.Row{int64(1), "ALICE"}))
	require.NoError(t, ds1.Close())

	// Phase 2
	ds2, err := NewDiskStorage(dir)
	require.NoError(t, err)
	defer ds2.Close()
	require.NoError(t, ds2.LoadAll())

	rows, err := ds2.Scan("users")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "ALICE", rows[0][1])
}

func TestDropTablePersistence(t *testing.T) {
	dir := t.TempDir()

	// Phase 1
	ds1, err := NewDiskStorage(dir)
	require.NoError(t, err)

	info := &storage.TableInfo{
		Name: "temp",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
		},
		PrimaryKeyCol: -1,
	}
	ds1.CreateTable(info)
	require.NoError(t, ds1.Insert("temp", storage.Row{int64(1)}))
	ds1.DropTable("temp")

	// Phase 2
	ds2, err := NewDiskStorage(dir)
	require.NoError(t, err)
	defer ds2.Close()
	require.NoError(t, ds2.LoadAll())

	tables := ds2.ListTables()
	assert.Empty(t, tables)
}

func TestTruncateTablePersistence(t *testing.T) {
	dir := t.TempDir()

	// Phase 1
	ds1, err := NewDiskStorage(dir)
	require.NoError(t, err)

	info := &storage.TableInfo{
		Name: "users",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}
	ds1.CreateTable(info)
	require.NoError(t, ds1.Insert("users", storage.Row{int64(1), "alice"}))
	ds1.TruncateTable("users")
	require.NoError(t, ds1.Close())

	// Phase 2
	ds2, err := NewDiskStorage(dir)
	require.NoError(t, err)
	defer ds2.Close()
	require.NoError(t, ds2.LoadAll())

	rows, err := ds2.Scan("users")
	require.NoError(t, err)
	assert.Empty(t, rows)
}

func TestIndexPersistence(t *testing.T) {
	dir := t.TempDir()

	// Phase 1
	ds1, err := NewDiskStorage(dir)
	require.NoError(t, err)

	info := &storage.TableInfo{
		Name: "users",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}
	ds1.CreateTable(info)
	require.NoError(t, ds1.Insert("users", storage.Row{int64(1), "alice"}))
	require.NoError(t, ds1.Insert("users", storage.Row{int64(2), "bob"}))

	idxInfo := &storage.IndexInfo{
		Name:        "idx_name",
		TableName:   "users",
		ColumnNames: []string{"name"},
		ColumnIdxs:  []int{1},
		Type:        "BTREE",
		Unique:      false,
	}
	require.NoError(t, ds1.CreateIndex(idxInfo))
	require.NoError(t, ds1.Close())

	// Phase 2
	ds2, err := NewDiskStorage(dir)
	require.NoError(t, err)
	defer ds2.Close()
	require.NoError(t, ds2.LoadAll())

	assert.True(t, ds2.HasIndex("idx_name"))
	idx := ds2.LookupSingleColumnIndex("users", 1)
	require.NotNil(t, idx)
	keys := idx.Lookup([]storage.Value{"alice"})
	require.Len(t, keys, 1)
	assert.Equal(t, int64(1), keys[0])
}

func TestNullValuePersistence(t *testing.T) {
	dir := t.TempDir()

	ds1, err := NewDiskStorage(dir)
	require.NoError(t, err)

	info := &storage.TableInfo{
		Name: "data",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
			{Name: "value", DataType: "TEXT", Index: 1},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}
	ds1.CreateTable(info)
	require.NoError(t, ds1.Insert("data", storage.Row{int64(1), nil}))
	require.NoError(t, ds1.Insert("data", storage.Row{int64(2), "hello"}))
	require.NoError(t, ds1.Close())

	ds2, err := NewDiskStorage(dir)
	require.NoError(t, err)
	defer ds2.Close()
	require.NoError(t, ds2.LoadAll())

	rows, err := ds2.Scan("data")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Nil(t, rows[0][1])
	assert.Equal(t, "hello", rows[1][1])
}

func TestFloatValuePersistence(t *testing.T) {
	dir := t.TempDir()

	ds1, err := NewDiskStorage(dir)
	require.NoError(t, err)

	info := &storage.TableInfo{
		Name: "measures",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
			{Name: "value", DataType: "FLOAT", Index: 1},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}
	ds1.CreateTable(info)
	require.NoError(t, ds1.Insert("measures", storage.Row{int64(1), float64(3.14)}))
	require.NoError(t, ds1.Insert("measures", storage.Row{int64(2), float64(-2.71)}))
	require.NoError(t, ds1.Close())

	ds2, err := NewDiskStorage(dir)
	require.NoError(t, err)
	defer ds2.Close()
	require.NoError(t, ds2.LoadAll())

	rows, err := ds2.Scan("measures")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, float64(3.14), rows[0][1])
	assert.Equal(t, float64(-2.71), rows[1][1])
}

func TestMultipleTablesReload(t *testing.T) {
	dir := t.TempDir()

	ds1, err := NewDiskStorage(dir)
	require.NoError(t, err)

	info1 := &storage.TableInfo{
		Name: "users",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}
	info2 := &storage.TableInfo{
		Name: "orders",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
			{Name: "amount", DataType: "INT", Index: 1},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}
	ds1.CreateTable(info1)
	ds1.CreateTable(info2)
	require.NoError(t, ds1.Insert("users", storage.Row{int64(1), "alice"}))
	require.NoError(t, ds1.Insert("orders", storage.Row{int64(100), int64(500)}))
	require.NoError(t, ds1.Close())

	ds2, err := NewDiskStorage(dir)
	require.NoError(t, err)
	defer ds2.Close()
	require.NoError(t, ds2.LoadAll())

	users, err := ds2.Scan("users")
	require.NoError(t, err)
	require.Len(t, users, 1)
	assert.Equal(t, "alice", users[0][1])

	orders, err := ds2.Scan("orders")
	require.NoError(t, err)
	require.Len(t, orders, 1)
	assert.Equal(t, int64(500), orders[0][1])
}

func TestRowCount(t *testing.T) {
	ds := newTestStorage(t)
	defer ds.Close()

	info := &storage.TableInfo{
		Name: "t",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}
	ds.CreateTable(info)

	count, err := ds.RowCount("t")
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	ds.Insert("t", storage.Row{int64(1)})
	ds.Insert("t", storage.Row{int64(2)})

	count, err = ds.RowCount("t")
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestGetRow(t *testing.T) {
	ds := newTestStorage(t)
	defer ds.Close()

	info := &storage.TableInfo{
		Name: "t",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
			{Name: "val", DataType: "TEXT", Index: 1},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}
	ds.CreateTable(info)
	ds.Insert("t", storage.Row{int64(1), "hello"})

	row, ok := ds.GetRow("t", 1)
	assert.True(t, ok)
	assert.Equal(t, "hello", row[1])

	_, ok = ds.GetRow("t", 999)
	assert.False(t, ok)
}

func TestForEachRow(t *testing.T) {
	ds := newTestStorage(t)
	defer ds.Close()

	info := &storage.TableInfo{
		Name: "t",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}
	ds.CreateTable(info)

	for i := int64(1); i <= 10; i++ {
		ds.Insert("t", storage.Row{i})
	}

	var keys []int64
	ds.ForEachRow("t", false, func(key int64, row storage.Row) bool {
		keys = append(keys, key)
		return len(keys) < 5
	}, 0)
	assert.Len(t, keys, 5)

	// With limit
	var keys2 []int64
	ds.ForEachRow("t", false, func(key int64, row storage.Row) bool {
		keys2 = append(keys2, key)
		return true
	}, 3)
	assert.Len(t, keys2, 3)
}

func TestScanOrdered(t *testing.T) {
	ds := newTestStorage(t)
	defer ds.Close()

	info := &storage.TableInfo{
		Name: "t",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}
	ds.CreateTable(info)

	for i := int64(1); i <= 5; i++ {
		ds.Insert("t", storage.Row{i})
	}

	// Forward with limit
	rows, err := ds.ScanOrdered("t", false, 3)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Equal(t, int64(1), rows[0][0])
	assert.Equal(t, int64(3), rows[2][0])

	// Reverse
	rows, err = ds.ScanOrdered("t", true, 2)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, int64(5), rows[0][0])
	assert.Equal(t, int64(4), rows[1][0])
}

func TestDuplicatePrimaryKey(t *testing.T) {
	ds := newTestStorage(t)
	defer ds.Close()

	info := &storage.TableInfo{
		Name: "t",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}
	ds.CreateTable(info)

	require.NoError(t, ds.Insert("t", storage.Row{int64(1)}))
	err := ds.Insert("t", storage.Row{int64(1)})
	assert.Error(t, err, "should error on duplicate PK")
}

func TestListTablesAndLoadTableMeta(t *testing.T) {
	ds := newTestStorage(t)
	defer ds.Close()

	info1 := &storage.TableInfo{
		Name: "t1",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
		},
		PrimaryKeyCol: -1,
	}
	info2 := &storage.TableInfo{
		Name: "t2",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
		},
		PrimaryKeyCol: -1,
	}
	ds.CreateTable(info1)
	ds.CreateTable(info2)

	tables := ds.ListTables()
	assert.Len(t, tables, 2)

	loadedInfo, _, _, err := ds.LoadTableMeta("t1")
	require.NoError(t, err)
	assert.Equal(t, "t1", loadedInfo.Name)
}

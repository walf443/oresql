package file

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/walf443/oresql/storage"
)

func newTestStorage(t *testing.T) *FileStorage {
	t.Helper()
	dir := t.TempDir()
	fs, err := NewFileStorage(dir)
	require.NoError(t, err)
	return fs
}

func TestCreateTableAndInsert(t *testing.T) {
	fs := newTestStorage(t)

	info := &storage.TableInfo{
		Name: "users",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
			{Name: "name", DataType: "TEXT", Index: 1},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}
	fs.CreateTable(info)

	err := fs.Insert("users", storage.Row{int64(1), "alice"})
	require.NoError(t, err)
	err = fs.Insert("users", storage.Row{int64(2), "bob"})
	require.NoError(t, err)

	rows, err := fs.Scan("users")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, int64(1), rows[0][0])
	assert.Equal(t, "alice", rows[0][1])
	assert.Equal(t, int64(2), rows[1][0])
	assert.Equal(t, "bob", rows[1][1])
}

func TestPersistenceReload(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create table and insert data
	fs1, err := NewFileStorage(dir)
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
	fs1.CreateTable(info)
	require.NoError(t, fs1.Insert("users", storage.Row{int64(1), "alice"}))
	require.NoError(t, fs1.Insert("users", storage.Row{int64(2), "bob"}))

	// Verify .dat file exists
	datPath := filepath.Join(dir, "users.dat")
	_, err = os.Stat(datPath)
	require.NoError(t, err, "users.dat should exist")

	// Phase 2: Create new FileStorage and load from disk
	fs2, err := NewFileStorage(dir)
	require.NoError(t, err)
	require.NoError(t, fs2.LoadAll())

	rows, err := fs2.Scan("users")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, int64(1), rows[0][0])
	assert.Equal(t, "alice", rows[0][1])
	assert.Equal(t, int64(2), rows[1][0])
	assert.Equal(t, "bob", rows[1][1])
}

func TestPersistenceWithAutoIncrement(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create table with composite PK (auto-increment keys)
	fs1, err := NewFileStorage(dir)
	require.NoError(t, err)

	info := &storage.TableInfo{
		Name: "items",
		Columns: []storage.ColumnInfo{
			{Name: "a", DataType: "TEXT", Index: 0},
			{Name: "b", DataType: "INT", Index: 1},
		},
		PrimaryKeyCol: -1, // no single INT PK → auto-increment
	}
	fs1.CreateTable(info)
	require.NoError(t, fs1.Insert("items", storage.Row{"x", int64(10)}))
	require.NoError(t, fs1.Insert("items", storage.Row{"y", int64(20)}))

	// Phase 2: Reload
	fs2, err := NewFileStorage(dir)
	require.NoError(t, err)
	require.NoError(t, fs2.LoadAll())

	rows, err := fs2.Scan("items")
	require.NoError(t, err)
	require.Len(t, rows, 2)

	// Insert more data to check auto-increment continues
	require.NoError(t, fs2.Insert("items", storage.Row{"z", int64(30)}))

	rows, err = fs2.Scan("items")
	require.NoError(t, err)
	require.Len(t, rows, 3)
}

func TestDeletePersistence(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create, insert, delete
	fs1, err := NewFileStorage(dir)
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
	fs1.CreateTable(info)
	require.NoError(t, fs1.Insert("users", storage.Row{int64(1), "alice"}))
	require.NoError(t, fs1.Insert("users", storage.Row{int64(2), "bob"}))
	require.NoError(t, fs1.Insert("users", storage.Row{int64(3), "charlie"}))
	require.NoError(t, fs1.DeleteByKeys("users", []int64{2}))

	// Phase 2: Reload and verify deletion persisted
	fs2, err := NewFileStorage(dir)
	require.NoError(t, err)
	require.NoError(t, fs2.LoadAll())

	rows, err := fs2.Scan("users")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, int64(1), rows[0][0])
	assert.Equal(t, int64(3), rows[1][0])
}

func TestUpdatePersistence(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create, insert, update
	fs1, err := NewFileStorage(dir)
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
	fs1.CreateTable(info)
	require.NoError(t, fs1.Insert("users", storage.Row{int64(1), "alice"}))
	require.NoError(t, fs1.UpdateRow("users", 1, storage.Row{int64(1), "ALICE"}))

	// Phase 2: Reload and verify update persisted
	fs2, err := NewFileStorage(dir)
	require.NoError(t, err)
	require.NoError(t, fs2.LoadAll())

	rows, err := fs2.Scan("users")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "ALICE", rows[0][1])
}

func TestDropTablePersistence(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create table, then drop it
	fs1, err := NewFileStorage(dir)
	require.NoError(t, err)

	info := &storage.TableInfo{
		Name: "temp",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0},
		},
		PrimaryKeyCol: -1,
	}
	fs1.CreateTable(info)
	require.NoError(t, fs1.Insert("temp", storage.Row{int64(1)}))
	fs1.DropTable("temp")

	// Verify .dat file is gone
	datPath := filepath.Join(dir, "temp.dat")
	_, err = os.Stat(datPath)
	assert.True(t, os.IsNotExist(err))

	// Phase 2: Reload - should have no tables
	fs2, err := NewFileStorage(dir)
	require.NoError(t, err)
	require.NoError(t, fs2.LoadAll())

	tables := fs2.ListTables()
	assert.Empty(t, tables)
}

func TestTruncateTablePersistence(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create, insert, truncate
	fs1, err := NewFileStorage(dir)
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
	fs1.CreateTable(info)
	require.NoError(t, fs1.Insert("users", storage.Row{int64(1), "alice"}))
	fs1.TruncateTable("users")

	// Phase 2: Reload and verify table is empty
	fs2, err := NewFileStorage(dir)
	require.NoError(t, err)
	require.NoError(t, fs2.LoadAll())

	rows, err := fs2.Scan("users")
	require.NoError(t, err)
	assert.Empty(t, rows)
}

func TestIndexPersistence(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create table with index
	fs1, err := NewFileStorage(dir)
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
	fs1.CreateTable(info)
	require.NoError(t, fs1.Insert("users", storage.Row{int64(1), "alice"}))
	require.NoError(t, fs1.Insert("users", storage.Row{int64(2), "bob"}))

	idxInfo := &storage.IndexInfo{
		Name:        "idx_name",
		TableName:   "users",
		ColumnNames: []string{"name"},
		ColumnIdxs:  []int{1},
		Type:        "BTREE",
		Unique:      false,
	}
	require.NoError(t, fs1.CreateIndex(idxInfo))

	// Phase 2: Reload and verify index works
	fs2, err := NewFileStorage(dir)
	require.NoError(t, err)
	require.NoError(t, fs2.LoadAll())

	assert.True(t, fs2.HasIndex("idx_name"))
	idx := fs2.LookupSingleColumnIndex("users", 1)
	require.NotNil(t, idx)
	keys := idx.Lookup([]storage.Value{"alice"})
	require.Len(t, keys, 1)
	assert.Equal(t, int64(1), keys[0])
}

func TestNullValuePersistence(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create table with nullable columns
	fs1, err := NewFileStorage(dir)
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
	fs1.CreateTable(info)
	require.NoError(t, fs1.Insert("data", storage.Row{int64(1), nil}))
	require.NoError(t, fs1.Insert("data", storage.Row{int64(2), "hello"}))

	// Phase 2: Reload and verify nulls are preserved
	fs2, err := NewFileStorage(dir)
	require.NoError(t, err)
	require.NoError(t, fs2.LoadAll())

	rows, err := fs2.Scan("data")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Nil(t, rows[0][1])
	assert.Equal(t, "hello", rows[1][1])
}

func TestFloatValuePersistence(t *testing.T) {
	dir := t.TempDir()

	fs1, err := NewFileStorage(dir)
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
	fs1.CreateTable(info)
	require.NoError(t, fs1.Insert("measures", storage.Row{int64(1), float64(3.14)}))
	require.NoError(t, fs1.Insert("measures", storage.Row{int64(2), float64(-2.71)}))

	// Reload
	fs2, err := NewFileStorage(dir)
	require.NoError(t, err)
	require.NoError(t, fs2.LoadAll())

	rows, err := fs2.Scan("measures")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, float64(3.14), rows[0][1])
	assert.Equal(t, float64(-2.71), rows[1][1])
}

func TestListTablesAndLoadTableMeta(t *testing.T) {
	fs := newTestStorage(t)

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
	fs.CreateTable(info1)
	fs.CreateTable(info2)

	tables := fs.ListTables()
	assert.Len(t, tables, 2)

	loadedInfo, _, _, err := fs.LoadTableMeta("t1")
	require.NoError(t, err)
	assert.Equal(t, "t1", loadedInfo.Name)
}

func TestV2PersistenceReload(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create table, insert, and force v2 write
	fs1, err := NewFileStorage(dir)
	require.NoError(t, err)

	info := &storage.TableInfo{
		Name: "users",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
			{Name: "name", DataType: "TEXT", Index: 1},
			{Name: "score", DataType: "FLOAT", Index: 2},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}
	fs1.CreateTable(info)
	require.NoError(t, fs1.Insert("users", storage.Row{int64(1), "alice", float64(95.5)}))
	require.NoError(t, fs1.Insert("users", storage.Row{int64(2), "bob", nil}))
	require.NoError(t, fs1.Insert("users", storage.Row{int64(3), "charlie", float64(-1.0)}))

	// Phase 2: Reload from v2
	fs2, err := NewFileStorage(dir)
	require.NoError(t, err)
	require.NoError(t, fs2.LoadAll())

	rows, err := fs2.Scan("users")
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Equal(t, int64(1), rows[0][0])
	assert.Equal(t, "alice", rows[0][1])
	assert.Equal(t, float64(95.5), rows[0][2])
	assert.Equal(t, int64(2), rows[1][0])
	assert.Equal(t, "bob", rows[1][1])
	assert.Nil(t, rows[1][2])
	assert.Equal(t, int64(3), rows[2][0])
	assert.Equal(t, "charlie", rows[2][1])
	assert.Equal(t, float64(-1.0), rows[2][2])
}

func TestV2WithSecondaryIndex(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create table with index
	fs1, err := NewFileStorage(dir)
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
	fs1.CreateTable(info)
	require.NoError(t, fs1.Insert("users", storage.Row{int64(1), "alice"}))
	require.NoError(t, fs1.Insert("users", storage.Row{int64(2), "bob"}))
	require.NoError(t, fs1.Insert("users", storage.Row{int64(3), "alice"})) // duplicate name

	idxInfo := &storage.IndexInfo{
		Name:        "idx_name",
		TableName:   "users",
		ColumnNames: []string{"name"},
		ColumnIdxs:  []int{1},
		Type:        "BTREE",
		Unique:      false,
	}
	require.NoError(t, fs1.CreateIndex(idxInfo))

	// Phase 2: Reload
	fs2, err := NewFileStorage(dir)
	require.NoError(t, err)
	require.NoError(t, fs2.LoadAll())

	assert.True(t, fs2.HasIndex("idx_name"))
	idx := fs2.LookupSingleColumnIndex("users", 1)
	require.NotNil(t, idx)

	// Verify index lookups
	aliceKeys := idx.Lookup([]storage.Value{"alice"})
	assert.Len(t, aliceKeys, 2)
	bobKeys := idx.Lookup([]storage.Value{"bob"})
	assert.Len(t, bobKeys, 1)
	assert.Equal(t, int64(2), bobKeys[0])
}

func TestV2IncrementalWriteAndReload(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create table with v2 snapshot
	fs1, err := NewFileStorage(dir)
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
	fs1.CreateTable(info)
	require.NoError(t, fs1.Insert("users", storage.Row{int64(1), "alice"}))

	// Phase 2: Reload, then add more data (incremental append-only log)
	fs2, err := NewFileStorage(dir)
	require.NoError(t, err)
	require.NoError(t, fs2.LoadAll())
	require.NoError(t, fs2.Insert("users", storage.Row{int64(2), "bob"}))

	// Phase 3: Reload again - should have both rows (snapshot + log replay)
	fs3, err := NewFileStorage(dir)
	require.NoError(t, err)
	require.NoError(t, fs3.LoadAll())

	rows, err := fs3.Scan("users")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, int64(1), rows[0][0])
	assert.Equal(t, "alice", rows[0][1])
	assert.Equal(t, int64(2), rows[1][0])
	assert.Equal(t, "bob", rows[1][1])
}

func TestV1ToV3Migration(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Manually create a v1 file
	info := &storage.TableInfo{
		Name: "legacy",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
			{Name: "val", DataType: "TEXT", Index: 1},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}
	keyRows := []storage.KeyRow{
		{Key: 1, Row: storage.Row{int64(1), "one"}},
		{Key: 2, Row: storage.Row{int64(2), "two"}},
	}
	v1Path := filepath.Join(dir, "legacy.dat")
	require.NoError(t, writeFullFileV1(v1Path, info, nil, 3, keyRows))

	// Verify it's v1
	data, err := os.ReadFile(v1Path)
	require.NoError(t, err)
	assert.Equal(t, byte(0x01), data[6], "should be v1 before migration")

	// Phase 2: Load (should auto-migrate to v3)
	fs, err := NewFileStorage(dir)
	require.NoError(t, err)
	require.NoError(t, fs.LoadAll())

	rows, err := fs.Scan("legacy")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, int64(1), rows[0][0])
	assert.Equal(t, "one", rows[0][1])
	assert.Equal(t, int64(2), rows[1][0])
	assert.Equal(t, "two", rows[1][1])

	// Verify file is now v3
	data, err = os.ReadFile(v1Path)
	require.NoError(t, err)
	assert.Equal(t, byte(0x03), data[6], "should be v3 after migration")

	// Phase 3: Reload from v3 and verify data intact
	fs2, err := NewFileStorage(dir)
	require.NoError(t, err)
	require.NoError(t, fs2.LoadAll())

	rows, err = fs2.Scan("legacy")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, int64(1), rows[0][0])
	assert.Equal(t, "one", rows[0][1])
}

func TestV2ToV3Migration(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create table with v3, then downgrade to v2 header to simulate a v2 file
	info := &storage.TableInfo{
		Name: "legacy2",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
			{Name: "val", DataType: "TEXT", Index: 1},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}

	// Write using v3 format first, then patch version byte to v2
	v2Path := filepath.Join(dir, "legacy2.dat")
	require.NoError(t, writeFullFileV3(v2Path, info, nil, 3, nil, nil))

	// Patch version byte from 0x03 to 0x02
	data, err := os.ReadFile(v2Path)
	require.NoError(t, err)
	assert.Equal(t, byte(0x03), data[6], "should be v3 initially")
	data[6] = 0x02
	require.NoError(t, os.WriteFile(v2Path, data, 0644))

	// Insert data by writing directly (we'll use the v1 format for simplicity and re-patch)
	// Actually, the simplest approach: write a v1 file, load it (migrates to v3),
	// then patch that file's version byte back to v2, and load again to test v2→v3
	v2Path2 := filepath.Join(dir, "legacy2b.dat")
	keyRows := []storage.KeyRow{
		{Key: 1, Row: storage.Row{int64(1), "alpha"}},
		{Key: 2, Row: storage.Row{int64(2), "beta"}},
		{Key: 3, Row: storage.Row{int64(3), "gamma"}},
	}
	info2 := &storage.TableInfo{
		Name: "legacy2b",
		Columns: []storage.ColumnInfo{
			{Name: "id", DataType: "INT", Index: 0, PrimaryKey: true},
			{Name: "val", DataType: "TEXT", Index: 1},
		},
		PrimaryKeyCol:  0,
		PrimaryKeyCols: []int{0},
	}
	// Write as v1, load to get v3, then patch to v2
	require.NoError(t, writeFullFileV1(v2Path2, info2, nil, 4, keyRows))
	fs1, err := NewFileStorage(dir)
	require.NoError(t, err)
	require.NoError(t, fs1.loadTable("legacy2b"))

	// Now legacy2b.dat should be v3; patch to v2
	data2, err := os.ReadFile(v2Path2)
	require.NoError(t, err)
	assert.Equal(t, byte(0x03), data2[6], "should be v3 after v1 migration")
	data2[6] = 0x02
	require.NoError(t, os.WriteFile(v2Path2, data2, 0644))

	// Phase 2: Load from v2 (should migrate to v3)
	fs2, err := NewFileStorage(dir)
	require.NoError(t, err)
	require.NoError(t, fs2.loadTable("legacy2b"))

	rows, err := fs2.Scan("legacy2b")
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Equal(t, int64(1), rows[0][0])
	assert.Equal(t, "alpha", rows[0][1])
	assert.Equal(t, int64(2), rows[1][0])
	assert.Equal(t, "beta", rows[1][1])
	assert.Equal(t, int64(3), rows[2][0])
	assert.Equal(t, "gamma", rows[2][1])

	// Verify file is now v3
	data3, err := os.ReadFile(v2Path2)
	require.NoError(t, err)
	assert.Equal(t, byte(0x03), data3[6], "should be v3 after v2 migration")
}

func TestV2AutoIncrementReload(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create table without PK (auto-increment)
	fs1, err := NewFileStorage(dir)
	require.NoError(t, err)

	info := &storage.TableInfo{
		Name: "items",
		Columns: []storage.ColumnInfo{
			{Name: "a", DataType: "TEXT", Index: 0},
			{Name: "b", DataType: "INT", Index: 1},
		},
		PrimaryKeyCol: -1,
	}
	fs1.CreateTable(info)
	require.NoError(t, fs1.Insert("items", storage.Row{"x", int64(10)}))
	require.NoError(t, fs1.Insert("items", storage.Row{"y", int64(20)}))

	// Phase 2: Reload
	fs2, err := NewFileStorage(dir)
	require.NoError(t, err)
	require.NoError(t, fs2.LoadAll())

	rows, err := fs2.Scan("items")
	require.NoError(t, err)
	require.Len(t, rows, 2)

	// Insert more data to check auto-increment continues
	require.NoError(t, fs2.Insert("items", storage.Row{"z", int64(30)}))

	rows, err = fs2.Scan("items")
	require.NoError(t, err)
	require.Len(t, rows, 3)
}

func TestMultipleTablesReload(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create two tables
	fs1, err := NewFileStorage(dir)
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
	fs1.CreateTable(info1)
	fs1.CreateTable(info2)
	require.NoError(t, fs1.Insert("users", storage.Row{int64(1), "alice"}))
	require.NoError(t, fs1.Insert("orders", storage.Row{int64(100), int64(500)}))

	// Phase 2: Reload
	fs2, err := NewFileStorage(dir)
	require.NoError(t, err)
	require.NoError(t, fs2.LoadAll())

	users, err := fs2.Scan("users")
	require.NoError(t, err)
	require.Len(t, users, 1)
	assert.Equal(t, "alice", users[0][1])

	orders, err := fs2.Scan("orders")
	require.NoError(t, err)
	require.Len(t, orders, 1)
	assert.Equal(t, int64(500), orders[0][1])
}

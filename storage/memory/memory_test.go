package memory

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/walf443/oresql/storage"
)

// --- helpers ---

// makeTable creates a simple TableInfo with the given columns.
// Columns are specified as pairs of (name, dataType).
func makeTable(name string, pkCol int, cols ...string) *storage.TableInfo {
	info := &storage.TableInfo{
		Name:          name,
		PrimaryKeyCol: pkCol,
	}
	for i := 0; i < len(cols); i += 2 {
		info.Columns = append(info.Columns, storage.ColumnInfo{
			Name:     cols[i],
			DataType: cols[i+1],
			Index:    i / 2,
		})
	}
	if pkCol >= 0 {
		info.PrimaryKeyCols = []int{pkCol}
		info.Columns[pkCol].PrimaryKey = true
	}
	return info
}

func setupUsersTable(t *testing.T) *MemoryStorage {
	t.Helper()
	s := NewMemoryStorage()
	info := makeTable("users", 0, "id", "INT", "name", "TEXT")
	s.CreateTable(info)
	return s
}

func insertUsers(t *testing.T, s *MemoryStorage) {
	t.Helper()
	require.NoError(t, s.Insert("users", storage.Row{int64(1), "alice"}))
	require.NoError(t, s.Insert("users", storage.Row{int64(2), "bob"}))
	require.NoError(t, s.Insert("users", storage.Row{int64(3), "charlie"}))
}

// --- Basic lifecycle ---

func TestCreateInsertScan(t *testing.T) {
	s := setupUsersTable(t)
	insertUsers(t, s)

	rows, err := s.Scan("users")
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Equal(t, "alice", rows[0][1])
	assert.Equal(t, "bob", rows[1][1])
	assert.Equal(t, "charlie", rows[2][1])
}

// --- Insert ---

func TestInsertWithPK(t *testing.T) {
	s := setupUsersTable(t)
	require.NoError(t, s.Insert("users", storage.Row{int64(10), "alice"}))

	row, ok := s.GetRow("users", 10)
	assert.True(t, ok)
	assert.Equal(t, "alice", row[1])
}

func TestInsertWithAutoIncrement(t *testing.T) {
	s := NewMemoryStorage()
	info := makeTable("logs", -1, "msg", "TEXT")
	s.CreateTable(info)

	require.NoError(t, s.Insert("logs", storage.Row{"first"}))
	require.NoError(t, s.Insert("logs", storage.Row{"second"}))

	rows, err := s.ScanWithKeys("logs")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, int64(1), rows[0].Key)
	assert.Equal(t, int64(2), rows[1].Key)
}

func TestInsertDuplicatePK(t *testing.T) {
	s := setupUsersTable(t)
	require.NoError(t, s.Insert("users", storage.Row{int64(1), "alice"}))
	err := s.Insert("users", storage.Row{int64(1), "bob"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate primary key")
}

func TestInsertNonexistentTable(t *testing.T) {
	s := NewMemoryStorage()
	err := s.Insert("nonexistent", storage.Row{int64(1)})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

// --- DeleteByKeys ---

func TestDeleteByKeys(t *testing.T) {
	s := setupUsersTable(t)
	insertUsers(t, s)

	require.NoError(t, s.DeleteByKeys("users", []int64{2}))

	rows, err := s.Scan("users")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, "alice", rows[0][1])
	assert.Equal(t, "charlie", rows[1][1])
}

func TestDeleteByKeysWithIndex(t *testing.T) {
	s := setupUsersTable(t)
	insertUsers(t, s)

	idxInfo := &storage.IndexInfo{
		Name: "idx_name", TableName: "users",
		ColumnNames: []string{"name"}, ColumnIdxs: []int{1},
		Type: "BTREE",
	}
	require.NoError(t, s.CreateIndex(idxInfo))

	require.NoError(t, s.DeleteByKeys("users", []int64{1}))

	// Index should no longer contain "alice"
	idx := s.LookupSingleColumnIndex("users", 1)
	require.NotNil(t, idx)
	keys := idx.Lookup([]storage.Value{"alice"})
	assert.Empty(t, keys)

	// "bob" should still be in index
	keys = idx.Lookup([]storage.Value{"bob"})
	assert.Len(t, keys, 1)
}

// --- UpdateRow ---

func TestUpdateRow(t *testing.T) {
	s := setupUsersTable(t)
	insertUsers(t, s)

	require.NoError(t, s.UpdateRow("users", 1, storage.Row{int64(1), "alicia"}))

	row, ok := s.GetRow("users", 1)
	assert.True(t, ok)
	assert.Equal(t, "alicia", row[1])
}

func TestUpdateRowUniqueViolationRollback(t *testing.T) {
	s := setupUsersTable(t)
	insertUsers(t, s)

	idxInfo := &storage.IndexInfo{
		Name: "idx_name_unique", TableName: "users",
		ColumnNames: []string{"name"}, ColumnIdxs: []int{1},
		Type: "BTREE", Unique: true,
	}
	require.NoError(t, s.CreateIndex(idxInfo))

	// Try to update alice's name to "bob" (duplicate)
	err := s.UpdateRow("users", 1, storage.Row{int64(1), "bob"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unique constraint")

	// Original value should be preserved
	row, ok := s.GetRow("users", 1)
	assert.True(t, ok)
	assert.Equal(t, "alice", row[1])
}

// --- TruncateTable ---

func TestTruncateTable(t *testing.T) {
	s := setupUsersTable(t)
	insertUsers(t, s)

	idxInfo := &storage.IndexInfo{
		Name: "idx_name", TableName: "users",
		ColumnNames: []string{"name"}, ColumnIdxs: []int{1},
		Type: "BTREE",
	}
	require.NoError(t, s.CreateIndex(idxInfo))

	s.TruncateTable("users")

	count, err := s.RowCount("users")
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Index should also be cleared
	idx := s.LookupSingleColumnIndex("users", 1)
	require.NotNil(t, idx)
	keys := idx.Lookup([]storage.Value{"alice"})
	assert.Empty(t, keys)

	// Auto-increment should be reset: inserting should use key=1 for non-PK
	// For PK table, just verify we can insert again
	require.NoError(t, s.Insert("users", storage.Row{int64(1), "newuser"}))
	count, err = s.RowCount("users")
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// --- DropTable ---

func TestDropTable(t *testing.T) {
	s := setupUsersTable(t)
	insertUsers(t, s)

	idxInfo := &storage.IndexInfo{
		Name: "idx_name", TableName: "users",
		ColumnNames: []string{"name"}, ColumnIdxs: []int{1},
		Type: "BTREE",
	}
	require.NoError(t, s.CreateIndex(idxInfo))

	s.DropTable("users")

	// Table no longer exists
	_, err := s.Scan("users")
	assert.Error(t, err)

	// Index registry cleaned up
	assert.False(t, s.HasIndex("idx_name"))
}

// --- AddColumn ---

func TestAddColumn(t *testing.T) {
	s := setupUsersTable(t)
	insertUsers(t, s)

	require.NoError(t, s.AddColumn("users", "default_value"))

	rows, err := s.Scan("users")
	require.NoError(t, err)
	for _, row := range rows {
		require.Len(t, row, 3)
		assert.Equal(t, "default_value", row[2])
	}
}

func TestAddColumnNilDefault(t *testing.T) {
	s := setupUsersTable(t)
	insertUsers(t, s)

	require.NoError(t, s.AddColumn("users", nil))

	rows, err := s.Scan("users")
	require.NoError(t, err)
	for _, row := range rows {
		require.Len(t, row, 3)
		assert.Nil(t, row[2])
	}
}

// --- DropColumn ---

func TestDropColumn(t *testing.T) {
	s := NewMemoryStorage()
	info := makeTable("t", -1, "a", "INT", "b", "TEXT", "c", "INT")
	s.CreateTable(info)
	require.NoError(t, s.Insert("t", storage.Row{int64(1), "hello", int64(100)}))

	require.NoError(t, s.DropColumn("t", 1)) // drop column "b"

	rows, err := s.Scan("t")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Len(t, rows[0], 2)
	assert.Equal(t, int64(1), rows[0][0])
	assert.Equal(t, int64(100), rows[0][1])
}

func TestDropColumnDeletesSingleColumnIndex(t *testing.T) {
	s := NewMemoryStorage()
	info := makeTable("t", -1, "a", "INT", "b", "TEXT")
	s.CreateTable(info)

	idxInfo := &storage.IndexInfo{
		Name: "idx_b", TableName: "t",
		ColumnNames: []string{"b"}, ColumnIdxs: []int{1},
		Type: "BTREE",
	}
	require.NoError(t, s.CreateIndex(idxInfo))

	require.NoError(t, s.DropColumn("t", 1))
	assert.False(t, s.HasIndex("idx_b"))
}

func TestDropColumnCompositeIndexError(t *testing.T) {
	s := NewMemoryStorage()
	info := makeTable("t", -1, "a", "INT", "b", "TEXT", "c", "INT")
	s.CreateTable(info)

	idxInfo := &storage.IndexInfo{
		Name: "idx_ab", TableName: "t",
		ColumnNames: []string{"a", "b"}, ColumnIdxs: []int{0, 1},
		Type: "BTREE",
	}
	require.NoError(t, s.CreateIndex(idxInfo))

	err := s.DropColumn("t", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "composite index")
}

// --- ScanOrdered ---

func TestScanOrdered(t *testing.T) {
	s := setupUsersTable(t)
	insertUsers(t, s)

	t.Run("ascending", func(t *testing.T) {
		rows, err := s.ScanOrdered("users", false, 0)
		require.NoError(t, err)
		require.Len(t, rows, 3)
		assert.Equal(t, int64(1), rows[0][0])
		assert.Equal(t, int64(2), rows[1][0])
		assert.Equal(t, int64(3), rows[2][0])
	})

	t.Run("descending", func(t *testing.T) {
		rows, err := s.ScanOrdered("users", true, 0)
		require.NoError(t, err)
		require.Len(t, rows, 3)
		assert.Equal(t, int64(3), rows[0][0])
		assert.Equal(t, int64(2), rows[1][0])
		assert.Equal(t, int64(1), rows[2][0])
	})

	t.Run("with limit", func(t *testing.T) {
		rows, err := s.ScanOrdered("users", false, 2)
		require.NoError(t, err)
		require.Len(t, rows, 2)
		assert.Equal(t, int64(1), rows[0][0])
		assert.Equal(t, int64(2), rows[1][0])
	})
}

// --- ScanWithKeys ---

func TestScanWithKeys(t *testing.T) {
	s := setupUsersTable(t)
	insertUsers(t, s)

	keyRows, err := s.ScanWithKeys("users")
	require.NoError(t, err)
	require.Len(t, keyRows, 3)
	assert.Equal(t, int64(1), keyRows[0].Key)
	assert.Equal(t, "alice", keyRows[0].Row[1])
}

// --- GetByKeys / GetKeyRowsByKeys ---

func TestGetByKeys(t *testing.T) {
	s := setupUsersTable(t)
	insertUsers(t, s)

	rows, err := s.GetByKeys("users", []int64{1, 3})
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, "alice", rows[0][1])
	assert.Equal(t, "charlie", rows[1][1])
}

func TestGetByKeysNonexistentKey(t *testing.T) {
	s := setupUsersTable(t)
	insertUsers(t, s)

	rows, err := s.GetByKeys("users", []int64{999})
	require.NoError(t, err)
	assert.Empty(t, rows)
}

func TestGetKeyRowsByKeys(t *testing.T) {
	s := setupUsersTable(t)
	insertUsers(t, s)

	keyRows, err := s.GetKeyRowsByKeys("users", []int64{2})
	require.NoError(t, err)
	require.Len(t, keyRows, 1)
	assert.Equal(t, int64(2), keyRows[0].Key)
	assert.Equal(t, "bob", keyRows[0].Row[1])
}

// --- GetRow ---

func TestGetRow(t *testing.T) {
	s := setupUsersTable(t)
	insertUsers(t, s)

	row, ok := s.GetRow("users", 2)
	assert.True(t, ok)
	assert.Equal(t, "bob", row[1])
}

func TestGetRowNotFound(t *testing.T) {
	s := setupUsersTable(t)

	row, ok := s.GetRow("users", 999)
	assert.False(t, ok)
	assert.Nil(t, row)
}

func TestGetRowNonexistentTable(t *testing.T) {
	s := NewMemoryStorage()
	row, ok := s.GetRow("nonexistent", 1)
	assert.False(t, ok)
	assert.Nil(t, row)
}

// --- ForEachRow ---

func TestForEachRow(t *testing.T) {
	s := setupUsersTable(t)
	insertUsers(t, s)

	t.Run("forward", func(t *testing.T) {
		var ids []int64
		err := s.ForEachRow("users", false, func(key int64, row storage.Row) bool {
			ids = append(ids, key)
			return true
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{1, 2, 3}, ids)
	})

	t.Run("reverse", func(t *testing.T) {
		var ids []int64
		err := s.ForEachRow("users", true, func(key int64, row storage.Row) bool {
			ids = append(ids, key)
			return true
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{3, 2, 1}, ids)
	})

	t.Run("early stop", func(t *testing.T) {
		var ids []int64
		err := s.ForEachRow("users", false, func(key int64, row storage.Row) bool {
			ids = append(ids, key)
			return key < 2 // stop after key=2
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{1, 2}, ids)
	})
}

// --- RowCount ---

func TestRowCount(t *testing.T) {
	s := setupUsersTable(t)

	count, err := s.RowCount("users")
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	insertUsers(t, s)
	count, err = s.RowCount("users")
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestRowCountNonexistentTable(t *testing.T) {
	s := NewMemoryStorage()
	_, err := s.RowCount("nonexistent")
	assert.Error(t, err)
}

// --- CreateIndex ---

func TestCreateIndex(t *testing.T) {
	s := setupUsersTable(t)
	insertUsers(t, s)

	idxInfo := &storage.IndexInfo{
		Name: "idx_name", TableName: "users",
		ColumnNames: []string{"name"}, ColumnIdxs: []int{1},
		Type: "BTREE",
	}
	require.NoError(t, s.CreateIndex(idxInfo))

	assert.True(t, s.HasIndex("idx_name"))

	// Index should be built from existing data
	idx := s.LookupSingleColumnIndex("users", 1)
	require.NotNil(t, idx)
	keys := idx.Lookup([]storage.Value{"alice"})
	assert.Equal(t, []int64{1}, keys)
}

func TestCreateUniqueIndexViolation(t *testing.T) {
	s := setupUsersTable(t)
	require.NoError(t, s.Insert("users", storage.Row{int64(1), "alice"}))
	require.NoError(t, s.Insert("users", storage.Row{int64(2), "alice"})) // duplicate name

	idxInfo := &storage.IndexInfo{
		Name: "idx_name_unique", TableName: "users",
		ColumnNames: []string{"name"}, ColumnIdxs: []int{1},
		Type: "BTREE", Unique: true,
	}
	err := s.CreateIndex(idxInfo)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unique constraint")
}

// --- DropIndex ---

func TestDropIndex(t *testing.T) {
	s := setupUsersTable(t)
	idxInfo := &storage.IndexInfo{
		Name: "idx_name", TableName: "users",
		ColumnNames: []string{"name"}, ColumnIdxs: []int{1},
		Type: "BTREE",
	}
	require.NoError(t, s.CreateIndex(idxInfo))
	assert.True(t, s.HasIndex("idx_name"))

	require.NoError(t, s.DropIndex("idx_name"))
	assert.False(t, s.HasIndex("idx_name"))
}

func TestDropIndexNonexistent(t *testing.T) {
	s := NewMemoryStorage()
	err := s.DropIndex("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

// --- HasIndex ---

func TestHasIndex(t *testing.T) {
	s := setupUsersTable(t)
	assert.False(t, s.HasIndex("idx_name"))

	idxInfo := &storage.IndexInfo{
		Name: "idx_name", TableName: "users",
		ColumnNames: []string{"name"}, ColumnIdxs: []int{1},
		Type: "BTREE",
	}
	require.NoError(t, s.CreateIndex(idxInfo))
	assert.True(t, s.HasIndex("idx_name"))
}

// --- LookupIndex / LookupSingleColumnIndex ---

func TestLookupIndex(t *testing.T) {
	s := setupUsersTable(t)
	idxInfo := &storage.IndexInfo{
		Name: "idx_name", TableName: "users",
		ColumnNames: []string{"name"}, ColumnIdxs: []int{1},
		Type: "BTREE",
	}
	require.NoError(t, s.CreateIndex(idxInfo))

	idx := s.LookupIndex("users", []int{1})
	assert.NotNil(t, idx)
	assert.Equal(t, "idx_name", idx.GetInfo().Name)

	// Non-matching columns
	idx = s.LookupIndex("users", []int{0})
	assert.Nil(t, idx)

	// Non-existing table
	idx = s.LookupIndex("nonexistent", []int{1})
	assert.Nil(t, idx)
}

func TestLookupSingleColumnIndex(t *testing.T) {
	s := setupUsersTable(t)
	idxInfo := &storage.IndexInfo{
		Name: "idx_name", TableName: "users",
		ColumnNames: []string{"name"}, ColumnIdxs: []int{1},
		Type: "BTREE",
	}
	require.NoError(t, s.CreateIndex(idxInfo))

	idx := s.LookupSingleColumnIndex("users", 1)
	assert.NotNil(t, idx)

	idx = s.LookupSingleColumnIndex("users", 0)
	assert.Nil(t, idx)
}

// --- GetIndexes ---

func TestGetIndexes(t *testing.T) {
	s := setupUsersTable(t)

	indexes := s.GetIndexes("users")
	assert.Empty(t, indexes)

	idxInfo := &storage.IndexInfo{
		Name: "idx_name", TableName: "users",
		ColumnNames: []string{"name"}, ColumnIdxs: []int{1},
		Type: "BTREE",
	}
	require.NoError(t, s.CreateIndex(idxInfo))

	indexes = s.GetIndexes("users")
	assert.Len(t, indexes, 1)
}

func TestGetIndexesNonexistentTable(t *testing.T) {
	s := NewMemoryStorage()
	indexes := s.GetIndexes("nonexistent")
	assert.Nil(t, indexes)
}

// --- SecondaryIndex.Lookup ---

func TestSecondaryIndexLookup(t *testing.T) {
	s := setupUsersTable(t)
	insertUsers(t, s)

	idxInfo := &storage.IndexInfo{
		Name: "idx_name", TableName: "users",
		ColumnNames: []string{"name"}, ColumnIdxs: []int{1},
		Type: "BTREE",
	}
	require.NoError(t, s.CreateIndex(idxInfo))

	idx := s.LookupSingleColumnIndex("users", 1)
	require.NotNil(t, idx)

	keys := idx.Lookup([]storage.Value{"bob"})
	assert.Equal(t, []int64{2}, keys)

	keys = idx.Lookup([]storage.Value{"nonexistent"})
	assert.Nil(t, keys)
}

// --- SecondaryIndex.RangeScan ---

func TestSecondaryIndexRangeScan(t *testing.T) {
	s := NewMemoryStorage()
	info := makeTable("scores", 0, "id", "INT", "score", "INT")
	s.CreateTable(info)

	require.NoError(t, s.Insert("scores", storage.Row{int64(1), int64(10)}))
	require.NoError(t, s.Insert("scores", storage.Row{int64(2), int64(20)}))
	require.NoError(t, s.Insert("scores", storage.Row{int64(3), int64(30)}))
	require.NoError(t, s.Insert("scores", storage.Row{int64(4), int64(40)}))
	require.NoError(t, s.Insert("scores", storage.Row{int64(5), int64(50)}))

	idxInfo := &storage.IndexInfo{
		Name: "idx_score", TableName: "scores",
		ColumnNames: []string{"score"}, ColumnIdxs: []int{1},
		Type: "BTREE",
	}
	require.NoError(t, s.CreateIndex(idxInfo))

	idx := s.LookupSingleColumnIndex("scores", 1)
	require.NotNil(t, idx)

	t.Run("inclusive range", func(t *testing.T) {
		from := storage.Value(int64(20))
		to := storage.Value(int64(40))
		keys := idx.RangeScan(&from, true, &to, true)
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		assert.Equal(t, []int64{2, 3, 4}, keys)
	})

	t.Run("exclusive range", func(t *testing.T) {
		from := storage.Value(int64(20))
		to := storage.Value(int64(40))
		keys := idx.RangeScan(&from, false, &to, false)
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		assert.Equal(t, []int64{3}, keys)
	})

	t.Run("unbounded from", func(t *testing.T) {
		to := storage.Value(int64(20))
		keys := idx.RangeScan(nil, false, &to, true)
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		assert.Equal(t, []int64{1, 2}, keys)
	})

	t.Run("unbounded to", func(t *testing.T) {
		from := storage.Value(int64(40))
		keys := idx.RangeScan(&from, true, nil, false)
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		assert.Equal(t, []int64{4, 5}, keys)
	})
}

func TestRangeScanCompositeReturnsNil(t *testing.T) {
	s := NewMemoryStorage()
	info := makeTable("t", -1, "a", "INT", "b", "INT")
	s.CreateTable(info)

	idxInfo := &storage.IndexInfo{
		Name: "idx_ab", TableName: "t",
		ColumnNames: []string{"a", "b"}, ColumnIdxs: []int{0, 1},
		Type: "BTREE",
	}
	require.NoError(t, s.CreateIndex(idxInfo))

	idx := s.LookupIndex("t", []int{0, 1})
	require.NotNil(t, idx)

	// RangeScan should return nil for composite indexes
	from := storage.Value(int64(1))
	keys := idx.RangeScan(&from, true, nil, false)
	assert.Nil(t, keys)
}

// --- SecondaryIndex.CompositeRangeScan ---

func TestSecondaryIndexCompositeRangeScan(t *testing.T) {
	s := NewMemoryStorage()
	info := makeTable("orders", -1, "dept", "INT", "score", "INT")
	s.CreateTable(info)

	// dept=1: scores 10, 20, 30
	require.NoError(t, s.Insert("orders", storage.Row{int64(1), int64(10)}))
	require.NoError(t, s.Insert("orders", storage.Row{int64(1), int64(20)}))
	require.NoError(t, s.Insert("orders", storage.Row{int64(1), int64(30)}))
	// dept=2: scores 15, 25
	require.NoError(t, s.Insert("orders", storage.Row{int64(2), int64(15)}))
	require.NoError(t, s.Insert("orders", storage.Row{int64(2), int64(25)}))

	idxInfo := &storage.IndexInfo{
		Name: "idx_dept_score", TableName: "orders",
		ColumnNames: []string{"dept", "score"}, ColumnIdxs: []int{0, 1},
		Type: "BTREE",
	}
	require.NoError(t, s.CreateIndex(idxInfo))

	idx := s.LookupIndex("orders", []int{0, 1})
	require.NotNil(t, idx)

	t.Run("range within dept=1", func(t *testing.T) {
		from := storage.Value(int64(15))
		to := storage.Value(int64(25))
		keys := idx.CompositeRangeScan(
			[]storage.Value{int64(1)},
			&from, true, &to, true,
		)
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		assert.Equal(t, []int64{2}, keys) // only score=20 is in [15,25]
	})

	t.Run("unbounded range in dept=2", func(t *testing.T) {
		keys := idx.CompositeRangeScan(
			[]storage.Value{int64(2)},
			nil, false, nil, false,
		)
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		assert.Equal(t, []int64{4, 5}, keys) // all dept=2 rows
	})
}

// --- SecondaryIndex.OrderedRangeScan ---

func TestSecondaryIndexOrderedRangeScan(t *testing.T) {
	s := NewMemoryStorage()
	info := makeTable("scores", 0, "id", "INT", "score", "INT")
	s.CreateTable(info)

	require.NoError(t, s.Insert("scores", storage.Row{int64(1), int64(10)}))
	require.NoError(t, s.Insert("scores", storage.Row{int64(2), int64(20)}))
	require.NoError(t, s.Insert("scores", storage.Row{int64(3), int64(30)}))

	idxInfo := &storage.IndexInfo{
		Name: "idx_score", TableName: "scores",
		ColumnNames: []string{"score"}, ColumnIdxs: []int{1},
		Type: "BTREE",
	}
	require.NoError(t, s.CreateIndex(idxInfo))

	idx := s.LookupSingleColumnIndex("scores", 1)
	require.NotNil(t, idx)

	t.Run("ascending", func(t *testing.T) {
		var keys []int64
		idx.OrderedRangeScan(nil, false, nil, false, false, func(rowKey int64) bool {
			keys = append(keys, rowKey)
			return true
		})
		assert.Equal(t, []int64{1, 2, 3}, keys)
	})

	t.Run("descending", func(t *testing.T) {
		var keys []int64
		idx.OrderedRangeScan(nil, false, nil, false, true, func(rowKey int64) bool {
			keys = append(keys, rowKey)
			return true
		})
		assert.Equal(t, []int64{3, 2, 1}, keys)
	})

	t.Run("with range ascending", func(t *testing.T) {
		from := storage.Value(int64(10))
		to := storage.Value(int64(30))
		var keys []int64
		idx.OrderedRangeScan(&from, false, &to, false, false, func(rowKey int64) bool {
			keys = append(keys, rowKey)
			return true
		})
		assert.Equal(t, []int64{2}, keys) // only score=20
	})

	t.Run("early stop", func(t *testing.T) {
		var keys []int64
		idx.OrderedRangeScan(nil, false, nil, false, false, func(rowKey int64) bool {
			keys = append(keys, rowKey)
			return len(keys) < 2
		})
		assert.Equal(t, []int64{1, 2}, keys)
	})
}

// --- NULL handling ---

func TestNullInIndex(t *testing.T) {
	s := NewMemoryStorage()
	info := makeTable("t", -1, "val", "INT")
	s.CreateTable(info)

	require.NoError(t, s.Insert("t", storage.Row{nil}))
	require.NoError(t, s.Insert("t", storage.Row{int64(1)}))

	idxInfo := &storage.IndexInfo{
		Name: "idx_val", TableName: "t",
		ColumnNames: []string{"val"}, ColumnIdxs: []int{0},
		Type: "BTREE",
	}
	require.NoError(t, s.CreateIndex(idxInfo))

	idx := s.LookupSingleColumnIndex("t", 0)
	require.NotNil(t, idx)

	// NULL should be findable via Lookup
	keys := idx.Lookup([]storage.Value{nil})
	assert.Len(t, keys, 1)

	// Non-NULL should also work
	keys = idx.Lookup([]storage.Value{int64(1)})
	assert.Len(t, keys, 1)
}

func TestNullUniqueConstraintAllowsMultipleNulls(t *testing.T) {
	s := NewMemoryStorage()
	info := makeTable("t", -1, "val", "INT")
	s.CreateTable(info)

	require.NoError(t, s.Insert("t", storage.Row{nil}))
	require.NoError(t, s.Insert("t", storage.Row{nil}))

	idxInfo := &storage.IndexInfo{
		Name: "idx_val_unique", TableName: "t",
		ColumnNames: []string{"val"}, ColumnIdxs: []int{0},
		Type: "BTREE", Unique: true,
	}
	// Multiple NULLs should not violate unique constraint
	require.NoError(t, s.CreateIndex(idxInfo))

	// But duplicate non-NULL should fail
	require.NoError(t, s.Insert("t", storage.Row{int64(1)}))
	err := s.Insert("t", storage.Row{int64(1)})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unique constraint")
}

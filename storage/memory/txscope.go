package memory

import (
	"sort"
	"strings"

	"github.com/walf443/oresql/storage"
)

// Compile-time verification that MemoryStorage satisfies the TableLocker interface.
var _ storage.TableLocker = (*MemoryStorage)(nil)

// WithTableLocks acquires table-level locks in alphabetical order, executes fn,
// then releases locks in reverse order.
//
// s.mu is held only briefly (RLock) for table lookup, then released before fn()
// is called. This allows DDL storage methods inside fn() to acquire s.mu.Lock()
// without deadlock. Table-level locks (tbl.mu) remain held for the duration of fn().
func (s *MemoryStorage) WithTableLocks(locks []storage.TableLock, catalogWrite bool, fn func() error) error {
	// Sort by table name for consistent ordering to prevent deadlocks
	sorted := make([]storage.TableLock, len(locks))
	copy(sorted, locks)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].TableName < sorted[j].TableName
	})

	// Acquire storage-level RLock briefly for table lookup
	s.mu.RLock()

	// Acquire table-level locks in sorted order
	lockedTables := make([]*table, 0, len(sorted))
	lockedModes := make([]storage.TableLockMode, 0, len(sorted))
	for _, lock := range sorted {
		tbl, ok := s.tables[strings.ToLower(lock.TableName)]
		if !ok {
			// Table doesn't exist; unlock what we've locked and let fn handle the error
			for i := len(lockedTables) - 1; i >= 0; i-- {
				if lockedModes[i] == storage.TableLockWrite {
					lockedTables[i].mu.Unlock()
				} else {
					lockedTables[i].mu.RUnlock()
				}
			}
			s.mu.RUnlock()
			return fn()
		}
		if lock.Mode == storage.TableLockWrite {
			tbl.mu.Lock()
		} else {
			tbl.mu.RLock()
		}
		lockedTables = append(lockedTables, tbl)
		lockedModes = append(lockedModes, lock.Mode)
	}

	// Release s.mu before calling fn — DDL methods acquire s.mu.Lock() internally
	s.mu.RUnlock()

	// Execute the function
	err := fn()

	// Release table locks in reverse order
	for i := len(lockedTables) - 1; i >= 0; i-- {
		if lockedModes[i] == storage.TableLockWrite {
			lockedTables[i].mu.Unlock()
		} else {
			lockedTables[i].mu.RUnlock()
		}
	}

	return err
}

// WithCatalogLock acquires only the storage-level lock (no table locks).
// Used for operations where no table exists yet (e.g., CreateTable).
// DDL storage methods handle s.mu.Lock() internally, so this is a no-op pass-through.
func (s *MemoryStorage) WithCatalogLock(write bool, fn func() error) error {
	return fn()
}

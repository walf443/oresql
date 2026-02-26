package memory

import (
	"sort"
	"strings"

	"github.com/walf443/oresql/storage"
)

// Compile-time verification that MemoryStorage satisfies the TableLocker interface.
var _ storage.TableLocker = (*MemoryStorage)(nil)

// WithTableLocks acquires table-level locks in alphabetical order, executes fn,
// then releases locks in reverse order. catalogWrite controls whether the
// storage-level mutex is acquired as a write lock (for DDL) or read lock (for DML).
func (s *MemoryStorage) WithTableLocks(locks []storage.TableLock, catalogWrite bool, fn func() error) error {
	// Sort by table name for consistent ordering to prevent deadlocks
	sorted := make([]storage.TableLock, len(locks))
	copy(sorted, locks)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].TableName < sorted[j].TableName
	})

	// Acquire storage-level lock (protects tables map)
	if catalogWrite {
		s.mu.Lock()
	} else {
		s.mu.RLock()
	}

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
			if catalogWrite {
				s.mu.Unlock()
			} else {
				s.mu.RUnlock()
			}
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

	// Release storage-level lock
	if catalogWrite {
		s.mu.Unlock()
	} else {
		s.mu.RUnlock()
	}

	return err
}

// WithCatalogLock acquires only the storage-level lock (no table locks).
// Used for CreateTable where the table doesn't exist yet.
func (s *MemoryStorage) WithCatalogLock(write bool, fn func() error) error {
	if write {
		s.mu.Lock()
		defer s.mu.Unlock()
	} else {
		s.mu.RLock()
		defer s.mu.RUnlock()
	}
	return fn()
}

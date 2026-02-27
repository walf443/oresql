package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// DatabaseManager manages multiple named databases.
type DatabaseManager struct {
	mu          sync.RWMutex
	databases   map[string]*Database // key: lowercase name
	dataDir     string               // root data directory (empty for in-memory)
	storageType string               // "memory", "file" (default), or "disk"
}

// NewDatabaseManager creates a new DatabaseManager with a "default" database.
func NewDatabaseManager(dataDir string, storageType ...string) *DatabaseManager {
	st := ""
	if len(storageType) > 0 {
		st = storageType[0]
	}
	mgr := &DatabaseManager{
		databases:   make(map[string]*Database),
		dataDir:     dataDir,
		storageType: st,
	}

	// Create the default database
	var dbOpts []DatabaseOption
	if dataDir != "" {
		dbDir := filepath.Join(dataDir, "default")
		dbOpts = append(dbOpts, WithDataDir(dbDir))
	}
	if st != "" {
		dbOpts = append(dbOpts, WithStorageType(st))
	}
	mgr.databases["default"] = NewDatabase("default", dbOpts...)

	return mgr
}

// CreateDatabase creates a new database with the given name.
func (mgr *DatabaseManager) CreateDatabase(name string) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	key := strings.ToLower(name)
	if _, exists := mgr.databases[key]; exists {
		return fmt.Errorf("database %q already exists", key)
	}

	var dbOpts []DatabaseOption
	if mgr.dataDir != "" {
		dbDir := filepath.Join(mgr.dataDir, key)
		if err := os.MkdirAll(dbDir, 0755); err != nil {
			return fmt.Errorf("failed to create database directory: %w", err)
		}
		dbOpts = append(dbOpts, WithDataDir(dbDir))
	}
	if mgr.storageType != "" {
		dbOpts = append(dbOpts, WithStorageType(mgr.storageType))
	}

	mgr.databases[key] = NewDatabase(key, dbOpts...)
	return nil
}

// DropDatabase removes a database. The "default" database cannot be dropped.
func (mgr *DatabaseManager) DropDatabase(name string) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	key := strings.ToLower(name)
	if key == "default" {
		return fmt.Errorf("cannot drop the default database")
	}
	if _, exists := mgr.databases[key]; !exists {
		return fmt.Errorf("database %q does not exist", key)
	}

	delete(mgr.databases, key)

	// Remove data directory if persistent
	if mgr.dataDir != "" {
		dbDir := filepath.Join(mgr.dataDir, key)
		os.RemoveAll(dbDir)
	}

	return nil
}

// GetDatabase returns the database with the given name.
func (mgr *DatabaseManager) GetDatabase(name string) (*Database, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	key := strings.ToLower(name)
	db, exists := mgr.databases[key]
	if !exists {
		return nil, fmt.Errorf("database %q does not exist", key)
	}
	return db, nil
}

// ListDatabases returns a sorted list of database names.
func (mgr *DatabaseManager) ListDatabases() []string {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	names := make([]string, 0, len(mgr.databases))
	for name := range mgr.databases {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// LoadExistingDatabases scans the data directory for existing database subdirectories.
func (mgr *DatabaseManager) LoadExistingDatabases() error {
	if mgr.dataDir == "" {
		return nil
	}

	// Migrate legacy layout first
	mgr.migrateLegacyLayout()

	entries, err := os.ReadDir(mgr.dataDir)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %w", err)
	}

	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := strings.ToLower(entry.Name())
		if _, exists := mgr.databases[name]; exists {
			continue // already loaded (e.g., "default")
		}
		dbDir := filepath.Join(mgr.dataDir, name)
		dbOpts := []DatabaseOption{WithDataDir(dbDir)}
		if mgr.storageType != "" {
			dbOpts = append(dbOpts, WithStorageType(mgr.storageType))
		}
		mgr.databases[name] = NewDatabase(name, dbOpts...)
	}

	return nil
}

// migrateLegacyLayout moves *.dat files from the root data directory into default/.
func (mgr *DatabaseManager) migrateLegacyLayout() {
	if mgr.dataDir == "" {
		return
	}

	entries, err := os.ReadDir(mgr.dataDir)
	if err != nil {
		return
	}

	var datFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".dat") {
			datFiles = append(datFiles, entry.Name())
		}
	}

	if len(datFiles) == 0 {
		return
	}

	defaultDir := filepath.Join(mgr.dataDir, "default")
	os.MkdirAll(defaultDir, 0755)

	for _, name := range datFiles {
		src := filepath.Join(mgr.dataDir, name)
		dst := filepath.Join(defaultDir, name)
		os.Rename(src, dst)
	}
}

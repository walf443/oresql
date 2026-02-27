package engine

import (
	"fmt"

	"github.com/walf443/oresql/storage"
	"github.com/walf443/oresql/storage/disk"
	"github.com/walf443/oresql/storage/file"
	"github.com/walf443/oresql/storage/memory"
)

// Database represents a named database instance.
// It bundles a catalog (schema) and a storage engine together.
type Database struct {
	Name        string
	DataDir     string // empty string means in-memory only
	StorageType string // "memory", "file" (default), or "disk"
	catalog     *Catalog
	storage     StorageEngine
}

// DatabaseOption configures a Database.
type DatabaseOption func(*Database)

// WithDataDir sets the data directory for persistent storage.
func WithDataDir(dir string) DatabaseOption {
	return func(db *Database) {
		db.DataDir = dir
	}
}

// WithDatabaseStorage sets the storage engine for the Database.
func WithDatabaseStorage(s StorageEngine) DatabaseOption {
	return func(db *Database) {
		db.storage = s
	}
}

// WithStorageType sets the storage type ("memory", "file", or "disk").
func WithStorageType(storageType string) DatabaseOption {
	return func(db *Database) {
		db.StorageType = storageType
	}
}

// NewDatabase creates a new Database with the given name and options.
// By default, it uses an in-memory storage engine.
// If WithDataDir is specified, a FileStorage is created and all existing
// tables are loaded from disk into both the storage engine and the catalog.
func NewDatabase(name string, opts ...DatabaseOption) *Database {
	db := &Database{
		Name:    name,
		catalog: NewCatalog(),
	}
	for _, opt := range opts {
		opt(db)
	}

	// If DataDir is set and no custom storage was provided, create storage based on type
	if db.DataDir != "" && db.storage == nil {
		switch db.StorageType {
		case "disk":
			ds, err := disk.NewDiskStorage(db.DataDir)
			if err != nil {
				panic(fmt.Sprintf("failed to create disk storage: %v", err))
			}
			db.storage = ds

			if err := ds.LoadAll(); err != nil {
				panic(fmt.Sprintf("failed to load data from disk: %v", err))
			}

			db.restoreCatalog(ds)
		default:
			// Default: use FileStorage
			fs, err := file.NewFileStorage(db.DataDir)
			if err != nil {
				panic(fmt.Sprintf("failed to create file storage: %v", err))
			}
			db.storage = fs

			// Load all tables from disk
			if err := fs.LoadAll(); err != nil {
				panic(fmt.Sprintf("failed to load data from disk: %v", err))
			}

			// Restore catalog from the MetadataProvider
			db.restoreCatalog(fs)
		}
	}

	// Default to in-memory storage if none was set
	if db.storage == nil {
		db.storage = memory.NewMemoryStorage()
	}

	return db
}

// restoreCatalog loads table schemas from a MetadataProvider into the catalog.
func (db *Database) restoreCatalog(mp storage.MetadataProvider) {
	for _, tableName := range mp.ListTables() {
		info, _, _, err := mp.LoadTableMeta(tableName)
		if err != nil {
			continue
		}
		db.catalog.RestoreTable(info)
	}
}

// Catalog returns the database's catalog.
func (db *Database) Catalog() *Catalog {
	return db.catalog
}

// Storage returns the database's storage engine.
func (db *Database) Storage() StorageEngine {
	return db.storage
}

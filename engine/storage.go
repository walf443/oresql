package engine

import (
	"github.com/walf443/oresql/storage"
)

// Type aliases for backward compatibility with existing executor code.
type Value = storage.Value
type Row = storage.Row
type KeyRow = storage.KeyRow
type ColumnInfo = storage.ColumnInfo
type TableInfo = storage.TableInfo
type IndexInfo = storage.IndexInfo
type KeyEncoding = storage.KeyEncoding
type StorageEngine = storage.Engine
type IndexReader = storage.IndexReader

// encodeValue is a package-level alias for storage.EncodeValue,
// preserving backward compatibility with executor files.
var encodeValue = storage.EncodeValue

// encodeValues is a package-level alias for storage.EncodeValues,
// preserving backward compatibility with executor files.
func encodeValues(vals []Value) KeyEncoding {
	return storage.EncodeValues(vals)
}

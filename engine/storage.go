package engine

import (
	"github.com/walf443/oresql/storage"
)

// Type aliases for backward compatibility with existing executor code.
type Value = storage.Value
type Row = storage.Row
type ColumnInfo = storage.ColumnInfo
type TableInfo = storage.TableInfo

// encodeValue is a package-level alias for storage.EncodeValue,
// preserving backward compatibility with executor files.
var encodeValue = storage.EncodeValue

// encodeValues is a package-level alias for storage.EncodeValues,
// preserving backward compatibility with executor files.
func encodeValues(vals []Value) storage.KeyEncoding {
	return storage.EncodeValues(vals)
}

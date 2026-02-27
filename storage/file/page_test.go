package file

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/walf443/oresql/btree"
	"github.com/walf443/oresql/storage"
)

func TestPrimaryPageRoundTrip_Leaf(t *testing.T) {
	data := btree.NodeData[int64]{
		Leaf: true,
		Entries: []btree.EntryData[int64]{
			{Key: 1, Value: storage.Row{int64(1), "alice"}},
			{Key: 2, Value: storage.Row{int64(2), "bob"}},
			{Key: 3, Value: storage.Row{int64(3), nil}},
		},
	}

	encoded := encodePrimaryPage(data)
	decoded, err := decodePrimaryPage(encoded)
	require.NoError(t, err)

	assert.True(t, decoded.Leaf)
	require.Len(t, decoded.Entries, 3)
	assert.Nil(t, decoded.Children)

	assert.Equal(t, int64(1), decoded.Entries[0].Key)
	row0 := decoded.Entries[0].Value.(storage.Row)
	assert.Equal(t, int64(1), row0[0])
	assert.Equal(t, "alice", row0[1])

	assert.Equal(t, int64(2), decoded.Entries[1].Key)
	row1 := decoded.Entries[1].Value.(storage.Row)
	assert.Equal(t, int64(2), row1[0])
	assert.Equal(t, "bob", row1[1])

	assert.Equal(t, int64(3), decoded.Entries[2].Key)
	row2 := decoded.Entries[2].Value.(storage.Row)
	assert.Equal(t, int64(3), row2[0])
	assert.Nil(t, row2[1])
}

func TestPrimaryPageRoundTrip_Internal(t *testing.T) {
	// In B+Tree, internal nodes have nil values (routing keys only)
	data := btree.NodeData[int64]{
		Leaf: false,
		Entries: []btree.EntryData[int64]{
			{Key: 10, Value: nil},
		},
		Children: []uint32{0, 1},
	}

	encoded := encodePrimaryPage(data)
	decoded, err := decodePrimaryPage(encoded)
	require.NoError(t, err)

	assert.False(t, decoded.Leaf)
	require.Len(t, decoded.Entries, 1)
	assert.Equal(t, int64(10), decoded.Entries[0].Key)
	assert.Nil(t, decoded.Entries[0].Value)
	require.Len(t, decoded.Children, 2)
	assert.Equal(t, uint32(0), decoded.Children[0])
	assert.Equal(t, uint32(1), decoded.Children[1])
}

func TestPrimaryPageRoundTrip_Empty(t *testing.T) {
	data := btree.NodeData[int64]{
		Leaf:    true,
		Entries: nil,
	}

	encoded := encodePrimaryPage(data)
	decoded, err := decodePrimaryPage(encoded)
	require.NoError(t, err)

	assert.True(t, decoded.Leaf)
	assert.Len(t, decoded.Entries, 0)
}

func TestPrimaryPageRoundTrip_FloatAndNull(t *testing.T) {
	data := btree.NodeData[int64]{
		Leaf: true,
		Entries: []btree.EntryData[int64]{
			{Key: 1, Value: storage.Row{int64(1), float64(3.14), nil, "text"}},
		},
	}

	encoded := encodePrimaryPage(data)
	decoded, err := decodePrimaryPage(encoded)
	require.NoError(t, err)

	require.Len(t, decoded.Entries, 1)
	row := decoded.Entries[0].Value.(storage.Row)
	assert.Equal(t, int64(1), row[0])
	assert.Equal(t, float64(3.14), row[1])
	assert.Nil(t, row[2])
	assert.Equal(t, "text", row[3])
}

func TestPrimaryPageRoundTrip_NilValue(t *testing.T) {
	data := btree.NodeData[int64]{
		Leaf: true,
		Entries: []btree.EntryData[int64]{
			{Key: 1, Value: nil},
		},
	}

	encoded := encodePrimaryPage(data)
	decoded, err := decodePrimaryPage(encoded)
	require.NoError(t, err)

	require.Len(t, decoded.Entries, 1)
	assert.Nil(t, decoded.Entries[0].Value)
}

func TestSecondaryPageRoundTrip_Leaf(t *testing.T) {
	data := btree.NodeData[storage.KeyEncoding]{
		Leaf: true,
		Entries: []btree.EntryData[storage.KeyEncoding]{
			{Key: storage.EncodeValues([]storage.Value{"alice"}), Value: map[int64]struct{}{1: {}, 3: {}}},
			{Key: storage.EncodeValues([]storage.Value{"bob"}), Value: map[int64]struct{}{2: {}}},
		},
	}

	encoded := encodeSecondaryPage(data)
	decoded, err := decodeSecondaryPage(encoded)
	require.NoError(t, err)

	assert.True(t, decoded.Leaf)
	require.Len(t, decoded.Entries, 2)

	assert.Equal(t, data.Entries[0].Key, decoded.Entries[0].Key)
	keySet0 := decoded.Entries[0].Value.(map[int64]struct{})
	assert.Len(t, keySet0, 2)
	assert.Contains(t, keySet0, int64(1))
	assert.Contains(t, keySet0, int64(3))

	assert.Equal(t, data.Entries[1].Key, decoded.Entries[1].Key)
	keySet1 := decoded.Entries[1].Value.(map[int64]struct{})
	assert.Len(t, keySet1, 1)
	assert.Contains(t, keySet1, int64(2))
}

func TestSecondaryPageRoundTrip_Internal(t *testing.T) {
	// In B+Tree, internal nodes have nil values (routing keys only)
	data := btree.NodeData[storage.KeyEncoding]{
		Leaf: false,
		Entries: []btree.EntryData[storage.KeyEncoding]{
			{Key: storage.EncodeValues([]storage.Value{int64(42)}), Value: nil},
		},
		Children: []uint32{10, 20},
	}

	encoded := encodeSecondaryPage(data)
	decoded, err := decodeSecondaryPage(encoded)
	require.NoError(t, err)

	assert.False(t, decoded.Leaf)
	require.Len(t, decoded.Entries, 1)
	assert.Equal(t, data.Entries[0].Key, decoded.Entries[0].Key)
	assert.Nil(t, decoded.Entries[0].Value)
	require.Len(t, decoded.Children, 2)
	assert.Equal(t, uint32(10), decoded.Children[0])
	assert.Equal(t, uint32(20), decoded.Children[1])
}

func TestSecondaryPageRoundTrip_Empty(t *testing.T) {
	data := btree.NodeData[storage.KeyEncoding]{
		Leaf:    true,
		Entries: nil,
	}

	encoded := encodeSecondaryPage(data)
	decoded, err := decodeSecondaryPage(encoded)
	require.NoError(t, err)

	assert.True(t, decoded.Leaf)
	assert.Len(t, decoded.Entries, 0)
}

func TestSecondaryPageRoundTrip_EmptyKeySet(t *testing.T) {
	data := btree.NodeData[storage.KeyEncoding]{
		Leaf: true,
		Entries: []btree.EntryData[storage.KeyEncoding]{
			{Key: storage.EncodeValues([]storage.Value{nil}), Value: map[int64]struct{}{}},
		},
	}

	encoded := encodeSecondaryPage(data)
	decoded, err := decodeSecondaryPage(encoded)
	require.NoError(t, err)

	require.Len(t, decoded.Entries, 1)
	keySet := decoded.Entries[0].Value.(map[int64]struct{})
	assert.Len(t, keySet, 0)
}

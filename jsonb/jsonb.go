package jsonb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/walf443/oresql/json_path"
)

// Type tags for the custom binary format.
const (
	TagInvalid         byte = 0x00
	TagNull            byte = 0x01
	TagBool            byte = 0x02 // legacy, decode only
	TagInt             byte = 0x03
	TagFloat           byte = 0x04
	TagString          byte = 0x05
	TagArray           byte = 0x06
	TagObject          byte = 0x07
	TagIntArray        byte = 0x08
	TagFloatArray      byte = 0x09
	TagTrue            byte = 0x0A
	TagFalse           byte = 0x0B
	TagLargeObject     byte = 0x0C // count as uint16 (>255 fields)
	TagLargeArray      byte = 0x0D // count as uint32 (>255 elements)
	TagLargeIntArray   byte = 0x0E // count as uint32 (>255 elements)
	TagLargeFloatArray byte = 0x0F // count as uint32 (>255 elements)
	TagEmptyObject     byte = 0x10 // empty object {} in a single byte
	TagEmptyArray      byte = 0x11 // empty array [] in a single byte
	TagStringRef       byte = 0x12 // reference to string in value pool, followed by uint16 index

	// Inline short strings: tags 0x20-0x3F encode string length 0-31 in the tag.
	// Length = tag - TagShortStringBase. Data follows immediately (no separate length byte).
	TagShortStringBase byte = 0x20

	// Inline small integers: tags 0x80-0xFF encode values 0-127 in a single byte.
	// Value = tag & 0x7F.
	TagInlineIntBase byte = 0x80
)

// Encode serializes a Go value into the custom JSONB binary format.
// The binary starts with a key dictionary header, followed by the encoded body.
// All object keys across the entire value are stored once in the dictionary,
// and objects reference keys by uint16 index. Repeated string values are
// stored in a string pool and referenced by index.
//
// Format:
//
//	[keyCount: uint16][key entries...][poolCount: uint16][pool entries...][body]
func Encode(val any) ([]byte, error) {
	// Phase 1: Collect all unique keys from the entire value.
	keySet := make(map[string]struct{})
	collectKeys(val, keySet)

	sortedKeys := make([]string, 0, len(keySet))
	for k := range keySet {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	keyToIdx := make(map[string]uint16, len(sortedKeys))
	for i, k := range sortedKeys {
		keyToIdx[k] = uint16(i)
	}

	// Phase 1b: Collect string values and build pool for repeated ones.
	valCounts := make(map[string]int)
	collectStringValues(val, valCounts)

	// Select pool candidates: strings that appear 2+ times and save space.
	// Pool reference: TagStringRef(1) + uint16 index(2) = 3 bytes.
	// Inline: TagShortString(1) + data(N) = 1+N bytes (for N≤31), or TagString(1) + len(1) + data = 2+N.
	// Pool entry cost: 1 (len) + N (data).
	// Savings = count * inlineSize - (entrySize + count * 3).
	var poolKeys []string
	for s, count := range valCounts {
		if count < 2 {
			continue
		}
		n := len(s)
		var inlineSize int
		if n < 32 {
			inlineSize = 1 + n // TagShortString + data
		} else if n < 128 {
			inlineSize = 2 + n // TagString + len + data
		} else {
			inlineSize = 6 + n // TagString + 0x80 + uint32 len + data
		}
		entrySize := 1 + n // length byte + data (same as encodeDictKey for short keys)
		if n >= 128 {
			entrySize = 5 + n
		}
		savings := count*inlineSize - (entrySize + count*3)
		if savings > 0 {
			poolKeys = append(poolKeys, s)
		}
	}
	sort.Strings(poolKeys)

	// Build pool index map: string -> absolute index (keyCount + pool position)
	poolIdx := make(map[string]uint16, len(poolKeys))
	keyCount := uint16(len(sortedKeys))
	for i, s := range poolKeys {
		poolIdx[s] = keyCount + uint16(i)
	}

	// Phase 2: Write dictionary header.
	// Dictionary keys are written without TagString prefix (all entries are known to be strings).
	var buf []byte
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(sortedKeys)))
	for _, k := range sortedKeys {
		buf = encodeDictKey(buf, k)
	}

	// Write string value pool.
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(poolKeys)))
	for _, s := range poolKeys {
		buf = encodeDictKey(buf, s)
	}

	// Phase 3: Encode body.
	var err error
	buf, err = encodeValue(buf, val, keyToIdx, poolIdx)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// collectStringValues recursively counts occurrences of string values.
func collectStringValues(val any, counts map[string]int) {
	switch v := val.(type) {
	case string:
		counts[v]++
	case map[string]any:
		for _, child := range v {
			collectStringValues(child, counts)
		}
	case []any:
		for _, child := range v {
			collectStringValues(child, counts)
		}
	}
}

// collectKeys recursively finds all unique object keys in the value.
func collectKeys(val any, keys map[string]struct{}) {
	switch v := val.(type) {
	case map[string]any:
		for k, child := range v {
			keys[k] = struct{}{}
			collectKeys(child, keys)
		}
	case []any:
		for _, child := range v {
			collectKeys(child, keys)
		}
	}
}

func encodeValue(buf []byte, val any, dict map[string]uint16, pool map[string]uint16) ([]byte, error) {
	if val == nil {
		return append(buf, TagNull), nil
	}
	switch v := val.(type) {
	case bool:
		if v {
			return append(buf, TagTrue), nil
		}
		return append(buf, TagFalse), nil
	case int64:
		return encodeCompactInt(buf, v), nil
	case int:
		return encodeCompactInt(buf, int64(v)), nil
	case float64:
		buf = append(buf, TagFloat)
		buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(v))
		return buf, nil
	case string:
		if idx, ok := pool[v]; ok {
			buf = append(buf, TagStringRef)
			buf = binary.BigEndian.AppendUint16(buf, idx)
			return buf, nil
		}
		return encodeCompactString(buf, v), nil
	case []any:
		return encodeArray(buf, v, dict, pool)
	case map[string]any:
		return encodeObject(buf, v, dict, pool)
	default:
		return nil, fmt.Errorf("unsupported type: %T", val)
	}
}

// encodeCompactInt encodes an integer with minimum byte width.
// Small non-negative integers (0-127) are encoded as a single inline tag byte.
// Larger values use: [TagInt][width: uint8][value: width bytes]
func encodeCompactInt(buf []byte, v int64) []byte {
	// Inline small integers: single byte 0x80 | value
	if v >= 0 && v <= 127 {
		return append(buf, TagInlineIntBase|byte(v))
	}
	w := intWidthScalar(v)
	buf = append(buf, TagInt, w)
	switch w {
	case 1:
		buf = append(buf, byte(v))
	case 2:
		buf = binary.BigEndian.AppendUint16(buf, uint16(v))
	case 4:
		buf = binary.BigEndian.AppendUint32(buf, uint32(v))
	case 8:
		buf = binary.BigEndian.AppendUint64(buf, uint64(v))
	}
	return buf
}

// intWidthScalar returns the minimum byte width for a single int64 value.
func intWidthScalar(v int64) uint8 {
	if v < 0 || v > math.MaxUint32 {
		return 8
	}
	if v > math.MaxUint16 {
		return 4
	}
	if v > math.MaxUint8 {
		return 2
	}
	return 1
}

// encodeCompactString encodes a string with compact length.
// Inline short strings (≤ 31 bytes): [TagShortStringBase | len][data]
// Medium strings (32-127 bytes): [TagString][1-byte length][data]
// Long strings (≥ 128 bytes): [TagString][0x80][4-byte length][data]
func encodeCompactString(buf []byte, v string) []byte {
	n := len(v)
	if n < 32 {
		buf = append(buf, TagShortStringBase|byte(n))
		buf = append(buf, v...)
		return buf
	}
	buf = append(buf, TagString)
	if n < 128 {
		buf = append(buf, byte(n))
	} else {
		buf = append(buf, 0x80)
		buf = binary.BigEndian.AppendUint32(buf, uint32(n))
	}
	buf = append(buf, v...)
	return buf
}

// encodeDictKey encodes a dictionary key without the TagString prefix.
// Short keys (< 128 bytes): [1-byte length][data]
// Long keys (>= 128 bytes): [0x80][4-byte length][data]
func encodeDictKey(buf []byte, v string) []byte {
	n := len(v)
	if n < 128 {
		buf = append(buf, byte(n))
	} else {
		buf = append(buf, 0x80)
		buf = binary.BigEndian.AppendUint32(buf, uint32(n))
	}
	buf = append(buf, v...)
	return buf
}

func encodeArray(buf []byte, arr []any, dict map[string]uint16, pool map[string]uint16) ([]byte, error) {
	if len(arr) == 0 {
		return append(buf, TagEmptyArray), nil
	}

	// Check if all elements are the same type for typed array optimization.
	if b, err := tryEncodeIntArray(buf, arr); err == nil && b != nil {
		return b, nil
	}
	if b, err := tryEncodeFloatArray(buf, arr); err == nil && b != nil {
		return b, nil
	}

	// Generic array encoding with offset table.
	count := len(arr)
	if count <= 255 {
		buf = append(buf, TagArray, byte(count))
	} else {
		buf = append(buf, TagLargeArray)
		buf = binary.BigEndian.AppendUint32(buf, uint32(count))
	}

	// Encode each element into a temporary buffer to compute offsets.
	elements := make([][]byte, len(arr))
	for i, v := range arr {
		elem, err := encodeValue(nil, v, dict, pool)
		if err != nil {
			return nil, err
		}
		elements[i] = elem
	}

	// Write offset table: each offset is relative to the start of the data section.
	offset := uint32(0)
	for _, elem := range elements {
		buf = binary.BigEndian.AppendUint32(buf, offset)
		offset += uint32(len(elem))
	}

	// Write element data.
	for _, elem := range elements {
		buf = append(buf, elem...)
	}
	return buf, nil
}

// intWidth returns the minimum byte width (1, 2, 4, or 8) needed to represent all values.
func intWidth(vals []int64) uint8 {
	for _, v := range vals {
		if v < 0 || v > math.MaxUint32 {
			return 8
		}
	}
	for _, v := range vals {
		if v > math.MaxUint16 {
			return 4
		}
	}
	for _, v := range vals {
		if v > math.MaxUint8 {
			return 2
		}
	}
	return 1
}

// tryEncodeIntArray attempts to encode arr as a typed int array.
// Returns (nil, nil) if the array is not all int64.
// Format (≤255): [TagIntArray][count: uint8][width: uint8][values: width × count]
// Format (>255): [TagLargeIntArray][count: uint32][width: uint8][values: width × count]
func tryEncodeIntArray(buf []byte, arr []any) ([]byte, error) {
	vals := make([]int64, len(arr))
	for i, v := range arr {
		n, ok := v.(int64)
		if !ok {
			return nil, nil // not all int64
		}
		vals[i] = n
	}

	width := intWidth(vals)

	count := len(vals)
	if count <= 255 {
		buf = append(buf, TagIntArray, byte(count))
	} else {
		buf = append(buf, TagLargeIntArray)
		buf = binary.BigEndian.AppendUint32(buf, uint32(count))
	}
	buf = append(buf, width)

	for _, v := range vals {
		switch width {
		case 1:
			buf = append(buf, byte(v))
		case 2:
			buf = binary.BigEndian.AppendUint16(buf, uint16(v))
		case 4:
			buf = binary.BigEndian.AppendUint32(buf, uint32(v))
		case 8:
			buf = binary.BigEndian.AppendUint64(buf, uint64(v))
		}
	}
	return buf, nil
}

// tryEncodeFloatArray attempts to encode arr as a typed float array.
// Returns (nil, nil) if the array is not all float64.
// Format (≤255): [TagFloatArray][count: uint8][values: 8 × count]
// Format (>255): [TagLargeFloatArray][count: uint32][values: 8 × count]
func tryEncodeFloatArray(buf []byte, arr []any) ([]byte, error) {
	vals := make([]float64, len(arr))
	for i, v := range arr {
		f, ok := v.(float64)
		if !ok {
			return nil, nil // not all float64
		}
		vals[i] = f
	}

	count := len(vals)
	if count <= 255 {
		buf = append(buf, TagFloatArray, byte(count))
	} else {
		buf = append(buf, TagLargeFloatArray)
		buf = binary.BigEndian.AppendUint32(buf, uint32(count))
	}

	for _, v := range vals {
		buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(v))
	}
	return buf, nil
}

// offsetWidth returns the minimum byte width (1, 2, or 4) for an unsigned offset value.
func offsetWidth(maxVal uint32) uint8 {
	if maxVal <= math.MaxUint8 {
		return 1
	}
	if maxVal <= math.MaxUint16 {
		return 2
	}
	return 4
}

// appendUintN appends a uint value using the specified byte width.
func appendUintN(buf []byte, v uint32, width uint8) []byte {
	switch width {
	case 1:
		return append(buf, byte(v))
	case 2:
		return binary.BigEndian.AppendUint16(buf, uint16(v))
	case 4:
		return binary.BigEndian.AppendUint32(buf, uint32(v))
	}
	return buf
}

// readUintN reads a uint value of the specified byte width.
func readUintN(b []byte, pos int, width uint8) (uint32, int) {
	switch width {
	case 1:
		return uint32(b[pos]), pos + 1
	case 2:
		return uint32(binary.BigEndian.Uint16(b[pos : pos+2])), pos + 2
	case 4:
		return binary.BigEndian.Uint32(b[pos : pos+4]), pos + 4
	}
	return 0, pos
}

// encodeObject encodes a map as an object with dictionary key references.
//
// Format (≤255 fields):
//
//	[TagObject][count: uint8][keyIdxWidth: uint8][valOffWidth: uint8]
//	[entry table: (keyIdx: keyIdxWidth, valOff: valOffWidth) × count]
//	[value data]
//
// Format (>255 fields):
//
//	[TagLargeObject][count: uint16][keyIdxWidth: uint8][valOffWidth: uint8]
//	[entry table: (keyIdx: keyIdxWidth, valOff: valOffWidth) × count]
//	[value data]
//
// Keys are written in sorted order (by dictionary index, which is already sorted).
func encodeObject(buf []byte, obj map[string]any, dict map[string]uint16, pool map[string]uint16) ([]byte, error) {
	count := len(obj)
	if count == 0 {
		return append(buf, TagEmptyObject), nil
	}
	if count <= 255 {
		buf = append(buf, TagObject, byte(count))
	} else {
		buf = append(buf, TagLargeObject)
		buf = binary.BigEndian.AppendUint16(buf, uint16(count))
	}

	// Sort keys.
	keys := make([]string, 0, count)
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Encode values.
	encodedVals := make([][]byte, len(keys))
	for i, k := range keys {
		ev, err := encodeValue(nil, obj[k], dict, pool)
		if err != nil {
			return nil, err
		}
		encodedVals[i] = ev
	}

	// Compute widths for key index and value offset.
	maxKeyIdx := uint32(0)
	for _, k := range keys {
		if uint32(dict[k]) > maxKeyIdx {
			maxKeyIdx = uint32(dict[k])
		}
	}
	keyIdxW := offsetWidth(maxKeyIdx)

	totalValSize := uint32(0)
	for _, ev := range encodedVals {
		totalValSize += uint32(len(ev))
	}
	valOffW := offsetWidth(totalValSize)

	buf = append(buf, keyIdxW, valOffW)

	// Entry table with adaptive widths.
	valOffset := uint32(0)
	for i, k := range keys {
		buf = appendUintN(buf, uint32(dict[k]), keyIdxW)
		buf = appendUintN(buf, valOffset, valOffW)
		valOffset += uint32(len(encodedVals[i]))
	}

	// Write value data.
	for _, ev := range encodedVals {
		buf = append(buf, ev...)
	}

	return buf, nil
}

// --- Decode ---

// readDictHeader reads the key dictionary from the header.
// Returns the dictionary keys, the body start position, and any error.
// readDictOffsets reads the dictionary header and returns byte offsets
// (position of each key's string body in b) and lengths, without decoding to Go strings.
// Each entry is {offset, length} pair pointing to the raw key bytes in b.
type dictEntry struct {
	offset int
	length int
}

func readDictOffsets(b []byte) ([]dictEntry, int, error) {
	if len(b) < 2 {
		return nil, 0, fmt.Errorf("unexpected end of data for dictionary header")
	}
	keyCount := int(binary.BigEndian.Uint16(b[0:2]))
	pos := 2
	pos, entries, err := readDictOffsetEntries(b, pos, keyCount)
	if err != nil {
		return nil, 0, err
	}
	// Read pool section.
	if pos+2 > len(b) {
		return nil, 0, fmt.Errorf("unexpected end of data for pool count")
	}
	poolCount := int(binary.BigEndian.Uint16(b[pos : pos+2]))
	pos += 2
	if poolCount > 0 {
		var poolEntries []dictEntry
		pos, poolEntries, err = readDictOffsetEntries(b, pos, poolCount)
		if err != nil {
			return nil, 0, err
		}
		entries = append(entries, poolEntries...)
	}
	return entries, pos, nil
}

// readDictOffsetEntries reads count dictionary-style entries as byte offset/length pairs.
func readDictOffsetEntries(b []byte, pos int, count int) (int, []dictEntry, error) {
	entries := make([]dictEntry, count)
	for i := 0; i < count; i++ {
		if pos >= len(b) {
			return 0, nil, fmt.Errorf("unexpected end of data for dictionary string length")
		}
		var length int
		if b[pos]&0x80 == 0 {
			length = int(b[pos])
			pos++
		} else {
			pos++
			if pos+4 > len(b) {
				return 0, nil, fmt.Errorf("unexpected end of data for dictionary long string length")
			}
			length = int(binary.BigEndian.Uint32(b[pos : pos+4]))
			pos += 4
		}
		if pos+length > len(b) {
			return 0, nil, fmt.Errorf("unexpected end of data for dictionary string body")
		}
		entries[i] = dictEntry{offset: pos, length: length}
		pos += length
	}
	return pos, entries, nil
}

// searchDictBytes performs binary search on dictionary entries by comparing
// raw bytes directly, avoiding string allocation.
func searchDictBytes(b []byte, entries []dictEntry, key string) int {
	keyBytes := []byte(key)
	lo, hi := 0, len(entries)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		e := entries[mid]
		cmp := bytesCompare(b[e.offset:e.offset+e.length], keyBytes)
		if cmp == 0 {
			return mid
		} else if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return -1
}

// bytesCompare compares two byte slices lexicographically.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func bytesCompare(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

func readDictHeader(b []byte) ([]string, int, error) {
	if len(b) < 2 {
		return nil, 0, fmt.Errorf("unexpected end of data for dictionary header")
	}
	keyCount := int(binary.BigEndian.Uint16(b[0:2]))
	pos := 2
	// Read key entries and pool entries into a single slice.
	// Keys are at indices [0, keyCount), pool at [keyCount, keyCount+poolCount).
	pos, entries, err := readDictEntries(b, pos, keyCount)
	if err != nil {
		return nil, 0, err
	}
	// Read pool section.
	if pos+2 > len(b) {
		return nil, 0, fmt.Errorf("unexpected end of data for pool count")
	}
	poolCount := int(binary.BigEndian.Uint16(b[pos : pos+2]))
	pos += 2
	if poolCount > 0 {
		var poolEntries []string
		pos, poolEntries, err = readDictEntries(b, pos, poolCount)
		if err != nil {
			return nil, 0, err
		}
		entries = append(entries, poolEntries...)
	}
	return entries, pos, nil
}

// readDictEntries reads count dictionary-style string entries starting at pos.
func readDictEntries(b []byte, pos int, count int) (int, []string, error) {
	entries := make([]string, count)
	for i := 0; i < count; i++ {
		if pos >= len(b) {
			return 0, nil, fmt.Errorf("unexpected end of data for dictionary string length")
		}
		var length int
		if b[pos]&0x80 == 0 {
			length = int(b[pos])
			pos++
		} else {
			pos++
			if pos+4 > len(b) {
				return 0, nil, fmt.Errorf("unexpected end of data for dictionary long string length")
			}
			length = int(binary.BigEndian.Uint32(b[pos : pos+4]))
			pos += 4
		}
		if pos+length > len(b) {
			return 0, nil, fmt.Errorf("unexpected end of data for dictionary string body")
		}
		entries[i] = string(b[pos : pos+length])
		pos += length
	}
	return pos, entries, nil
}

// Decode deserializes the custom JSONB binary format into a Go value.
func Decode(b []byte) (any, error) {
	dict, bodyPos, err := readDictHeader(b)
	if err != nil {
		return nil, err
	}
	val, _, err := decodeValue(b, bodyPos, dict)
	return val, err
}

func decodeValue(b []byte, pos int, dict []string) (any, int, error) {
	if pos >= len(b) {
		return nil, pos, fmt.Errorf("unexpected end of data")
	}
	tag := b[pos]
	pos++
	// Inline small integer: 0x80-0xFF → value 0-127
	if tag >= TagInlineIntBase {
		return int64(tag & 0x7F), pos, nil
	}
	// Inline short string: 0x20-0x3F → length 0-31
	if tag >= TagShortStringBase && tag < TagShortStringBase+32 {
		length := int(tag - TagShortStringBase)
		if pos+length > len(b) {
			return nil, pos, fmt.Errorf("unexpected end of data for inline short string")
		}
		return string(b[pos : pos+length]), pos + length, nil
	}
	switch tag {
	case TagNull:
		return nil, pos, nil
	case TagTrue:
		return true, pos, nil
	case TagFalse:
		return false, pos, nil
	case TagStringRef:
		if pos+2 > len(b) {
			return nil, pos, fmt.Errorf("unexpected end of data for string ref index")
		}
		idx := int(binary.BigEndian.Uint16(b[pos : pos+2]))
		if idx >= len(dict) {
			return nil, pos, fmt.Errorf("string ref index %d out of range (dict size %d)", idx, len(dict))
		}
		return dict[idx], pos + 2, nil
	case TagBool:
		// Legacy format support
		if pos >= len(b) {
			return nil, pos, fmt.Errorf("unexpected end of data for bool")
		}
		v := b[pos] != 0
		return v, pos + 1, nil
	case TagInt:
		if pos >= len(b) {
			return nil, pos, fmt.Errorf("unexpected end of data for int width")
		}
		width := int(b[pos])
		pos++
		if pos+width > len(b) {
			return nil, pos, fmt.Errorf("unexpected end of data for int value")
		}
		var v int64
		switch width {
		case 1:
			v = int64(b[pos])
		case 2:
			v = int64(binary.BigEndian.Uint16(b[pos : pos+2]))
		case 4:
			v = int64(binary.BigEndian.Uint32(b[pos : pos+4]))
		case 8:
			v = int64(binary.BigEndian.Uint64(b[pos : pos+8]))
		default:
			return nil, pos, fmt.Errorf("invalid int width: %d", width)
		}
		return v, pos + width, nil
	case TagFloat:
		if pos+8 > len(b) {
			return nil, pos, fmt.Errorf("unexpected end of data for float")
		}
		v := math.Float64frombits(binary.BigEndian.Uint64(b[pos : pos+8]))
		return v, pos + 8, nil
	case TagString:
		if pos >= len(b) {
			return nil, pos, fmt.Errorf("unexpected end of data for string length")
		}
		var length int
		if b[pos]&0x80 == 0 {
			// Short string: 1-byte length (0-127)
			length = int(b[pos])
			pos++
		} else {
			// Long string: marker 0x80 + 4-byte length
			pos++
			if pos+4 > len(b) {
				return nil, pos, fmt.Errorf("unexpected end of data for long string length")
			}
			length = int(binary.BigEndian.Uint32(b[pos : pos+4]))
			pos += 4
		}
		if pos+length > len(b) {
			return nil, pos, fmt.Errorf("unexpected end of data for string body")
		}
		v := string(b[pos : pos+length])
		return v, pos + length, nil
	case TagEmptyArray:
		return []any{}, pos, nil
	case TagArray:
		return decodeArray(b, pos, 1, dict)
	case TagLargeArray:
		return decodeArray(b, pos, 4, dict)
	case TagEmptyObject:
		return map[string]any{}, pos, nil
	case TagObject:
		return decodeObject(b, pos, 1, dict)
	case TagLargeObject:
		return decodeObject(b, pos, 2, dict)
	case TagIntArray:
		return decodeIntArray(b, pos, 1)
	case TagLargeIntArray:
		return decodeIntArray(b, pos, 4)
	case TagFloatArray:
		return decodeFloatArray(b, pos, 1)
	case TagLargeFloatArray:
		return decodeFloatArray(b, pos, 4)
	default:
		return nil, pos, fmt.Errorf("unknown tag: 0x%02x", tag)
	}
}

// readCount reads a count field of the given byte width (1 or 4 for arrays, 1 or 2 for objects).
func readCount(b []byte, pos int, width int) (int, int, error) {
	if pos+width > len(b) {
		return 0, pos, fmt.Errorf("unexpected end of data for count")
	}
	switch width {
	case 1:
		return int(b[pos]), pos + 1, nil
	case 2:
		return int(binary.BigEndian.Uint16(b[pos : pos+2])), pos + 2, nil
	case 4:
		return int(binary.BigEndian.Uint32(b[pos : pos+4])), pos + 4, nil
	}
	return 0, pos, fmt.Errorf("invalid count width: %d", width)
}

func decodeArray(b []byte, pos int, countWidth int, dict []string) (any, int, error) {
	count, pos, err := readCount(b, pos, countWidth)
	if err != nil {
		return nil, pos, err
	}

	if count == 0 {
		return []any{}, pos, nil
	}

	// Skip offset table — decode elements sequentially instead.
	offsetTableSize := count * 4
	if pos+offsetTableSize > len(b) {
		return nil, pos, fmt.Errorf("unexpected end of data for array offsets")
	}
	dataStart := pos + offsetTableSize

	result := make([]any, count)
	cur := dataStart
	for i := 0; i < count; i++ {
		val, newPos, err := decodeValue(b, cur, dict)
		if err != nil {
			return nil, newPos, err
		}
		result[i] = val
		cur = newPos
	}
	return result, cur, nil
}

func decodeIntArray(b []byte, pos int, countWidth int) (any, int, error) {
	count, pos, err := readCount(b, pos, countWidth)
	if err != nil {
		return nil, pos, err
	}

	if count == 0 {
		return []any{}, pos, nil
	}

	if pos >= len(b) {
		return nil, pos, fmt.Errorf("unexpected end of data for int array width")
	}
	width := int(b[pos])
	pos++

	dataSize := count * width
	if pos+dataSize > len(b) {
		return nil, pos, fmt.Errorf("unexpected end of data for int array values")
	}

	result := make([]any, count)
	for i := 0; i < count; i++ {
		off := pos + i*width
		switch width {
		case 1:
			result[i] = int64(b[off])
		case 2:
			result[i] = int64(binary.BigEndian.Uint16(b[off : off+2]))
		case 4:
			result[i] = int64(binary.BigEndian.Uint32(b[off : off+4]))
		case 8:
			result[i] = int64(binary.BigEndian.Uint64(b[off : off+8]))
		}
	}
	return result, pos + dataSize, nil
}

func decodeFloatArray(b []byte, pos int, countWidth int) (any, int, error) {
	count, pos, err := readCount(b, pos, countWidth)
	if err != nil {
		return nil, pos, err
	}

	if count == 0 {
		return []any{}, pos, nil
	}

	dataSize := count * 8
	if pos+dataSize > len(b) {
		return nil, pos, fmt.Errorf("unexpected end of data for float array values")
	}

	result := make([]any, count)
	for i := 0; i < count; i++ {
		off := pos + i*8
		result[i] = math.Float64frombits(binary.BigEndian.Uint64(b[off : off+8]))
	}
	return result, pos + dataSize, nil
}

// decodeObject decodes an object with dictionary key references and adaptive-width entry table.
// countWidth is 1 (TagObject, uint8 count) or 2 (TagLargeObject, uint16 count).
func decodeObject(b []byte, pos int, countWidth int, dict []string) (any, int, error) {
	count, pos, err := readCount(b, pos, countWidth)
	if err != nil {
		return nil, pos, err
	}

	if count == 0 {
		return map[string]any{}, pos, nil
	}

	if pos+2 > len(b) {
		return nil, pos, fmt.Errorf("unexpected end of data for object widths")
	}
	keyIdxW := uint8(b[pos])
	valOffW := uint8(b[pos+1])
	pos += 2

	entryWidth := int(keyIdxW) + int(valOffW)
	entryTableSize := count * entryWidth
	if pos+entryTableSize > len(b) {
		return nil, pos, fmt.Errorf("unexpected end of data for object entry table")
	}

	valDataStart := pos + entryTableSize
	result := make(map[string]any, count)
	epos := pos
	cur := valDataStart
	for i := 0; i < count; i++ {
		keyIdx, nextEpos := readUintN(b, epos, keyIdxW)
		epos = nextEpos + int(valOffW) // skip valOff — decode sequentially
		if int(keyIdx) >= len(dict) {
			return nil, pos, fmt.Errorf("key index %d out of range (dict size %d)", keyIdx, len(dict))
		}
		key := dict[keyIdx]
		val, newPos, err := decodeValue(b, cur, dict)
		if err != nil {
			return nil, newPos, err
		}
		result[key] = val
		cur = newPos
	}
	return result, cur, nil
}

// --- Partial access ---

// LookupKey performs a binary search on an encoded object to find the value for the given key
// without fully deserializing the entire object.
// The binary data must start with the dictionary header.
func LookupKey(b []byte, key string) (any, bool, error) {
	dict, bodyPos, err := readDictHeader(b)
	if err != nil {
		return nil, false, err
	}
	
	// Binary search dictionary for the key.
	targetIdx := -1
	lo, hi := 0, len(dict)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		cmp := strings.Compare(dict[mid], key)
		if cmp == 0 {
			targetIdx = mid
			break
		} else if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if targetIdx < 0 {
		return nil, false, nil // key not in dictionary at all
	}

	// Check the body is an object.
	if bodyPos >= len(b) {
		return nil, false, fmt.Errorf("not an object")
	}
	tag := b[bodyPos]
	pos := bodyPos + 1

	var countWidth int
	switch tag {
	case TagObject:
		countWidth = 1
	case TagLargeObject:
		countWidth = 2
	default:
		return nil, false, fmt.Errorf("not an object")
	}

	count, pos, err := readCount(b, pos, countWidth)
	if err != nil {
		return nil, false, err
	}
	if count == 0 {
		return nil, false, nil
	}

	if pos+2 > len(b) {
		return nil, false, fmt.Errorf("unexpected end of data")
	}
	keyIdxW := b[pos]
	valOffW := b[pos+1]
	pos += 2

	// Binary search entry table for targetIdx.
	// Entry table: [keyIndex: keyIdxW, valOffset: valOffW] × count.
	entrySize := int(keyIdxW) + int(valOffW)
	entryTableStart := pos
	lo, hi = 0, count-1
	for lo <= hi {
		mid := (lo + hi) / 2
		entryPos := entryTableStart + mid*entrySize
		midKeyIdx, _ := readUintN(b, entryPos, keyIdxW)
		if int(midKeyIdx) == targetIdx {
			valOff, _ := readUintN(b, entryPos+int(keyIdxW), valOffW)
			valDataStart := entryTableStart + count*entrySize
			val, _, err := decodeValue(b, valDataStart+int(valOff), dict)
			if err != nil {
				return nil, false, err
			}
			return val, true, nil
		} else if int(midKeyIdx) < targetIdx {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return nil, false, nil // key in dictionary but not in this object
}

// LookupIndex retrieves the element at the given index from an encoded array
// without fully deserializing the entire array.
// The binary data must start with the dictionary header.
func LookupIndex(b []byte, idx int) (any, bool, error) {
	dict, bodyPos, err := readDictHeader(b)
	if err != nil {
		return nil, false, err
	}

	if bodyPos >= len(b) {
		return nil, false, fmt.Errorf("not an array")
	}

	tag := b[bodyPos]
	switch tag {
	case TagIntArray, TagLargeIntArray:
		return lookupIntArrayIndex(b, bodyPos, idx)
	case TagFloatArray, TagLargeFloatArray:
		return lookupFloatArrayIndex(b, bodyPos, idx)
	case TagArray, TagLargeArray:
		return lookupGenericArrayIndex(b, bodyPos, idx, dict)
	default:
		return nil, false, fmt.Errorf("not an array")
	}
}

func lookupIntArrayIndex(b []byte, start int, idx int) (any, bool, error) {
	tag := b[start]
	pos := start + 1 // skip tag
	countWidth := 1
	if tag == TagLargeIntArray {
		countWidth = 4
	}
	count, pos, err := readCount(b, pos, countWidth)
	if err != nil {
		return nil, false, err
	}
	if idx < 0 || idx >= count {
		return nil, false, nil
	}
	width := int(b[pos])
	pos++
	off := pos + idx*width
	if off+width > len(b) {
		return nil, false, fmt.Errorf("unexpected end of data")
	}
	switch width {
	case 1:
		return int64(b[off]), true, nil
	case 2:
		return int64(binary.BigEndian.Uint16(b[off : off+2])), true, nil
	case 4:
		return int64(binary.BigEndian.Uint32(b[off : off+4])), true, nil
	case 8:
		return int64(binary.BigEndian.Uint64(b[off : off+8])), true, nil
	default:
		return nil, false, fmt.Errorf("invalid int array width: %d", width)
	}
}

func lookupFloatArrayIndex(b []byte, start int, idx int) (any, bool, error) {
	tag := b[start]
	pos := start + 1 // skip tag
	countWidth := 1
	if tag == TagLargeFloatArray {
		countWidth = 4
	}
	count, pos, err := readCount(b, pos, countWidth)
	if err != nil {
		return nil, false, err
	}
	if idx < 0 || idx >= count {
		return nil, false, nil
	}
	off := pos + idx*8
	if off+8 > len(b) {
		return nil, false, fmt.Errorf("unexpected end of data")
	}
	return math.Float64frombits(binary.BigEndian.Uint64(b[off : off+8])), true, nil
}

func lookupGenericArrayIndex(b []byte, start int, idx int, dict []string) (any, bool, error) {
	tag := b[start]
	pos := start + 1 // skip tag
	countWidth := 1
	if tag == TagLargeArray {
		countWidth = 4
	}
	count, pos, err := readCount(b, pos, countWidth)
	if err != nil {
		return nil, false, err
	}
	if idx < 0 || idx >= count {
		return nil, false, nil
	}
	offsetPos := pos + idx*4
	if offsetPos+4 > len(b) {
		return nil, false, fmt.Errorf("unexpected end of data for offset")
	}
	offset := int(binary.BigEndian.Uint32(b[offsetPos : offsetPos+4]))
	dataStart := pos + count*4
	val, _, err := decodeValue(b, dataStart+offset, dict)
	if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

// KeyExists checks whether a key exists in an encoded object using binary search.
func KeyExists(b []byte, key string) bool {
	_, found, err := LookupKey(b, key)
	return err == nil && found
}

// LookupKeys traverses a path of string keys and/or int indices
// without full deserialization at each intermediate level.
// Each path element must be a string (object key) or int (array index).
// An empty path returns the root value.
func LookupKeys(b []byte, path ...any) (any, bool, error) {
	dict, bodyPos, err := readDictHeader(b)
	if err != nil {
		return nil, false, err
	}

	pos := bodyPos
	for i, step := range path {
		if pos >= len(b) {
			return nil, false, fmt.Errorf("unexpected end of data at path step %d", i)
		}
		isLast := i == len(path)-1
		switch s := step.(type) {
		case string:
			newPos, found, err := lookupKeyPos(b, pos, s, dict)
			if err != nil {
				return nil, false, err
			}
			if !found {
				return nil, false, nil
			}
			pos = newPos
		case int:
			if isLast {
				tag := b[pos]
				if tag == TagIntArray || tag == TagLargeIntArray {
					return lookupIntArrayIndex(b, pos, s)
				}
				if tag == TagFloatArray || tag == TagLargeFloatArray {
					return lookupFloatArrayIndex(b, pos, s)
				}
			}
			newPos, found, err := lookupIndexPos(b, pos, s, dict)
			if err != nil {
				return nil, false, err
			}
			if !found {
				return nil, false, nil
			}
			pos = newPos
		default:
			return nil, false, fmt.Errorf("path element %d: expected string or int, got %T", i, step)
		}
	}

	val, _, err := decodeValue(b, pos, dict)
	if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

// KeysExists checks whether a value exists at the given path.
func KeysExists(b []byte, path ...any) bool {
	_, found, err := LookupKeys(b, path...)
	return err == nil && found
}

// QueryPath traverses JSONB binary data using a compiled json_path.Path
// without full deserialization at intermediate levels.
func QueryPath(b []byte, p *json_path.Path) (any, bool, error) {
	args := make([]any, len(p.Steps))
	for i, step := range p.Steps {
		switch step.Kind {
		case json_path.StepMember:
			args[i] = step.Key
		case json_path.StepIndex:
			args[i] = step.Index
		}
	}
	return LookupKeys(b, args...)
}

// ExistsPath checks whether a value exists at the given JSON path.
// Optimized: resolves string keys to dictionary indices upfront and
// skips value decoding at the final step.
func ExistsPath(b []byte, p *json_path.Path) bool {
	if len(p.Steps) == 0 {
		return len(b) > 0
	}

	dictOffsets, bodyPos, err := readDictOffsets(b)
	if err != nil {
		return false
	}

	// Pre-resolve all StepMember keys to dictionary indices using byte comparison.
	type resolvedStep struct {
		isIndex  bool
		dictIdx  int
		arrayIdx int
	}
	steps := make([]resolvedStep, len(p.Steps))
	for i, step := range p.Steps {
		switch step.Kind {
		case json_path.StepMember:
			idx := searchDictBytes(b, dictOffsets, step.Key)
			if idx < 0 {
				return false
			}
			steps[i] = resolvedStep{isIndex: false, dictIdx: idx}
		case json_path.StepIndex:
			steps[i] = resolvedStep{isIndex: true, arrayIdx: step.Index}
		}
	}

	pos := bodyPos
	for i, step := range steps {
		if pos >= len(b) {
			return false
		}
		isLast := i == len(steps)-1
		if step.isIndex {
			if isLast {
				tag := b[pos]
				if tag == TagIntArray || tag == TagLargeIntArray {
					_, found, _ := lookupIntArrayIndex(b, pos, step.arrayIdx)
					return found
				}
				if tag == TagFloatArray || tag == TagLargeFloatArray {
					_, found, _ := lookupFloatArrayIndex(b, pos, step.arrayIdx)
					return found
				}
			}
			newPos, found, err := lookupIndexPos(b, pos, step.arrayIdx, nil)
			if err != nil || !found {
				return false
			}
			pos = newPos
		} else {
			newPos, found, err := lookupKeyPosByIdx(b, pos, step.dictIdx)
			if err != nil || !found {
				return false
			}
			pos = newPos
		}
	}
	return true
}

// searchDict performs binary search on a sorted dictionary, returning the index or -1.
func searchDict(dict []string, key string) int {
	lo, hi := 0, len(dict)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		cmp := strings.Compare(dict[mid], key)
		if cmp == 0 {
			return mid
		} else if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return -1
}

// lookupKeyPosByIdx finds a value's byte position in an object by pre-resolved dictionary index.
// Skips the dictionary search step for better performance.
func lookupKeyPosByIdx(b []byte, pos int, targetIdx int) (int, bool, error) {
	tag := b[pos]
	var countWidth int
	switch tag {
	case TagObject:
		countWidth = 1
	case TagLargeObject:
		countWidth = 2
	default:
		return 0, false, fmt.Errorf("not an object")
	}

	p := pos + 1
	count, p, err := readCount(b, p, countWidth)
	if err != nil {
		return 0, false, err
	}
	if count == 0 {
		return 0, false, nil
	}

	if p+2 > len(b) {
		return 0, false, fmt.Errorf("unexpected end of data")
	}
	keyIdxW := b[p]
	valOffW := b[p+1]
	p += 2

	entrySize := int(keyIdxW) + int(valOffW)
	entryTableStart := p
	lo, hi := 0, count-1
	for lo <= hi {
		mid := (lo + hi) / 2
		entryPos := entryTableStart + mid*entrySize
		midKeyIdx, _ := readUintN(b, entryPos, keyIdxW)
		if int(midKeyIdx) == targetIdx {
			valOff, _ := readUintN(b, entryPos+int(keyIdxW), valOffW)
			valDataStart := entryTableStart + count*entrySize
			return valDataStart + int(valOff), true, nil
		} else if int(midKeyIdx) < targetIdx {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return 0, false, nil
}

// lookupKeyPos returns the byte position of the value for the given key
// in an object starting at pos, without decoding the value.
func lookupKeyPos(b []byte, pos int, key string, dict []string) (int, bool, error) {
	targetIdx := searchDict(dict, key)
	if targetIdx < 0 {
		return 0, false, nil
	}
	return lookupKeyPosByIdx(b, pos, targetIdx)
}

// lookupIndexPos returns the byte position of the element at the given index
// in an array starting at pos, without decoding the value.
func lookupIndexPos(b []byte, pos int, idx int, dict []string) (int, bool, error) {
	tag := b[pos]
	switch tag {
	case TagIntArray, TagFloatArray, TagLargeIntArray, TagLargeFloatArray:
		// Typed array elements are scalars; cannot traverse further.
		return 0, false, fmt.Errorf("cannot traverse into typed array element")
	case TagArray, TagLargeArray:
		p := pos + 1
		countWidth := 1
		if tag == TagLargeArray {
			countWidth = 4
		}
		count, p, err := readCount(b, p, countWidth)
		if err != nil {
			return 0, false, err
		}
		if idx < 0 || idx >= count {
			return 0, false, nil
		}
		offsetPos := p + idx*4
		if offsetPos+4 > len(b) {
			return 0, false, fmt.Errorf("unexpected end of data for offset")
		}
		offset := int(binary.BigEndian.Uint32(b[offsetPos : offsetPos+4]))
		dataStart := p + count*4
		return dataStart + offset, true, nil
	default:
		return 0, false, fmt.Errorf("not an array")
	}
}

// --- JSON conversion ---

// FromJSON converts a JSON string to the custom JSONB binary format.
func FromJSON(jsonStr string) ([]byte, error) {
	var val any
	d := json.NewDecoder(strings.NewReader(jsonStr))
	d.UseNumber()
	if err := d.Decode(&val); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	goVal := convertJSONNumbers(val)
	return Encode(goVal)
}

// convertJSONNumbers recursively converts json.Number to int64 or float64.
func convertJSONNumbers(val any) any {
	switch v := val.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			s := v.String()
			if !strings.Contains(s, ".") && !strings.Contains(s, "e") && !strings.Contains(s, "E") {
				return i
			}
		}
		f, _ := v.Float64()
		return f
	case map[string]any:
		for k, v2 := range v {
			v[k] = convertJSONNumbers(v2)
		}
		return v
	case []any:
		for i, v2 := range v {
			v[i] = convertJSONNumbers(v2)
		}
		return v
	default:
		return val
	}
}

// ToJSON converts the custom JSONB binary format to a JSON string.
// Writes JSON directly from the binary without intermediate Go value allocation.
func ToJSON(b []byte) (string, error) {
	dictOffsets, bodyPos, err := readDictOffsets(b)
	if err != nil {
		return "", err
	}
	// Estimate capacity: roughly same size as JSONB binary.
	buf := make([]byte, 0, len(b))
	buf, err = writeJSONValue(buf, b, bodyPos, dictOffsets)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

// writeJSONValue writes a single JSONB value at pos as JSON into buf.
func writeJSONValue(buf []byte, b []byte, pos int, dict []dictEntry) ([]byte, error) {
	if pos >= len(b) {
		return nil, fmt.Errorf("unexpected end of data")
	}
	tag := b[pos]
	pos++
	// Inline small integer: 0x80-0xFF → value 0-127
	if tag >= TagInlineIntBase {
		return strconv.AppendInt(buf, int64(tag&0x7F), 10), nil
	}
	// Inline short string: 0x20-0x3F → length 0-31
	if tag >= TagShortStringBase && tag < TagShortStringBase+32 {
		length := int(tag - TagShortStringBase)
		if pos+length > len(b) {
			return nil, fmt.Errorf("unexpected end of data for inline short string")
		}
		return writeJSONStringBytes(buf, b[pos:pos+length]), nil
	}
	switch tag {
	case TagNull:
		return append(buf, "null"...), nil
	case TagTrue:
		return append(buf, "true"...), nil
	case TagFalse:
		return append(buf, "false"...), nil
	case TagStringRef:
		if pos+2 > len(b) {
			return nil, fmt.Errorf("unexpected end of data for string ref index")
		}
		idx := int(binary.BigEndian.Uint16(b[pos : pos+2]))
		if idx >= len(dict) {
			return nil, fmt.Errorf("string ref index %d out of range", idx)
		}
		de := dict[idx]
		return writeJSONStringBytes(buf, b[de.offset:de.offset+de.length]), nil
	case TagBool:
		// Legacy format support
		if pos >= len(b) {
			return nil, fmt.Errorf("unexpected end of data for bool")
		}
		if b[pos] != 0 {
			return append(buf, "true"...), nil
		}
		return append(buf, "false"...), nil
	case TagInt:
		if pos >= len(b) {
			return nil, fmt.Errorf("unexpected end of data for int width")
		}
		width := int(b[pos])
		pos++
		if pos+width > len(b) {
			return nil, fmt.Errorf("unexpected end of data for int value")
		}
		var v int64
		switch width {
		case 1:
			v = int64(b[pos])
		case 2:
			v = int64(binary.BigEndian.Uint16(b[pos : pos+2]))
		case 4:
			v = int64(binary.BigEndian.Uint32(b[pos : pos+4]))
		case 8:
			v = int64(binary.BigEndian.Uint64(b[pos : pos+8]))
		}
		return strconv.AppendInt(buf, v, 10), nil
	case TagFloat:
		if pos+8 > len(b) {
			return nil, fmt.Errorf("unexpected end of data for float")
		}
		v := math.Float64frombits(binary.BigEndian.Uint64(b[pos : pos+8]))
		return strconv.AppendFloat(buf, v, 'f', -1, 64), nil
	case TagString:
		return writeJSONString(buf, b, pos)
	case TagEmptyArray:
		return append(buf, '[', ']'), nil
	case TagArray:
		return writeJSONArray(buf, b, pos, 1, dict)
	case TagLargeArray:
		return writeJSONArray(buf, b, pos, 4, dict)
	case TagEmptyObject:
		return append(buf, '{', '}'), nil
	case TagObject:
		return writeJSONObject(buf, b, pos, 1, dict)
	case TagLargeObject:
		return writeJSONObject(buf, b, pos, 2, dict)
	case TagIntArray:
		return writeJSONIntArray(buf, b, pos, 1)
	case TagLargeIntArray:
		return writeJSONIntArray(buf, b, pos, 4)
	case TagFloatArray:
		return writeJSONFloatArray(buf, b, pos, 1)
	case TagLargeFloatArray:
		return writeJSONFloatArray(buf, b, pos, 4)
	default:
		return nil, fmt.Errorf("unknown tag: 0x%02x", tag)
	}
}

// writeJSONString writes a JSONB string value at pos as a JSON quoted string.
func writeJSONString(buf []byte, b []byte, pos int) ([]byte, error) {
	if pos >= len(b) {
		return nil, fmt.Errorf("unexpected end of data for string length")
	}
	var length int
	if b[pos]&0x80 == 0 {
		length = int(b[pos])
		pos++
	} else {
		pos++
		if pos+4 > len(b) {
			return nil, fmt.Errorf("unexpected end of data for long string length")
		}
		length = int(binary.BigEndian.Uint32(b[pos : pos+4]))
		pos += 4
	}
	if pos+length > len(b) {
		return nil, fmt.Errorf("unexpected end of data for string body")
	}
	return writeJSONStringBytes(buf, b[pos:pos+length]), nil
}

// writeJSONStringBytes writes a JSON-escaped quoted string from raw bytes.
func writeJSONStringBytes(buf []byte, data []byte) []byte {
	buf = append(buf, '"')
	for _, c := range data {
		switch c {
		case '"':
			buf = append(buf, '\\', '"')
		case '\\':
			buf = append(buf, '\\', '\\')
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case '\t':
			buf = append(buf, '\\', 't')
		default:
			if c < 0x20 {
				buf = append(buf, '\\', 'u', '0', '0', hexDigit(c>>4), hexDigit(c&0x0f))
			} else {
				buf = append(buf, c)
			}
		}
	}
	buf = append(buf, '"')
	return buf
}

func hexDigit(v byte) byte {
	if v < 10 {
		return '0' + v
	}
	return 'a' + v - 10
}

// writeJSONArray writes a generic JSONB array at pos as JSON.
func writeJSONArray(buf []byte, b []byte, pos int, countWidth int, dict []dictEntry) ([]byte, error) {
	count, pos, err := readCount(b, pos, countWidth)
	if err != nil {
		return nil, err
	}

	buf = append(buf, '[')
	if count == 0 {
		return append(buf, ']'), nil
	}

	// Skip offset table, decode sequentially.
	dataStart := pos + count*4
	cur := dataStart
	for i := 0; i < count; i++ {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf, err = writeJSONValue(buf, b, cur, dict)
		if err != nil {
			return nil, err
		}
		// Advance cur by skipping over the value we just wrote.
		_, cur, err = skipValue(b, cur)
		if err != nil {
			return nil, err
		}
	}
	return append(buf, ']'), nil
}

// writeJSONIntArray writes a typed int array as JSON.
func writeJSONIntArray(buf []byte, b []byte, pos int, countWidth int) ([]byte, error) {
	count, pos, err := readCount(b, pos, countWidth)
	if err != nil {
		return nil, err
	}

	buf = append(buf, '[')
	if count == 0 {
		return append(buf, ']'), nil
	}

	if pos >= len(b) {
		return nil, fmt.Errorf("unexpected end of data for int array width")
	}
	width := int(b[pos])
	pos++

	for i := 0; i < count; i++ {
		if i > 0 {
			buf = append(buf, ',')
		}
		off := pos + i*width
		var v int64
		switch width {
		case 1:
			v = int64(b[off])
		case 2:
			v = int64(binary.BigEndian.Uint16(b[off : off+2]))
		case 4:
			v = int64(binary.BigEndian.Uint32(b[off : off+4]))
		case 8:
			v = int64(binary.BigEndian.Uint64(b[off : off+8]))
		}
		buf = strconv.AppendInt(buf, v, 10)
	}
	return append(buf, ']'), nil
}

// writeJSONFloatArray writes a typed float array as JSON.
func writeJSONFloatArray(buf []byte, b []byte, pos int, countWidth int) ([]byte, error) {
	count, pos, err := readCount(b, pos, countWidth)
	if err != nil {
		return nil, err
	}

	buf = append(buf, '[')
	for i := 0; i < count; i++ {
		if i > 0 {
			buf = append(buf, ',')
		}
		off := pos + i*8
		v := math.Float64frombits(binary.BigEndian.Uint64(b[off : off+8]))
		buf = strconv.AppendFloat(buf, v, 'f', -1, 64)
	}
	return append(buf, ']'), nil
}

// writeJSONObject writes a JSONB object at pos as JSON.
func writeJSONObject(buf []byte, b []byte, pos int, countWidth int, dict []dictEntry) ([]byte, error) {
	count, pos, err := readCount(b, pos, countWidth)
	if err != nil {
		return nil, err
	}

	buf = append(buf, '{')
	if count == 0 {
		return append(buf, '}'), nil
	}

	if pos+2 > len(b) {
		return nil, fmt.Errorf("unexpected end of data for object widths")
	}
	keyIdxW := uint8(b[pos])
	valOffW := uint8(b[pos+1])
	pos += 2

	entryWidth := int(keyIdxW) + int(valOffW)
	entryTableStart := pos
	valDataStart := pos + count*entryWidth

	cur := valDataStart
	for i := 0; i < count; i++ {
		if i > 0 {
			buf = append(buf, ',')
		}
		// Read key index from entry table.
		epos := entryTableStart + i*entryWidth
		keyIdx, _ := readUintN(b, epos, keyIdxW)

		// Write key directly from dictionary bytes (no string allocation).
		if int(keyIdx) >= len(dict) {
			return nil, fmt.Errorf("key index %d out of range", keyIdx)
		}
		de := dict[keyIdx]
		buf = append(buf, '"')
		// Keys in JSON objects generally don't need escaping (alphanumeric + underscore),
		// but we do it correctly for safety.
		for j := 0; j < de.length; j++ {
			c := b[de.offset+j]
			switch c {
			case '"':
				buf = append(buf, '\\', '"')
			case '\\':
				buf = append(buf, '\\', '\\')
			default:
				buf = append(buf, c)
			}
		}
		buf = append(buf, '"', ':')

		// Write value.
		var err error
		buf, err = writeJSONValue(buf, b, cur, dict)
		if err != nil {
			return nil, err
		}
		_, cur, err = skipValue(b, cur)
		if err != nil {
			return nil, err
		}
	}
	return append(buf, '}'), nil
}

// skipValue advances past a value at pos without decoding it, returning the new position.
func skipValue(b []byte, pos int) (byte, int, error) {
	if pos >= len(b) {
		return 0, pos, fmt.Errorf("unexpected end of data")
	}
	tag := b[pos]
	pos++
	// Inline small integer: single byte, already consumed
	if tag >= TagInlineIntBase {
		return tag, pos, nil
	}
	// Inline short string: tag encodes length
	if tag >= TagShortStringBase && tag < TagShortStringBase+32 {
		return tag, pos + int(tag-TagShortStringBase), nil
	}
	switch tag {
	case TagNull, TagTrue, TagFalse, TagEmptyObject, TagEmptyArray:
		return tag, pos, nil
	case TagStringRef:
		return tag, pos + 2, nil // skip uint16 index
	case TagBool:
		return tag, pos + 1, nil
	case TagInt:
		width := int(b[pos])
		return tag, pos + 1 + width, nil
	case TagFloat:
		return tag, pos + 8, nil
	case TagString:
		var length int
		if b[pos]&0x80 == 0 {
			length = int(b[pos])
			pos++
		} else {
			pos++
			length = int(binary.BigEndian.Uint32(b[pos : pos+4]))
			pos += 4
		}
		return tag, pos + length, nil
	case TagArray, TagLargeArray:
		countWidth := 1
		if tag == TagLargeArray {
			countWidth = 4
		}
		count, newPos, err := readCount(b, pos, countWidth)
		if err != nil {
			return 0, 0, err
		}
		pos = newPos
		// Skip offset table.
		dataStart := pos + count*4
		cur := dataStart
		for i := 0; i < count; i++ {
			_, next, err := skipValue(b, cur)
			if err != nil {
				return 0, 0, err
			}
			cur = next
		}
		return tag, cur, nil
	case TagObject, TagLargeObject:
		countWidth := 1
		if tag == TagLargeObject {
			countWidth = 2
		}
		count, newPos, err := readCount(b, pos, countWidth)
		if err != nil {
			return 0, 0, err
		}
		pos = newPos
		if count == 0 {
			return tag, pos, nil
		}
		keyIdxW := uint8(b[pos])
		valOffW := uint8(b[pos+1])
		pos += 2
		entryWidth := int(keyIdxW) + int(valOffW)
		valDataStart := pos + count*entryWidth
		cur := valDataStart
		for i := 0; i < count; i++ {
			_, next, err := skipValue(b, cur)
			if err != nil {
				return 0, 0, err
			}
			cur = next
		}
		return tag, cur, nil
	case TagIntArray, TagLargeIntArray:
		countWidth := 1
		if tag == TagLargeIntArray {
			countWidth = 4
		}
		count, newPos, err := readCount(b, pos, countWidth)
		if err != nil {
			return 0, 0, err
		}
		pos = newPos
		width := int(b[pos])
		pos++
		return tag, pos + count*width, nil
	case TagFloatArray, TagLargeFloatArray:
		countWidth := 1
		if tag == TagLargeFloatArray {
			countWidth = 4
		}
		count, newPos, err := readCount(b, pos, countWidth)
		if err != nil {
			return 0, 0, err
		}
		pos = newPos
		return tag, pos + count*8, nil
	default:
		return 0, 0, fmt.Errorf("unknown tag: 0x%02x", tag)
	}
}

// BodyTag returns the type tag of the body (after the dictionary header).
// Useful for testing to check the internal encoding format.
func BodyTag(b []byte) byte {
	_, bodyPos, err := readDictHeader(b)
	if err != nil || bodyPos >= len(b) {
		return TagInvalid
	}
	return b[bodyPos]
}

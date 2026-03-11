package jsonb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
)

// Type tags for the custom binary format.
const (
	TagInvalid    byte = 0x00
	TagNull       byte = 0x01
	TagBool       byte = 0x02
	TagInt        byte = 0x03
	TagFloat      byte = 0x04
	TagString     byte = 0x05
	TagArray      byte = 0x06
	TagObject     byte = 0x07
	TagIntArray   byte = 0x08
	TagFloatArray byte = 0x09
)

// Encode serializes a Go value into the custom JSONB binary format.
// The binary starts with a key dictionary header, followed by the encoded body.
// All object keys across the entire value are stored once in the dictionary,
// and objects reference keys by uint16 index.
//
// Format:
//
//	[keyCount: uint16][key0: TagString...][key1: TagString...]...[body]
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

	// Phase 2: Write dictionary header.
	var buf []byte
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(sortedKeys)))
	for _, k := range sortedKeys {
		buf = encodeCompactString(buf, k)
	}

	// Phase 3: Encode body.
	var err error
	buf, err = encodeValue(buf, val, keyToIdx)
	if err != nil {
		return nil, err
	}
	return buf, nil
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

func encodeValue(buf []byte, val any, dict map[string]uint16) ([]byte, error) {
	if val == nil {
		return append(buf, TagNull), nil
	}
	switch v := val.(type) {
	case bool:
		buf = append(buf, TagBool)
		if v {
			buf = append(buf, 0x01)
		} else {
			buf = append(buf, 0x00)
		}
		return buf, nil
	case int64:
		return encodeCompactInt(buf, v), nil
	case int:
		return encodeCompactInt(buf, int64(v)), nil
	case float64:
		buf = append(buf, TagFloat)
		buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(v))
		return buf, nil
	case string:
		return encodeCompactString(buf, v), nil
	case []any:
		return encodeArray(buf, v, dict)
	case map[string]any:
		return encodeObject(buf, v, dict)
	default:
		return nil, fmt.Errorf("unsupported type: %T", val)
	}
}

// encodeCompactInt encodes an integer with minimum byte width.
// Format: [TagInt][width: uint8][value: width bytes]
func encodeCompactInt(buf []byte, v int64) []byte {
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
// Short strings (< 128 bytes): [TagString][1-byte length][data]
// Long strings (>= 128 bytes): [TagString][0x80 | high byte][low 3 bytes as uint24] — actually simpler:
// We use the high bit of the first length byte as a flag:
//
//	0xxxxxxx = length is this byte (0-127)
//	1xxxxxxx = length is next 4 bytes (big-endian uint32), this byte is just the marker 0x80
func encodeCompactString(buf []byte, v string) []byte {
	buf = append(buf, TagString)
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

func encodeArray(buf []byte, arr []any, dict map[string]uint16) ([]byte, error) {
	if len(arr) == 0 {
		buf = append(buf, TagArray)
		buf = binary.BigEndian.AppendUint32(buf, 0)
		return buf, nil
	}

	// Check if all elements are the same type for typed array optimization.
	if b, err := tryEncodeIntArray(buf, arr); err == nil && b != nil {
		return b, nil
	}
	if b, err := tryEncodeFloatArray(buf, arr); err == nil && b != nil {
		return b, nil
	}

	// Generic array encoding with offset table.
	buf = append(buf, TagArray)
	count := uint32(len(arr))
	buf = binary.BigEndian.AppendUint32(buf, count)

	// Encode each element into a temporary buffer to compute offsets.
	elements := make([][]byte, len(arr))
	for i, v := range arr {
		elem, err := encodeValue(nil, v, dict)
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
// Format: [TagIntArray][count: uint32][width: uint8][values: width × count]
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

	buf = append(buf, TagIntArray)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(vals)))
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
// Format: [TagFloatArray][count: uint32][values: 8 × count]
func tryEncodeFloatArray(buf []byte, arr []any) ([]byte, error) {
	vals := make([]float64, len(arr))
	for i, v := range arr {
		f, ok := v.(float64)
		if !ok {
			return nil, nil // not all float64
		}
		vals[i] = f
	}

	buf = append(buf, TagFloatArray)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(vals)))

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
// Format:
//
//	[TagObject][count: uint16][keyIdxWidth: uint8][valOffWidth: uint8]
//	[entry table: (keyIdx: keyIdxWidth, valOff: valOffWidth) × count]
//	[value data]
//
// Keys are written in sorted order (by dictionary index, which is already sorted).
func encodeObject(buf []byte, obj map[string]any, dict map[string]uint16) ([]byte, error) {
	buf = append(buf, TagObject)
	count := len(obj)
	buf = binary.BigEndian.AppendUint16(buf, uint16(count))

	if count == 0 {
		return buf, nil
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
		ev, err := encodeValue(nil, obj[k], dict)
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
func readDictHeader(b []byte) ([]string, int, error) {
	if len(b) < 2 {
		return nil, 0, fmt.Errorf("unexpected end of data for dictionary header")
	}
	keyCount := int(binary.BigEndian.Uint16(b[0:2]))
	pos := 2
	dict := make([]string, keyCount)
	for i := 0; i < keyCount; i++ {
		val, newPos, err := decodeValue(b, pos, nil)
		if err != nil {
			return nil, 0, fmt.Errorf("invalid dictionary entry: %w", err)
		}
		s, ok := val.(string)
		if !ok {
			return nil, 0, fmt.Errorf("dictionary entry must be string, got %T", val)
		}
		dict[i] = s
		pos = newPos
	}
	return dict, pos, nil
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
	switch tag {
	case TagNull:
		return nil, pos, nil
	case TagBool:
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
	case TagArray:
		return decodeArray(b, pos, dict)
	case TagObject:
		return decodeObject(b, pos, dict)
	case TagIntArray:
		return decodeIntArray(b, pos)
	case TagFloatArray:
		return decodeFloatArray(b, pos)
	default:
		return nil, pos, fmt.Errorf("unknown tag: 0x%02x", tag)
	}
}

func decodeArray(b []byte, pos int, dict []string) (any, int, error) {
	if pos+4 > len(b) {
		return nil, pos, fmt.Errorf("unexpected end of data for array count")
	}
	count := int(binary.BigEndian.Uint32(b[pos : pos+4]))
	pos += 4

	if count == 0 {
		return []any{}, pos, nil
	}

	// Read offset table.
	offsetTableSize := count * 4
	if pos+offsetTableSize > len(b) {
		return nil, pos, fmt.Errorf("unexpected end of data for array offsets")
	}
	offsets := make([]uint32, count)
	for i := 0; i < count; i++ {
		offsets[i] = binary.BigEndian.Uint32(b[pos+i*4 : pos+i*4+4])
	}
	pos += offsetTableSize

	// Data section starts at pos.
	dataStart := pos
	result := make([]any, count)
	for i := 0; i < count; i++ {
		val, newPos, err := decodeValue(b, dataStart+int(offsets[i]), dict)
		if err != nil {
			return nil, newPos, err
		}
		result[i] = val
		if newPos > pos {
			pos = newPos
		}
	}
	return result, pos, nil
}

func decodeIntArray(b []byte, pos int) (any, int, error) {
	if pos+4 > len(b) {
		return nil, pos, fmt.Errorf("unexpected end of data for int array count")
	}
	count := int(binary.BigEndian.Uint32(b[pos : pos+4]))
	pos += 4

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

func decodeFloatArray(b []byte, pos int) (any, int, error) {
	if pos+4 > len(b) {
		return nil, pos, fmt.Errorf("unexpected end of data for float array count")
	}
	count := int(binary.BigEndian.Uint32(b[pos : pos+4]))
	pos += 4

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
//
// Format:
//
//	[count: uint16][keyIdxWidth: uint8][valOffWidth: uint8]
//	[entry table: (keyIdx: keyIdxWidth, valOff: valOffWidth) × count]
//	[value data]
func decodeObject(b []byte, pos int, dict []string) (any, int, error) {
	if pos+2 > len(b) {
		return nil, pos, fmt.Errorf("unexpected end of data for object count")
	}
	count := int(binary.BigEndian.Uint16(b[pos : pos+2]))
	pos += 2

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

	keyIndices := make([]uint32, count)
	valOffsets := make([]uint32, count)
	epos := pos
	for i := 0; i < count; i++ {
		keyIndices[i], epos = readUintN(b, epos, keyIdxW)
		valOffsets[i], epos = readUintN(b, epos, valOffW)
	}
	pos += entryTableSize

	valDataStart := pos
	result := make(map[string]any, count)
	furthest := valDataStart
	for i := 0; i < count; i++ {
		if int(keyIndices[i]) >= len(dict) {
			return nil, pos, fmt.Errorf("key index %d out of range (dict size %d)", keyIndices[i], len(dict))
		}
		key := dict[keyIndices[i]]
		val, newPos, err := decodeValue(b, valDataStart+int(valOffsets[i]), dict)
		if err != nil {
			return nil, newPos, err
		}
		result[key] = val
		if newPos > furthest {
			furthest = newPos
		}
	}
	return result, furthest, nil
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
	if bodyPos >= len(b) || b[bodyPos] != TagObject {
		return nil, false, fmt.Errorf("not an object")
	}
	pos := bodyPos + 1

	if pos+4 > len(b) {
		return nil, false, fmt.Errorf("unexpected end of data")
	}
	count := int(binary.BigEndian.Uint16(b[pos : pos+2]))
	pos += 2
	keyIdxW := b[pos]
	valOffW := b[pos+1]
	pos += 2

	if count == 0 {
		return nil, false, nil
	}

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
	case TagIntArray:
		return lookupIntArrayIndex(b, bodyPos, idx)
	case TagFloatArray:
		return lookupFloatArrayIndex(b, bodyPos, idx)
	case TagArray:
		return lookupGenericArrayIndex(b, bodyPos, idx, dict)
	default:
		return nil, false, fmt.Errorf("not an array")
	}
}

func lookupIntArrayIndex(b []byte, start int, idx int) (any, bool, error) {
	pos := start + 1 // skip tag
	if pos+5 > len(b) {
		return nil, false, fmt.Errorf("unexpected end of data")
	}
	count := int(binary.BigEndian.Uint32(b[pos : pos+4]))
	pos += 4
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
	pos := start + 1 // skip tag
	if pos+4 > len(b) {
		return nil, false, fmt.Errorf("unexpected end of data")
	}
	count := int(binary.BigEndian.Uint32(b[pos : pos+4]))
	if idx < 0 || idx >= count {
		return nil, false, nil
	}
	pos += 4
	off := pos + idx*8
	if off+8 > len(b) {
		return nil, false, fmt.Errorf("unexpected end of data")
	}
	return math.Float64frombits(binary.BigEndian.Uint64(b[off : off+8])), true, nil
}

func lookupGenericArrayIndex(b []byte, start int, idx int, dict []string) (any, bool, error) {
	pos := start + 1 // skip tag
	if pos+4 > len(b) {
		return nil, false, fmt.Errorf("unexpected end of data")
	}
	count := int(binary.BigEndian.Uint32(b[pos : pos+4]))
	pos += 4
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
func ToJSON(b []byte) (string, error) {
	val, err := Decode(b)
	if err != nil {
		return "", err
	}
	out, err := json.Marshal(convertForJSON(val))
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func convertForJSON(val any) any {
	switch v := val.(type) {
	case int64:
		return v
	case map[string]any:
		m := make(map[string]any, len(v))
		for k, v2 := range v {
			m[k] = convertForJSON(v2)
		}
		return m
	case []any:
		a := make([]any, len(v))
		for i, v2 := range v {
			a[i] = convertForJSON(v2)
		}
		return a
	default:
		return val
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

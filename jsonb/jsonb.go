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
	TagInvalid byte = 0x00
	TagNull    byte = 0x01
	TagBool    byte = 0x02
	TagInt     byte = 0x03
	TagFloat   byte = 0x04
	TagString  byte = 0x05
	TagArray   byte = 0x06
	TagObject  byte = 0x07
)

// Encode serializes a Go value into the custom JSONB binary format.
// Supported types: nil, bool, int64, int, float64, string, []any, map[string]any.
func Encode(val any) ([]byte, error) {
	var buf []byte
	var err error
	buf, err = encodeValue(buf, val)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func encodeValue(buf []byte, val any) ([]byte, error) {
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
		buf = append(buf, TagInt)
		buf = binary.BigEndian.AppendUint64(buf, uint64(v))
		return buf, nil
	case int:
		buf = append(buf, TagInt)
		buf = binary.BigEndian.AppendUint64(buf, uint64(int64(v)))
		return buf, nil
	case float64:
		buf = append(buf, TagFloat)
		buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(v))
		return buf, nil
	case string:
		buf = append(buf, TagString)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
		buf = append(buf, v...)
		return buf, nil
	case []any:
		return encodeArray(buf, v)
	case map[string]any:
		return encodeObject(buf, v)
	default:
		return nil, fmt.Errorf("unsupported type: %T", val)
	}
}

func encodeArray(buf []byte, arr []any) ([]byte, error) {
	buf = append(buf, TagArray)
	count := uint32(len(arr))
	buf = binary.BigEndian.AppendUint32(buf, count)

	// Encode each element into a temporary buffer to compute offsets.
	elements := make([][]byte, len(arr))
	for i, v := range arr {
		elem, err := encodeValue(nil, v)
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

func encodeObject(buf []byte, obj map[string]any) ([]byte, error) {
	buf = append(buf, TagObject)
	count := uint32(len(obj))
	buf = binary.BigEndian.AppendUint32(buf, count)

	// Sort keys.
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Encode keys and values separately.
	encodedKeys := make([][]byte, len(keys))
	encodedVals := make([][]byte, len(keys))
	for i, k := range keys {
		ek, err := encodeValue(nil, k)
		if err != nil {
			return nil, err
		}
		encodedKeys[i] = ek

		ev, err := encodeValue(nil, obj[k])
		if err != nil {
			return nil, err
		}
		encodedVals[i] = ev
	}

	// Compute total sizes for key data and value data.
	totalKeySize := uint32(0)
	for _, ek := range encodedKeys {
		totalKeySize += uint32(len(ek))
	}

	// Key entry table: [keyOffset: uint32, valOffset: uint32] per entry.
	// Key offsets are relative to the start of the key data section.
	// Value offsets are relative to the start of the value data section.
	keyOffset := uint32(0)
	valOffset := uint32(0)
	for i := range keys {
		buf = binary.BigEndian.AppendUint32(buf, keyOffset)
		buf = binary.BigEndian.AppendUint32(buf, valOffset)
		keyOffset += uint32(len(encodedKeys[i]))
		valOffset += uint32(len(encodedVals[i]))
	}

	// Write key data.
	for _, ek := range encodedKeys {
		buf = append(buf, ek...)
	}

	// Write value data.
	for _, ev := range encodedVals {
		buf = append(buf, ev...)
	}

	return buf, nil
}

// Decode deserializes the custom JSONB binary format into a Go value.
func Decode(b []byte) (any, error) {
	val, _, err := decodeValue(b, 0)
	return val, err
}

func decodeValue(b []byte, pos int) (any, int, error) {
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
		if pos+8 > len(b) {
			return nil, pos, fmt.Errorf("unexpected end of data for int")
		}
		v := int64(binary.BigEndian.Uint64(b[pos : pos+8]))
		return v, pos + 8, nil
	case TagFloat:
		if pos+8 > len(b) {
			return nil, pos, fmt.Errorf("unexpected end of data for float")
		}
		v := math.Float64frombits(binary.BigEndian.Uint64(b[pos : pos+8]))
		return v, pos + 8, nil
	case TagString:
		if pos+4 > len(b) {
			return nil, pos, fmt.Errorf("unexpected end of data for string length")
		}
		length := int(binary.BigEndian.Uint32(b[pos : pos+4]))
		pos += 4
		if pos+length > len(b) {
			return nil, pos, fmt.Errorf("unexpected end of data for string body")
		}
		v := string(b[pos : pos+length])
		return v, pos + length, nil
	case TagArray:
		return decodeArray(b, pos)
	case TagObject:
		return decodeObject(b, pos)
	default:
		return nil, pos, fmt.Errorf("unknown tag: 0x%02x", tag)
	}
}

func decodeArray(b []byte, pos int) (any, int, error) {
	if pos+4 > len(b) {
		return nil, pos, fmt.Errorf("unexpected end of data for array count")
	}
	count := int(binary.BigEndian.Uint32(b[pos : pos+4]))
	pos += 4

	if count == 0 {
		// Skip offset table (empty), data section starts immediately.
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
		val, newPos, err := decodeValue(b, dataStart+int(offsets[i]))
		if err != nil {
			return nil, newPos, err
		}
		result[i] = val
		// Track the furthest position read.
		if newPos > pos {
			pos = newPos
		}
	}
	return result, pos, nil
}

func decodeObject(b []byte, pos int) (any, int, error) {
	if pos+4 > len(b) {
		return nil, pos, fmt.Errorf("unexpected end of data for object count")
	}
	count := int(binary.BigEndian.Uint32(b[pos : pos+4]))
	pos += 4

	if count == 0 {
		return map[string]any{}, pos, nil
	}

	// Read entry table: [keyOffset, valOffset] per entry, each uint32.
	entryTableSize := count * 8
	if pos+entryTableSize > len(b) {
		return nil, pos, fmt.Errorf("unexpected end of data for object entry table")
	}
	keyOffsets := make([]uint32, count)
	valOffsets := make([]uint32, count)
	for i := 0; i < count; i++ {
		keyOffsets[i] = binary.BigEndian.Uint32(b[pos+i*8 : pos+i*8+4])
		valOffsets[i] = binary.BigEndian.Uint32(b[pos+i*8+4 : pos+i*8+8])
	}
	pos += entryTableSize

	// Compute key data section start and total key data size.
	// Key data size = last key offset + size of last key entry.
	// We need to figure out the total key data size to find where value data starts.
	// We can determine key section end by decoding all keys.
	keyDataStart := pos

	// Decode all keys to find key section size.
	keys := make([]string, count)
	maxKeyEnd := keyDataStart
	for i := 0; i < count; i++ {
		val, newPos, err := decodeValue(b, keyDataStart+int(keyOffsets[i]))
		if err != nil {
			return nil, newPos, err
		}
		s, ok := val.(string)
		if !ok {
			return nil, newPos, fmt.Errorf("object key must be string, got %T", val)
		}
		keys[i] = s
		if newPos > maxKeyEnd {
			maxKeyEnd = newPos
		}
	}

	valDataStart := maxKeyEnd
	result := make(map[string]any, count)
	furthest := valDataStart
	for i := 0; i < count; i++ {
		val, newPos, err := decodeValue(b, valDataStart+int(valOffsets[i]))
		if err != nil {
			return nil, newPos, err
		}
		result[keys[i]] = val
		if newPos > furthest {
			furthest = newPos
		}
	}
	return result, furthest, nil
}

// LookupKey performs a binary search on an encoded object to find the value for the given key
// without fully deserializing the entire object.
func LookupKey(b []byte, key string) (any, bool, error) {
	if len(b) == 0 || b[0] != TagObject {
		return nil, false, fmt.Errorf("not an object")
	}
	pos := 1
	if pos+4 > len(b) {
		return nil, false, fmt.Errorf("unexpected end of data")
	}
	count := int(binary.BigEndian.Uint32(b[pos : pos+4]))
	pos += 4

	if count == 0 {
		return nil, false, nil
	}

	// Read entry table.
	entryTableStart := pos
	entryTableSize := count * 8
	if pos+entryTableSize > len(b) {
		return nil, false, fmt.Errorf("unexpected end of data for entry table")
	}
	pos += entryTableSize
	keyDataStart := pos

	// Compute key data section size by finding the end of the last key.
	// We need to figure out where value data starts.
	// The last key offset + its encoded size = end of key data section.
	// To avoid scanning all keys, let's find the max key end by reading the last key offset.
	lastKeyOffset := int(binary.BigEndian.Uint32(b[entryTableStart+(count-1)*8 : entryTableStart+(count-1)*8+4]))
	_, lastKeyEnd, err := decodeValue(b, keyDataStart+lastKeyOffset)
	if err != nil {
		return nil, false, err
	}
	valDataStart := lastKeyEnd

	// Binary search on sorted keys.
	lo, hi := 0, count-1
	for lo <= hi {
		mid := (lo + hi) / 2
		keyOff := int(binary.BigEndian.Uint32(b[entryTableStart+mid*8 : entryTableStart+mid*8+4]))
		midKey, _, err := decodeValue(b, keyDataStart+keyOff)
		if err != nil {
			return nil, false, err
		}
		midKeyStr := midKey.(string)
		cmp := strings.Compare(midKeyStr, key)
		if cmp == 0 {
			// Found. Decode value.
			valOff := int(binary.BigEndian.Uint32(b[entryTableStart+mid*8+4 : entryTableStart+mid*8+8]))
			val, _, err := decodeValue(b, valDataStart+valOff)
			if err != nil {
				return nil, false, err
			}
			return val, true, nil
		} else if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return nil, false, nil
}

// LookupIndex retrieves the element at the given index from an encoded array
// without fully deserializing the entire array.
func LookupIndex(b []byte, idx int) (any, bool, error) {
	if len(b) == 0 || b[0] != TagArray {
		return nil, false, fmt.Errorf("not an array")
	}
	pos := 1
	if pos+4 > len(b) {
		return nil, false, fmt.Errorf("unexpected end of data")
	}
	count := int(binary.BigEndian.Uint32(b[pos : pos+4]))
	pos += 4

	if idx < 0 || idx >= count {
		return nil, false, nil
	}

	// Read offset for the requested index.
	offsetPos := pos + idx*4
	if offsetPos+4 > len(b) {
		return nil, false, fmt.Errorf("unexpected end of data for offset")
	}
	offset := int(binary.BigEndian.Uint32(b[offsetPos : offsetPos+4]))

	// Data section starts after the offset table.
	dataStart := pos + count*4
	val, _, err := decodeValue(b, dataStart+offset)
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
			// Check if it's actually a float (has decimal point or exponent).
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
	// Convert int64 to json.Number for proper serialization, then marshal.
	out, err := json.Marshal(convertForJSON(val))
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// convertForJSON converts int64 values to float64 for JSON marshaling compatibility,
// since json.Marshal produces float64 by default for numbers.
func convertForJSON(val any) any {
	switch v := val.(type) {
	case int64:
		// Keep as int64; json.Marshal handles it correctly.
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

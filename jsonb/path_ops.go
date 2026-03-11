package jsonb

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
)

// PathOpsTokenize extracts GIN index tokens from JSONB binary data using
// the jsonb_path_ops strategy. Each leaf value produces a token that is
// the FNV-64a hash of the path (object keys) concatenated with the value.
// Array indices are not included in the path (following PostgreSQL's approach).
// Duplicate tokens are deduplicated.
func PathOpsTokenize(b []byte) ([]string, error) {
	dict, bodyPos, err := readDictHeader(b)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]struct{})
	var tokens []string
	var path []string

	var walkValue func(pos int) (int, error)
	walkValue = func(pos int) (int, error) {
		if pos >= len(b) {
			return pos, fmt.Errorf("unexpected end of data")
		}
		tag := b[pos]
		pos++

		// Inline small integer: 0x80-0xFF → value 0-127
		if tag >= TagInlineIntBase {
			val := int64(tag & 0x7F)
			addToken(&tokens, seen, path, fmt.Sprintf("i:%d", val))
			return pos, nil
		}

		// Inline short string: tag encodes length
		if tag >= TagShortStringBase && tag < TagShortStringBase+32 {
			n := int(tag - TagShortStringBase)
			if pos+n > len(b) {
				return pos, fmt.Errorf("unexpected end for short string")
			}
			addToken(&tokens, seen, path, "s:"+string(b[pos:pos+n]))
			return pos + n, nil
		}

		switch tag {
		case TagNull:
			addToken(&tokens, seen, path, "null")
			return pos, nil

		case TagTrue:
			addToken(&tokens, seen, path, "true")
			return pos, nil

		case TagFalse:
			addToken(&tokens, seen, path, "false")
			return pos, nil

		case TagInt:
			if pos >= len(b) {
				return pos, fmt.Errorf("unexpected end for int width")
			}
			w := int(b[pos])
			pos++
			if pos+w > len(b) {
				return pos, fmt.Errorf("unexpected end for int value")
			}
			val := decodeSignedInt(b[pos:pos+w], w)
			addToken(&tokens, seen, path, fmt.Sprintf("i:%d", val))
			return pos + w, nil

		case TagFloat:
			if pos+8 > len(b) {
				return pos, fmt.Errorf("unexpected end for float")
			}
			bits := binary.BigEndian.Uint64(b[pos : pos+8])
			val := math.Float64frombits(bits)
			addToken(&tokens, seen, path, fmt.Sprintf("f:%v", val))
			return pos + 8, nil

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
			if pos+length > len(b) {
				return pos, fmt.Errorf("unexpected end for string body")
			}
			addToken(&tokens, seen, path, "s:"+string(b[pos:pos+length]))
			return pos + length, nil

		case TagStringRef:
			if pos+2 > len(b) {
				return pos, fmt.Errorf("unexpected end for string ref")
			}
			idx := int(binary.BigEndian.Uint16(b[pos : pos+2]))
			if idx >= len(dict) {
				return pos, fmt.Errorf("string ref index %d out of range (dict len %d)", idx, len(dict))
			}
			addToken(&tokens, seen, path, "s:"+dict[idx])
			return pos + 2, nil

		case TagEmptyObject, TagEmptyArray:
			return pos, nil

		case TagObject, TagLargeObject:
			countWidth := 1
			if tag == TagLargeObject {
				countWidth = 2
			}
			count, newPos, err := readCount(b, pos, countWidth)
			if err != nil {
				return 0, err
			}
			pos = newPos
			if count == 0 {
				return pos, nil
			}
			if pos+2 > len(b) {
				return 0, fmt.Errorf("unexpected end for object widths")
			}
			keyIdxW := uint8(b[pos])
			valOffW := uint8(b[pos+1])
			pos += 2

			entryWidth := int(keyIdxW) + int(valOffW)
			entryStart := pos
			dataStart := pos + count*entryWidth

			for i := 0; i < count; i++ {
				ep := entryStart + i*entryWidth
				keyIdx, _ := readUintN(b, ep, keyIdxW)
				valOff, _ := readUintN(b, ep+int(keyIdxW), valOffW)

				if int(keyIdx) >= len(dict) {
					return 0, fmt.Errorf("key index %d out of range", keyIdx)
				}
				keyName := dict[keyIdx]

				path = append(path, keyName)
				_, err := walkValue(dataStart + int(valOff))
				if err != nil {
					return 0, err
				}
				path = path[:len(path)-1]
			}

			// Skip to end by walking all values sequentially
			cur := dataStart
			for i := 0; i < count; i++ {
				_, next, err := skipValue(b, cur)
				if err != nil {
					return 0, err
				}
				cur = next
			}
			return cur, nil

		case TagArray, TagLargeArray:
			countWidth := 1
			if tag == TagLargeArray {
				countWidth = 4
			}
			count, newPos, err := readCount(b, pos, countWidth)
			if err != nil {
				return 0, err
			}
			// Skip offset table
			dataStart := newPos + count*4
			cur := dataStart
			for i := 0; i < count; i++ {
				next, err := walkValue(cur)
				if err != nil {
					return 0, err
				}
				cur = next
			}
			return cur, nil

		case TagIntArray, TagLargeIntArray:
			countWidth := 1
			if tag == TagLargeIntArray {
				countWidth = 4
			}
			count, newPos, err := readCount(b, pos, countWidth)
			if err != nil {
				return 0, err
			}
			pos = newPos
			if count == 0 {
				return pos, nil
			}
			if pos >= len(b) {
				return pos, fmt.Errorf("unexpected end for int array width")
			}
			width := int(b[pos])
			pos++
			for i := 0; i < count; i++ {
				val := decodeSignedInt(b[pos:pos+width], width)
				addToken(&tokens, seen, path, fmt.Sprintf("i:%d", val))
				pos += width
			}
			return pos, nil

		case TagFloatArray, TagLargeFloatArray:
			countWidth := 1
			if tag == TagLargeFloatArray {
				countWidth = 4
			}
			count, newPos, err := readCount(b, pos, countWidth)
			if err != nil {
				return 0, err
			}
			pos = newPos
			for i := 0; i < count; i++ {
				if pos+8 > len(b) {
					return pos, fmt.Errorf("unexpected end for float array element")
				}
				bits := binary.BigEndian.Uint64(b[pos : pos+8])
				val := math.Float64frombits(bits)
				addToken(&tokens, seen, path, fmt.Sprintf("f:%v", val))
				pos += 8
			}
			return pos, nil

		default:
			return pos, fmt.Errorf("unknown tag: 0x%02x", tag)
		}
	}

	_, err = walkValue(bodyPos)
	if err != nil {
		return nil, err
	}
	return tokens, nil
}

// addToken computes a path hash and adds it to the token list if not already present.
func addToken(tokens *[]string, seen map[string]struct{}, path []string, value string) {
	h := fnv.New64a()
	for _, p := range path {
		h.Write([]byte(p))
		h.Write([]byte{0}) // separator
	}
	h.Write([]byte(value))
	tok := fmt.Sprintf("%016x", h.Sum64())
	if _, ok := seen[tok]; !ok {
		seen[tok] = struct{}{}
		*tokens = append(*tokens, tok)
	}
}

// decodeSignedInt decodes a big-endian signed integer of the given width.
func decodeSignedInt(b []byte, width int) int64 {
	switch width {
	case 1:
		return int64(int8(b[0]))
	case 2:
		return int64(int16(binary.BigEndian.Uint16(b[:2])))
	case 4:
		return int64(int32(binary.BigEndian.Uint32(b[:4])))
	case 8:
		return int64(binary.BigEndian.Uint64(b[:8]))
	}
	return 0
}

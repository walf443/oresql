package jsonb

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/walf443/oresql/json_path"
)

// Test data generators for various JSON shapes.

func smallObject() string {
	return `{"name":"alice","age":30,"active":true}`
}

func mediumObject() string {
	return `{"id":12345,"name":"alice","email":"alice@example.com","age":30,"active":true,"tags":["go","sql","db"],"address":{"city":"Tokyo","zip":"100-0001"}}`
}

func largeObject() string {
	obj := map[string]any{
		"id":      12345,
		"name":    "alice",
		"email":   "alice@example.com",
		"active":  true,
		"score":   98.5,
		"address": map[string]any{"city": "Tokyo", "zip": "100-0001", "country": "Japan"},
	}
	tags := make([]any, 20)
	for i := range tags {
		tags[i] = fmt.Sprintf("tag_%d", i)
	}
	obj["tags"] = tags
	items := make([]any, 10)
	for i := range items {
		items[i] = map[string]any{
			"product": fmt.Sprintf("item_%d", i),
			"qty":     i + 1,
			"price":   float64(i)*10.5 + 100,
		}
	}
	obj["items"] = items
	b, _ := json.Marshal(obj)
	return string(b)
}

func arrayOf100Ints() string {
	arr := make([]int, 100)
	for i := range arr {
		arr[i] = i
	}
	b, _ := json.Marshal(arr)
	return string(b)
}

func deeplyNested() string {
	inner := map[string]any{"value": 42}
	for i := 0; i < 10; i++ {
		inner = map[string]any{fmt.Sprintf("level_%d", i): inner}
	}
	b, _ := json.Marshal(inner)
	return string(b)
}

// BenchmarkSpaceEfficiency measures the byte size of JSON text vs JSONB binary format.
// This is not a traditional benchmark but uses testing.B to report custom metrics.
func BenchmarkSpaceEfficiency(b *testing.B) {
	cases := []struct {
		name string
		json string
	}{
		{"SmallObject", smallObject()},
		{"MediumObject", mediumObject()},
		{"LargeObject", largeObject()},
		{"ArrayOf100Ints", arrayOf100Ints()},
		{"DeeplyNested", deeplyNested()},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			// Compact JSON for fair comparison
			var compacted []byte
			compacted, _ = compactJSON(tc.json)

			jsonSize := len(compacted)

			jsonbData, err := FromJSON(tc.json)
			if err != nil {
				b.Fatal(err)
			}
			jsonbSize := len(jsonbData)

			ratio := float64(jsonbSize) / float64(jsonSize) * 100

			b.ReportMetric(float64(jsonSize), "JSON_bytes")
			b.ReportMetric(float64(jsonbSize), "JSONB_bytes")
			b.ReportMetric(ratio, "%_of_JSON")

			// Run actual encode benchmark
			for i := 0; i < b.N; i++ {
				_, _ = FromJSON(tc.json)
			}
		})
	}
}

func BenchmarkEncodeJSON(b *testing.B) {
	cases := []struct {
		name string
		json string
	}{
		{"SmallObject", smallObject()},
		{"MediumObject", mediumObject()},
		{"LargeObject", largeObject()},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = FromJSON(tc.json)
			}
		})
	}
}

func BenchmarkDecodeToJSON(b *testing.B) {
	cases := []struct {
		name string
		json string
	}{
		{"SmallObject", smallObject()},
		{"MediumObject", mediumObject()},
		{"LargeObject", largeObject()},
	}
	for _, tc := range cases {
		data, _ := FromJSON(tc.json)
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = ToJSON(data)
			}
		})
	}
}

func BenchmarkLookupKey(b *testing.B) {
	cases := []struct {
		name string
		json string
		key  string
	}{
		{"SmallObject", smallObject(), "name"},
		{"MediumObject", mediumObject(), "email"},
		{"LargeObject", largeObject(), "score"},
	}
	for _, tc := range cases {
		data, _ := FromJSON(tc.json)
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _, _ = LookupKey(data, tc.key)
			}
		})
	}
}

func BenchmarkLookupKey_vs_FullDecode(b *testing.B) {
	data, _ := FromJSON(largeObject())

	b.Run("LookupKey", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, _ = LookupKey(data, "score")
		}
	})

	b.Run("FullDecode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			val, _ := Decode(data)
			_ = val.(map[string]any)["score"]
		}
	})
}

func BenchmarkLookupIndex_vs_FullDecode(b *testing.B) {
	data, _ := FromJSON(arrayOf100Ints())

	b.Run("LookupIndex_50", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, _ = LookupIndex(data, 50)
		}
	})

	b.Run("FullDecode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			val, _ := Decode(data)
			_ = val.([]any)[50]
		}
	})
}

func BenchmarkLookupKeys_vs_FullDecode(b *testing.B) {
	data, _ := FromJSON(largeObject())

	// $.address.city — 2-level nested access
	b.Run("LookupKeys_2level", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, _ = LookupKeys(data, "address", "city")
		}
	})

	// $.items[0].product — 3-level mixed access (key, index, key)
	b.Run("LookupKeys_3level", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, _ = LookupKeys(data, "items", 0, "product")
		}
	})

	// Equivalent using chained LookupKey + LookupIndex (re-parsing dict each time)
	b.Run("ChainedLookup_3level", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			items, _, _ := LookupKey(data, "items")
			arr := items.([]any)
			obj := arr[0].(map[string]any)
			_ = obj["product"]
		}
	})

	b.Run("FullDecode_3level", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			val, _ := Decode(data)
			m := val.(map[string]any)
			items := m["items"].([]any)
			obj := items[0].(map[string]any)
			_ = obj["product"]
		}
	})
}

func BenchmarkQueryPath_vs_ExistsPath(b *testing.B) {
	data, _ := FromJSON(largeObject())
	path2, _ := json_path.Parse("$.address.city")
	path3, _ := json_path.Parse("$.items[0].product")

	b.Run("QueryPath_2level", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, _ = QueryPath(data, path2)
		}
	})

	b.Run("ExistsPath_2level", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = ExistsPath(data, path2)
		}
	})

	b.Run("QueryPath_3level", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, _ = QueryPath(data, path3)
		}
	})

	b.Run("ExistsPath_3level", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = ExistsPath(data, path3)
		}
	})

	// Comparison: json_path.Execute on fully decoded value
	decoded, _ := Decode(data)
	b.Run("json_path.Execute_3level", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = path3.Execute(decoded)
		}
	})

	b.Run("json_path.Exists_3level", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = path3.Exists(decoded)
		}
	})

	// Full decode + path execution (what happens per row without JSONB optimization)
	b.Run("FullDecode+Execute_3level", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			val, _ := Decode(data)
			_ = path3.Execute(val)
		}
	})
}

func compactJSON(s string) ([]byte, error) {
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

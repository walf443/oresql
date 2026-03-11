package engine

import (
	"github.com/walf443/oresql/jsonb"
)

// jsonToMsgpack converts a JSON string to custom JSONB binary format.
// The function name is kept for backward compatibility but now uses the custom format.
func jsonToMsgpack(jsonStr string) ([]byte, error) {
	return jsonb.FromJSON(jsonStr)
}

// msgpackToJSON converts custom JSONB binary format to a JSON string.
func msgpackToJSON(data []byte) (string, error) {
	return jsonb.ToJSON(data)
}

// msgpackToValue converts custom JSONB binary format to a Go value.
func msgpackToValue(data []byte) (any, error) {
	return jsonb.Decode(data)
}

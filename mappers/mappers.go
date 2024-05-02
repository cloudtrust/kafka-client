package mappers

import (
	"context"
	"encoding/base64"
)

// DecodeBase64Bytes is a mapper to convert a content from base64
func DecodeBase64Bytes(ctx context.Context, in any) (any, error) {
	var bytes = in.([]byte)
	return base64.StdEncoding.DecodeString(string(bytes))
}

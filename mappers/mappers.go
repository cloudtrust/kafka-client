package mappers

import (
	"context"
	"encoding/base64"
)

func DecodeBase64Bytes(ctx context.Context, in interface{}) (interface{}, error) {
	var bytes = in.([]byte)
	return base64.StdEncoding.DecodeString(string(bytes))
}

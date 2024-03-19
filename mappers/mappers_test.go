package mappers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecodeBase64Bytes(t *testing.T) {
	var ctx = context.TODO()

	t.Run("Success", func(t *testing.T) {
		var res, err = DecodeBase64Bytes(ctx, []byte("VGVzdCBvZiB0aGUgZnVuY3Rpb24="))
		assert.Nil(t, err)
		assert.Equal(t, "Test of the function", string(res.([]byte)))
	})
	t.Run("Failure", func(t *testing.T) {
		var _, err = DecodeBase64Bytes(ctx, []byte("ey"))
		assert.NotNil(t, err)
	})
}

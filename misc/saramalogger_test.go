package misc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWrite(t *testing.T) {
	var message = "test"
	var loggerFunc = func(ctx context.Context, keyvals ...any) {
		assert.NotNil(t, ctx)
		assert.Equal(t, "msg", keyvals[0])
		assert.Contains(t, keyvals[1], "[Sarama] ")
		assert.Contains(t, keyvals[1], message)
		assert.Equal(t, "tag", keyvals[2])
		assert.Equal(t, "sarama", keyvals[3])
	}
	t.Run("Enabled", func(t *testing.T) {
		var saramaLogger = NewSaramaLogger(loggerFunc, true)
		saramaLogger.Print(message)
	})
	t.Run("Disabled", func(t *testing.T) {
		var saramaLogger = NewSaramaLogger(loggerFunc, false)
		saramaLogger.Print(message)
	})
}

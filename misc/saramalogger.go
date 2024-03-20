package misc

import (
	"context"
	"io"
	"log"

	"github.com/IBM/sarama"
)

// InfoLogger is a basic logger function
type InfoLogger func(ctx context.Context, keyvals ...interface{})

func NewSaramaLogger(logger InfoLogger, enabled bool) sarama.StdLogger {
	if enabled {
		return log.New(&loggerWrapper{logger: logger}, "[Sarama] ", log.LstdFlags)
	}
	return log.New(io.Discard, "[Sarama] ", log.LstdFlags)
}

type loggerWrapper struct {
	logger func(ctx context.Context, keyvals ...interface{})
}

func (c *loggerWrapper) Write(p []byte) (n int, err error) {
	c.logger(context.Background(), "msg", string(p), "tag", "sarama")
	return len(p), nil
}

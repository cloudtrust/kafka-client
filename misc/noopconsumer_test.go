package misc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNoopKafkaConsumerGroup(t *testing.T) {
	var consumer = &NoopKafkaConsumerGroup{}

	t.Run("Consume", func(t *testing.T) {
		assert.Nil(t, consumer.Consume(context.TODO(), nil, nil))
	})
	t.Run("Errors", func(t *testing.T) {
		assert.NotNil(t, consumer.Errors())
	})
	t.Run("Close", func(t *testing.T) {
		assert.Nil(t, consumer.Close())
	})
	t.Run("Pause", func(t *testing.T) {
		assert.NotPanics(t, func() { consumer.Pause(nil) })
	})
	t.Run("Resume", func(t *testing.T) {
		assert.NotPanics(t, func() { consumer.Resume(nil) })
	})
	t.Run("PauseAll", func(t *testing.T) {
		assert.NotPanics(t, func() { consumer.PauseAll() })
	})
	t.Run("ResumeAll", func(t *testing.T) {
		assert.NotPanics(t, func() { consumer.ResumeAll() })
	})
}

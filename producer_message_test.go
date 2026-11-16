package kafkauniverse

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
)

func TestProducerMessage(t *testing.T) {
	content := []byte("test content")
	topic := "test-topic"
	partKey := "test-partition-key"

	producerMessage := NewProducerMessage(content, &partKey, nil).(*producerMessage)
	producerMessage.saramaMessage.Topic = topic
	producerMessage.AddCustomField("key", "value")

	assert.Equal(t, topic, producerMessage.GetTopic())
	assert.Equal(t, sarama.StringEncoder(content), producerMessage.GetContent())
	assert.Equal(t, partKey, producerMessage.GetPartitionKey())

	t.Run("ExportCustomFields", func(t *testing.T) {
		customFields := producerMessage.ExportCustomFields()
		assert.Len(t, customFields, 2)
		assert.Equal(t, []any{"key", "value"}, customFields)
	})

	t.Run("ProducerMessage without partition key", func(t *testing.T) {
		assert.Equal(t, "", NewProducerMessage([]byte(""), nil, nil).GetPartitionKey())
	})
}

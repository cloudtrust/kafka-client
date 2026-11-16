package kafkauniverse

import (
	"maps"

	"github.com/IBM/sarama"
)

// ProducerMessage represents a message to be sent to Kafka, encapsulating the content, partition key, topic, and custom fields.
type ProducerMessage interface {
	GetSaramaMessage() any
	GetContent() any
	GetPartitionKey() string
	GetTopic() string
	AddCustomField(key string, value any)
	ExportCustomFields() []any
}

// producerMessage is the concrete implementation of the ProducerMessage interface.
type producerMessage struct {
	origin        KafkaMessage
	saramaMessage *sarama.ProducerMessage
	custom        customFields
}

// NewProducerMessage creates a new producer message with the given content, optional partition key, and origin Kafka message.
func NewProducerMessage(content []byte, partitionKey *string, origin KafkaMessage) ProducerMessage {
	var key sarama.Encoder
	if partitionKey != nil {
		key = sarama.StringEncoder(*partitionKey)
	}
	var customFields = customFields{}
	if origin != nil {
		maps.Copy(customFields, origin.GetCustomFields())
	}

	return &producerMessage{
		origin:        origin,
		saramaMessage: &sarama.ProducerMessage{Value: sarama.StringEncoder(content), Key: key},
		custom:        customFields,
	}
}

// NewProducerMessageWithMap creates a new producer message with the given content, partition key, and custom fields provided as a map.
func NewProducerMessageWithMap(content []byte, partitionKey string, details map[string]any) ProducerMessage {
	var customFields = customFields{}
	maps.Copy(customFields, details)

	return &producerMessage{
		origin:        nil,
		saramaMessage: &sarama.ProducerMessage{Value: sarama.StringEncoder(content), Key: sarama.StringEncoder(partitionKey)},
		custom:        customFields,
	}
}

// GetSaramaMessage returns the underlying sarama.ProducerMessage of the producer message.
func (p *producerMessage) GetSaramaMessage() any {
	return p.saramaMessage
}

// GetContent returns the content of the producer message.
func (p *producerMessage) GetContent() any {
	return p.saramaMessage.Value
}

// GetPartition returns the partition of the producer message.
func (p *producerMessage) GetPartitionKey() string {
	if p.saramaMessage.Key == nil {
		return ""
	}
	str := p.saramaMessage.Key.(sarama.StringEncoder)
	return string(str)
}

// GetTopic returns the topic of the producer message.
func (p *producerMessage) GetTopic() string {
	return p.saramaMessage.Topic
}

// AddCustomField adds a custom field to the producer message. This can be used to attach additional metadata to the message.
func (p *producerMessage) AddCustomField(key string, value any) {
	p.custom[key] = value
}

// ExportCustomFields returns all custom fields attached to the producer message as a slice of key-value pairs.
func (p *producerMessage) ExportCustomFields() []any {
	return p.custom.export()
}

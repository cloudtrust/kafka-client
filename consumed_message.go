package kafkauniverse

import (
	"fmt"

	"github.com/IBM/sarama"
)

// KafkaMessage interface
type KafkaMessage interface {
	GetContent() any
	GetOffset() int64
	GetPartition() int32
	GetPartitionKey() string
	GetTopic() string
	Commit()
	CommitWithMessage(message string)
	SendToFailureTopic() error
	AbortConsuming()
	AddCustomField(key string, value any)
	GetCustomFields() map[string]any
	ExportCustomFields() []any
}

type consumedMessage struct {
	msg      *sarama.ConsumerMessage
	content  any
	consumer *consumer
	session  sarama.ConsumerGroupSession
	abort    bool
	custom   customFields
}

func newConsumedMessage(msg *sarama.ConsumerMessage, content any, consumer *consumer, session sarama.ConsumerGroupSession) *consumedMessage {
	return &consumedMessage{
		msg:      msg,
		content:  content,
		consumer: consumer,
		session:  session,
		abort:    false,
		custom:   customFields{},
	}
}

// GetContent returns the content of the consumed message. Mappers have already been applied to the original received content.
func (cm *consumedMessage) GetContent() any {
	return cm.content
}

// GetOffset gets the offset
func (cm *consumedMessage) GetOffset() int64 {
	return cm.msg.Offset
}

// GetPartition gets the partition
func (cm *consumedMessage) GetPartition() int32 {
	return cm.msg.Partition
}

// GetPartitionKey returns the partition key
func (cm *consumedMessage) GetPartitionKey() string {
	return string(cm.msg.Key)
}

// GetTopic gets the topic
func (cm *consumedMessage) GetTopic() string {
	return cm.msg.Topic
}

// Commit confirms that the consumed message has been processed
func (cm *consumedMessage) Commit() {
	cm.CommitWithMessage("")
}

// CommitWithMessage confirms that the consumed message has been processed
func (cm *consumedMessage) CommitWithMessage(message string) {
	cm.session.MarkMessage(cm.msg, message)
}

// SendToFailureTopic sends the consumed message to the failure topic if it is configured. This call will be
func (cm *consumedMessage) SendToFailureTopic() error {
	if cm.consumer.failureProducerName == nil {
		// No automatic failure mechanism configured
		return nil
	}
	if cm.consumer.failureProducer == nil {
		return fmt.Errorf("failed to send message to uninitialized producer %s", *cm.consumer.failureProducerName)
	}
	return cm.consumer.failureProducer.SendMessageBytes(cm.msg.Value)
}

// AbortConsuming let the consuming main process stops. The abort command will be taken into account only if the message handler returns an error
func (cm *consumedMessage) AbortConsuming() {
	cm.abort = true
}

// AddCustomField adds a custom field to the consumed message. This can be used to attach additional metadata to the message.
func (cm *consumedMessage) AddCustomField(key string, value any) {
	cm.custom[key] = value
}

// ExportCustomFields returns all custom fields attached to the consumed message as a slice of key-value pairs.
func (cm *consumedMessage) ExportCustomFields() []any {
	return cm.custom.export()
}

// GetCustomFields returns all custom fields attached to the consumed message.
func (cm *consumedMessage) GetCustomFields() map[string]any {
	return cm.custom
}

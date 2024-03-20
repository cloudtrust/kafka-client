package misc

import (
	"context"

	"github.com/IBM/sarama"
)

// NoopKafkaConsumerGroup is an consumer group that does nothing.
type NoopKafkaConsumerGroup struct{}

// Consume does noop
func (n *NoopKafkaConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	return nil
}

// Errors creates a channel for errors
func (n *NoopKafkaConsumerGroup) Errors() <-chan error {
	return make(<-chan error)
}

// Close does noop
func (n *NoopKafkaConsumerGroup) Close() error {
	return nil
}

// Pause does noop
func (n *NoopKafkaConsumerGroup) Pause(partitions map[string][]int32) {}

// Resume does noop
func (n *NoopKafkaConsumerGroup) Resume(partitions map[string][]int32) {}

// PauseAll does noop
func (n *NoopKafkaConsumerGroup) PauseAll() {}

// ResumeAll does noop
func (n *NoopKafkaConsumerGroup) ResumeAll() {}

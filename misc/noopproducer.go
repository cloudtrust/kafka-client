package misc

import (
	"github.com/IBM/sarama"
)

// NoopKafkaProducer struct
type NoopKafkaProducer struct{}

// SendMessage does noop
func (n *NoopKafkaProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	return 0, 0, nil
}

// SendMessages does noop
func (n *NoopKafkaProducer) SendMessages(msgs []*sarama.ProducerMessage) error { return nil }

// Close does noop
func (n *NoopKafkaProducer) Close() error { return nil }

// TxnStatus does noop
func (n *NoopKafkaProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	return 0
}

// IsTransactional does noop
func (n *NoopKafkaProducer) IsTransactional() bool { return true }

// BeginTxn does noop
func (n *NoopKafkaProducer) BeginTxn() error { return nil }

// CommitTxn does noop
func (n *NoopKafkaProducer) CommitTxn() error { return nil }

// AbortTxn does noop
func (n *NoopKafkaProducer) AbortTxn() error { return nil }

// AddOffsetsToTxn does noop
func (n *NoopKafkaProducer) AddOffsetsToTxn(offsets map[string][]*sarama.PartitionOffsetMetadata, groupID string) error {
	return nil
}

// AddMessageToTxn does noop
func (n *NoopKafkaProducer) AddMessageToTxn(msg *sarama.ConsumerMessage, groupID string, metadata *string) error {
	return nil
}

package misc

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
)

func TestNoopKafkaProducer(t *testing.T) {
	var (
		topic        = "test"
		noopProducer = NoopKafkaProducer{}
		msg          = sarama.ProducerMessage{Topic: topic}
	)

	t.Run("SendMessage", func(t *testing.T) {
		partition, offset, err := noopProducer.SendMessage(&msg)

		assert.Nil(t, err)
		assert.Zero(t, partition)
		assert.Zero(t, offset)
	})
	t.Run("SendMessages", func(t *testing.T) {
		err := noopProducer.SendMessages([]*sarama.ProducerMessage{&msg})
		assert.Nil(t, err)
	})
	t.Run("Close", func(t *testing.T) {
		err := noopProducer.Close()
		assert.Nil(t, err)
	})
	t.Run("TxnStatus", func(t *testing.T) {
		txnStatus := noopProducer.TxnStatus()
		assert.Equal(t, sarama.ProducerTxnStatusFlag(0), txnStatus)
	})
	t.Run("IsTransactional", func(t *testing.T) {
		b := noopProducer.IsTransactional()
		assert.True(t, b)
	})
	t.Run("BeginTxn", func(t *testing.T) {
		err := noopProducer.BeginTxn()
		assert.Nil(t, err)
	})
	t.Run("CommitTxn", func(t *testing.T) {
		err := noopProducer.CommitTxn()
		assert.Nil(t, err)
	})
	t.Run("AbortTxn", func(t *testing.T) {
		err := noopProducer.AbortTxn()
		assert.Nil(t, err)
	})
	t.Run("AddOffsetsToTxn", func(t *testing.T) {
		err := noopProducer.AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata{}, "")
		assert.Nil(t, err)
	})
	t.Run("AddMessageToTxn", func(t *testing.T) {
		err := noopProducer.AddMessageToTxn(nil, "", nil)
		assert.Nil(t, err)
	})
}

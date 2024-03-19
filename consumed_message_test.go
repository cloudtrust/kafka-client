package kafkauniverse

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/cloudtrust/kafka-client/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestConsumedMessage(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var mockConsumerGroupSession = mock.NewConsumerGroupSession(mockCtrl)

	var content = []byte("content of the consumed message modified by mappers")
	var offset = int64(456789)

	var km = &consumedMessage{
		msg: &sarama.ConsumerMessage{
			Offset: offset,
		},
		consumer: &consumer{},
		content:  content,
		session:  mockConsumerGroupSession,
	}
	t.Run("GetContent", func(t *testing.T) {
		assert.Equal(t, content, km.GetContent())
	})
	t.Run("GetOffset", func(t *testing.T) {
		assert.Equal(t, offset, km.GetOffset())
	})
	t.Run("Commit", func(t *testing.T) {
		mockConsumerGroupSession.EXPECT().MarkMessage(km.msg, "")
		km.Commit()
	})
	t.Run("Send to unconfigured failure topic", func(t *testing.T) {
		km.consumer.failureProducerName = nil
		assert.Nil(t, km.SendToFailureTopic())
	})
	t.Run("Send to uninitialized failure topic", func(t *testing.T) {
		km.consumer.failureProducerName = ptrString("failure-topic")
		km.consumer.failureProducer = nil
		assert.Contains(t, km.SendToFailureTopic().Error(), "uninitialized producer")
	})
	t.Run("Abort", func(t *testing.T) {
		assert.False(t, km.abort)
		km.AbortConsuming()
		assert.True(t, km.abort)
	})
}

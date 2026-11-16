package kafkauniverse

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
)

func TestReportError(t *testing.T) {
	var dummyError = errors.New("dummy error")
	errorManager := &errorManager{}
	producerMessage := NewProducerMessage([]byte("dummy content"), nil, nil)
	consMessage := &consumedMessage{
		msg: &sarama.ConsumerMessage{
			Topic:     "dummy-topic",
			Partition: 10,
		},
	}
	count := 0
	errorManager.addErrorHandler(func(ctx context.Context, kafkaStatus KafkaStatus) {
		count++
	})
	errorManager.addErrorLogger(func(ctx context.Context, keyvals ...any) {
		count += 2
	})
	assert.Equal(t, 0, count)
	errorManager.reportSendMessageError(context.TODO(), producerMessage, dummyError)
	assert.Equal(t, 3, count)
	errorManager.reportMapperError(context.TODO(), consMessage, dummyError)
	assert.Equal(t, 6, count)
	errorManager.reportMessageHandlerError(context.TODO(), consMessage, dummyError)
	assert.Equal(t, 9, count)
}

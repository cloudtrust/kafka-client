package kafkauniverse

import (
	"testing"

	"github.com/cloudtrust/kafka-client/mock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestClose(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	t.Run("Not initialized", func(t *testing.T) {
		var producer = &producer{
			initialized: false,
		}
		assert.Nil(t, producer.Close())
	})
	t.Run("Not enabled", func(t *testing.T) {
		var producer = &producer{
			initialized: true,
			enabled:     false,
		}
		assert.Nil(t, producer.Close())
	})
	t.Run("Success", func(t *testing.T) {
		var mockProducer = mock.NewSyncProducer(mockCtrl)
		var producer = &producer{
			initialized: true,
			enabled:     true,
			producer:    mockProducer,
		}
		mockProducer.EXPECT().Close().Return(nil)
		assert.Nil(t, producer.Close())
	})
}

func TestInitialize(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var logger = mock.NewLogger(mockCtrl)
	var producer, _ = newProducer(&cluster{enabled: true}, KafkaProducerRepresentation{
		ID: ptrString("producer1"),
	}, logger)

	logger.EXPECT().Error(gomock.Any(), gomock.Any())

	t.Run("Already initialized", func(t *testing.T) {
		producer.initialized = true
		assert.NotNil(t, producer.initialize())
	})
	t.Run("Disabled", func(t *testing.T) {
		producer.initialized = false
		producer.enabled = *ptrBool(false)
		assert.Nil(t, producer.initialize())
		assert.Nil(t, producer.SendMessageBytes([]byte("test")))
	})
	t.Run("Sarama not configured", func(t *testing.T) {
		producer.initialized = false
		producer.enabled = *ptrBool(true)
		assert.NotNil(t, producer.initialize())
	})
}

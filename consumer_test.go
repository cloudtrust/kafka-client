package kafkauniverse

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/IBM/sarama"
	"github.com/cloudtrust/kafka-client/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

type contextKey int

const (
	ctxKey1 contextKey = iota
	ctxKey2 contextKey = iota
)

func createDefaultConsumerConfiguration() KafkaConsumerRepresentation {
	return KafkaConsumerRepresentation{
		ID:                ptrString("id-consumer"),
		Enabled:           ptrBool(true),
		Topic:             ptrString("topic"),
		ConsumerGroupName: ptrString("consumer-group"),
		FailureProducer:   ptrString("failure-producer"),
	}
}

func TestConsumerSimpleFunctions(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var logger = mock.NewLogger(mockCtrl)
	var cluster = &cluster{
		logger: logger,
	}
	var consumerConf = createDefaultConsumerConfiguration()
	var consumer, _ = newConsumer(cluster, consumerConf, logger)

	var mockConsumerGroup = mock.NewConsumerGroup(mockCtrl)
	var mockConsumerGroupSession = mock.NewConsumerGroupSession(mockCtrl)
	var anError = errors.New("an error")

	logger.EXPECT().Warn(gomock.Any(), gomock.Any()).AnyTimes()

	t.Run("Default context initializer", func(t *testing.T) {
		var ctx = context.TODO()
		assert.Equal(t, ctx, consumer.contextInit(ctx))
	})
	t.Run("Default handler", func(t *testing.T) {
		assert.NotNil(t, consumer.handler(context.TODO(), nil))
	})
	t.Run("Close", func(t *testing.T) {
		t.Run("Not initialized", func(t *testing.T) {
			var err = consumer.Close()
			assert.Nil(t, err)
		})
		t.Run("Close fails", func(t *testing.T) {
			consumer.initialized = true
			consumer.enabled = true
			consumer.consumerGroup = mockConsumerGroup
			mockConsumerGroup.EXPECT().Close().Return(anError)
			var err = consumer.Close()
			assert.Equal(t, anError, err)
		})
		t.Run("SetLogEventRate", func(t *testing.T) {
			consumer.SetLogEventRate(100)
			assert.Equal(t, int64(100), consumer.logEventRate)
			consumer.SetLogEventRate(0) // Invalid values are ignored
			assert.Equal(t, int64(100), consumer.logEventRate)
		})
	})
	t.Run("Initialize", func(t *testing.T) {
		t.Run("Already initialized", func(t *testing.T) {
			var consumer, _ = newConsumer(cluster, consumerConf, logger)
			consumer.initialized = true
			var err = consumer.initialize()
			assert.NotNil(t, err)
		})
		t.Run("Consumer disabled", func(t *testing.T) {
			var consumer, _ = newConsumer(cluster, consumerConf, logger)
			consumer.enabled = false
			var err = consumer.initialize()
			assert.Nil(t, err)
		})
		t.Run("Enabled by default but incorrectly configured", func(t *testing.T) {
			cluster.enabled = true
			var consumer, _ = newConsumer(cluster, consumerConf, logger)
			var err = consumer.initialize()
			assert.NotNil(t, err)
		})
		t.Run("Success", func(t *testing.T) {
			cluster.enabled = false
			var consumer, _ = newConsumer(cluster, consumerConf, logger)
			var err = consumer.initialize()
			assert.Nil(t, err)
		})
	})
	t.Run("Setup", func(t *testing.T) {
		var consumer, _ = newConsumer(cluster, consumerConf, logger)
		assert.Nil(t, consumer.Setup(mockConsumerGroupSession))
	})
	t.Run("Cleanup", func(t *testing.T) {
		var consumer, _ = newConsumer(cluster, consumerConf, logger)
		assert.Nil(t, consumer.Cleanup(mockConsumerGroupSession))
	})
}

func fillMessageChannel(messages chan *sarama.ConsumerMessage, values ...string) {
	go func() {
		for _, value := range values {
			messages <- &sarama.ConsumerMessage{
				Value: []byte(value),
			}
		}
		close(messages)
	}()
}

func TestConsumeClaim(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var logger = mock.NewLogger(mockCtrl)
	var cluster = &cluster{
		logger: logger,
	}
	var consumerConf = createDefaultConsumerConfiguration()
	var consumer, _ = newConsumer(cluster, consumerConf, logger)

	var mockConsumerGroupSession = mock.NewConsumerGroupSession(mockCtrl)
	var mockConsumerGroupClaim = mock.NewConsumerGroupClaim(mockCtrl)
	var handlerError = errors.New("error from handler")

	logger.EXPECT().Error(gomock.Any(), gomock.Any()).AnyTimes()
	logger.EXPECT().Info(gomock.Any(), gomock.Any()).AnyTimes()

	t.Run("Empty messages", func(t *testing.T) {
		var messages = make(chan *sarama.ConsumerMessage)
		fillMessageChannel(messages)
		mockConsumerGroupClaim.EXPECT().Messages().Return(messages)
		var err = consumer.ConsumeClaim(mockConsumerGroupSession, mockConsumerGroupClaim)
		assert.Nil(t, err)
	})
	t.Run("SetContextInitializer", func(t *testing.T) {
		var messages = make(chan *sarama.ConsumerMessage)
		fillMessageChannel(messages, "345")
		var ctxValue = "abc"
		consumer.SetContextInitializer(func(ctx context.Context) context.Context { return context.WithValue(ctx, ctxKey1, ctxValue) })
		consumer.SetHandler(func(ctx context.Context, msg KafkaMessage) error {
			assert.Equal(t, ctxValue, ctx.Value(ctxKey1))
			msg.AbortConsuming()
			return handlerError
		})

		mockConsumerGroupClaim.EXPECT().Messages().Return(messages)
		mockConsumerGroupClaim.EXPECT().Topic().Return("topic")

		var err = consumer.ConsumeClaim(mockConsumerGroupSession, mockConsumerGroupClaim)
		assert.Equal(t, handlerError, err)
	})
	t.Run("AddContentMapper-Disable autocommit", func(t *testing.T) {
		var messages = make(chan *sarama.ConsumerMessage)
		var content = 345
		fillMessageChannel(messages, strconv.Itoa(content), "invalid")
		var handlerError = errors.New("error from handler")
		consumer.AddContentMapper(func(ctx context.Context, in any) (any, error) {
			return strconv.Atoi(string(in.([]byte)))
		})
		consumer.SetHandler(func(ctx context.Context, msg KafkaMessage) error {
			assert.Equal(t, content, msg.GetContent().(int))
			return handlerError
		})
		consumer.SetAutoCommit(true)

		mockConsumerGroupClaim.EXPECT().Messages().Return(messages).AnyTimes()
		mockConsumerGroupClaim.EXPECT().Topic().Return("topic").AnyTimes()
		mockConsumerGroupSession.EXPECT().MarkMessage(gomock.Any(), "").Times(2)

		var err = consumer.ConsumeClaim(mockConsumerGroupSession, mockConsumerGroupClaim)
		assert.Nil(t, err)
	})
}

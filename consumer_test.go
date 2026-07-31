package kafkauniverse

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/cloudtrust/kafka-client/mock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

type contextKey int

const (
	ctxKey1 contextKey = iota
	ctxKey2 contextKey = iota
)

func createSaramaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	return config
}

func createDefaultConsumerConfiguration() KafkaConsumerRepresentation {
	return KafkaConsumerRepresentation{
		ID:                new("id-consumer"),
		Enabled:           new(true),
		Topic:             new("topic"),
		ConsumerGroupName: new("consumer-group"),
		FailureProducer:   new("failure-producer"),
		InitialOffset:     nil,
	}
}

func createOffsetNewestConsumerConfiguration() KafkaConsumerRepresentation {
	return KafkaConsumerRepresentation{
		ID:                new("id-consumer"),
		Enabled:           new(true),
		Topic:             new("topic"),
		ConsumerGroupName: new("consumer-group"),
		FailureProducer:   new("failure-producer"),
		InitialOffset:     new("newest"),
	}
}

func TestConsumerSimpleFunctions(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var logger = mock.NewLogger(mockCtrl)
	var cluster = &cluster{
		logger:       logger,
		saramaConfig: createSaramaConfig(),
	}
	var consumerConf = createDefaultConsumerConfiguration()
	var consumer = newConsumer(cluster, consumerConf, logger)

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
			var consumer = newConsumer(cluster, consumerConf, logger)
			consumer.initialized = true
			var err = consumer.initialize()
			assert.NotNil(t, err)
		})
		t.Run("Consumer disabled", func(t *testing.T) {
			var consumer = newConsumer(cluster, consumerConf, logger)
			consumer.enabled = false
			var err = consumer.initialize()
			assert.Nil(t, err)
		})
		t.Run("Enabled by default but incorrectly configured", func(t *testing.T) {
			cluster.enabled = true
			var consumer = newConsumer(cluster, consumerConf, logger)
			var err = consumer.initialize()
			assert.NotNil(t, err)
		})
		t.Run("Success", func(t *testing.T) {
			cluster.enabled = false
			var consumer = newConsumer(cluster, consumerConf, logger)
			var err = consumer.initialize()
			assert.Nil(t, err)
			assert.Equal(t, sarama.OffsetOldest, consumer.initialOffset)
		})
	})
	t.Run("Setup", func(t *testing.T) {
		var consumer = newConsumer(cluster, consumerConf, logger)
		assert.Nil(t, consumer.Setup(mockConsumerGroupSession))
	})
	t.Run("Cleanup", func(t *testing.T) {
		var consumer = newConsumer(cluster, consumerConf, logger)
		assert.Nil(t, consumer.Cleanup(mockConsumerGroupSession))
	})
}

func TestOffsetNewestConsumerSimpleFunctions(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var logger = mock.NewLogger(mockCtrl)
	var cluster = &cluster{
		logger:       logger,
		saramaConfig: createSaramaConfig(),
	}
	var consumerConf = createOffsetNewestConsumerConfiguration()
	var consumer = newConsumer(cluster, consumerConf, logger)

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
			var consumer = newConsumer(cluster, consumerConf, logger)
			consumer.initialized = true
			var err = consumer.initialize()
			assert.NotNil(t, err)
		})
		t.Run("Consumer disabled", func(t *testing.T) {
			var consumer = newConsumer(cluster, consumerConf, logger)
			consumer.enabled = false
			var err = consumer.initialize()
			assert.Nil(t, err)
		})
		t.Run("Enabled by default but incorrectly configured", func(t *testing.T) {
			cluster.enabled = true
			var consumer = newConsumer(cluster, consumerConf, logger)
			var err = consumer.initialize()
			assert.NotNil(t, err)
		})
		t.Run("Success", func(t *testing.T) {
			cluster.enabled = false
			var consumer = newConsumer(cluster, consumerConf, logger)
			var err = consumer.initialize()
			assert.Nil(t, err)
			assert.Equal(t, sarama.OffsetNewest, consumer.initialOffset)
		})
	})
	t.Run("Setup", func(t *testing.T) {
		var consumer = newConsumer(cluster, consumerConf, logger)
		assert.Nil(t, consumer.Setup(mockConsumerGroupSession))
	})
	t.Run("Cleanup", func(t *testing.T) {
		var consumer = newConsumer(cluster, consumerConf, logger)
		assert.Nil(t, consumer.Cleanup(mockConsumerGroupSession))
	})
}

func TestConsumerGo(t *testing.T) {
	t.Run("Not initialized", func(t *testing.T) {
		var mockCtrl = gomock.NewController(t)
		defer mockCtrl.Finish()

		var logger = mock.NewLogger(mockCtrl)
		var cluster = &cluster{
			logger:       logger,
			saramaConfig: createSaramaConfig(),
		}
		var consumerConf = createDefaultConsumerConfiguration()
		var consumer = newConsumer(cluster, consumerConf, logger)

		consumer.enabled = true
		consumer.initialized = false
		consumer.Go()

		assert.Nil(t, consumer.cancel)
	})

	t.Run("Disabled", func(t *testing.T) {
		var mockCtrl = gomock.NewController(t)
		defer mockCtrl.Finish()

		var logger = mock.NewLogger(mockCtrl)
		var cluster = &cluster{
			logger:       logger,
			saramaConfig: createSaramaConfig(),
		}
		var consumerConf = createDefaultConsumerConfiguration()
		var consumer = newConsumer(cluster, consumerConf, logger)

		consumer.initialized = true
		consumer.enabled = false
		consumer.Go()

		assert.Nil(t, consumer.cancel)
	})

	t.Run("Initialized and enabled", func(t *testing.T) {
		var mockCtrl = gomock.NewController(t)
		defer mockCtrl.Finish()

		var logger = mock.NewLogger(mockCtrl)
		var cluster = &cluster{
			logger:       logger,
			saramaConfig: createSaramaConfig(),
		}
		var consumerConf = createDefaultConsumerConfiguration()
		var consumer = newConsumer(cluster, consumerConf, logger)
		var mockConsumerGroup = mock.NewConsumerGroup(mockCtrl)

		consumer.initialized = true
		consumer.enabled = true
		consumer.consumerGroup = mockConsumerGroup

		var consumeCalled = make(chan struct{}, 1)
		var errorsChannel = make(chan error)
		close(errorsChannel)

		logger.EXPECT().Info(gomock.Any(), gomock.Any()).AnyTimes()
		logger.EXPECT().Error(gomock.Any(), gomock.Any()).AnyTimes()

		mockConsumerGroup.EXPECT().Errors().Return(errorsChannel).AnyTimes()
		mockConsumerGroup.EXPECT().Consume(gomock.Any(), []string{consumer.topic}, consumer).DoAndReturn(
			func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
				select {
				case consumeCalled <- struct{}{}:
				default:
				}
				<-ctx.Done()
				return nil
			},
		).Times(1)
		mockConsumerGroup.EXPECT().Close().Return(nil)

		consumer.Go()
		assert.NotNil(t, consumer.cancel)

		select {
		case <-consumeCalled:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected consumer group Consume to be called")
		}

		assert.True(t, consumer.IsLive())
		assert.Nil(t, consumer.Close())
	})

	t.Run("Consumer group errors are logged", func(t *testing.T) {
		var mockCtrl = gomock.NewController(t)
		defer mockCtrl.Finish()

		var logger = mock.NewLogger(mockCtrl)
		var cluster = &cluster{
			logger:       logger,
			saramaConfig: createSaramaConfig(),
		}
		var consumerConf = createDefaultConsumerConfiguration()
		var consumer = newConsumer(cluster, consumerConf, logger)
		var mockConsumerGroup = mock.NewConsumerGroup(mockCtrl)

		consumer.initialized = true
		consumer.enabled = true
		consumer.consumerGroup = mockConsumerGroup

		var errorsChannel = make(chan error, 1)
		var errorLogged = make(chan struct{}, 1)

		logger.EXPECT().Info(gomock.Any(), gomock.Any()).AnyTimes()
		logger.EXPECT().Error(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, keyvals ...any) {
			select {
			case errorLogged <- struct{}{}:
			default:
			}
		}).Times(1)

		mockConsumerGroup.EXPECT().Errors().Return(errorsChannel).AnyTimes()
		mockConsumerGroup.EXPECT().Consume(gomock.Any(), []string{consumer.topic}, consumer).DoAndReturn(
			func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
				<-ctx.Done()
				return nil
			},
		).Times(1)
		mockConsumerGroup.EXPECT().Close().Return(nil)

		consumer.Go()
		errorsChannel <- errors.New("error from consumer group")

		select {
		case <-errorLogged:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected consumer group error to be logged")
		}

		close(errorsChannel)
		assert.Nil(t, consumer.Close())
	})

	t.Run("Consume error triggers reinitialize failure log", func(t *testing.T) {
		var mockCtrl = gomock.NewController(t)
		defer mockCtrl.Finish()

		var logger = mock.NewLogger(mockCtrl)
		var cluster = &cluster{
			enabled:      true,
			logger:       logger,
			saramaConfig: createSaramaConfig(),
		}
		var consumerConf = createDefaultConsumerConfiguration()
		var consumer = newConsumer(cluster, consumerConf, logger)
		var mockConsumerGroup = mock.NewConsumerGroup(mockCtrl)

		consumer.initialized = true
		consumer.enabled = true
		consumer.consumerGroup = mockConsumerGroup

		var consumeErr = errors.New("consume failure")
		var consumeCalled = make(chan struct{}, 1)
		var errorLogged = make(chan struct{}, 2)
		var errorsChannel = make(chan error)
		close(errorsChannel)
		logger.EXPECT().Info(gomock.Any(), gomock.Any()).AnyTimes()
		logger.EXPECT().Warn(gomock.Any(), gomock.Any()).AnyTimes()
		logger.EXPECT().Error(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, keyvals ...any) {
			select {
			case errorLogged <- struct{}{}:
			default:
			}
		}).Times(2)

		mockConsumerGroup.EXPECT().Errors().Return(errorsChannel).AnyTimes()
		mockConsumerGroup.EXPECT().Consume(gomock.Any(), []string{consumer.topic}, consumer).DoAndReturn(
			func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
				select {
				case consumeCalled <- struct{}{}:
				default:
				}
				return consumeErr
			},
		).Times(1)
		mockConsumerGroup.EXPECT().Close().Return(nil)

		consumer.Go()

		select {
		case <-consumeCalled:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected consumer group Consume to be called")
		}

		select {
		case <-errorLogged:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected consume error to be logged")
		}

		select {
		case <-errorLogged:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected reinitialize failure to be logged")
		}

		consumer.initialized = true
		consumer.consumerGroup = mockConsumerGroup
		assert.Nil(t, consumer.Close())
	})
}

func fillMessageChannel(messages chan *sarama.ConsumerMessage, values ...string) {
	go func() {
		for _, value := range values {
			messages <- &sarama.ConsumerMessage{
				Timestamp: time.Now(),
				Value:     []byte(value),
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
		logger:       logger,
		saramaConfig: createSaramaConfig(),
	}
	var consumerConf = createDefaultConsumerConfiguration()
	var consumer = newConsumer(cluster, consumerConf, logger)

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
		assert.NotNil(t, err)
		assert.True(t, errors.Is(err, handlerError))
	})
	t.Run("AddContentMapper-Disable autocommit", func(t *testing.T) {
		var messages = make(chan *sarama.ConsumerMessage)
		var content = 345
		fillMessageChannel(messages, strconv.Itoa(content), "invalid")
		var handlerError = errors.New("error from handler")
		consumer.AddContentMapper(func(ctx context.Context, messageOffset int64, in any) (any, error) {
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

func TestConsumeClaimWithDelay(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var mockConsumerGroupSession = mock.NewConsumerGroupSession(mockCtrl)
	var mockConsumerGroupClaim = mock.NewConsumerGroupClaim(mockCtrl)

	var logger = mock.NewLogger(mockCtrl)
	logger.EXPECT().Error(gomock.Any(), gomock.Any()).AnyTimes()
	logger.EXPECT().Info(gomock.Any(), gomock.Any()).AnyTimes()
	var cluster = &cluster{
		logger:       logger,
		saramaConfig: createSaramaConfig(),
	}

	delay, _ := time.ParseDuration("1ms")
	var consumerConf = createDefaultConsumerConfiguration()
	consumerConf.ConsumptionDelay = &delay
	var consumer = newConsumer(cluster, consumerConf, logger)
	consumer.SetHandler(func(ctx context.Context, msg KafkaMessage) error {
		return nil
	})

	var messages = make(chan *sarama.ConsumerMessage)
	fillMessageChannel(messages, "message")
	mockConsumerGroupClaim.EXPECT().Messages().Return(messages)
	mockConsumerGroupClaim.EXPECT().Topic().Return("topic")
	mockConsumerGroupSession.EXPECT().MarkMessage(gomock.Any(), "")
	var err = consumer.ConsumeClaim(mockConsumerGroupSession, mockConsumerGroupClaim)
	assert.Nil(t, err)
}

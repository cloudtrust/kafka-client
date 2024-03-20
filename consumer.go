package kafkauniverse

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/IBM/sarama"
	"github.com/cloudtrust/kafka-client/misc"
)

// KafkaMessageHandler interface shall be implemented by clients
type KafkaMessageHandler func(context.Context, KafkaMessage) error

// KafkaMessageMapper function type
type KafkaMessageMapper func(context.Context, any) (any, error)

// KafkaContextInitializer function type
type KafkaContextInitializer func(context.Context) context.Context

type consumer struct {
	initialized         bool
	cluster             *cluster
	id                  string
	enabled             bool
	topic               string
	consumerGroupName   string
	failureProducerName *string
	failureProducer     *producer
	consumerGroup       sarama.ConsumerGroup
	mappers             []KafkaMessageMapper
	autoCommit          bool
	handler             KafkaMessageHandler
	contextInit         KafkaContextInitializer
	logger              Logger
	logEventRate        int64
}

func newConsumer(cluster *cluster, consumerRep KafkaConsumerRepresentation, logger Logger) (*consumer, error) {
	var enabled = true
	if !cluster.enabled || (consumerRep.Enabled != nil && !*consumerRep.Enabled) {
		enabled = false
	}
	return &consumer{
		initialized:         false,
		cluster:             cluster,
		id:                  *consumerRep.ID,
		enabled:             enabled,
		topic:               *consumerRep.Topic,
		consumerGroupName:   *consumerRep.ConsumerGroupName,
		failureProducerName: consumerRep.FailureProducer,
		failureProducer:     nil,
		consumerGroup:       nil,
		mappers:             nil,
		autoCommit:          true,
		handler:             func(ctx context.Context, msg KafkaMessage) error { return errors.New("handler not implemented") },
		contextInit:         func(ctx context.Context) context.Context { return ctx },
		logger:              logger,
		logEventRate:        1000,
	}, nil
}

func (c *consumer) Close() error {
	if !c.initialized || !c.enabled {
		return nil
	}
	var anError error
	if err := c.consumerGroup.Close(); err != nil {
		c.logger.Warn(context.Background(), "msg", "Failed to close consumer group", "group", c.consumerGroupName, "err", err)
		anError = err
	}
	return anError
}

func (c *consumer) initialize() error {
	if c.initialized {
		return fmt.Errorf("consumer %s already initialized", c.id)
	}
	// Is consumer enabled?
	if !c.enabled {
		c.consumerGroup = &misc.NoopKafkaConsumerGroup{}
		c.initialized = true
		return nil
	}
	// Consumer group
	var err error
	if c.consumerGroup, err = c.cluster.getConsumerGroup(c.consumerGroupName); err != nil {
		return err
	}
	// Done
	c.initialized = true
	return nil
}

func (c *consumer) SetHandler(handler KafkaMessageHandler) *consumer {
	c.handler = handler
	return c
}

func (c *consumer) SetLogEventRate(rate int64) *consumer {
	if rate > 0 {
		c.logEventRate = rate
	}
	return c
}

func (c *consumer) SetContextInitializer(ctxInitializer KafkaContextInitializer) *consumer {
	c.contextInit = ctxInitializer
	return c
}

func (c *consumer) AddContentMapper(mapper KafkaMessageMapper) *consumer {
	c.mappers = append(c.mappers, mapper)
	return c
}

func (c *consumer) SetAutoCommit(enabled bool) {
	c.autoCommit = enabled
}

func (c *consumer) Go() {
	if c.initialized && c.enabled {
		go func() {
			var failureTopic = "none"
			if c.failureProducerName != nil {
				failureTopic = *c.failureProducerName
			}
			c.logger.Info(context.Background(), "msg", "Just started thread to consume queue", "topic", c.topic, "failure-topic", failureTopic)
			for {
				c.consumerGroup.Consume(context.Background(), []string{c.topic}, c)
				select {
				case err := <-c.consumerGroup.Errors():
					c.logger.Error(context.Background(), "msg", "Failure during message processing. Exit", "err", err, "topic", c.topic)
					os.Exit(1)
				default:
				}
			}
		}()
	}
}

func (c *consumer) applyMappers(ctx context.Context, kafkaMsg *sarama.ConsumerMessage) (any, error) {
	var content any = kafkaMsg.Value
	for idx, mapper := range c.mappers {
		var err error
		if content, err = mapper(ctx, content); err != nil {
			logMsg := fmt.Sprintf("Mapper #%d failed to map content", idx+1)
			c.logger.Error(ctx, "msg", logMsg, "err", err, "topic", c.topic, "offset", kafkaMsg.Offset,
				"partition", kafkaMsg.Partition, "contentLength", len(kafkaMsg.Value))
			return nil, err
		}
	}
	return content, nil
}

func (c *consumer) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

// This function is called in several goroutines ==> needs to be thread safe
func (c *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for kafkaMsg := range claim.Messages() {
		ctx := c.contextInit(context.Background())
		var content, err = c.applyMappers(ctx, kafkaMsg)
		var msg = &consumedMessage{
			msg:      kafkaMsg,
			content:  content,
			consumer: c,
			abort:    false,
		}
		if err != nil {
			msg.SendToFailureTopic()
		} else {
			err = c.handler(ctx, msg)
			if err != nil {
				c.logger.Error(ctx, "msg", "Failed to handle event", "err", err.Error(), "topic", claim.Topic())
				if msg.abort {
					return err
				}
			}
			if kafkaMsg.Offset%c.logEventRate == 0 {
				logMsg := fmt.Sprintf("Messages from %d to %d offset are processed", kafkaMsg.Offset-c.logEventRate, kafkaMsg.Offset)
				c.logger.Info(ctx, "msg", logMsg, "topic", c.topic, "partition", kafkaMsg.Partition, "topic", claim.Topic())
			}
		}

		// Commit event
		if c.autoCommit {
			session.MarkMessage(kafkaMsg, "")
		}
	}

	return nil
}

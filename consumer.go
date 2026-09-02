package kafkauniverse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
	"uuid"

	"github.com/IBM/sarama"
	"github.com/cloudtrust/kafka-client/misc"
)

// KafkaMessageHandler interface shall be implemented by clients
type KafkaMessageHandler func(context.Context, KafkaMessage) error

// KafkaMessageMapper function type
type KafkaMessageMapper func(ctx context.Context, messageOffset int64, in any) (any, error)

// KafkaContextInitializer function type
type KafkaContextInitializer func(context.Context) context.Context

var abortingError = errors.New("aborting message consumption")

type consumer struct {
	initialized         bool
	cluster             *cluster
	id                  string
	enabled             bool
	topic               string
	consumerGroupName   string
	failureProducerName *string
	failureProducer     *producer
	consumptionDelay    *time.Duration
	consumerGroup       sarama.ConsumerGroup
	mappers             []KafkaMessageMapper
	autoCommit          bool
	handler             KafkaMessageHandler
	contextInit         KafkaContextInitializer
	logger              Logger
	logEventRate        int64
	initialOffset       int64
	cancel              context.CancelFunc
	live                bool
}

func newConsumer(cluster *cluster, consumerRep KafkaConsumerRepresentation, logger Logger) *consumer {
	var enabled = true
	if !cluster.enabled || (consumerRep.Enabled != nil && !*consumerRep.Enabled) {
		enabled = false
	}

	groupName := *consumerRep.ConsumerGroupName

	// Replace <UUID> in groupName with a random UUID
	groupName = strings.Replace(groupName, "<UUID>", uuid.New().String(), 1)

	var initialOffset = sarama.OffsetOldest
	if consumerRep.InitialOffset != nil {
		switch *consumerRep.InitialOffset {
		case offsetNewestParam:
			initialOffset = sarama.OffsetNewest
		case offsetOldestParam:
			initialOffset = sarama.OffsetOldest
		}
	}

	return &consumer{
		initialized:         false,
		cluster:             cluster,
		id:                  *consumerRep.ID,
		enabled:             enabled,
		topic:               *consumerRep.Topic,
		consumerGroupName:   groupName,
		failureProducerName: consumerRep.FailureProducer,
		failureProducer:     nil,
		consumptionDelay:    consumerRep.ConsumptionDelay,
		consumerGroup:       nil,
		mappers:             nil,
		autoCommit:          true,
		handler:             func(ctx context.Context, msg KafkaMessage) error { return errors.New("handler not implemented") },
		contextInit:         func(ctx context.Context) context.Context { return ctx },
		logger:              logger,
		logEventRate:        1000,
		initialOffset:       initialOffset,
		live:                true,
	}
}

func (c *consumer) Close() error {
	if !c.initialized || !c.enabled {
		return nil
	}
	if c.cancel != nil {
		c.cancel()
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

	groupConfig := *c.cluster.saramaConfig
	switch c.initialOffset {
	case sarama.OffsetNewest:
		groupConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	case sarama.OffsetOldest:
		groupConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	// Consumer group
	var err error
	if c.consumerGroup, err = c.cluster.getConsumerGroup(c.consumerGroupName, groupConfig); err != nil {
		return err
	}
	// Done
	c.initialized = true
	return nil
}

func (c *consumer) reinitialize() error {
	c.initialized = false
	return c.initialize()
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
		ctx, cancel := context.WithCancel(context.Background())
		c.cancel = cancel

		go func() {
			for err := range c.consumerGroup.Errors() {
				if errors.Is(err, abortingError) {
					c.logger.Info(ctx, "msg", "Aborting message consumption. Exit", "err", err, "topic", c.topic, "consumerGroup", c.consumerGroupName)
					os.Exit(1)
				}
				c.logger.Error(ctx, "msg", "Failure during message processing. Not exiting", "err", err, "topic", c.topic, "consumerGroup", c.consumerGroupName)
			}
		}()

		go func() {
			failureTopic := "none"
			if c.failureProducerName != nil {
				failureTopic = *c.failureProducerName
			}
			c.logger.Info(ctx, "msg", "Just started thread to consume queue", "topic", c.topic, "failure-topic", failureTopic, "consumerGroup", c.consumerGroupName)

			for {
				c.live = true
				if err := c.consumerGroup.Consume(ctx, []string{c.topic}, c); err != nil {
					c.live = false
					// known cases : rollout in kafka cluster, kafka cluster is not available for a while
					c.logger.Error(ctx, "msg", "Consume error", "err", err, "topic", c.topic, "consumerGroup", c.consumerGroupName)
					err := c.reinitialize()
					if err != nil {
						c.logger.Error(ctx, "msg", "Failed to reinitialize consumer", "err", err, "topic", c.topic, "consumerGroup", c.consumerGroupName)
					}
					time.Sleep(10 * time.Second)
				}
				// Consume returns nil after a rebalance; check if context was cancelled
				if ctx.Err() != nil {
					c.logger.Info(ctx, "msg", "Context cancelled.", "err", ctx.Err(), "topic", c.topic, "consumerGroup", c.consumerGroupName)
					return
				}
			}
		}()
	}
}

func (c *consumer) applyMappers(ctx context.Context, kafkaMsg *sarama.ConsumerMessage) (any, error) {
	var content any = kafkaMsg.Value
	for idx, mapper := range c.mappers {
		var err error
		if content, err = mapper(ctx, kafkaMsg.Offset, content); err != nil {
			logMsg := fmt.Sprintf("Mapper #%d failed to map content", idx+1)
			c.logger.Error(ctx, "msg", logMsg, "err", err, "topic", c.topic, "offset", kafkaMsg.Offset,
				"partition", kafkaMsg.Partition, "contentLength", len(kafkaMsg.Value), "consumerGroup", c.consumerGroupName)
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

		if c.consumptionDelay != nil {
			sinceMessageProduction := time.Since(kafkaMsg.Timestamp)
			if sinceMessageProduction < *c.consumptionDelay {
				pauseDuration := *c.consumptionDelay - sinceMessageProduction
				c.logger.Info(ctx, "msg", "pause consumption because of consumption delay", "pauseDuration", pauseDuration, "consumptionDelay", *c.consumptionDelay, "consumerGroupName", c.consumerGroupName)
				time.Sleep(pauseDuration)
			}
		}

		var content, err = c.applyMappers(ctx, kafkaMsg)
		var msg = &consumedMessage{
			msg:      kafkaMsg,
			content:  content,
			consumer: c,
			session:  session,
			abort:    false,
		}
		if err != nil {
			msg.SendToFailureTopic()
		} else {
			err = c.handler(ctx, msg)
			if err != nil {
				c.logger.Error(ctx, "msg", "Failed to handle event", "err", err.Error(), "topic", claim.Topic(), "consumerGroup", c.consumerGroupName)
				if msg.abort {
					return fmt.Errorf("%w. Due to %w", abortingError, err)
				}
			}
			if kafkaMsg.Offset%c.logEventRate == 0 {
				logMsg := fmt.Sprintf("Messages from %d to %d offset are processed", kafkaMsg.Offset-c.logEventRate, kafkaMsg.Offset)
				c.logger.Info(ctx, "msg", logMsg, "topic", c.topic, "partition", kafkaMsg.Partition, "topic", claim.Topic(), "consumerGroup", c.consumerGroupName)
			}
		}

		// Commit event
		if c.autoCommit {
			session.MarkMessage(kafkaMsg, "")
		}
	}

	return nil
}

func (c *consumer) IsLive() bool {
	return c.initialized && c.enabled && c.consumerGroup != nil && c.live
}

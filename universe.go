package kafkauniverse

import (
	"context"
	"errors"
	"fmt"
)

// KafkaUniverse struct
type KafkaUniverse struct {
	clusters  []*cluster
	producers map[string]*producer
	consumers map[string]*consumer
}

// Logger interface for logging with level
type Logger interface {
	Debug(ctx context.Context, keyvals ...interface{})
	Info(ctx context.Context, keyvals ...interface{})
	Warn(ctx context.Context, keyvals ...interface{})
	Error(ctx context.Context, keyvals ...interface{})
}

// ConfigurationProvider interface
type ConfigurationProvider func(target interface{}) error

// NewKafkaUniverse creates a KafkaUniverse from a provided configuration
func NewKafkaUniverse(ctx context.Context, logger Logger, envKeyPrefix string, confUnmarshal ConfigurationProvider) (*KafkaUniverse, error) {
	var clusterRepresentations = []KafkaClusterRepresentation{}
	var err error
	if err = confUnmarshal(&clusterRepresentations); err != nil {
		logger.Error(ctx, "msg", "Failed to unmarshal Kafka configuration", "err", err)
		return nil, err
	}
	if len(clusterRepresentations) == 0 {
		logger.Error(ctx, "msg", "Kafka universe is empty")
		return nil, errors.New("kafka universe is empty")
	}
	for idx, clusterRep := range clusterRepresentations {
		if err = clusterRep.Validate(); err != nil {
			logger.Error(ctx, "msg", "Loaded Kafka configuration is invalid", "err", err, "idx", idx)
			return nil, err
		}
	}
	var res = KafkaUniverse{
		producers: map[string]*producer{},
		consumers: map[string]*consumer{},
	}
	for _, clusterRepresentation := range clusterRepresentations {
		var cluster, err = newCluster(ctx, clusterRepresentation, envKeyPrefix, logger)
		if err != nil {
			return nil, err
		}
		res.clusters = append(res.clusters, cluster)
		for _, producerRep := range clusterRepresentation.Producers {
			res.producers[*producerRep.ID], err = newProducer(cluster, producerRep, logger)
			if err != nil {
				return nil, err
			}
		}
		for _, consumerRep := range clusterRepresentation.Consumers {
			var consumer, err = newConsumer(cluster, consumerRep, logger)
			if err != nil {
				return nil, err
			}
			res.consumers[*consumerRep.ID] = consumer
			if consumerRep.FailureProducer != nil {
				var ok bool
				if consumer.failureProducer, ok = res.producers[*consumerRep.FailureProducer]; !ok {
					return nil, fmt.Errorf("invalid failure producer %s for consumer %s", *consumerRep.FailureProducer, *consumerRep.ID)
				}
			}
		}
	}
	return &res, nil
}

// Close releases all instantiated resources
func (ku *KafkaUniverse) Close() error {
	var anError error
	for _, consumer := range ku.consumers {
		if err := consumer.Close(); err != nil {
			anError = err
		}
	}
	for _, producer := range ku.producers {
		if err := producer.Close(); err != nil {
			anError = err
		}
	}
	for _, cluster := range ku.clusters {
		if err := cluster.Close(); err != nil {
			anError = err
		}
	}
	return anError
}

// InitializeProducers initializes all specified producers
func (ku *KafkaUniverse) InitializeProducers(producerIDs ...string) error {
	for _, producerName := range producerIDs {
		if producer, ok := ku.producers[producerName]; ok {
			if err := producer.initialize(); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("unknown producer %s", producerIDs)
		}
	}
	return nil
}

// InitializeConsumers initializes all specified consumers
func (ku *KafkaUniverse) InitializeConsumers(consumerIDs ...string) error {
	for _, consumerID := range consumerIDs {
		if consumer, ok := ku.consumers[consumerID]; ok {
			if err := consumer.initialize(); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("unknown consumer %s", consumerID)
		}
	}
	return nil
}

// GetConsumer gets the specified consumer
func (ku *KafkaUniverse) GetConsumer(consumerID string) *consumer {
	return ku.consumers[consumerID]
}

// StartConsumers starts all specified consumers
func (ku *KafkaUniverse) StartConsumers(consumerIDs ...string) {
	for _, consumerName := range consumerIDs {
		ku.GetConsumer(consumerName).Go()
	}
}

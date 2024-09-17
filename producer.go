package kafkauniverse

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/cloudtrust/kafka-client/misc"
)

type producer struct {
	initialized bool
	cluster     *cluster
	id          string
	enabled     bool
	topic       *string
	producer    sarama.SyncProducer
	logger      Logger
}

func newProducer(cluster *cluster, producerRep KafkaProducerRepresentation, logger Logger) (*producer, error) {
	var enabled = true
	if !cluster.enabled || (producerRep.Enabled != nil && !*producerRep.Enabled) {
		enabled = false
	}
	return &producer{
		initialized: false,
		cluster:     cluster,
		id:          *producerRep.ID,
		enabled:     enabled,
		topic:       producerRep.Topic,
		logger:      logger,
	}, nil
}

// Close closes all resources
func (p *producer) Close() error {
	if !p.initialized || !p.enabled {
		return nil
	}
	return p.producer.Close()
}

func (p *producer) initialize() error {
	if p.initialized {
		return fmt.Errorf("producer %s already initialized", p.id)
	}
	if !p.enabled {
		p.producer = &misc.NoopKafkaProducer{}
		p.initialized = true
		return nil
	}
	var err error
	if p.producer, err = sarama.NewSyncProducer(p.cluster.brokers, p.cluster.saramaConfig); err != nil {
		p.logger.Error(context.Background(), "msg", "Failed to initialize Kafka producer", "err", err)
		return err
	}
	p.initialized = true
	return nil
}

// SendMessageBytes sends a message in the producer topic
func (p *producer) SendMessageBytes(content []byte) error {
	if !p.enabled {
		return nil
	}
	msg := &sarama.ProducerMessage{Topic: *p.topic, Value: sarama.StringEncoder(content)}
	var _, _, err = p.producer.SendMessage(msg)
	return err
}

// SendPartitionedMessageBytes sends a message in the producer topic
func (p *producer) SendPartitionedMessageBytes(partitionKey string, content []byte) error {
	if !p.enabled {
		return nil
	}
	msg := &sarama.ProducerMessage{Topic: *p.topic, Key: sarama.StringEncoder(partitionKey), Value: sarama.StringEncoder(content)}
	var _, _, err = p.producer.SendMessage(msg)
	return err
}

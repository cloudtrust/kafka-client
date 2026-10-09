package kafkauniverse

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/cloudtrust/kafka-client/misc"
)

type Producer interface {
	SendMessageBytes(content []byte) error
	SendPartitionedMessageBytes(partitionKey string, content []byte) error
	SendWithMap(content []byte, partitionKey string, details map[string]any) error
	Send(producerMessage ProducerMessage) error
	Close() error
}

type producer struct {
	initialized  bool
	cluster      *cluster
	id           string
	enabled      bool
	topic        *string
	producer     sarama.SyncProducer
	errorManager *errorManager
	logger       Logger
}

func newProducer(cluster *cluster, producerRep KafkaProducerRepresentation, errorManager *errorManager, logger Logger) *producer {
	var enabled = true
	if !cluster.enabled || (producerRep.Enabled != nil && !*producerRep.Enabled) {
		enabled = false
	}
	return &producer{
		initialized:  false,
		cluster:      cluster,
		id:           *producerRep.ID,
		enabled:      enabled,
		topic:        producerRep.Topic,
		errorManager: errorManager,
		logger:       logger,
	}
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
	return p.Send(NewProducerMessage(content, nil, nil))
}

// SendPartitionedMessageBytes sends a message in the producer topic
func (p *producer) SendPartitionedMessageBytes(partitionKey string, content []byte) error {
	return p.Send(NewProducerMessage(content, &partitionKey, nil))
}

func (p *producer) SendWithMap(content []byte, partitionKey string, details map[string]any) error {
	return p.Send(NewProducerMessageWithMap(content, partitionKey, details))
}

func (p *producer) Send(producerMessage ProducerMessage) error {
	if !p.enabled {
		return nil
	}
	saramaMessage := producerMessage.GetSaramaMessage().(*sarama.ProducerMessage)
	saramaMessage.Topic = *p.topic
	var _, _, err = p.producer.SendMessage(saramaMessage)
	if err != nil {
		p.errorManager.reportSendMessageError(context.Background(), producerMessage, err)
	}
	return err
}

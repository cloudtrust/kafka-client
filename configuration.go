package kafkauniverse

import (
	"errors"
	"time"
)

const (
	offsetNewestParam = "newest"
	offsetOldestParam = "oldest"
)

// KafkaClusterRepresentation struct
type KafkaClusterRepresentation struct {
	ID               *string                       `mapstructure:"id"`
	Enabled          *bool                         `mapstructure:"enabled"`
	Version          *string                       `mapstructure:"version"`
	TLSEnabled       *bool                         `mapstructure:"tls-enabled"`
	KeepAlive        *time.Duration                `mapstructure:"keep-alive"`
	MaxOpenRequests  *int                          `mapstructure:"max-open-requests"`
	SaramaLogEnabled *bool                         `mapstructure:"sarama-log-enabled"`
	Brokers          []string                      `mapstructure:"brokers"`
	Security         *KafkaSecurityRepresentation  `mapstructure:"security"`
	Producers        []KafkaProducerRepresentation `mapstructure:"producers"`
	Consumers        []KafkaConsumerRepresentation `mapstructure:"consumers"`
}

// KafkaSecurityRepresentation struct
type KafkaSecurityRepresentation struct {
	ClientID     *string `mapstructure:"client-id"`
	ClientSecret *string `mapstructure:"client-secret"`
	TokenURL     *string `mapstructure:"token-url"`
}

// KafkaProducerRepresentation struct
type KafkaProducerRepresentation struct {
	ID      *string `mapstructure:"id"`
	Enabled *bool   `mapstructure:"enabled"`
	Topic   *string `mapstructure:"topic"`
}

// KafkaConsumerRepresentation struct
type KafkaConsumerRepresentation struct {
	ID                *string        `mapstructure:"id"`
	Enabled           *bool          `mapstructure:"enabled"`
	Topic             *string        `mapstructure:"topic"`
	ConsumerGroupName *string        `mapstructure:"consumer-group-name"`
	FailureProducer   *string        `mapstructure:"failure-producer"`
	ConsumptionDelay  *time.Duration `mapstructure:"consumption-delay"`
	InitialOffset     *string        `mapstructure:"initial-offset"`
}

// Validate validates a KafkaClusterRepresentation instance
func (kcr *KafkaClusterRepresentation) Validate() error {
	var err error

	if kcr.ID == nil || *kcr.ID == "" {
		return errors.New("cluster ID should be set and not empty")
	}
	if kcr.Version == nil || *kcr.Version == "" {
		return errors.New("cluster Version should be set and not empty")
	}
	if len(kcr.Brokers) == 0 {
		return errors.New("cluster brokers should contain at least one broker")
	}
	for _, broker := range kcr.Brokers {
		if broker == "" {
			return errors.New("cluster broker value should not be empty")
		}
	}
	if kcr.Security == nil {
		return errors.New("cluster is missing security configuration")
	}
	if err = kcr.Security.Validate(); err != nil {
		return err
	}
	if len(kcr.Producers)+len(kcr.Consumers) == 0 {
		return errors.New("do you really need to configure a cluster with neither producer nor consumer?")
	}
	for _, producer := range kcr.Producers {
		if err = producer.Validate(); err != nil {
			return err
		}
	}
	for _, consumer := range kcr.Consumers {
		if err = consumer.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Validate validates a KafkaSecurityRepresentation instance
func (ksr *KafkaSecurityRepresentation) Validate() error {
	if ksr.ClientID == nil || *ksr.ClientID == "" {
		return errors.New("client-id is mandatory and should not be empty")
	}
	if ksr.ClientSecret == nil || *ksr.ClientSecret == "" {
		return errors.New("client-secret is mandatory and should not be empty")
	}
	if ksr.TokenURL == nil || *ksr.TokenURL == "" {
		return errors.New("token-url is mandatory and should not be empty")
	}
	return nil
}

// Validate validates a KafkaProducerRepresentation instance
func (kpr *KafkaProducerRepresentation) Validate() error {
	if kpr.ID == nil || *kpr.ID == "" {
		return errors.New("producer id is mandatory and should not be empty")
	}
	if kpr.Topic == nil || *kpr.Topic == "" {
		return errors.New("producer topic is mandatory and should not be empty")
	}
	return nil
}

// Validate validates a KafkaConsumerRepresentation instance
func (kcr *KafkaConsumerRepresentation) Validate() error {
	if kcr.ID == nil || *kcr.ID == "" {
		return errors.New("consumer id is mandatory and should not be empty")
	}
	if kcr.Topic == nil || *kcr.Topic == "" {
		return errors.New("consumer topic is mandatory and should not be empty")
	}
	if kcr.ConsumerGroupName == nil || *kcr.ConsumerGroupName == "" {
		return errors.New("consumer group name is mandatory and should not be empty")
	}
	if kcr.FailureProducer != nil && *kcr.FailureProducer == "" {
		return errors.New("consumer failure producer is optional but should not be empty")
	}
	if kcr.InitialOffset != nil && !(*kcr.InitialOffset == offsetOldestParam || *kcr.InitialOffset == offsetNewestParam) {
		return errors.New("consumer initial offset is optional but should be either 'oldest' or 'newest'")
	}

	return nil
}

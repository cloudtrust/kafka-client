package kafkauniverse

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/IBM/sarama"
	"github.com/cloudtrust/kafka-client/misc"
)

type cluster struct {
	enabled        bool
	brokers        []string
	saramaConfig   *sarama.Config
	consumerGroups map[string]sarama.ConsumerGroup
	logger         Logger
}

func newCluster(ctx context.Context, conf KafkaClusterRepresentation, envKeyPrefix string, logger Logger) (*cluster, error) {
	var secret = getEnvVariable(envKeyPrefix, *conf.ID, "_CLIENT_SECRET")
	if secret != nil {
		conf.Security.ClientSecret = secret
	}

	var saramaConfig, err = newSaramaConfig(ctx, conf, logger)
	if err != nil {
		return nil, err
	}

	if conf.SaramaLogEnabled != nil {
		sarama.Logger = misc.NewSaramaLogger(logger.Info, *conf.SaramaLogEnabled)
	}

	var enabled = conf.Enabled == nil || *conf.Enabled
	return &cluster{
		enabled:        enabled,
		brokers:        conf.Brokers,
		saramaConfig:   saramaConfig,
		consumerGroups: make(map[string]sarama.ConsumerGroup),
		logger:         logger,
	}, nil
}

func getEnvVariable(prefix string, clusterID string, suffix string) *string {
	var key = prefix + getEnvVariableName(clusterID) + suffix
	var value = os.Getenv(key)
	if value != "" {
		return &value
	}
	return nil
}

func getEnvVariableName(clusterID string) string {
	return strings.ReplaceAll(strings.ToUpper(clusterID), "-", "_")
}

func newSaramaConfig(ctx context.Context, conf KafkaClusterRepresentation, logger Logger) (*sarama.Config, error) {
	version, err := sarama.ParseKafkaVersion(*conf.Version)
	if err != nil {
		logger.Warn(ctx, "msg", "Failed to parse Kafka version", "err", err, "version", *conf.Version)
		return nil, fmt.Errorf("can't parse kafka version %s", *conf.Version)
	}
	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	config.Producer.Return.Successes = true

	// Enables Oauth2 authentification
	config.Net.SASL.Enable = true
	config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
	config.Net.SASL.TokenProvider = misc.NewTokenProvider(*conf.Security.ClientID, *conf.Security.ClientSecret, *conf.Security.TokenURL)

	config.Net.TLS.Enable = *conf.TLSEnabled

	return config, nil
}

func (c *cluster) Close() error {
	var anError error
	for name, consumerGroup := range c.consumerGroups {
		if err := consumerGroup.Close(); err != nil {
			c.logger.Warn(context.Background(), "msg", "Failed to close consumer group", "group", name, "err", err)
			anError = err
		}
	}
	return anError
}

func (c *cluster) getConsumerGroup(consumerGroupName string, groupConfig sarama.Config) (sarama.ConsumerGroup, error) {
	if !c.enabled {
		return &misc.NoopKafkaConsumerGroup{}, nil
	}
	if cg, ok := c.consumerGroups[consumerGroupName]; ok {
		return cg, nil
	}

	consumer, err := sarama.NewConsumerGroup(c.brokers, consumerGroupName, &groupConfig)
	if err != nil {
		c.logger.Warn(context.Background(), "msg", "Failed to create consumer group", "group", consumerGroupName, "err", err)
		return nil, err
	}

	c.consumerGroups[consumerGroupName] = consumer
	return consumer, nil
}

package kafkauniverse

import (
	"context"
	"errors"
	"testing"

	"github.com/cloudtrust/kafka-client/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestKafkaUniverse(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var logger = mock.NewLogger(mockCtrl)
	var ctx = context.TODO()
	var anError = errors.New("any error")

	logger.EXPECT().Error(gomock.Any(), gomock.Any()).AnyTimes()
	var createDefaultUniverse = func(target any) error {
		var conf = target.(*[]KafkaClusterRepresentation)
		*conf = append(*conf, createValidKafkaClusterRepresentation())
		return nil
	}

	t.Run("Empty universe", func(t *testing.T) {
		var _, err = NewKafkaUniverse(ctx, logger, "CT_KAFKA_CLIENT_SECRET_", func(target any) error {
			return anError
		})
		assert.Equal(t, anError, err)
	})
	t.Run("Empty universe", func(t *testing.T) {
		var universe, err = NewKafkaUniverse(ctx, logger, "CT_KAFKA_CLIENT_SECRET_", func(target any) error {
			return nil
		})
		assert.NotNil(t, err)
		assert.Nil(t, universe)
	})
	t.Run("Invalid configuration", func(t *testing.T) {
		var universe, err = NewKafkaUniverse(ctx, logger, "CT_KAFKA_CLIENT_SECRET_", func(target any) error {
			var conf = target.(*[]KafkaClusterRepresentation)
			*conf = append(*conf, KafkaClusterRepresentation{})
			return nil
		})
		assert.NotNil(t, err)
		assert.Nil(t, universe)
	})
	t.Run("Valid configuration", func(t *testing.T) {
		var universe, err = NewKafkaUniverse(ctx, logger, "CT_KAFKA_CLIENT_SECRET_", createDefaultUniverse)
		assert.Nil(t, err)
		assert.NotNil(t, universe)
	})
	t.Run("Initialize unknown producer", func(t *testing.T) {
		var universe, _ = NewKafkaUniverse(ctx, logger, "CT_KAFKA_CLIENT_SECRET_", createDefaultUniverse)
		var err = universe.InitializeProducers("unknown")
		assert.NotNil(t, err)
	})
	t.Run("Initialize unknown consumer", func(t *testing.T) {
		var universe, _ = NewKafkaUniverse(ctx, logger, "CT_KAFKA_CLIENT_SECRET_", createDefaultUniverse)
		var err = universe.InitializeConsumers("unknown")
		assert.NotNil(t, err)
	})
	t.Run("Get unknown producer", func(t *testing.T) {
		var universe, _ = NewKafkaUniverse(ctx, logger, "CT_KAFKA_CLIENT_SECRET_", createDefaultUniverse)
		assert.Nil(t, universe.GetProducer("unknown"))
	})
	t.Run("Get unknown consumer", func(t *testing.T) {
		var universe, _ = NewKafkaUniverse(ctx, logger, "CT_KAFKA_CLIENT_SECRET_", createDefaultUniverse)
		assert.Nil(t, universe.GetConsumer("unknown"))
	})
}

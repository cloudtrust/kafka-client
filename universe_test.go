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

	t.Run("Empty universe", func(t *testing.T) {
		var _, err = NewKafkaUniverse(ctx, logger, func(target interface{}) error {
			return anError
		})
		assert.Equal(t, anError, err)
	})
	t.Run("Empty universe", func(t *testing.T) {
		var universe, err = NewKafkaUniverse(ctx, logger, func(target interface{}) error {
			return nil
		})
		assert.NotNil(t, err)
		assert.Nil(t, universe)
	})
	t.Run("Invalid configuration", func(t *testing.T) {
		var universe, err = NewKafkaUniverse(ctx, logger, func(target interface{}) error {
			var conf = target.(*[]KafkaClusterRepresentation)
			*conf = append(*conf, KafkaClusterRepresentation{})
			return nil
		})
		assert.NotNil(t, err)
		assert.Nil(t, universe)
	})
	t.Run("Valid configuration", func(t *testing.T) {
		var universe, err = NewKafkaUniverse(ctx, logger, func(target interface{}) error {
			var conf = target.(*[]KafkaClusterRepresentation)
			*conf = append(*conf, createValidKafkaClusterRepresentation())
			return nil
		})
		assert.Nil(t, err)
		assert.NotNil(t, universe)
	})
}

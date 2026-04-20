package kafkauniverse

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func createValidKafkaClusterRepresentation() KafkaClusterRepresentation {
	return KafkaClusterRepresentation{
		ID:               new("cluster-id"),
		Enabled:          new(true),
		Version:          new("3.1.0"),
		TLSEnabled:       new(false),
		SaramaLogEnabled: new(false),
		Brokers:          []string{"kafka1", "kafka2"},
		Security: &KafkaSecurityRepresentation{
			ClientID:     new("client-id"),
			ClientSecret: new("client-secret"),
			TokenURL:     new("https://token/path"),
		},
		Producers: []KafkaProducerRepresentation{
			{
				ID:      new("producer-1"),
				Enabled: new(true),
				Topic:   new("topic-producer-1"),
			},
		},
		Consumers: []KafkaConsumerRepresentation{
			{
				ID:                new("consumer-1"),
				Enabled:           new(true),
				Topic:             new("topic-consumer-1"),
				ConsumerGroupName: new("consumer-group-1"),
				FailureProducer:   new("producer-1"),
				InitialOffset:     nil,
			},
			{
				ID:                new("consumer-2"),
				Enabled:           new(true),
				Topic:             new("topic-consumer-2"),
				ConsumerGroupName: new("consumer-group-2"),
				FailureProducer:   new("producer-1"),
				InitialOffset:     new("newest"),
			},
		},
	}
}

func TestValidateCluster(t *testing.T) {
	var cluster = createValidKafkaClusterRepresentation()
	assert.Nil(t, cluster.Validate())

	var emptyString = new("")
	var invalidCases []KafkaClusterRepresentation
	for range 36 {
		invalidCases = append(invalidCases, createValidKafkaClusterRepresentation())
	}
	invalidCases[0].ID = nil
	invalidCases[1].ID = emptyString
	invalidCases[2].Version = nil
	invalidCases[3].Version = emptyString
	invalidCases[4].Brokers = nil
	invalidCases[5].Brokers = []string{""}
	invalidCases[6].Security = nil
	invalidCases[7].Security.ClientID = nil
	invalidCases[8].Security.ClientID = emptyString
	invalidCases[9].Security.ClientSecret = nil
	invalidCases[10].Security.ClientSecret = emptyString
	invalidCases[11].Security.TokenURL = nil
	invalidCases[12].Security.TokenURL = emptyString
	invalidCases[13].Producers = nil
	invalidCases[13].Consumers = nil
	invalidCases[14].Producers[0].ID = nil
	invalidCases[15].Producers[0].ID = emptyString
	invalidCases[16].Producers[0].Topic = nil
	invalidCases[17].Producers[0].Topic = emptyString
	invalidCases[18].Consumers[0].ID = nil
	invalidCases[19].Consumers[0].ID = emptyString
	invalidCases[20].Consumers[0].Topic = nil
	invalidCases[21].Consumers[0].Topic = emptyString
	invalidCases[22].Consumers[0].ConsumerGroupName = nil
	invalidCases[23].Consumers[0].ConsumerGroupName = emptyString
	invalidCases[24].Consumers[0].FailureProducer = emptyString
	invalidCases[25].Consumers[0].InitialOffset = new("not oldest nor newest")
	invalidCases[26].Consumers[0].InitialOffset = emptyString
	invalidCases[27].Consumers[1].ID = nil
	invalidCases[28].Consumers[1].ID = emptyString
	invalidCases[29].Consumers[1].Topic = nil
	invalidCases[30].Consumers[1].Topic = emptyString
	invalidCases[31].Consumers[1].ConsumerGroupName = nil
	invalidCases[32].Consumers[1].ConsumerGroupName = emptyString
	invalidCases[33].Consumers[1].FailureProducer = emptyString
	invalidCases[34].Consumers[1].InitialOffset = new("not oldest nor newest")
	invalidCases[35].Consumers[0].InitialOffset = emptyString

	for idx, value := range invalidCases {
		t.Run(fmt.Sprintf("Invalid case #%d", idx), func(t *testing.T) {
			assert.NotNil(t, value.Validate())
		})
	}

}

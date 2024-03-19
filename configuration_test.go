package kafkauniverse

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func createValidKafkaClusterRepresentation() KafkaClusterRepresentation {
	return KafkaClusterRepresentation{
		ID:               ptrString("cluster-id"),
		Enabled:          ptrBool(true),
		Version:          ptrString("3.1.0"),
		TLSEnabled:       ptrBool(false),
		SaramaLogEnabled: ptrBool(false),
		Brokers:          []string{"kafka1", "kafka2"},
		Security: &KafkaSecurityRepresentation{
			ClientID:     ptrString("client-id"),
			ClientSecret: ptrString("client-secret"),
			TokenURL:     ptrString("https://token/path"),
		},
		Producers: []KafkaProducerRepresentation{
			{
				ID:      ptrString("producer-1"),
				Enabled: ptrBool(true),
				Topic:   ptrString("topic-producer-1"),
			},
		},
		Consumers: []KafkaConsumerRepresentation{
			{
				ID:                ptrString("consumer-1"),
				Enabled:           ptrBool(true),
				Topic:             ptrString("topic-consumer-1"),
				ConsumerGroupName: ptrString("consumer-group"),
				FailureProducer:   ptrString("producer-1"),
			},
		},
	}
}

func TestValidateCluster(t *testing.T) {
	var cluster = createValidKafkaClusterRepresentation()
	assert.Nil(t, cluster.Validate())

	var emptyString = ptrString("")
	var invalidCases []KafkaClusterRepresentation
	for i := 0; i < 25; i++ {
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

	for idx, value := range invalidCases {
		t.Run(fmt.Sprintf("Invalid case #%d", idx), func(t *testing.T) {
			assert.NotNil(t, value.Validate())
		})
	}
}

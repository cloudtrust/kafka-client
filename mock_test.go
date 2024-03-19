package kafkauniverse

//go:generate mockgen --build_flags=--mod=mod -destination=./mock/universe.go -package=mock -mock_names=Logger=Logger github.com/cloudtrust/kafka-client Logger
//go:generate mockgen --build_flags=--mod=mod -destination=./mock/sarama.go -package=mock -mock_names=ConsumerGroup=ConsumerGroup,ConsumerGroupSession=ConsumerGroupSession,ConsumerGroupClaim=ConsumerGroupClaim,SyncProducer=SyncProducer github.com/IBM/sarama ConsumerGroup,ConsumerGroupSession,ConsumerGroupClaim,SyncProducer

func ptrString(value string) *string {
	return &value
}

func ptrBool(value bool) *bool {
	return &value
}

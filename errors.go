package kafkauniverse

import (
	"context"
)

type KafkaStep int

// KafkaStep represents the step in the Kafka processing pipeline where an error occurred.
const (
	STEP_MAPPER KafkaStep = iota
	STEP_MESSAGEHANDLER
	STEP_SENDMESSAGE
)

var stepErrorMessage = map[KafkaStep]string{
	STEP_MAPPER:         "Kafka message mapper error",
	STEP_MESSAGEHANDLER: "User handler error",
	STEP_SENDMESSAGE:    "Send message error",
}

type customFields map[string]any

func (cf customFields) export() []any {
	var res []any
	for key, value := range cf {
		res = append(res, key, value)
	}
	return res
}

// MessageDetails represents the details of a Kafka message (consumed or produced).
type MessageDetails interface {
	GetContent() any
	GetTopic() string
	GetPartitionKey() string
	ExportCustomFields() []any
}

// KafkaStatus struct represents the status of a Kafka execution
type KafkaStatus struct {
	Step    KafkaStep
	Message MessageDetails
	Error   error
}

type errorHandler func(ctx context.Context, kafkaStatus KafkaStatus)

type errorManager struct {
	errorHandlers []errorHandler
}

func (em *errorManager) dispatchError(ctx context.Context, kafkaStatus KafkaStatus) {
	for _, handler := range em.errorHandlers {
		handler(ctx, kafkaStatus)
	}
}

func (em *errorManager) reportStepError(ctx context.Context, message MessageDetails, err error, step KafkaStep) {
	em.dispatchError(ctx, KafkaStatus{
		Step:    step,
		Message: message,
		Error:   err,
	})
}

func (em *errorManager) reportMapperError(ctx context.Context, message MessageDetails, err error) {
	em.reportStepError(ctx, message, err, STEP_MAPPER)
}

func (em *errorManager) reportMessageHandlerError(ctx context.Context, message MessageDetails, err error) {
	em.reportStepError(ctx, message, err, STEP_MESSAGEHANDLER)
}

func (em *errorManager) reportSendMessageError(ctx context.Context, message MessageDetails, err error) {
	em.reportStepError(ctx, message, err, STEP_SENDMESSAGE)
}

func (em *errorManager) addErrorHandler(handler errorHandler) {
	em.errorHandlers = append(em.errorHandlers, handler)
}

func (em *errorManager) addErrorLogger(logger ErrorLogger) {
	em.addErrorHandler(func(ctx context.Context, kafkaStatus KafkaStatus) {
		var logDetails = kafkaStatus.Message.ExportCustomFields()
		if msg, ok := stepErrorMessage[kafkaStatus.Step]; ok {
			logDetails = append(logDetails, "msg", msg)
		} else {
			logDetails = append(logDetails, "msg", "Unknown error step")
		}
		if kafkaStatus.Message.GetPartitionKey() != "" {
			logDetails = append(logDetails, "partitionKey", kafkaStatus.Message.GetPartitionKey())
		}
		logDetails = append(logDetails, "topic", kafkaStatus.Message.GetTopic(), "content", kafkaStatus.Message.GetContent())
		if kafkaStatus.Error != nil {
			logDetails = append(logDetails, "err", kafkaStatus.Error.Error())
		}
		logger(ctx, logDetails...)
	})
}

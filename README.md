# kafka-client

Kafka-client is a library used to simplify configuration and access to Kafka producers and consumers.

## Configuration

Example of a yaml configuration file

```
my-kafka-key:
- id: cluster1
  enabled: true
  version: "3.1.0"
  tls-enabled: false
  brokers:
  - "kafka11.domain.ch:9093"
  - "kafka12.domain.ch:9093"
  security:
    client-id: your-client-id
    client-secret: your-client-secret
    token-url: https://path.to/your/token/provider/protocol/openid-connect/token
  producers:
  - id: producer-id1
    topic: my.topic1
  consumers:
  - id: consumer-id1
    enabled: true
    topic: my.consumed.topic1
    consumer-group-name: consumer-group1
    failure-producer: producer-id2
- id: cluster2
  enabled: true
  version: "3.1.0"
  tls-enabled: false
  brokers:
  - "kafka21.domain.ch:9093"
  - "kafka22.domain.ch:9093"
  security:
    client-id: your-client-id
    client-secret: your-client-secret
    token-url: https://path.to/your/token/provider/protocol/openid-connect/token
  producers:
  - id: producer-id2
    topic: my.topic2
```

## Instantiate your Kafka Universe

```
	var kafkaUniverse *kafkauniverse.KafkaUniverse
	{
		var kafkaLogger = log.With(logger, "svc", "kafka")
		var c = getViperConfiguration()

		var err error
		kafkaUniverse, err = kafkauniverse.NewKafkaUniverse(ctx, kafkaLogger, "ENV_", func(value any) error {
			return c.UnmarshalKey("my-kafka-key", value)
		})
		if err != nil {
			kafkaLogger.Error(ctx, "msg", "could not configure Kafka", "err", err)
			return
		}
		logger.Info(ctx, "msg", "Kafka configuration loaded")
	}
	defer kafkaUniverse.Close()
```

Note that the client secret can be replaced by an environment variable... in the previous example, ENV_ will be the prefix of the environment variable, the cluster ID with uppercase and - replaced by _, and a suffix _CLIENT_SECRET. In this example, the environment variable should be ENV_CLUSTER1_CLIENT_SECRET.

## Initialize your producers

```
	if err := kafkaUniverse.InitializeProducers("producer-id1", "producer-id2"); err != nil {
		logger.Error(ctx, "msg", "can't initialize kafka producers", "err", err)
		return
	}
```

## Initialize your consumers

```
	if err := kafkaUniverse.InitializeConsumers("consumer-id1"); err != nil {
		logger.Error(ctx, "msg", "can't initialize kafka consumers", "err", err)
		return
	}
```

## Initialize each consumer instance

```
		// Override the default context initializer. In the following example, you can add a random UUID as a correlation ID
		var contextInitializer = func(ctx context.Context) context.Context {
			return context.WithValue(ctx, cs.CtContextCorrelationID, idGenerator.NextID())
		}

		// Add content mappers. By default, the content will be a slice of bytes containing the raw message consumed from a Kafka topic.
		// You can add some mappers to transform it in the something more confortable to use.
		// By default, no mapper is configured. A pre-defined mapper is available to decode the raw message from Base64: mappers.DecodeBase64Bytes
		var mapBytesToString = func(ctx context.Context, in any) (any, error) {
			return string(in.([]byte)), nil
		}
		var mapStringToInt = func(ctx context.Context, in any) (any, error) {
			return strconv.Atoi(in.(string))
		}

		// You have to provide an handler for each consumed message
		var myHandler = func(ctx context.Context, message kafkauniverse.KafkaMessage) error {
			var content = message.Content().(int)

			// process your content

			// by default, the consumer is configured to "AutoCommit": you can disable this AutoCommit and confirm the message is processed like this:
			message.Commit()

			// If you need to abort all processings of the current consumer, use the AbortConsuming function:
			message.AbortConsuming()

			return nil
		}

		kafkaUniverse.GetConsumer("consumer-id1").
			SetContextInitializer(contextInitializer).
			SetLogEventRate(100). // Write a message in logs every 100 consumed messages
			AddContentMapper(mappers.DecodeBase64Bytes).
			AddContentMapper(mapBytesToString).
			AddContentMapper(mapStringToInt).
			SetHandler(myHandler)
```

## Start consumers
When everything is configured, you can start consuming your topics:

```
		kafkaUniverse.StartConsumers("consumer-id1", ..., "consumer-idN")
```
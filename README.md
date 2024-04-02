# Kafka Streams

## Topics creation
```
~ % kafka-topics.sh --bootstrap-server localhost:9092 --topic favorite-color-input-v2 --create --partitions 1 --replication-factor 1

~ % kafka-topics.sh --bootstrap-server localhost:9092 --topic favorite-color-intermediate-v2 --create --partitions 1 --replication-factor 1

~ % kafka-topics.sh --bootstrap-server localhost:9092 --topic favorite-color-output-v2 --create --partitions 1 --replication-factor 1
```

## CLI Producer
```
~ % kafka-console-producer.sh --bootstrap-server localhost:9092 --topic favorite-color-input-v2
```

## CLI Consumer 
```
~ % kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic favorite-color-output-v2 --from-beginning --property print.key=true --property print.value=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

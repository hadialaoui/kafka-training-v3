package com.hadialaoui.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.List;
import java.util.Properties;

public class FavoriteColor {

    public static void main(String[] args) {

        //Configuration
        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-favorite-color");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Build the Kafka Streams application
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> input = streamsBuilder.stream("favorite-color-input-v2");

        input.filter((k, v) -> v.contains(","))
                //define the key (user id)
                .selectKey((k, v) -> v.split(",")[0].toLowerCase())
                //define the value (color)
                .mapValues(v -> v.split(",")[1].toLowerCase())
                .filter((k, v) -> List.of("red", "blue", "green").contains(v))
                .to("favorite-color-intermediate-v2", Produced.with(Serdes.String(), Serdes.String()));

        //Read from intermediate topic
       streamsBuilder.table("favorite-color-intermediate-v2")
               .groupBy((user, color) -> new KeyValue<>(color,color))
               .count()
               .toStream()
               .to("favorite-color-output-v2");


        KafkaStreams  streams = new KafkaStreams(streamsBuilder.build(), config);

        streams.cleanUp();
        // Start the Kafka Streams application
        streams.start();
        // Add shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}

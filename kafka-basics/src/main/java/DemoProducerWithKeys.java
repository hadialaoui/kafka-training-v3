import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class DemoProducerWithKeys {

    public static final Logger LOGGER = LoggerFactory.getLogger(DemoProducerWithKeys.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        LOGGER.info("Start Kafka producer");
        // Create producer properties
        Properties  properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create kafka producer
        var producer = new KafkaProducer<String, String>(properties);
        // Create producer Records
        for (int j = 0 ; j < 2 ; j++) {
            for (int i = 10; i < 14; i++) {
                String topic = "java-topic";
                String key = "id-" + i;
                String message = "Hello ("+j+"-"+i+")";
                var producerRecord = new ProducerRecord<>(topic, key, message);
                // Send data
                producer.send(producerRecord, (metadata, e) -> {
                    if (e == null) {
                        LOGGER.info("key: {} | Partition: {} | Offset: {}",
                                key, metadata.partition(), metadata.offset());
                    } else {
                        LOGGER.error("Error while producing", e);
                    }
                });

            }
            Thread.sleep(1000);
        }
        // Flush and close producer
        producer.close();

    }
}

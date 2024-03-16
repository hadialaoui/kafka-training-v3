import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class DemoProducer {

    public static final Logger LOGGER = LoggerFactory.getLogger(DemoProducer.class.getSimpleName());

    public static void main(String[] args) {
        LOGGER.info("Start Kafka producer");
        // Create producer properties
        Properties  properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // Producer Config
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create kafka producer
        var producer = new KafkaProducer<String, String>(properties);
        // Create producer Record
        var producerRecord = new ProducerRecord<String, String>("first-topic", "hello world from java producer");
        // Send data
        producer.send(producerRecord);

        // Flush and close producer
        producer.close();

    }
}

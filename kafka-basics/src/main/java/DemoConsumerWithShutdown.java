import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class DemoConsumerWithShutdown {

    public static final Logger LOGGER = LoggerFactory.getLogger(DemoConsumerWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        LOGGER.info("Start Kafka Consumer");
        // Create producer properties
        Properties  properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        //Consumer Config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("auto.offset.reset", "earliest");//none/latest/earliest
        properties.setProperty("group.id", "my-java-application");

        // Create kafka consumer
        var consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(List.of("java-topic"));

        // Get referenced of the main thread
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                super.run();
                LOGGER.info("Detection of a shutdown, let call consumer wakeup");
                consumer.wakeup();
                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            while(true) {
                // Poll Consumer Records
                var records = consumer.poll(Duration.ofMillis(1000));

                // Read data
                LOGGER.info("************ Polling ************");
                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info("Partition: {}, Offset: {}, key: {}, value: {}",
                            record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } catch (WakeupException e){
            LOGGER.info("consumer is starting to shutdown");
        } catch (Exception e){
            LOGGER.error("Unexpected error thrown", e);
        } finally {
            consumer.close();
            LOGGER.info("The consumer is gracefully shutdown");
        }

    }
}

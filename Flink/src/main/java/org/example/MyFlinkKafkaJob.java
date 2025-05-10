package org.example;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.Properties;

@Getter
@Setter
@ToString
public class MyFlinkKafkaJob {
    private static final Logger logger = LogManager.getLogger(MyFlinkKafkaJob.class);
    public static void main(String[] args) throws Exception {

        // 1. Execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        logger.info("Starting flink job");
        // 2. Kafka consumer properties
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "flink-consumer-group");

        // 3. Kafka consumer

        FlinkKafkaConsumer<UserEvent> kafkaConsumer = new FlinkKafkaConsumer<>(
                "input-topic",
                new UserEventDeserializationSchema(),
                kafkaProps
        );
        // 4. Source and process
        env.addSource(kafkaConsumer)
                .name("Kafka JSON Source")
                .uid("kafka-json-source")
                .map(event -> {
                    logger.info("User: {}, Action: {}, Time: {}", event.getUserId(), event.getAction(), event.getTimestamp());
                    return event;
                })
                .print();


//        // 5. Execute
//        env.execute("Flink Kafka Streaming Job");
    }
}

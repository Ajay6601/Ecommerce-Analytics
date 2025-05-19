package com.ecommerce.analytics;

import com.ecommerce.analytics.models.CustomerEvent;
import com.ecommerce.analytics.models.EnrichedEvent;
import com.ecommerce.analytics.functions.EnrichmentFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Properties;

public class RealTimeAnalytics {
    private static final Logger LOG = LoggerFactory.getLogger(RealTimeAnalytics.class);

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        LOG.info("Starting E-commerce Real-time Analytics...");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (kafkaBootstrapServers == null || kafkaBootstrapServers.isEmpty()) {
            kafkaBootstrapServers = "kafka:9092";
        }
        LOG.info("Using Kafka bootstrap servers: {}", kafkaBootstrapServers);

        // Configure Kafka consumer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBootstrapServers);
        properties.setProperty("group.id", "flink-analytics-java");

        // Create Kafka consumer (input)
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "uk-retail-raw",
                new SimpleStringSchema(),
                properties
        );
        // Start from earliest to process all available messages
        consumer.setStartFromEarliest();

        // Create Kafka producer (output) - simplest constructor for compatibility
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                kafkaBootstrapServers,
                "uk-retail-processed",
                new SimpleStringSchema()
        );

        DataStream<String> rawEvents = env.addSource(consumer);

        DataStream<CustomerEvent> customerEvents = rawEvents.map(new MapFunction<String, CustomerEvent>() {
            @Override
            public CustomerEvent map(String value) throws Exception {
                if (value == null || value.isEmpty()) {
                    LOG.warn("Received empty event");
                    return null;
                }

                try {
                    CustomerEvent event = JSON_MAPPER.readValue(value, CustomerEvent.class);
                    return event;
                } catch (Exception e) {
                    LOG.error("Failed to parse JSON event: {}", e.getMessage());
                    // Return null to filter out this event
                    return null;
                }
            }
        }).filter(event -> event != null); // Filter out null events from parsing errors

        // Add processing steps (keyBy userId and apply enrichment)
        DataStream<EnrichedEvent> enrichedEvents = customerEvents
                .keyBy(event -> event.getUserId()) // Keying by userId to maintain state per user
                .map(new EnrichmentFunction());

        // Convert enriched events to JSON and send to Kafka
        enrichedEvents.map(new MapFunction<EnrichedEvent, String>() {
            @Override
            public String map(EnrichedEvent value) throws Exception {
                try {
                    return JSON_MAPPER.writeValueAsString(value);
                } catch (Exception e) {
                    LOG.error("Failed to serialize enriched event: {}", e.getMessage());
                    // Return a minimal valid JSON to avoid breaking the pipeline
                    return "{\"error\":\"serialization_failed\",\"event_id\":\"" +
                            (value != null ? value.getEventId() : "unknown") + "\"}";
                }
            }
        }).addSink(producer);

        enrichedEvents.addSink(new MongoDBSink());

        LOG.info("Job configuration complete, executing Flink job");
        env.execute("E-commerce Real-time Analytics");
    }

    /**
     * MongoDB sink implementation for storing enriched events
     */
    public static class MongoDBSink extends RichSinkFunction<EnrichedEvent> {
        private static final long serialVersionUID = 1L;
        private transient MongoClient mongoClient;
        private transient MongoCollection<Document> collection;
        private transient ObjectMapper objectMapper;

        @Override
        public void open(Configuration parameters) {
            LOG.info("Initializing MongoDB sink");

            try {
                // Create a separate ObjectMapper for this sink
                objectMapper = new ObjectMapper();

                // Create MongoDB client
                String mongoURI = "mongodb://admin:password@mongodb:27017/ecommerce_analytics?authSource=admin";
                mongoClient = MongoClients.create(mongoURI);
                MongoDatabase database = mongoClient.getDatabase("ecommerce_analytics");
                collection = database.getCollection("processed_events");

                LOG.info("MongoDB sink initialized successfully");
            } catch (Exception e) {
                LOG.error("Failed to initialize MongoDB sink: {}", e.getMessage(), e);
            }
        }

        @Override
        public void invoke(EnrichedEvent event, Context context) {
            if (collection == null) {
                LOG.error("MongoDB collection not initialized, cannot store event");
                return;
            }

            try {
                // Convert event to Document
                String json = objectMapper.writeValueAsString(event);
                Document document = Document.parse(json);

                // Add stored timestamp
                document.append("stored_at", new java.util.Date());

                // Insert into MongoDB
                collection.insertOne(document);

                // Log occasional success to avoid flooding logs
                if (Math.random() < 0.001) { // Log approximately 0.1% of events
                    LOG.info("Successfully stored event in MongoDB: {}", event.getEventId());
                }
            } catch (Exception e) {
                LOG.error("Failed to store event in MongoDB: {}", e.getMessage());
            }
        }

        @Override
        public void close() {
            if (mongoClient != null) {
                try {
                    mongoClient.close();
                    LOG.info("MongoDB client closed");
                } catch (Exception e) {
                    LOG.error("Error closing MongoDB client: {}", e.getMessage());
                }
            }
        }
    }
}
import json
import time
from datetime import datetime, timedelta
import random
import os
import sys
import argparse
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("event_simulator")

try:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:
    logger.error("confluent-kafka package not installed")
    Producer = None


class EventSimulator:
    def __init__(self, input_path, kafka_bootstrap_servers, topic_name, time_compression=1000):
        """
        Initialize the event simulator

        Args:
            input_path (str): Path to the JSON events file
            kafka_bootstrap_servers (str): Kafka bootstrap servers
            topic_name (str): Kafka topic to publish events to
            time_compression (int): Time compression factor
        """
        self.input_path = input_path
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.topic_name = topic_name
        self.time_compression = time_compression
        self.producer = None

        # Validate input path
        if not os.path.exists(self.input_path):
            raise FileNotFoundError(f"Input file not found: {self.input_path}")

    def load_events(self):
        """
        Load events from JSON file

        Returns:
            list: List of event dictionaries
        """
        logger.info(f"Loading events from {self.input_path}")

        try:
            with open(self.input_path, 'r') as f:
                events = json.load(f)

            # Log event statistics
            identified_events = sum(1 for e in events if e.get('metadata', {}).get('customer_type') != 'anonymous')
            anonymous_events = sum(1 for e in events if e.get('metadata', {}).get('customer_type') == 'anonymous')

            logger.info(f"Loaded {len(events):,} events total")
            logger.info(f"  - {identified_events:,} events from identified customers")
            logger.info(f"  - {anonymous_events:,} events from anonymous customers")

            # Log event type distribution
            event_types = {}
            for event in events:
                event_type = event.get('event_type', 'unknown')
                event_types[event_type] = event_types.get(event_type, 0) + 1

            logger.info("Event type distribution:")
            for event_type, count in sorted(event_types.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / len(events)) * 100
                logger.info(f"  - {event_type}: {count:,} events ({percentage:.1f}%)")

            return events

        except Exception as e:
            logger.error(f"Error loading events from {self.input_path}: {e}", exc_info=True)
            raise

    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            pass  # Uncomment for verbose logging: logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

    def connect_kafka(self):
        """
        Connect to Kafka using confluent-kafka

        Raises:
            ImportError: If confluent-kafka is not installed
            Exception: If connection to Kafka fails
        """
        if Producer is None:
            raise ImportError("confluent-kafka package is required. Run 'pip install confluent-kafka'")

        logger.info(f"Connecting to Kafka at {self.kafka_bootstrap_servers}")

        try:
            # Test Kafka connectivity
            admin_client = AdminClient({'bootstrap.servers': self.kafka_bootstrap_servers})
            metadata = admin_client.list_topics(timeout=10)
            logger.info(f"Successfully connected to Kafka. Available topics: {list(metadata.topics.keys())}")

            # Create topic if it doesn't exist
            if self.topic_name not in metadata.topics:
                logger.warning(f"Topic {self.topic_name} does not exist. Creating...")
                topic = NewTopic(self.topic_name, num_partitions=3, replication_factor=1)
                admin_client.create_topics([topic])
                logger.info(f"Topic {self.topic_name} created")

            # Create the producer
            self.producer = Producer({
                'bootstrap.servers': self.kafka_bootstrap_servers,
                'client.id': 'event-simulator',
                'acks': 'all',  # Strongest durability guarantee
                'retries': 3,
                'retry.backoff.ms': 500,
                'delivery.timeout.ms': 10000  # 10 seconds
            })
            logger.info("Kafka producer created successfully")

        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}", exc_info=True)
            raise

    def simulate(self, events=None, batch_size=1000):
        """
        Simulate events in real-time (with time compression)

        Args:
            events (list, optional): Events to simulate. If None, load from file.
            batch_size (int): Number of events to process in one batch
        """
        # Load events if not provided
        if events is None:
            events = self.load_events()

        # Connect to Kafka if not already connected
        if self.producer is None:
            self.connect_kafka()

        # Sort events by timestamp
        logger.info("Sorting events by timestamp")
        events.sort(key=lambda x: x['timestamp'])

        # Get first and last timestamps
        first_ts = datetime.fromisoformat(events[0]['timestamp'])
        last_ts = datetime.fromisoformat(events[-1]['timestamp'])
        original_time_range = (last_ts - first_ts).total_seconds()
        compressed_time = original_time_range / self.time_compression

        # Calculate statistics
        days_span = original_time_range / 86400
        events_per_second = len(events) / compressed_time if compressed_time > 0 else 0

        logger.info(f"Original data spans {days_span:.1f} days ({original_time_range/3600:.1f} hours)")
        logger.info(f"With time compression factor {self.time_compression}, simulation will take approximately {compressed_time/60:.1f} minutes")
        logger.info(f"Expected throughput: {events_per_second:.1f} events/second")
        logger.info(f"Publishing to Kafka topic: {self.topic_name}")

        # Start simulation
        sim_start = datetime.now()
        prev_event_ts = first_ts
        events_sent = 0
        error_count = 0

        try:
            for i, event in enumerate(events):
                # Calculate delay based on timestamp difference
                curr_event_ts = datetime.fromisoformat(event['timestamp'])
                time_diff = (curr_event_ts - prev_event_ts).total_seconds()

                # Apply time compression
                delay = time_diff / self.time_compression

                # Add jitter for more realistic behavior
                if delay > 0:
                    jitter = random.uniform(0.9, 1.1)  # 10% jitter
                    delay = delay * jitter
                    time.sleep(max(0, delay))  # Ensure we don't sleep for negative time

                # Add simulation metadata
                event_copy = event.copy()
                event_copy['simulation'] = {
                    'original_timestamp': event['timestamp'],
                    'simulated_timestamp': datetime.now().isoformat(),
                    'compression_factor': self.time_compression
                }

                try:
                    # Convert event to JSON and send to Kafka
                    event_json = json.dumps(event_copy).encode('utf-8')
                    self.producer.produce(
                        self.topic_name,
                        value=event_json,
                        key=event_copy.get('event_id', '').encode('utf-8'),  # Using event_id as key
                        callback=self.delivery_report
                    )
                    events_sent += 1

                    # Poll to handle delivery reports
                    self.producer.poll(0)

                except Exception as e:
                    logger.error(f"Error sending event {event.get('event_id', '')}: {e}")
                    error_count += 1

                # Update previous timestamp
                prev_event_ts = curr_event_ts

                # Log progress for batches
                if i > 0 and i % batch_size == 0:
                    elapsed = (datetime.now() - sim_start).total_seconds()
                    progress = i / len(events) * 100
                    current_rate = i / elapsed if elapsed > 0 else 0
                    eta_seconds = (len(events) - i) / current_rate if current_rate > 0 else 0
                    eta_text = str(timedelta(seconds=int(eta_seconds)))

                    logger.info(f"Progress: {progress:.1f}% ({i:,}/{len(events):,}), "
                                f"Rate: {current_rate:.1f} events/sec, "
                                f"ETA: {eta_text}, "
                                f"Errors: {error_count}")

                    # Flush producer periodically (to handle any pending messages)
                    self.producer.flush(timeout=5)

            # Final flush to make sure all messages are sent
            remaining = self.producer.flush(timeout=10)
            if remaining > 0:
                logger.warning(f"{remaining} messages were not delivered within timeout")

            # Log completion
            elapsed = (datetime.now() - sim_start).total_seconds()
            final_rate = events_sent / elapsed if elapsed > 0 else 0

            logger.info(f"Simulation completed: {events_sent:,} events sent, {error_count} errors")
            logger.info(f"Total time: {elapsed:.1f} seconds, Average rate: {final_rate:.1f} events/sec")

        except KeyboardInterrupt:
            logger.info("Simulation interrupted by user")
            # Ensure producer flushes before exiting
            self.producer.flush(timeout=5)

        except Exception as e:
            logger.error(f"Simulation failed: {e}", exc_info=True)
            raise


def test_kafka_connection(bootstrap_servers):
    if Producer is None:
        print("confluent-kafka package not installed")
        return

    logger.info(f"Testing connection to Kafka at {bootstrap_servers}")

    try:
        admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
        topics = admin_client.list_topics(timeout=10)

        logger.info(f"Successfully connected to Kafka cluster")
        logger.info(f"Available topics: {', '.join(topics.topics.keys())}")

    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        logger.info("Troubleshooting tips:")
        logger.info("1. Check if Kafka container is running: docker-compose ps")
        logger.info("2. Verify docker-compose.yml has ports properly exposed")
        logger.info("3. For local Docker, try using 'kafka:9092' instead of 'localhost:29092'")


def parse_args():
    parser = argparse.ArgumentParser(description='Event Simulator with confluent-kafka')
    parser.add_argument('--input', type=str, required=True, help='Path to events JSON file')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:29092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', type=str, default='uk-retail-raw', help='Kafka topic name')
    parser.add_argument('--compression', type=int, default=1000, help='Time compression factor')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for logging')
    parser.add_argument('--test-connection', action='store_true', help='Test Kafka connection and exit')

    return parser.parse_args()


if __name__ == "__main__":
    try:
        args = parse_args()
        if Producer is None:
            logger.error("confluent-kafka is not installed")
            logger.info("Please install it with: pip install confluent-kafka")
            sys.exit(1)
        if args.test_connection:
            test_kafka_connection(args.bootstrap_servers)
            sys.exit(0)

        # Create and run simulator
        simulator = EventSimulator(
            args.input,
            args.bootstrap_servers,
            args.topic,
            args.compression
        )

        simulator.simulate(batch_size=args.batch_size)

    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user")
        print("\nSimulation interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print(f"Error: {e}")
        sys.exit(1)
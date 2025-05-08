import json
import time
from datetime import datetime, timedelta
import random
import logging
from kafka import KafkaProducer
import argparse

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("event_simulator")


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

    def load_events(self):
        """Load events from JSON file"""
        logger.info(f"Loading events from {self.input_path}")
        with open(self.input_path, 'r') as f:
            events = json.load(f)

        logger.info(f"Loaded {len(events)} events")
        return events

    def connect_kafka(self):
        """Connect to Kafka"""
        logger.info(f"Connecting to Kafka at {self.kafka_bootstrap_servers}")
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers.split(','),
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        logger.info("Connected to Kafka")

    def simulate(self, events=None, batch_size=100):
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
        events.sort(key=lambda x: x['timestamp'])

        # Get first and last timestamps
        first_ts = datetime.fromisoformat(events[0]['timestamp'])
        last_ts = datetime.fromisoformat(events[-1]['timestamp'])
        time_range = (last_ts - first_ts).total_seconds()

        logger.info(
            f"Simulating {len(events)} events over {time_range} seconds (compressed by {self.time_compression}x)")

        # Start simulation
        sim_start = datetime.now()
        prev_event_ts = first_ts

        for i, event in enumerate(events):
            # Calculate delay based on timestamp difference
            curr_event_ts = datetime.fromisoformat(event['timestamp'])
            delay = (curr_event_ts - prev_event_ts).total_seconds() / self.time_compression

            # Sleep to simulate time passage
            if delay > 0:
                time.sleep(delay)

            # Add simulation metadata
            event['simulation'] = {
                'original_timestamp': event['timestamp'],
                'simulated_timestamp': datetime.now().isoformat()
            }

            # Send to Kafka
            self.producer.send(self.topic_name, event)

            # Update previous timestamp
            prev_event_ts = curr_event_ts

            # Log progress for batches
            if i > 0 and i % batch_size == 0:
                elapsed = (datetime.now() - sim_start).total_seconds()
                progress = i / len(events) * 100
                logger.info(f"Progress: {progress:.2f}% ({i}/{len(events)}), Elapsed: {elapsed:.2f}s")

                # Flush producer periodically
                self.producer.flush()

        # Final flush
        self.producer.flush()

        # Log completion
        elapsed = (datetime.now() - sim_start).total_seconds()
        logger.info(f"Simulation completed in {elapsed:.2f}s")


def parse_args():
    parser = argparse.ArgumentParser(description='Event Simulator')
    parser.add_argument('--input', type=str, required=True, help='Path to events JSON file')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:29092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', type=str, default='uk-retail-raw', help='Kafka topic name')
    parser.add_argument('--compression', type=int, default=1000, help='Time compression factor')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for logging and flushing')

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    simulator = EventSimulator(
        args.input,
        args.bootstrap_servers,
        args.topic,
        args.compression
    )

    simulator.simulate(batch_size=args.batch_size)
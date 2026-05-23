#!/usr/bin/env python3
"""
Flink Counter Job - YBIGTA Spark & Flink HA Homework
=====================================================
Reads events from Kafka topic 'events', counts occurrences per event_type,
and writes running totals to /data/flink-output/counts.txt.

Event format in Kafka: "event_type_N:sequence_number"
Output format: "event_type_N<TAB>count"

Usage:
    flink run -py /jobs/flink_counter.py [--parallelism N]
"""

import os
import sys
import json
import logging
from typing import Tuple

logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                    format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger('flink_counter')

# ---------------------------------------------------------------------------
# Validate required configuration before importing PyFlink
# ---------------------------------------------------------------------------

REQUIRED_CONFIGS = {
    'execution.checkpointing.interval': 'Checkpointing interval must be set (e.g., 5000)',
    'execution.checkpointing.mode': 'Checkpointing mode must be set (e.g., EXACTLY_ONCE)',
    'state.checkpoints.dir': 'Checkpoint directory must be set (e.g., file:///data/checkpoints)',
}

def check_flink_config() -> bool:
    """Check that required Flink configuration is present."""
    config_path = '/opt/flink/conf/flink-conf.yaml'
    if not os.path.exists(config_path):
        logger.warning("flink-conf.yaml not found at %s, skipping config check", config_path)
        return True

    with open(config_path, 'r') as f:
        content = f.read()

    missing = []
    for key, description in REQUIRED_CONFIGS.items():
        # Check if the key appears uncommented
        found = False
        for line in content.splitlines():
            stripped = line.strip()
            if stripped.startswith('#'):
                continue
            if stripped.startswith(key + ':') or stripped.startswith(key + ' '):
                found = True
                break
        if not found:
            missing.append(f"  - {key}: {description}")

    if missing:
        logger.error("=" * 60)
        logger.error("CONFIGURATION ERROR: Required Flink settings are missing!")
        logger.error("Please edit student/conf/flink-conf.yaml and uncomment:")
        for m in missing:
            logger.error(m)
        logger.error("=" * 60)
        return False
    return True


if not check_flink_config():
    print(json.dumps({
        "error": "configuration_missing",
        "message": "Required Flink configuration is not set. See logs for details.",
        "hint": "Edit student/conf/flink-conf.yaml and uncomment required settings."
    }))
    sys.exit(1)


# ---------------------------------------------------------------------------
# PyFlink imports
# ---------------------------------------------------------------------------
try:
    from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
    from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.common.typeinfo import Types
    from pyflink.datastream.functions import (
        MapFunction, KeyedProcessFunction, RuntimeContext
    )
    from pyflink.datastream.state import ValueStateDescriptor
    from pyflink.common.watermark_strategy import WatermarkStrategy
except ImportError as e:
    logger.error("Failed to import PyFlink: %s", e)
    logger.error("Ensure apache-flink is installed: pip3 install apache-flink==1.17.2")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP', 'kafka:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'events')
OUTPUT_DIR = '/data/flink-output'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'counts.txt')
KAFKA_GROUP_ID = 'flink-counter-group'


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------

class ParseEvent(MapFunction):
    """Parse raw Kafka message 'event_type:seq' into (event_type, 1)."""

    def map(self, value: str) -> Tuple[str, int]:
        try:
            if ':' in value:
                event_type = value.split(':', 1)[0].strip()
            else:
                event_type = value.strip()
            return (event_type, 1)
        except Exception:
            return ('unknown', 1)


class CountingFunction(KeyedProcessFunction):
    """Stateful counter that maintains running count per event_type."""

    def __init__(self):
        self.count_state = None

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor('count', Types.LONG())
        self.count_state = runtime_context.get_state(descriptor)

    def process_element(self, value: Tuple[str, int], ctx):
        current = self.count_state.value()
        if current is None:
            current = 0
        current += value[1]
        self.count_state.update(current)
        yield (value[0], current)


class WriteToFile(MapFunction):
    """Write (event_type, count) pairs to output file."""

    def __init__(self, output_file: str):
        self.output_file = output_file
        self.counts = {}

    def map(self, value: Tuple[str, int]) -> str:
        event_type, count = value
        self.counts[event_type] = count
        # Write current state to file
        try:
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            with open(self.output_file, 'w') as f:
                for et in sorted(self.counts.keys()):
                    f.write(f"{et}\t{self.counts[et]}\n")
        except Exception as e:
            logger.warning("Failed to write output: %s", e)
        return f"{event_type}\t{count}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    env = StreamExecutionEnvironment.get_execution_environment()

    # Add Kafka connector JAR if present
    jar_paths = [
        '/opt/flink/lib/flink-sql-kafka-0-10_2.12-1.17.2.jar',
        '/opt/flink/lib/flink-connector-kafka-1.17.2.jar',
    ]
    for jar_path in jar_paths:
        if os.path.exists(jar_path):
            env.add_jars(f"file://{jar_path}")
            logger.info("Added JAR: %s", jar_path)
            break

    # Checkpointing configuration is loaded from flink-conf.yaml automatically.
    # We also set it programmatically as a safety net.
    env.enable_checkpointing(5000)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_checkpoint_storage_dir('file:///data/checkpoints')

    logger.info("Connecting to Kafka at %s, topic: %s", KAFKA_BOOTSTRAP, KAFKA_TOPIC)

    # Kafka consumer properties
    kafka_props = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': 'false',
    }

    kafka_consumer = FlinkKafkaConsumer(
        topics=KAFKA_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    # Use committed group offsets so each scenario only reads its own events.
    # If no offset exists yet, auto.offset.reset='earliest' applies.
    kafka_consumer.set_start_from_group_offsets()

    # Build pipeline
    stream = env.add_source(kafka_consumer, source_name='KafkaSource')

    counts = (
        stream
        .map(ParseEvent(), output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
        .key_by(lambda x: x[0])
        .process(CountingFunction(), output_type=Types.TUPLE([Types.STRING(), Types.LONG()]))
        .map(WriteToFile(OUTPUT_FILE), output_type=Types.STRING()).set_parallelism(1)
    )

    # Print to stdout as well (useful for debugging)
    counts.print()

    logger.info("Starting Flink job: flink-counter")
    env.execute("flink-counter")


if __name__ == '__main__':
    main()

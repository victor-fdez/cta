"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

KAFKA_BROKER_URL = 'localhost:9092'
KAFKA_REGISTRY_URL = 'http://localhost:8081'



class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        # configure kafka registry
        self.schema_registry = CachedSchemaRegistryClient(KAFKA_REGISTRY_URL)

        self.broker_properties = {
            'bootstrap.servers': KAFKA_BROKER_URL
        }

        self.client = AdminClient(self.broker_properties)
        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties, 
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
            schema_registry=self.schema_registry)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        logger.info("topic creation kafka integration incomplete - skipping")
        # If the topic already exists don't create it
        if not client.topic_exists(client, self.topic_name):
            logger.warn(f"topic {self.topic_name} already exists in kafka broker - skipping")
            return False
        #
        fs = client.create_topic([NewTopic(
            topic=self.topic_name,
            num_partitions=self.num_partitions,
            replication_factor=self.num_replicas
        )])
        # wait for all topics to be created
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                logger.info("topic {} created".format(topic))
            except Exception as e:
                logger.error("failed to create topic {}: {}".format(topic, e))


    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()
        logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

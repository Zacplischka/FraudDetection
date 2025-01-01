from kafka import KafkaProducer
from json import dumps

host_ip = 'kafka'

def connect_kafka_producer(host_ip) -> KafkaProducer:
    """
    Establish a connection to the Kafka broker and return a Kafka producer instance.
    """
    _producer = None
    try:
        _producer = KafkaProducer(
            bootstrap_servers=[f'{host_ip}:9092'],
            value_serializer=lambda x: dumps(x).encode('utf-8')  # Serialize messages to JSON format
        )
    except Exception as ex:
        print('Exception while connecting Kafka.')
        print(str(ex))
    finally:
        return _producer


def publish_messages(producer, topic, records) -> None:
    """
    Publishes a batch of messages to a specified Kafka topic.

    :param producer: The Kafka producer instance.
    :param topic: The name of the Kafka topic.
    :param records: A list of dictionaries containing the data to send.
                    All values except 'ts' will be converted to strings.
    """
    try:
        # Convert all record values (except 'ts') to strings before sending
        formatted_records = [
            {key: str(value) if key != 'ts' else value for key, value in record.items()}
            for record in records
        ]

        # Send each record to the Kafka topic
        for record in formatted_records:
            producer.send(topic, value=record)

        # Ensure all messages are sent before exiting
        producer.flush()

        print(f"Successfully published {len(records)} messages to topic '{topic}'.")
    except Exception as error:
        print("An error occurred while publishing messages:")
        print(str(error))

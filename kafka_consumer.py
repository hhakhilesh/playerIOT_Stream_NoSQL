from confluent_kafka import Consumer, KafkaException

def consume_messages(bootstrap_servers, group_id, topic):
    # Consumer configuration
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Start reading from the beginning of the topic if no offset is stored
        'enable.auto.commit': False       # Disable automatic committing of offsets
    }

    # Create Consumer instance
    consumer = Consumer(consumer_config)

    # Subscribe to the topic
    consumer.subscribe([topic])

    try:
        while True:
            # Poll for messages
            msg = consumer.poll(1.0)  # Timeout in seconds

            if msg is not None and not msg.error():
                # Print the received message
                print('Received message: {}'.format(msg.value()))

                # Manually commit offsets after processing each message
                consumer.commit()

    except KeyboardInterrupt:
        # Handle keyboard interrupt to gracefully close the consumer
        pass
    finally:
        # Close the consumer
        consumer.close()

if __name__ == '__main__':

    bootstrap_servers = '192.168.178.78:9092'
    consumer_group_id = 'group1'
    kafka_topic = 'topic1'

    # Consume messages
    consume_messages(bootstrap_servers, consumer_group_id, kafka_topic)

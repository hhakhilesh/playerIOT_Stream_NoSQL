from confluent_kafka import Consumer, KafkaException

def consume_partition_messages(bootstrap_servers, group_id, topic, num_partitions):
    # Consumer configuration
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Start reading from the beginning of the topic if no offset is stored
        'enable.auto.commit': False,      # Disable automatic committing of offsets
        'partition.assignment.strategy': 'roundrobin',  # Distribute partitions evenly among consumers
    }

    # Create Consumer instances
    consumers = [Consumer(consumer_config) for _ in range(num_partitions)]

    # Subscribe each consumer
    for consumer in consumers:
        consumer.subscribe([topic])
        
    # for i, consumer in enumerate(consumers):
        
    #     partitions = [i % num_partitions]  # Distribute partitions among consumers
    #     consumer.subscribe([topic], on_assign=lambda x: print(f'Consumer {i} partitions assigned: {x}'), partitions=partitions)

    try:
        while True:
            for consumer in consumers:
                # Poll for messages
                msg = consumer.poll(1.0)  # Timeout in seconds

                if msg is not None and not msg.error():
                    # Access the message value
                    message_value = msg.value()
                    # Print the received message
                    print(f'Consumer {consumer} received message: {message_value}')

                    # Manually commit offsets after processing each message
                    consumer.commit()

    except KeyboardInterrupt:
        # Handle keyboard interrupt to gracefully close the consumers
        pass
    finally:
        # Close the consumers
        for consumer in consumers:
            consumer.close()

if __name__ == '__main__':

    bootstrap_servers = '192.168.178.78:9092'
    num_partitions_per_topic = 2

    # Consume messages from partitions for leagueA
    leagueA_group_id = 'leagueA_consumer_group'
    leagueA_topic = 'leagueA'
    consume_partition_messages(bootstrap_servers, leagueA_group_id, leagueA_topic, num_partitions_per_topic)

    # Consume messages from partitions for leagueB
    leagueB_group_id = 'leagueB_consumer_group'
    leagueB_topic = 'leagueB'
    consume_partition_messages(bootstrap_servers, leagueB_group_id, leagueB_topic, num_partitions_per_topic)

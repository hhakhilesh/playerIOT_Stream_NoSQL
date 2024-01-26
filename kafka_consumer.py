from confluent_kafka import Consumer, KafkaException
import streamschema_pb2
import asyncio
import pymongo

def write_to_collection(msg,collection):
    
    # Deserialize using protobuf
    print("After Parsing")
    position_instance =streamschema_pb2.Position()
    position_message = position_instance.ParseFromString(msg)
    print(position_message)


    ## MongoDB
    document = {
        "name":position_instance.player_info.name,
        "role":position_instance.player_info.role,
        "strategy":position_instance.player_info.strategy,
        "match":position_instance.match_info.match,
        "sensor_id":position_instance.sensor_id,
        "location":{
            "x":position_instance.location.x,
            "y":position_instance.location.y,
            "time": position_instance.timestamp_usec,
        }
    }
    print(document)
    inserted_document = collection.insert_one(document)
    print(inserted_document.inserted_id)

async def consume_partition_messages(bootstrap_servers, group_id, topic, num_partitions):
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

    # MongoDB Client
    mongo_client = pymongo.MongoClient(db_addr)
    database = mongo_client[db_name]
    collection = database[topic]

    try:
        while True:
            for consumer in consumers:
                # Poll for messages
                msg = consumer.poll(1.0)  # Timeout in seconds

                if msg is not None and not msg.error():
                    # Access the message value
                    message_value = msg.value()
                    # Print the received message
                    write_to_collection(message_value,collection)

                    # Manually commit offsets after processing each message
                    consumer.commit()

    except KeyboardInterrupt:
        # Handle keyboard interrupt to gracefully close the consumers
        pass
    finally:
        # Close the consumers
        for consumer in consumers:
            consumer.close()

async def main(bootstrap_servers,num_partitions_per_topic,leagueA_group_id,leagueA_topic,leagueB_group_id,leagueB_topic):
    
    task1 = asyncio.create_task(consume_partition_messages(bootstrap_servers, leagueA_group_id, leagueA_topic, num_partitions_per_topic))
    task2 = asyncio.create_task(consume_partition_messages(bootstrap_servers, leagueB_group_id, leagueB_topic, num_partitions_per_topic))

    await asyncio.gather(task1,task2)
    
if __name__ == '__main__':

    bootstrap_servers = '192.168.178.78:9092'
    num_partitions_per_topic = 2

    # Consume messages from partitions for leagueA
    leagueA_group_id = 'leagueA_consumer_group'
    leagueA_topic = 'leagueA'
    

    # Consume messages from partitions for leagueB
    leagueB_group_id = 'leagueB_consumer_group'
    leagueB_topic = 'leagueB'

    ## MongoDB database details
    db_name = "season2024"
    db_addr = "mongodb://localhost:27017/"
    
    asyncio.run(main(bootstrap_servers,num_partitions_per_topic,leagueA_group_id,leagueA_topic,leagueB_group_id,leagueB_topic))
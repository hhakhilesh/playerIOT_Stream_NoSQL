from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


def run():

    admin_config = {
        'bootstrap.servers': '192.168.178.78:9092'
    }

    admin_client = AdminClient(admin_config)

    exsiting_topics = admin_client.list_topics().topics

    print(exsiting_topics)
    if "topic1" in exsiting_topics:
        print("topic1 already exists")
        return
    new_topic = NewTopic(topic="topic1",num_partitions=1,replication_factor=1)

    admin_client.create_topics([new_topic])
    print("line22")
    exsiting_topic = admin_client.list_topics().topics
    
    print(exsiting_topic)
if __name__ == "__main__":
    run()

